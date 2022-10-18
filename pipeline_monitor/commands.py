import re

from prometheus_client import Gauge, REGISTRY
from pipeline_monitor.ssh import SSHAutoConnect


def _get_gauge(name: str, desc: str = "", **kwargs) -> "Gauge":
    """Get a prometheus gauge by name, or create a new one
    if no gauge is found.

    Parameters
    ----------
    name : str
        gauge name
    desc : str
        description of gauge
    """
    gauge = REGISTRY._names_to_collectors.get(name, None)

    if gauge is None:
        labelnames = kwargs.get("labelnames", ["type", "revision"])
        gauge = Gauge(name=name, documentation=desc, labelnames=labelnames)

    return gauge


def _parse(text: str) -> dict:
    """Parse a string including key-value pairs into a dictionary.
    
    Paramaters
    ----------
    text : str
        string including the desired key: value pairs
    
    Returns
    -------
    dict:
        key: value pairs with values cast to numeric types
    """
    # Get regex match for key: value pairs
    pattern = re.compile(r'(\w+):\s*[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)')
    match = pattern.findall(text)
    # Cast values to either floats or ints
    f = lambda x: float(x) if re.findall(r'[.eE]', x) else int(x)

    return {i[0]: f(i[1]) for i in match}


def _get_types(client: "SSHAutoConnect", root: str) -> list:
    """Get all processing types.

    Parameters
    ----------
    client : SSHAutoConnect
        ssh client
    root : str
        chp root diirectory

    Returns
    -------
    types : list[str]
        list of available types
    """
    available_types, _ = client.exec_get_result(f"chp --root {root} type list")
    return available_types.strip().split("\n")


def _get_revs(client: "SSHAutoConnect", root: str, type: str) -> list:
    """Get all available revisions for a given type.

    Parameters
    ----------
    client : SSHAutoConnect
        ssh client
    root : str
        chp root diirectory
    type : str
        processing type to find revisions for

    Returns
    -------
    types : list[str]
        list of available revisions for TYPE
    """
    available_revs, _ = client.exec_get_result(f"chp --root {root} rev list {type}")
    return available_revs.strip().split("\n")


def setup(config: dict) -> dict:
    """Configure the types and revision to monitor.
    
    Parameters
    ----------
    config : dict
        ssh configuration
    """
    # Get ssh client with connection established
    client = SSHAutoConnect.from_config(config["ssh"])
    # Get all available types and revisions
    ignoretypes = set(config.get("ignoretypes", set()))
    ignorerevs = set(config.get("ignorerevs", set()))
    to_monitor = []

    for t in _get_types(client, config["root"]):
        if t not in ignoretypes:
            revs = set(_get_revs(client, config["root"], t))
            if config["newest_only"]:
                revs = [sorted(revs)[-1]]
            else:
                revs = revs - ignorerevs
            to_monitor.extend([(t, r) for r in revs])

    return to_monitor


def fetch_chp_metrics(config: dict, to_monitor: list = []) -> None:
    """Open an ssh connection to the host defined
    in config and fetch metrics for each entry.

    Parameters
    ----------
    config : dict
        ssh configuration
    to_monitor : list
        list of (type, revision) pairs to watch

    Changes
    -------
    all 'chp_' Gauges
    """
    # Get ssh client with connection established
    client = SSHAutoConnect.from_config(config["ssh"])
    ignoremetrics = set(config.get("ignoremetrics", set()))

    def _fmt(text: str) -> str:
        return "chp_" + text.strip().lower().replace(" ", "_")

    for t, r in to_monitor:
        metric_str, _ = client.exec_get_result(
            f"chp --root {config['root']} item metrics {t}:{r} -u {config['user']}"
        )
        # Convert the stdout string return to a dict
        entry_metric = _parse(metric_str)
        # Update prometheus gauges with chp metric values.
        for k, v in entry_metric.items():
            if k == "fairshare":
                _get_gauge(_fmt(k), labelnames=[]).set(v)
                continue
            if k in ignoremetrics:
                continue
            _get_gauge(_fmt(k)).labels(type=str(t), revision=str(r)).set(v)
