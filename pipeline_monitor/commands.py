import ast
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
        gauge = Gauge(name=name, documentation=desc, labelnames=["id"])

    return gauge


def _parse_to_dict(
    text: str, spliton: str = ":", delim: str = "\n", eval: bool = False
) -> dict:
    """Parse a string of key-value pairs into a dictionary."""
    # Split input string based on delim
    line_to_kv_pair = lambda s: map(str.strip, s.split(spliton))
    # Map strings into key:value pairs based on spliton
    d = dict(map(line_to_kv_pair, text.strip().split(delim)))
    # Evaluate all values as literals
    if eval:
        d = {k: ast.literal_eval(v) for k, v in d.items()}

    return d


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


def fetch_chp_metrics(config: dict) -> None:
    """Open an ssh connection to the host defined
    in config and fetch metrics for each entry.

    Parameters
    ----------
    config : dict
        ssh configuration in dict format. Usually
        loaded from yaml file.

    Changes
    -------
    all 'chp_' Gauges
    """
    # Get ssh client with connection established
    client = SSHAutoConnect.from_yaml_dict(config)
    # Get all available types and revisions
    typerevs = [
        [t, _get_revs(client, config["root"], t)]
        for t in _get_types(client, config["root"])
    ]

    def _fmt(text: str) -> str:
        return "chp_" + text.strip().lower().replace(" ", "_")

    for t, revs in typerevs:
        for r in revs:
            # Execute chp metrics command and automatically
            # read resulting stdout
            metric_str, _ = client.exec_get_result(
                f"chp --root {config['root']} item metrics {t}:{r}"
            )
            # Convert the stdout string return to a dict
            entry_metric = _parse_to_dict(metric_str, eval=True)
            # Update prometheus gauges with chp metric values.
            for k, v in entry_metric.items():
                _get_gauge(_fmt(k)).labels(id=f"{t}:{r}").set(v)
