import re
import logging

from prometheus_client import Gauge
from pipeline_monitor.ssh import SSHAutoConnect

logger = logging.getLogger(__name__)


GAUGE = Gauge(
    name="daily_pipeline_metrics",
    documentation="Displays various metrics from the daily pipeline running on Cedar.",
    labelnames=["type", "revision", "metric"],
)


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
    pattern = re.compile(r"(\w+):\s*[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)")
    match = pattern.findall(text)
    # Cast values to either floats or ints
    f = lambda x: float(x) if re.findall(r"[.eE]", x) else int(x)

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
    ignoretypes = set(config.get("ignoretypes", []))
    ignorerevs = set(config.get("ignorerevs", []))
    to_monitor = []

    logger.info(f"Ignoring types: {list(ignoretypes)}.")
    logger.info(f"Ignoring revisions: {list(ignorerevs)}.")

    for t in _get_types(client, config["root"]):
        if t not in ignoretypes:
            revs = set(_get_revs(client, config["root"], t))
            if not revs:
                logger.info(f"Did not find revisions for type {t}")
                continue
            if config["newest_only"]:
                revs = [sorted(revs)[-1]]
            else:
                revs = revs - ignorerevs
            logger.info(f"Adding revisions {revs} for type {t}.")
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
    global GAUGE
    """
    global GAUGE
    # Refresh available types and revisions
    if config.get("always_refresh", False):
        to_monitor = setup(config)

    # Get ssh client with connection established
    client = SSHAutoConnect.from_config(config["ssh"])
    ignoremetrics = set(config.get("ignoremetrics", []))
    logger.info(f"Ignoring metrics: {list(ignoremetrics)}.")

    def _fmt(text: str) -> str:
        return "chp_" + text.strip().lower().replace(" ", "_")

    for t, r in to_monitor:
        cmd = f"chp --root {config['root']} item metrics {t}:{r} -u {config['user']}"
        metric_str, _ = client.exec_get_result(cmd)
        # Convert the stdout string return to a dict
        entry_metric = _parse(metric_str)
        if not entry_metric:
            logger.warn(
                f"No metrics collected for {t}:{r}.\n"
                f"Remote command was:\n{cmd}\n"
                f"Remote response was:\n{metric_str}"
            )
        # Update prometheus gauges with chp metric values.
        for k, v in entry_metric.items():
            if k in ignoremetrics:
                continue
            GAUGE.labels(type=str(t), revision=str(r), metric=_fmt(k)).set(v)
