import re
import logging

from prometheus_client import Gauge
from pipeline_monitor.ssh import SSHAutoConnect

logger = logging.getLogger(__name__)


TAG_STATUS_GAUGE = Gauge(
    name="chp_item_count_total",
    documentation="Available, not submitted, complete, and failed chp pipeline jobs.",
    labelnames=["type", "revision", "state"],
)

RUN_STATUS_GAUGE = Gauge(
    name="chp_item_processing",
    documentation="Chp pipeline jobs currently in the slurm queue.",
    labelnames=["type", "revision", "state"],
)

FAIRSHARE_GAUGE = Gauge(
    name="chp_fairshare",
    documentation="Current group fairshare on Cedar.",
    labelnames=[],
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


def _get_types(client: "SSHAutoConnect", root: str = "") -> list:
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
    cmd = f"chp --root {root} " if root else "chp "
    available_types, _ = client.exec_get_result(cmd + "type list")
    return available_types.strip().split("\n")


def _get_revs(client: "SSHAutoConnect", type: str, root: str = "") -> list:
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
    cmd = f"chp --root {root} " if root else "chp "
    available_revs, _ = client.exec_get_result(cmd + f"rev list {type}")
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

    for t in _get_types(client, config.get("root", "")):
        if t not in ignoretypes:
            revs = set(_get_revs(client, t, config.get("root", "")))
            if not revs:
                logger.info(f"Did not find revisions for type {t}")
                continue
            # Only track the most recent revision
            if config["newest_only"]:
                revs = [sorted(revs)[-1]]
            # Track all revisions not explicitly ignored
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
    global FAIRSHARE_GAUGE
    global TAG_STATUS_GAUGE
    global RUN_STATUS_GAUGE
    # Refresh available types and revisions
    if config.get("always_refresh", False):
        to_monitor = setup(config)

    # Get ssh client with connection established
    client = SSHAutoConnect.from_config(config["ssh"])
    ignoremetrics = set(config.get("ignoremetrics", []))
    logger.info(f"Ignoring metrics: {list(ignoremetrics)}.")

    for t, r in to_monitor:
        root = config.get("root", "")
        cmd = f"chp --root {root} " if root else "chp "
        cmd += f"item status {t}:{r} -u {config.get('user', 'chime')}"
        metric_str, _ = client.exec_get_result(cmd)

        # Convert the stdout string return to a dict
        entry_metric = _parse(metric_str)
        if not entry_metric:
            logger.warn(
                f"No metrics collected for {t}:{r}.\n"
                f"Command sent was:\n{cmd}\n"
                f"Remote response was:\n{metric_str}"
            )
        # Update prometheus gauges with chp metric values.
        for k, v in entry_metric.items():
            k = k.strip().lower().replace(" ", "_")

            if k in ignoremetrics:
                continue

            if k == "fairshare":
                FAIRSHARE_GAUGE.set(v)
            elif k in {"pending", "running"}:
                RUN_STATUS_GAUGE.labels(type=str(t), revision=str(t), state=k).set(v)
            else:
                TAG_STATUS_GAUGE.labels(type=str(t), revision=str(r), state=k).set(v)
