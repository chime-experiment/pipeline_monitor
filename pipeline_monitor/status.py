from __future__ import annotations

import re
import logging

from prometheus_client import Gauge

logger = logging.getLogger(__name__)


TAG_STATUS_GAUGE = Gauge(
    name="chp_item_count",
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


def _extract_values_from_message(text: str) -> dict:
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


def _extract_types_and_revs(text: str) -> list[tuple]:
    blocks = []

    for x in text.split("->"):
        if not x:
            continue

        x = x.strip().split("\n", 1)

        blocks.append((*x[0].split(":"), x[-1]))

    return blocks


def update(stdout: str, stderr: str) -> None:
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

    # Split between revisions and types
    for t, r, output in _extract_types_and_revs(stdout):
        # Convert the stdout string return to a dict
        entry_metric = _extract_values_from_message(output)

        if not entry_metric:
            logger.warn(
                f"No metrics collected for {t}:{r}.\n" f"Received text:\n{stdout}"
            )

        # Update prometheus gauges with chp metric values.
        for k, v in entry_metric.items():
            k = k.strip().lower().replace(" ", "_")

            if k == "fairshare":
                FAIRSHARE_GAUGE.set(v)
            elif k in {"pending", "running"}:
                RUN_STATUS_GAUGE.labels(type=t, revision=r, state=k).set(v)
            else:
                TAG_STATUS_GAUGE.labels(type=t, revision=r, state=k).set(v)
