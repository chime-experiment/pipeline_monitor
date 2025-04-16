import logging
import re
import subprocess
from datetime import datetime

from pipeline_monitor import status

logger = logging.getLogger(__name__)


def fetch_metrics(config: dict) -> None:
    """Fetch metrics from a remote host."""

    rid = config.get("remote_identity")
    rusr = config.get("remote_user")
    rhost = config.get("remote_host")

    if any(x is None for x in [rid, rusr, rhost]):
        logger.info("Invalid configuration")
        logger.debug(config)
        return

    # Some common errors
    permission_error = re.compile(r"\bpermission denied\b")
    connection_error = re.compile(r"\bconnection closed by remote host\b")

    logger.info(f"{datetime.now()!s} -> Fetching metrics with remote id {rid}")

    result = subprocess.run(
        ["ssh", "-i", rid, f"{rusr}@{rhost}", "metrics"], capture_output=True
    )
    stdout = result.stdout.decode()
    stderr = result.stderr.decode()

    # Check to see if the script output resulted in a permission error
    if permission_error.search(stderr.lower()):
        logger.warning("Unable to fetch metrics due to permission error.")
        return

    if connection_error.search(stderr.lower()):
        logger.warning("Unable to fetch metrics due to connection error.")
        return

    logger.debug("Handling output.")
    status.update(stdout, stderr)

    logger.info(
        f"{datetime.now()!s} -> Finished run. Next run in {config['frequency']} minutes."
    )
