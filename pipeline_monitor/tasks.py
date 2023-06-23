import logging
import re
from datetime import datetime

from pipeline_monitor import status
from pipeline_monitor.client import ScriptClient

logger = logging.getLogger(__name__)


# Functions to handle output from specific scripts
OUTPUT_HANDLER = {"bin/update_daily_status": status.update}


def run_scripts(config: dict) -> None:
    global OUTPUT_HANDLER

    scripts_list = config.get("scripts", [])

    if not scripts_list:
        logger.info("No scripts to run.")
        return

    permission_error = re.compile(r"\bpermission denied\b")

    client = ScriptClient(
        config["exec"],
        config["exec_args"],
    )

    for script in scripts_list:
        if isinstance(script, str):
            script = [script]

        logger.info(f"{str(datetime.now())} -> Running script {script[0]}")

        stdout, stderr = client.exec_script(script[0], script[1:])

        # Check to see if the script output resulted in a permission error
        if permission_error.search(stderr.lower()):
            logger.warning(
                f"Unable to execute script {script[0]} due to permission error."
            )
            continue

        if script[0] in OUTPUT_HANDLER:
            logger.info(f"Handling output for script {script[0]}.")
            OUTPUT_HANDLER[script[0]](stdout, stderr)

    logger.info("Finished running all scripts.")
