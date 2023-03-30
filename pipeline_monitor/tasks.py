import logging
from datetime import datetime

from pipeline_monitor import status
from pipeline_monitor.client import ScriptClient

logger = logging.getLogger(__name__)


# Functions to handle output from specific scripts
_output_handler = {"bin/update_daily_status": status.update}


def run_scripts(config: dict) -> None:
    global _output_handler

    scripts_list = config.get("scripts", [])

    if not scripts_list:
        logger.info("No scripts to run.")
        return

    client = ScriptClient(
        config["exec"],
        config["exec_args"],
    )

    for script in scripts_list:
        if isinstance(script, str):
            script = [script]

        logger.info(f"{str(datetime.now())} -> Running script {script[0]}")

        stdout, stderr = client.exec_script(script[0], script[1:])

        if script[0] in _output_handler:
            logger.info(f"Handling output for script {script[0]}.")
            _output_handler[script[0]](stdout, stderr)

    logger.info("Finished running all scripts.")
