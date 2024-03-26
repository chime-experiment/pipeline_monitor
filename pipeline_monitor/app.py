import atexit
import logging
import argparse
import yaml

from tzlocal import get_localzone
from datetime import datetime
from functools import partial

from flask import Flask
from prometheus_client import make_wsgi_app
from apscheduler.schedulers.background import BackgroundScheduler
from werkzeug.middleware.dispatcher import DispatcherMiddleware

from pipeline_monitor import fetch

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Defaults
_config = {
    "frequency": 60,
    "newest_only": True,
    "ignoretypes": [],
    "ignorerevs": [],
    "ignoremetrics": [],
    "target": "/metrics",
    "always_refresh": False,
}


def set_global_config() -> dict:
    global _config

    parser = argparse.ArgumentParser(description="Get config path")
    parser.add_argument("--appconfig", type=str)

    with open(parser.parse_args().appconfig, "r") as stream:
        config_file = yaml.safe_load(stream)

    _config.update(config_file["app"])
    _config.update(config_file["fetch"])


def schedule_monitor() -> None:
    global scheduler
    global _config

    # Start background task to periodically fetch metrics.
    # Task will run for the first time immediately.
    # In order for this to work, uwsgi server must be
    # run with threads enabled.
    scheduler = BackgroundScheduler(daemon=True, timezone=str(get_localzone()))
    scheduler.add_job(
        partial(fetch.fetch_metrics, config=_config),
        "interval",
        minutes=_config["frequency"],
        next_run_time=datetime.now(),
    )
    scheduler.start()
    # Shut down the scheduler when flask app closes
    atexit.register(lambda: scheduler.shutdown())


def serve() -> None:
    global app
    global _config
    # Flask app to provide minimal endpoint for prometheus
    app = Flask(__name__)
    # Add prometheus wsgi middleware to route requests
    app.wsgi_app = DispatcherMiddleware(
        app.wsgi_app, {_config["target"]: make_wsgi_app()}
    )
    logger.info(f"Started app {app}. Metrics at {_config['target']}")


# Blocks server from being started when importing from python
if __name__ in {"uwsgi_file_pipeline_monitor_app", "pipeline_monitor.app"}:
    set_global_config()
    serve()
    schedule_monitor()
