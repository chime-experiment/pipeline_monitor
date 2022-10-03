import yaml
import atexit
from tzlocal import get_localzone
from datetime import datetime
from pathlib import Path
from functools import partial

from flask import Flask, g
from prometheus_client import make_wsgi_app
from apscheduler.schedulers.background import BackgroundScheduler
from werkzeug.middleware.dispatcher import DispatcherMiddleware

from pipeline_monitor import commands


def get_config() -> dict:
    """Get config. If none exists, load
    from yaml file."""

    config = getattr(g, "_config", None)
    if not config:
        config = g._config = _get_yaml_config()

    return config


def _get_yaml_config() -> dict:
    """Load yaml config file. Expects global variable
    _CONFIG_FILE to exist."""

    if not Path(_CONFIG_FILE).exists():
        raise FileNotFoundError(
            f"No config file found. Expecting a config file '{_CONFIG_FILE}'"
        )
    with (Path(_CONFIG_FILE)).open() as fh:
        return yaml.safe_load(fh)


def run_app():
    # SSH config file
    global _CONFIG_FILE
    _CONFIG_FILE = "./pipeline_monitor/config/ssh_config.yml"
    # Prometheus metrics scrape path. Must match metrics_path
    # for job set in prometheus config file.
    _PROMETHEUS_METRICS_TARGET = "/metrics"
    # Flask app to provide minimal endpoint for prometheus
    global app
    app = Flask(__name__)
    # Add prometheus wsgi middleware to route requests
    app.wsgi_app = DispatcherMiddleware(
        app.wsgi_app, {_PROMETHEUS_METRICS_TARGET: make_wsgi_app()}
    )
    # Start background task to periodically fetch metrics.
    # Task will run for the first time immediately.
    # In order for this to work, uwsgi server must be
    # run with threads enabled.
    scheduler = BackgroundScheduler(daemon=True, timezone=str(get_localzone()))
    with app.app_context():
        # Job has to be created with app context in order to
        # access flask 'g' variable where the config is stored
        scheduler.add_job(
            partial(commands.fetch_chp_metrics, config=get_config()),
            "interval",
            minutes=get_config()["frequency"],
            next_run_time=datetime.now(),
        )
    scheduler.start()
    # Shut down the scheduler when flask app closes
    atexit.register(lambda: scheduler.shutdown())


# This is how it is called from uwsgi cli
if __name__ == "uwsgi_file_pipeline_monitor_app":
    run_app()
