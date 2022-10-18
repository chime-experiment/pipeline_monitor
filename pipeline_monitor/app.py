import atexit
from tzlocal import get_localzone
from datetime import datetime
from functools import partial

from flask import Flask
from prometheus_client import make_wsgi_app
from apscheduler.schedulers.background import BackgroundScheduler
from werkzeug.middleware.dispatcher import DispatcherMiddleware

from pipeline_monitor import commands

# Prometheus metrics scrape path. Must match metrics_path
# for job set in prometheus config file.
_PROMETHEUS_METRICS_TARGET = "/metrics"

# Global config
_config = {
    "ssh": {
        "username": "chime",
        "hostname": "cedar5.cedar.computecanada.ca",
        "key_filename": "/root/.ssh/id_cedar_shared",
        "private": True,
        "encoding": "utf-8",
        "venv": "/project/rpp-chime/lgray/chime_pipeline_dev/venv/bin/activate",
        "modpath": "~/chime_env/modules/modulefiles/",
        "modules": ["chime/python/2022.06"]
    },
    "frequency": 15,  # minutes
    "root": "/project/rpp-chime/chime/chime_processed/",
    "user": "chime",
    "newest_only": True,
    "ignoretypes": ["test_daily"],
    #"ignorerevs": set(),
    #"ignoremetrics": set(),
}


def schedule_monitor() -> None:
    global scheduler
    global _config
    # Start background task to periodically fetch metrics.
    # Task will run for the first time immediately.
    # In order for this to work, uwsgi server must be
    # run with threads enabled.
    scheduler = BackgroundScheduler(daemon=True, timezone=str(get_localzone()))
    # Get the list of type : revision to monitor
    monitor_list = commands.setup(_config)

    scheduler.add_job(
        partial(commands.fetch_chp_metrics, config=_config, to_monitor=monitor_list),
        "interval",
        minutes=_config["frequency"],
        next_run_time=datetime.now(),
    )
    scheduler.start()
    # Shut down the scheduler when flask app closes
    atexit.register(lambda: scheduler.shutdown())


def serve() -> None:
    global app
    # Flask app to provide minimal endpoint for prometheus
    app = Flask(__name__)
    # Add prometheus wsgi middleware to route requests
    app.wsgi_app = DispatcherMiddleware(
        app.wsgi_app, {_PROMETHEUS_METRICS_TARGET: make_wsgi_app()}
    )


# Blocks server from being started when importing from python
if __name__ in {"uwsgi_file_pipeline_monitor_app", "pipeline_monitor.app"}:
    serve()
    schedule_monitor()
