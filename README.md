Flask web app and Prometheus config to monitor CHIME pipeline progress. 

# Running
```console
(venv) foo@bar:~$ uwsgi uwsgi_file.ini
```

# Prometheus
The following snippet added to a prometheus config allows prometheus to scrape the pipeline metrics.
```yaml
- job_name: 'pipeline_monitor'

scrape_interval: 15s
scrape_timeout: 15s

metrics_path: "/metrics"

static_configs:
    - targets: ['localhost:5000']
```
