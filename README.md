Flask web app and Prometheus config to monitor CHIME pipeline progress. 

# Install
Installation should be done in a virtual environment.
```console
(venv) foo@bar:~$ pip install -r <path-to-file>/requirements.txt
```
Config must be set up by making a copy of config/sample_config.yml.


# Running
```console
(venv) foo@bar:~$ ./run.sh
```

If Prometheus is running as a system service, make sure to add the job found in 'config/prometheus.yml' to the prometheus config file. Otherwise, run
```console
(venv) foo@bar:~$ sudo /<path-to-prometheus> --config.file=pipeline_monitor/config/prometheus.yml
