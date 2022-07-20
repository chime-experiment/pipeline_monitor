uwsgi --http localhost:5000 --wsgi-file pipeline_monitor/app.py --callable app --enable-threads
