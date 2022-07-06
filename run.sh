uwsgi --http localhost:5000 --wsgi-fil pipeline_monitor/app.py --callable app --enable-threads
