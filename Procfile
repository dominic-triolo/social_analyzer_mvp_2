web: gunicorn wsgi:app --bind 0.0.0.0:$PORT --timeout 600 --workers 2
worker: rq worker --with-scheduler --url $REDIS_URL
