web: gunicorn wsgi:app --bind 0.0.0.0:$PORT --timeout 120 --workers 2 --threads 4 -k gthread
worker: rq worker-pool --num-workers 4 --url $REDIS_URL
