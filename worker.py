"""
RQ worker entry point — replaces celery_app.py.

Usage (Procfile): rq worker --with-scheduler --url $REDIS_URL
Usage (local):    python -m rq worker --with-scheduler
"""
