"""
Flask application factory.

Creates and configures the Flask app, registers all blueprints.
"""
import os
from datetime import datetime, timezone
from flask import Flask


def _time_since(iso_str):
    """Jinja2 filter: convert ISO timestamp to '2m ago' style string."""
    if not iso_str:
        return ''
    try:
        if isinstance(iso_str, str):
            dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        else:
            dt = iso_str
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        diff = (datetime.now(timezone.utc) - dt).total_seconds()
        if diff < 60:
            return 'just now'
        if diff < 3600:
            return f'{int(diff // 60)}m ago'
        if diff < 86400:
            return f'{int(diff // 3600)}h ago'
        return f'{int(diff // 86400)}d ago'
    except Exception:
        return ''


def create_app():
    """Create and configure the Flask application."""
    from app.logging_config import configure_logging

    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates'),
        static_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'static'),
    )

    configure_logging(app)

    app.jinja_env.filters['time_since'] = _time_since

    # Register blueprints
    from app.routes.dashboard import bp as dashboard_bp
    from app.routes.discovery import bp as discovery_bp
    from app.routes.webhook import bp as webhook_bp
    from app.routes.monitor import bp as monitor_bp
    from app.routes.evaluation import bp as evaluation_bp

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(discovery_bp)
    app.register_blueprint(webhook_bp)
    app.register_blueprint(monitor_bp)
    app.register_blueprint(evaluation_bp)

    # Initialize circuit breakers for external API services
    from app.extensions import redis_client
    from app.services.circuit_breaker import init_breakers
    init_breakers(redis_client)

    # Import models so Base.metadata knows about them (required for SQLAlchemy).
    # Schema is managed by Alembic — no init_db() call.
    import importlib
    importlib.import_module('app.models.db_run')
    importlib.import_module('app.models.lead')
    importlib.import_module('app.models.lead_run')
    importlib.import_module('app.models.filter_history')
    importlib.import_module('app.models.preset')
    importlib.import_module('app.models.metric_snapshot')

    return app
