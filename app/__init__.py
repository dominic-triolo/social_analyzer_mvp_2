"""
Flask application factory.

Creates and configures the Flask app, registers all blueprints.
"""
import os
from datetime import datetime, timezone
from flask import Flask, request, session, redirect, url_for, render_template_string


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

    # Secret key for sessions
    app.secret_key = os.getenv('SECRET_KEY', 'dev-secret-change-me')

    app.jinja_env.filters['time_since'] = _time_since

    # ── Simple password auth ────────────────────────────────────────────
    from app.config import DASHBOARD_PASSWORD

    LOGIN_PAGE = '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Login — TrovaTrip</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600&display=swap" rel="stylesheet">
        <style>body { font-family: 'DM Sans', sans-serif; }</style>
    </head>
    <body class="min-h-screen flex items-center justify-center" style="background:#eeece1;">
        <div style="background:white;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,0.06);padding:2.5rem;width:100%;max-width:360px;">
            <h1 class="text-lg font-bold mb-1" style="color:#005c69;">TrovaTrip Lead Pipeline</h1>
            <p class="text-sm mb-6" style="color:#3c4858;opacity:0.5;">Enter password to continue</p>
            {% if error %}
            <p class="text-xs mb-3" style="color:#f65c4e;">Wrong password</p>
            {% endif %}
            <form method="POST" action="/login">
                <input type="password" name="password" autofocus placeholder="Password"
                       class="w-full rounded-lg px-3 py-2.5 text-sm mb-4 outline-none"
                       style="border:1px solid rgba(60,72,88,0.15);background:white;">
                <button type="submit" class="w-full rounded-lg py-2.5 text-sm font-medium text-white"
                        style="background:#005c69;cursor:pointer;">
                    Log in
                </button>
            </form>
        </div>
    </body>
    </html>
    '''

    OPEN_PATHS = {'/health', '/login', '/webhook/hubspot'}

    @app.before_request
    def require_login():
        if not DASHBOARD_PASSWORD:
            return  # No password set — open access (local dev)
        if request.path in OPEN_PATHS or request.path.startswith('/static/'):
            return
        if session.get('authenticated'):
            return
        if request.method == 'POST' and request.path == '/login':
            return
        return redirect('/login')

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        if request.method == 'POST':
            if request.form.get('password') == DASHBOARD_PASSWORD:
                session['authenticated'] = True
                return redirect('/')
            return render_template_string(LOGIN_PAGE, error=True)
        return render_template_string(LOGIN_PAGE, error=False)

    @app.route('/logout')
    def logout():
        session.clear()
        return redirect('/login')

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
