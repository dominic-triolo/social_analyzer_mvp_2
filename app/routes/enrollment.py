"""
Enrollment Dispatcher routes — API + UI + HTMX partials + settings.
"""
from flask import Blueprint, request, jsonify, render_template, redirect, url_for

bp = Blueprint('enrollment', __name__)


# ── API ──────────────────────────────────────────────────────────────────────

@bp.route('/api/enrollment/dispatch', methods=['POST'])
def dispatch():
    """Enqueue an enrollment dispatch run via RQ. Returns 202."""
    try:
        data = request.json or {}
        force = data.get('force', False)
        dry_run = data.get('dry_run', False)

        from app.services.enrollment_dispatcher import run_enrollment_dispatcher
        from app.extensions import redis_client
        from rq import Queue

        q = Queue(connection=redis_client)
        job = q.enqueue(
            run_enrollment_dispatcher,
            force=force,
            dry_run=dry_run,
            job_timeout=600,
        )

        return jsonify({'job_id': job.id, 'status': 'queued'}), 202

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/api/enrollment/last-run')
def last_run():
    """Return the most recent enrollment run summary."""
    from app.services.enrollment_dispatcher import get_last_run
    result = get_last_run()
    if not result:
        return jsonify({'status': 'no_runs'}), 200
    return jsonify(result)


@bp.route('/api/enrollment/history')
def history():
    """Return enrollment run history list."""
    limit = request.args.get('limit', 20, type=int)
    from app.services.enrollment_dispatcher import get_run_history
    return jsonify(get_run_history(limit))


@bp.route('/api/enrollment/config')
def get_config_api():
    """Return current enrollment config as JSON."""
    from app.services.enrollment_config import load_enrollment_config
    return jsonify(load_enrollment_config())


@bp.route('/api/enrollment/config', methods=['PUT'])
def save_config_api():
    """Save enrollment config from JSON body."""
    from app.services.enrollment_config import save_enrollment_config
    data = request.json
    if not data:
        return jsonify({'error': 'empty body'}), 400
    if save_enrollment_config(data):
        return jsonify({'ok': True})
    return jsonify({'error': 'save failed'}), 500


@bp.route('/api/enrollment/config', methods=['DELETE'])
def reset_config_api():
    """Delete DB config, reverting to YAML file defaults."""
    from app.services.db import get_session
    from app.models.app_config import AppConfig
    try:
        session = get_session()
        try:
            session.query(AppConfig).filter_by(key='enrollment').delete()
            session.commit()
        finally:
            session.close()
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── UI ───────────────────────────────────────────────────────────────────────

@bp.route('/enrollment')
def enrollment_page():
    """Enrollment dispatcher monitor page."""
    from app.services.enrollment_dispatcher import get_last_run, get_run_history
    from app.services.enrollment_config import load_enrollment_config
    cfg = load_enrollment_config()
    last = get_last_run()
    runs = get_run_history(limit=20)
    return render_template('enrollment.html',
                           active_page='enrollment',
                           last_run=last,
                           runs=runs,
                           config_inboxes=cfg['inboxes'],
                           config_max=cfg['max_per_day'],
                           config_cadence=cfg['sequence_cadence'],
                           config_steps=cfg['sequence_steps'],
                           config_weights=cfg['outreach_weights'],
                           config_tz=cfg['timezone'],
                           config_hs_props=cfg['hubspot_properties'])


@bp.route('/enrollment/settings')
def enrollment_settings():
    """Config editor page — structured form."""
    from app.services.enrollment_config import load_enrollment_config
    cfg = load_enrollment_config()
    return render_template('enrollment_settings.html',
                           active_page='enrollment',
                           cfg=cfg,
                           save_error=request.args.get('error'),
                           save_ok=request.args.get('saved'))


@bp.route('/enrollment/settings', methods=['POST'])
def save_enrollment_settings():
    """Save config from the structured form."""
    from app.services.enrollment_config import load_enrollment_config, save_enrollment_config
    try:
        # Rebuild config from form fields
        current = load_enrollment_config()
        form = request.form

        # Inboxes: parallel arrays inbox_name[] and inbox_owner[]
        names = form.getlist('inbox_name')
        owners = form.getlist('inbox_owner')
        inboxes = {}
        for name, owner in zip(names, owners):
            name = name.strip()
            owner = owner.strip()
            if name and owner:
                inboxes[name] = owner

        # Outreach weights: weight_<segment> fields
        weights = {}
        for key, val in form.items():
            if key.startswith('weight_'):
                seg = key[len('weight_'):]
                try:
                    weights[seg] = float(val)
                except (ValueError, TypeError):
                    pass

        cfg = {
            'inboxes': inboxes,
            'max_per_day': int(form.get('max_per_day', current['max_per_day'])),
            'sequence_cadence': int(form.get('sequence_cadence', current['sequence_cadence'])),
            'sequence_steps': int(form.get('sequence_steps', current['sequence_steps'])),
            'outreach_weights': weights or current['outreach_weights'],
            'api_delay': float(form.get('api_delay', current.get('api_delay', 0.1))),
            'timezone': form.get('timezone', current['timezone']).strip(),
            'hubspot_properties': current['hubspot_properties'],  # read-only in UI
        }

        if save_enrollment_config(cfg):
            return redirect(url_for('enrollment.enrollment_settings', saved='1'))
        return redirect(url_for('enrollment.enrollment_settings', error='Database save failed'))
    except Exception as e:
        return redirect(url_for('enrollment.enrollment_settings', error=str(e)))


# ── HTMX partials ───────────────────────────────────────────────────────────

@bp.route('/partials/enrollment-status')
def enrollment_status_partial():
    """Auto-refreshing status card."""
    from app.services.enrollment_dispatcher import get_last_run
    last = get_last_run()
    return render_template('partials/enrollment_status.html', last_run=last)
