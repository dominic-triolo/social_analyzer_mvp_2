"""
Monitor routes — Run-centric views + pipeline API + HTMX partials + SSE.
"""
import time
from flask import Blueprint, request, jsonify, render_template, Response, stream_with_context

from app.models.run import Run
from app.pipeline.manager import launch_run, get_run_status, STAGE_REGISTRY
from app.pipeline.base import get_pipeline_info
from app.config import BDR_OWNER_IDS
from app.routes.dashboard import PLATFORM_SVG

bp = Blueprint('monitor', __name__)

STAGE_ORDER = ['discovery', 'pre_screen', 'enrichment', 'analysis', 'scoring', 'crm_sync']


def _build_stages(run_dict):
    """Compute stage statuses for a run dict."""
    current_stage = run_dict.get('current_stage', '')
    run_status = run_dict.get('status', '')
    current_idx = STAGE_ORDER.index(current_stage) if current_stage in STAGE_ORDER else -1

    stages = []
    for i, key in enumerate(STAGE_ORDER):
        if run_status == 'failed' and i == current_idx:
            s = 'failed'
        elif run_status == 'completed':
            s = 'completed'
        elif i < current_idx:
            s = 'completed'
        elif i == current_idx:
            s = 'running'
        else:
            s = 'pending'
        stages.append({'key': key, 'status': s})
    return stages


# ── Run-centric views ────────────────────────────────────────────────────────

@bp.route('/runs')
def runs_list():
    """List all pipeline runs."""
    runs = Run.list_recent(limit=50)
    return render_template('runs_list.html', runs=[run.to_dict() for run in runs])


@bp.route('/runs/<run_id>')
def run_detail(run_id):
    """Single run detail view with 6-stage pipeline tracker."""
    run = Run.load(run_id)
    if not run:
        return render_template('runs_list.html', runs=[], error='Run not found'), 404
    return render_template('run_detail.html', run=run.to_dict())


# ── Run API ──────────────────────────────────────────────────────────────────

@bp.route('/api/runs', methods=['POST'])
def create_run():
    """Create a new pipeline run."""
    try:
        data = request.json or {}
        platform = data.get('platform', 'instagram')
        filters = data.get('filters', {})
        bdr_names = data.get('bdr_names')

        if platform not in ('instagram', 'patreon', 'facebook'):
            return jsonify({'error': f'Unsupported platform: {platform}'}), 400

        run = launch_run(platform=platform, filters=filters, bdr_names=bdr_names)
        return jsonify(run.to_dict()), 202

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/api/runs')
def list_runs():
    """List recent pipeline runs."""
    limit = request.args.get('limit', 20, type=int)
    runs = Run.list_recent(limit=limit)
    return jsonify([run.to_dict() for run in runs])


@bp.route('/api/runs/<run_id>')
def get_run(run_id):
    """Get a single run's status."""
    status = get_run_status(run_id)
    if not status:
        return jsonify({'error': 'Run not found'}), 404
    return jsonify(status)


@bp.route('/api/runs/<run_id>/retry', methods=['POST'])
def retry_run(run_id):
    """Retry a run from a specific stage."""
    try:
        data = request.json or {}
        from_stage = data.get('from_stage')

        if from_stage and from_stage not in STAGE_ORDER:
            return jsonify({'error': f'Invalid stage: {from_stage}'}), 400

        original = Run.load(run_id)
        if not original:
            return jsonify({'error': 'Run not found'}), 404

        # Load stage_outputs from DB (not in Redis to_dict)
        stage_outputs = {}
        try:
            from app.database import get_session
            from app.models.db_run import DbRun
            session = get_session()
            try:
                db_run = session.get(DbRun, run_id)
                if db_run and db_run.stage_outputs:
                    stage_outputs = db_run.stage_outputs
            finally:
                session.close()
        except Exception:
            pass

        # Create a new run linked to the original, with retry metadata
        retry_filters = {
            **original.filters,
            '_retry_from': from_stage,
            '_parent_run_id': run_id,
        }
        new_run = Run(
            platform=original.platform,
            filters=retry_filters,
            bdr_assignment=original.bdr_assignment,
        )
        # Copy stage_outputs from original for checkpoint loading
        new_run.stage_outputs = stage_outputs
        new_run.save()

        from app.services.db import persist_run
        persist_run(new_run)

        # Enqueue with retry_from_stage
        from app.pipeline.manager import run_pipeline, _get_queue
        _get_queue().enqueue(run_pipeline, new_run.id, retry_from_stage=from_stage, job_timeout=14400)

        return jsonify(new_run.to_dict()), 202

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/partials/run-detail/<run_id>')
def run_detail_partial(run_id):
    """HTMX partial: full run detail content (fallback for non-SSE clients)."""
    run = Run.load(run_id)
    if not run:
        return '<div class="text-center py-8 text-sm" style="color:#f65c4e;">Run not found</div>', 404
    run_dict = run.to_dict()
    stages = _build_stages(run_dict)
    is_terminal = run_dict.get('status') in ('completed', 'failed')
    return render_template('partials/run_detail_content.html',
                           run=run_dict, stages=stages, is_terminal=is_terminal)


@bp.route('/stream/run/<run_id>')
def stream_run(run_id):
    """SSE stream: pushes run detail HTML whenever state changes."""
    def generate():
        last_hash = None
        while True:
            run = Run.load(run_id)
            if not run:
                break

            run_dict = run.to_dict()
            stages = _build_stages(run_dict)
            is_terminal = run_dict.get('status') in ('completed', 'failed')

            # Simple change detection: hash the volatile fields
            state_key = (
                run_dict.get('status'),
                run_dict.get('current_stage'),
                run_dict.get('profiles_found'),
                run_dict.get('contacts_synced'),
                str(run_dict.get('stage_progress')),
                len(run_dict.get('errors', [])),
            )

            if state_key != last_hash:
                last_hash = state_key
                html = render_template('partials/run_detail_content.html',
                                       run=run_dict, stages=stages, is_terminal=is_terminal)
                # SSE format: each line prefixed with "data: ", blank line terminates
                for line in html.split('\n'):
                    yield f"data: {line}\n"
                yield "\n"

            if is_terminal:
                break

            time.sleep(2)

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )


@bp.route('/partials/runs-table')
def runs_table_partial():
    """HTMX partial: filtered runs table."""
    platform = request.args.get('platform', 'all')
    status = request.args.get('status', 'all')

    all_runs = Run.list_recent(limit=50)
    all_dicts = [run.to_dict() for run in all_runs]
    filtered = all_dicts

    if platform != 'all':
        filtered = [r for r in filtered if r.get('platform') == platform]
    if status == 'active':
        filtered = [r for r in filtered if r.get('status') not in ('completed', 'failed')]
    elif status != 'all':
        filtered = [r for r in filtered if r.get('status') == status]

    return render_template(
        'partials/runs_table.html',
        runs=filtered,
        total_count=len(all_dicts),
        filtered_count=len(filtered),
        platform_svg=PLATFORM_SVG,
    )


@bp.route('/api/pipeline-info')
def pipeline_info():
    """Return metadata for all pipeline stages across all platforms.

    Response shape: { "instagram": { "discovery": { "description": "...", "apis": [...], "est": null }, ... }, ... }
    """
    return jsonify(get_pipeline_info(STAGE_REGISTRY))
