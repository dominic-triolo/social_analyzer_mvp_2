"""
Webhook routes — Legacy HubSpot enrichment webhooks (deprecated).

Single-profile processing is now handled via the run-centric pipeline.
Use POST /api/runs to create a pipeline run instead.
"""
from flask import Blueprint, request, jsonify

bp = Blueprint('webhook', __name__)


@bp.route('/webhook/async', methods=['POST'])
def handle_webhook_async():
    """Deprecated: use POST /api/runs instead."""
    return jsonify({
        'status': 'deprecated',
        'message': 'This endpoint is deprecated. Use POST /api/runs to create a pipeline run.',
    }), 410


@bp.route('/api/webhook/enrich', methods=['POST'])
def enrich_webhook():
    """Deprecated: use POST /api/runs instead."""
    return jsonify({
        'status': 'deprecated',
        'message': 'This endpoint is deprecated. Use POST /api/runs to create a pipeline run.',
    }), 410


@bp.route('/webhook/status/<task_id>')
def check_task_status(task_id):
    """Deprecated: Celery task status — use /runs/<run_id> instead."""
    return jsonify({
        'status': 'deprecated',
        'message': 'This endpoint is deprecated. Use GET /runs/<run_id> to check run status.',
    }), 410
