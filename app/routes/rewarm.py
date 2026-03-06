"""
Rewarm routes — Rewarm UI page + DB-backed segment API + sync endpoint.
"""
import os
import logging
from flask import Blueprint, render_template, request, jsonify

from app.config import HUBSPOT_API_KEY

logger = logging.getLogger(__name__)

bp = Blueprint('rewarm', __name__)

MOCK_SEGMENTS = [
    {'id': '101', 'name': 'Travel Hosts Q1 2026', 'size': 150, 'processing_type': 'MANUAL'},
    {'id': '102', 'name': 'Adventure Creators 2026', 'size': 87, 'processing_type': 'MANUAL'},
    {'id': '103', 'name': 'Top Engaged Leads', 'size': 312, 'processing_type': 'DYNAMIC'},
    {'id': '104', 'name': 'Wellness Retreat Hosts', 'size': 64, 'processing_type': 'MANUAL'},
    {'id': '105', 'name': 'Europe Trip Leaders', 'size': 203, 'processing_type': 'MANUAL'},
    {'id': '106', 'name': 'New Signups — Feb 2026', 'size': 41, 'processing_type': 'MANUAL'},
    {'id': '107', 'name': 'High Follower Count (50k+)', 'size': 178, 'processing_type': 'DYNAMIC'},
    {'id': '108', 'name': 'Stale Leads — Re-engage', 'size': 95, 'processing_type': 'MANUAL'},
    {'id': '109', 'name': 'Latin America Creators', 'size': 126, 'processing_type': 'MANUAL'},
    {'id': '110', 'name': 'Outdoor & Hiking Niche', 'size': 58, 'processing_type': 'MANUAL'},
]


def _use_mock():
    return bool(os.getenv('MOCK_PIPELINE')) or not HUBSPOT_API_KEY


def _mock_response():
    return jsonify({'segments': MOCK_SEGMENTS, 'synced_at': None})


def _db_segments():
    """Read segments + synced_at from Postgres. Returns (segments_list, synced_at)."""
    from app.database import get_session
    from app.models.hubspot_list import HubSpotList
    from app.models.app_config import AppConfig

    session = get_session()
    try:
        rows = session.query(HubSpotList).order_by(HubSpotList.name).all()
        cfg = session.get(AppConfig, 'hubspot_lists_synced_at')
        synced_at = cfg.value if cfg else None
        segments = [
            {'id': r.list_id, 'name': r.name, 'size': r.size, 'processing_type': r.processing_type}
            for r in rows
        ]
        return segments, synced_at
    finally:
        session.close()


@bp.route('/rewarm')
def rewarm_page():
    """Rewarm UI page."""
    return render_template('rewarm.html')


@bp.route('/api/rewarm/segments')
def get_segments():
    """Return segments. Mock in dev, Postgres in production."""
    if _use_mock():
        return _mock_response()

    try:
        segments, synced_at = _db_segments()
        if not segments and synced_at is None:
            return jsonify({'segments': [], 'synced_at': None, 'needs_sync': True})
        return jsonify({'segments': segments, 'synced_at': synced_at})
    except Exception as e:
        logger.warning("DB read failed for segments: %s", e)
        return jsonify({'segments': [], 'synced_at': None, 'needs_sync': True})


@bp.route('/api/rewarm/segments/sync', methods=['POST'])
def sync_segments():
    """Sync from HubSpot, then return the full segment list."""
    if _use_mock():
        return _mock_response()

    try:
        from app.services.hubspot import sync_hubspot_lists_to_db
        sync_hubspot_lists_to_db()
        segments, synced_at = _db_segments()
        return jsonify({'segments': segments, 'synced_at': synced_at})
    except Exception as e:
        logger.error("Segment sync failed: %s", e)
        return jsonify({'error': str(e)}), 500


@bp.route('/api/rewarm', methods=['POST'])
def launch_rewarm_run():
    """Launch a rewarm run. Reads JSON body with platform, hubspot_list_ids, dry_run."""
    try:
        data = request.json or {}
        platform = data.get('platform', 'instagram')
        list_ids = data.get('hubspot_list_ids', [])
        dry_run = data.get('dry_run', True)

        if not list_ids:
            return jsonify({'error': 'hubspot_list_ids is required'}), 400

        from app.pipeline.manager import launch_rewarm
        filters = {
            'hubspot_list_ids': list_ids,
            'dry_run': dry_run,
        }
        run = launch_rewarm(platform, filters)
        return jsonify(run.to_dict()), 202

    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error("Failed to launch rewarm run: %s", e)
        return jsonify({'error': str(e)}), 500
