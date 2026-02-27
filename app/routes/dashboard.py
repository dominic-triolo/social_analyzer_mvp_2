"""
Dashboard routes — Home page, stats API, health check, HTMX partials.
"""
import logging
from flask import Blueprint, jsonify, render_template

from app.extensions import redis_client as r
from app.models.run import Run

logger = logging.getLogger('routes.dashboard')

bp = Blueprint('dashboard', __name__)

PLATFORM_SVG = {
    'instagram': '<svg width="14" height="14" viewBox="0 0 16 16" fill="none"><rect x="1.5" y="1.5" width="13" height="13" rx="4" stroke="currentColor" stroke-width="1.3"/><circle cx="8" cy="8" r="3" stroke="currentColor" stroke-width="1.3"/><circle cx="11.8" cy="4.2" r="0.8" fill="currentColor"/></svg>',
    'patreon':   '<svg width="14" height="14" viewBox="0 0 16 16" fill="none"><circle cx="10" cy="5.5" r="3.5" stroke="currentColor" stroke-width="1.3"/><rect x="2" y="2" width="2" height="12" rx="1" fill="currentColor" opacity="0.6"/></svg>',
    'facebook':  '<svg width="14" height="14" viewBox="0 0 16 16" fill="none"><path d="M13 2H3V10C3 10.55 3.45 11 4 11H9L13 14V2Z" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/></svg>',
}


@bp.route('/')
def index():
    """Home hub."""
    return render_template('home.html')


@bp.route('/health')
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "healthy"}), 200


@bp.route('/api/stats')
def get_stats():
    """API endpoint for dashboard stats."""
    try:
        queue_size = r.llen('rq:queue:default') or 0
        active_workers = min(queue_size, 8) if queue_size > 0 else 0

        result_counts = r.hgetall('trovastats:results') or {}
        post_frequency = int(result_counts.get('post_frequency', 0))
        pre_screened = int(result_counts.get('pre_screened', 0))
        enriched = int(result_counts.get('enriched', 0))
        errors = int(result_counts.get('error', 0))

        tier_counts = r.hgetall('trovastats:priority_tiers') or {}
        auto_enroll = int(tier_counts.get('auto_enroll', 0))
        high_priority = int(tier_counts.get('high_priority_review', 0))
        standard_priority = int(tier_counts.get('standard_priority_review', 0))
        low_priority = int(tier_counts.get('low_priority_review', 0))

        total_completed = post_frequency + pre_screened + enriched
        total_errors = errors
        total_processed = total_completed + total_errors

        durations = r.lrange('trovastats:durations', 0, -1)
        avg_duration = 0
        if durations:
            durations_int = [int(d) for d in durations]
            avg_duration = sum(durations_int) / len(durations_int)

        est_time_remaining = 0
        if avg_duration > 0 and queue_size > 0:
            workers = 2
            est_time_remaining = (queue_size / workers) * avg_duration / 60

        total_passed = enriched
        pass_rate = (total_passed / total_processed * 100) if total_processed > 0 else 0

        if total_passed > 0:
            tier_percentages = {
                'auto_enroll': (auto_enroll / total_passed * 100),
                'high_priority_review': (high_priority / total_passed * 100),
                'standard_priority_review': (standard_priority / total_passed * 100),
                'low_priority_review': (low_priority / total_passed * 100),
            }
        else:
            tier_percentages = {
                'auto_enroll': 0,
                'high_priority_review': 0,
                'standard_priority_review': 0,
                'low_priority_review': 0,
            }

        return jsonify({
            'queue_size': queue_size,
            'active_workers': active_workers,
            'total_completed': total_completed,
            'total_errors': total_errors,
            'avg_duration': round(avg_duration, 1),
            'est_time_remaining': round(est_time_remaining, 1),
            'breakdown': {
                'post_frequency': post_frequency,
                'pre_screened': pre_screened,
                'enriched': enriched,
                'errors': errors,
            },
            'pre_screening': {
                'total_pre_screened': post_frequency + pre_screened,
                'low_post_frequency': post_frequency,
                'outside_icp': pre_screened,
            },
            'priority_tiers': {
                'auto_enroll': auto_enroll,
                'high_priority_review': high_priority,
                'standard_priority_review': standard_priority,
                'low_priority_review': low_priority,
                'total': total_passed,
            },
            'batch_quality': {
                'pass_rate': round(pass_rate, 1),
                'tier_percentages': {
                    'auto_enroll': round(tier_percentages['auto_enroll'], 1),
                    'high_priority_review': round(tier_percentages['high_priority_review'], 1),
                    'standard_priority_review': round(tier_percentages['standard_priority_review'], 1),
                    'low_priority_review': round(tier_percentages['low_priority_review'], 1),
                },
            },
        })

    except Exception as e:
        logger.error("Error generating stats: %s", e, exc_info=True)

        return jsonify({
            'queue_size': 0,
            'active_workers': 0,
            'total_completed': 0,
            'total_errors': 0,
            'avg_duration': 0,
            'est_time_remaining': 0,
            'breakdown': {'post_frequency': 0, 'pre_screened': 0, 'enriched': 0, 'errors': 0},
            'pre_screening': {'total_pre_screened': 0, 'low_post_frequency': 0, 'outside_icp': 0},
            'priority_tiers': {
                'auto_enroll': 0, 'high_priority_review': 0,
                'standard_priority_review': 0, 'low_priority_review': 0, 'total': 0,
            },
            'batch_quality': {
                'pass_rate': 0,
                'tier_percentages': {
                    'auto_enroll': 0, 'high_priority_review': 0,
                    'standard_priority_review': 0, 'low_priority_review': 0,
                },
            },
        }), 200


@bp.route('/partials/dashboard-stats')
def dashboard_stats_partial():
    """HTMX partial: KPI cards for the home page."""
    runs = Run.list_recent(limit=20)
    run_dicts = [run.to_dict() for run in runs]
    active = sum(1 for r in run_dicts if r.get('status') not in ('completed', 'failed'))
    found = sum(r.get('profiles_found', 0) for r in run_dicts)
    synced = sum(r.get('contacts_synced', 0) for r in run_dicts)
    stats = dict(total=len(run_dicts), active=active, found=found, synced=synced)
    return render_template('partials/dashboard_stats.html', stats=stats)


@bp.route('/partials/recent-runs')
def recent_runs_partial():
    """HTMX partial: recent runs list for the home page."""
    runs = Run.list_recent(limit=5)
    run_dicts = [run.to_dict() for run in runs]
    return render_template('partials/recent_runs.html', runs=run_dicts, platform_svg=PLATFORM_SVG)


@bp.route('/partials/sidebar-badge')
def sidebar_badge_partial():
    """HTMX partial: active run count for sidebar badge."""
    runs = Run.list_recent(limit=10)
    active = sum(1 for run in runs if run.to_dict().get('status') not in ('completed', 'failed'))
    return render_template('partials/sidebar_badge.html', active=active)


@bp.route('/api/stats/reset', methods=['POST'])
def reset_stats():
    """Reset dashboard stats (useful for starting a new batch)."""
    try:
        r.delete('trovastats:results')
        r.delete('trovastats:priority_tiers')
        r.delete('trovastats:durations')
        return jsonify({'status': 'success', 'message': 'Stats reset successfully'})
    except Exception as e:
        logger.error("Error resetting stats: %s", e)
        return jsonify({'status': 'error', 'message': str(e)}), 500
