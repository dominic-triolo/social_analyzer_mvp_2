"""
Discovery routes — Discovery UI page + HTMX partials + presets API + staleness check + keyword suggestions.
"""
import logging
import traceback
from flask import Blueprint, render_template, request, jsonify

from app.database import get_session
from app.models.preset import Preset

logger = logging.getLogger(__name__)

bp = Blueprint('discovery', __name__)


@bp.route('/discovery')
def discovery_page():
    """Discovery UI page."""
    return render_template('discovery.html')


@bp.route('/partials/pipeline-preview')
def pipeline_preview_partial():
    """HTMX partial: pipeline stage diagram for a platform."""
    from app.pipeline.manager import STAGE_REGISTRY
    from app.pipeline.base import get_pipeline_info

    platform = request.args.get('platform', 'instagram')
    info = get_pipeline_info(STAGE_REGISTRY)
    platform_info = info.get(platform, {})

    stage_order = ['discovery', 'pre_screen', 'enrichment', 'analysis', 'scoring', 'crm_sync']
    stages = []
    all_apis = set()
    total_est = 0
    has_est = False
    for key in stage_order:
        stage = platform_info.get(key)
        if stage:
            stages.append({'key': key, **stage})
            all_apis.update(stage.get('apis', []))
            if stage.get('est') is not None:
                total_est += stage['est']
                has_est = True

    return render_template('partials/pipeline_preview.html',
                           stages=stages, all_apis=sorted(all_apis),
                           total_est=round(total_est) if has_est else None)


# ── Presets API ──────────────────────────────────────────────────────────────

@bp.route('/api/presets')
def list_presets():
    """List presets, optionally filtered by platform."""
    platform = request.args.get('platform')
    session = get_session()
    try:
        query = session.query(Preset).order_by(Preset.created_at.desc())
        if platform:
            query = query.filter_by(platform=platform)
        presets = query.all()
        return jsonify([
            {
                'id': p.id,
                'name': p.name,
                'platform': p.platform,
                'filters': p.filters,
                'created_at': p.created_at.isoformat() if p.created_at else None,
            }
            for p in presets
        ])
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


@bp.route('/api/presets', methods=['POST'])
def create_preset():
    """Save current filters as a preset."""
    try:
        data = request.json or {}
        name = data.get('name', '').strip()
        platform = data.get('platform', '')
        filters = data.get('filters', {})

        if not name:
            return jsonify({'error': 'Name is required'}), 400
        if not platform:
            return jsonify({'error': 'Platform is required'}), 400

        session = get_session()
        try:
            preset = Preset(name=name, platform=platform, filters=filters)
            session.add(preset)
            session.commit()
            return jsonify({
                'id': preset.id,
                'name': preset.name,
                'platform': preset.platform,
                'filters': preset.filters,
            }), 201
        finally:
            session.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/api/presets/<int:preset_id>', methods=['DELETE'])
def delete_preset(preset_id):
    """Delete a preset."""
    session = get_session()
    try:
        preset = session.get(Preset, preset_id)
        if not preset:
            return jsonify({'error': 'Preset not found'}), 404
        session.delete(preset)
        session.commit()
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


# ── AI Keyword Suggestions ──────────────────────────────────────────

KEYWORD_PROMPTS = {
    'instagram': (
        "You are a social media marketing expert. Given these Instagram discovery keywords, "
        "suggest 8-10 related terms that would help find travel creators on Instagram. "
        "Include a mix of hashtags (with #) and bio phrases. "
        "Return one suggestion per line, nothing else."
    ),
    'patreon': (
        "You are a creator economy expert. Given these Patreon search keywords, "
        "suggest 8-10 related search terms for finding travel-related creators on Patreon. "
        "Focus on creator niches, content types, and travel sub-topics. "
        "Return one suggestion per line, nothing else."
    ),
    'facebook': (
        "You are a community marketing expert. Given these Facebook group search keywords, "
        "suggest 8-10 related terms for finding travel-related Facebook groups. "
        "Focus on group topics, travel niches, and community themes. "
        "Return one suggestion per line, nothing else."
    ),
}


@bp.route('/api/keyword-suggestions', methods=['POST'])
def keyword_suggestions():
    """Generate AI keyword suggestions using Claude Haiku."""
    from app.extensions import anthropic_client

    if not anthropic_client:
        return jsonify({'error': 'AI suggestions unavailable — ANTHROPIC_API_KEY not configured'}), 503

    data = request.json or {}
    platform = data.get('platform', 'instagram')
    keywords = data.get('keywords', [])

    if not keywords:
        return jsonify({'error': 'Provide at least one keyword'}), 400

    system_prompt = KEYWORD_PROMPTS.get(platform, KEYWORD_PROMPTS['instagram'])
    user_input = "Current keywords: " + ", ".join(keywords)

    try:
        response = anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            system=system_prompt,
            messages=[{"role": "user", "content": user_input}],
        )
        raw = response.content[0].text
        suggestions = [line.strip().lstrip('•-').strip() for line in raw.strip().splitlines() if line.strip()]

        # Deduplicate against user's existing keywords (case-insensitive)
        existing = {k.lower() for k in keywords}
        suggestions = [s for s in suggestions if s.lower() not in existing]

        return jsonify({'suggestions': suggestions})
    except Exception as e:
        logger.error("Keyword suggestion error: %s", e)
        return jsonify({'error': 'Failed to generate suggestions'}), 500


# ── Staleness check ──────────────────────────────────────────────────────────

@bp.route('/api/filter-staleness')
def filter_staleness():
    """Check if these filters have been run before and return novelty info."""
    try:
        platform = request.args.get('platform', 'instagram')
        # Parse filters from query string JSON
        import json
        filters_json = request.args.get('filters', '{}')
        filters = json.loads(filters_json)

        from app.services.db import get_filter_staleness
        info = get_filter_staleness(platform, filters)
        if not info:
            return jsonify({'stale': False})

        return jsonify({
            'stale': info['novelty_rate'] < 20,
            'last_run_days_ago': info['last_run_days_ago'],
            'novelty_rate': info['novelty_rate'],
            'total_found': info['total_found'],
            'new_found': info['new_found'],
        })
    except Exception:
        traceback.print_exc()
        return jsonify({'stale': False})
