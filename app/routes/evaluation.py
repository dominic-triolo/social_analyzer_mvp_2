"""
Evaluation blueprint — analytics dashboard + API endpoints + benchmarks.

Queries the database for pipeline performance metrics.
"""
from dataclasses import asdict
from datetime import datetime, timedelta
from flask import Blueprint, render_template, jsonify, request

from app.database import get_session
from app.services.benchmarks import get_baseline, compute_deviations

bp = Blueprint('evaluation', __name__)


# ── Page ──────────────────────────────────────────────────────────────────────

@bp.route('/evaluation')
def evaluation_page():
    return render_template('evaluation.html', active_page='evaluation')


# ── HTMX Partial: KPI cards ─────────────────────────────────────────────────

@bp.route('/partials/eval-kpis')
def eval_kpis_partial():
    """HTMX partial: evaluation KPI cards."""
    session = get_session()
    try:
        from sqlalchemy import func
        from app.models.lead_run import LeadRun

        tier_rows = session.query(
            LeadRun.priority_tier,
            func.count(LeadRun.id).label('count'),
        ).filter(
            LeadRun.priority_tier.isnot(None)
        ).group_by(LeadRun.priority_tier).all()

        tiers = {row.priority_tier: row.count for row in tier_rows}

        score_rows = session.query(
            func.avg(LeadRun.lead_score).label('avg_score'),
            func.count(LeadRun.id).label('scored_count'),
        ).filter(LeadRun.lead_score.isnot(None)).first()

        data = {
            'tier_distribution': tiers,
            'avg_lead_score': round(float(score_rows.avg_score or 0), 3),
            'total_scored': score_rows.scored_count,
        }
    except Exception:
        data = {'tier_distribution': {}, 'avg_lead_score': 0, 'total_scored': 0}
    finally:
        session.close()

    tiers = data.get('tier_distribution', {})
    total = sum(tiers.values()) or 1
    auto_rate = round((tiers.get('auto_enroll', 0) / total) * 100, 1)

    return render_template('partials/eval_kpis.html',
                           total_scored=data.get('total_scored', 0),
                           avg_score=data.get('avg_lead_score', 0),
                           auto_rate=auto_rate)


# ── API: Channel comparison ───────────────────────────────────────────────────

@bp.route('/api/evaluation/channels')
def api_channels():
    """Per-platform metrics: run count, avg scored, avg auto_enroll rate, avg duration."""
    session = get_session()
    try:
        from sqlalchemy import func
        from app.models.db_run import DbRun

        rows = session.query(
            DbRun.platform,
            func.count(DbRun.id).label('run_count'),
            func.avg(DbRun.profiles_found).label('avg_found'),
            func.avg(DbRun.profiles_scored).label('avg_scored'),
            func.avg(DbRun.contacts_synced).label('avg_synced'),
        ).filter(
            DbRun.status == 'completed'
        ).group_by(DbRun.platform).all()

        # Compute average duration in Python to avoid Postgres-specific extract('epoch')
        completed_runs = session.query(
            DbRun.platform, DbRun.created_at, DbRun.finished_at
        ).filter(
            DbRun.status == 'completed',
            DbRun.finished_at.isnot(None),
        ).all()

        # Aggregate durations per platform
        duration_sums = {}
        duration_counts = {}
        for run in completed_runs:
            if run.created_at and run.finished_at:
                secs = (run.finished_at - run.created_at).total_seconds()
                duration_sums[run.platform] = duration_sums.get(run.platform, 0) + secs
                duration_counts[run.platform] = duration_counts.get(run.platform, 0) + 1

        # 30-day rolling averages
        thirty_days_ago = datetime.now() - timedelta(days=30)
        recent_rows = session.query(
            DbRun.platform,
            func.avg(DbRun.profiles_found).label('avg_found_30d'),
            func.avg(DbRun.profiles_scored).label('avg_scored_30d'),
            func.avg(DbRun.contacts_synced).label('avg_synced_30d'),
        ).filter(
            DbRun.status == 'completed',
            DbRun.created_at >= thirty_days_ago,
        ).group_by(DbRun.platform).all()

        rolling = {r.platform: {
            'avg_found_30d': round(float(r.avg_found_30d or 0)),
            'avg_scored_30d': round(float(r.avg_scored_30d or 0)),
            'avg_synced_30d': round(float(r.avg_synced_30d or 0)),
        } for r in recent_rows}

        result = []
        for row in rows:
            avg_dur_sec = 0
            if row.platform in duration_counts and duration_counts[row.platform] > 0:
                avg_dur_sec = duration_sums[row.platform] / duration_counts[row.platform]
            entry = {
                'platform': row.platform,
                'run_count': row.run_count,
                'avg_found': round(float(row.avg_found or 0)),
                'avg_scored': round(float(row.avg_scored or 0)),
                'avg_synced': round(float(row.avg_synced or 0)),
                'avg_duration_min': round(avg_dur_sec / 60, 1),
            }
            entry.update(rolling.get(row.platform, {}))
            result.append(entry)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


# ── API: Funnel drop-off ─────────────────────────────────────────────────────

@bp.route('/api/evaluation/funnel')
def api_funnel():
    """Funnel drop-off derived from run-level aggregate counters on DbRun."""
    platform = request.args.get('platform')

    session = get_session()
    try:
        from sqlalchemy import func
        from app.models.db_run import DbRun

        query = session.query(
            func.coalesce(func.sum(DbRun.profiles_found), 0).label('found'),
            func.coalesce(func.sum(DbRun.profiles_pre_screened), 0).label('pre_screened'),
            func.coalesce(func.sum(DbRun.profiles_enriched), 0).label('enriched'),
            func.coalesce(func.sum(DbRun.profiles_scored), 0).label('scored'),
            func.coalesce(func.sum(DbRun.contacts_synced), 0).label('synced'),
        ).filter(DbRun.status == 'completed')

        if platform:
            query = query.filter(DbRun.platform == platform)

        row = query.first()

        # Analysis doesn't drop profiles, so use enriched count as proxy
        enriched = int(row.enriched)
        result = [
            {'stage': 'discovery',  'count': int(row.found)},
            {'stage': 'pre_screen', 'count': int(row.pre_screened)},
            {'stage': 'enrichment', 'count': enriched},
            {'stage': 'analysis',   'count': enriched},
            {'stage': 'scoring',    'count': int(row.scored)},
            {'stage': 'crm_sync',   'count': int(row.synced)},
        ]
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


# ── API: Scoring analysis ────────────────────────────────────────────────────

@bp.route('/api/evaluation/scoring')
def api_scoring():
    """Tier distribution + average section scores."""
    session = get_session()
    try:
        from sqlalchemy import func
        from app.models.lead_run import LeadRun

        # Tier distribution
        tier_rows = session.query(
            LeadRun.priority_tier,
            func.count(LeadRun.id).label('count'),
        ).filter(
            LeadRun.priority_tier.isnot(None)
        ).group_by(LeadRun.priority_tier).all()

        tiers = {row.priority_tier: row.count for row in tier_rows}

        # Average scores
        score_rows = session.query(
            func.avg(LeadRun.lead_score).label('avg_score'),
            func.count(LeadRun.id).label('scored_count'),
        ).filter(LeadRun.lead_score.isnot(None)).first()

        return jsonify({
            'tier_distribution': tiers,
            'avg_lead_score': round(float(score_rows.avg_score or 0), 3),
            'total_scored': score_rows.scored_count,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


# ── API: Trends (time-series for benchmark charts) ───────────────────────────

@bp.route('/api/evaluation/trends')
def api_trends():
    """Time-series data for trend charts. Returns daily aggregates."""
    days = request.args.get('days', 30, type=int)
    platform = request.args.get('platform')

    session = get_session()
    try:
        from sqlalchemy import func, cast, Date
        from app.models.db_run import DbRun

        cutoff = datetime.now() - timedelta(days=days)

        query = session.query(
            cast(DbRun.created_at, Date).label('date'),
            DbRun.platform,
            func.count(DbRun.id).label('runs'),
            func.avg(DbRun.profiles_found).label('avg_found'),
            func.avg(DbRun.profiles_scored).label('avg_scored'),
            func.avg(DbRun.contacts_synced).label('avg_synced'),
        ).filter(
            DbRun.status == 'completed',
            DbRun.created_at >= cutoff,
        )

        if platform:
            query = query.filter(DbRun.platform == platform)

        rows = query.group_by(
            cast(DbRun.created_at, Date), DbRun.platform
        ).order_by(cast(DbRun.created_at, Date)).all()

        result = []
        for row in rows:
            result.append({
                'date': row.date.isoformat() if row.date else None,
                'platform': row.platform,
                'runs': row.runs,
                'avg_found': round(float(row.avg_found or 0)),
                'avg_scored': round(float(row.avg_scored or 0)),
                'avg_synced': round(float(row.avg_synced or 0)),
            })

        # Compute 30-day rolling average for deviation detection
        if result:
            total_found = sum(r['avg_found'] for r in result) / len(result)
            total_scored = sum(r['avg_scored'] for r in result) / len(result)
            total_synced = sum(r['avg_synced'] for r in result) / len(result)
        else:
            total_found = total_scored = total_synced = 0

        # Fetch baseline from MetricSnapshot for reference line
        baseline = get_baseline(platform) if platform else None

        return jsonify({
            'daily': result,
            'rolling_avg': {
                'avg_found': round(total_found),
                'avg_scored': round(total_scored),
                'avg_synced': round(total_synced),
            },
            'baseline': baseline,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


# ── API: Benchmarks ──────────────────────────────────────────────────────────

@bp.route('/api/evaluation/benchmarks')
def api_benchmarks():
    """Per-platform baselines + deviations from the last 5 completed runs."""
    platform = request.args.get('platform')

    session = get_session()
    try:
        from app.models.db_run import DbRun

        # Get platforms to report on
        if platform:
            platforms = [platform]
        else:
            rows = session.query(DbRun.platform).filter(
                DbRun.status == 'completed'
            ).distinct().all()
            platforms = [r.platform for r in rows]

        result = {}
        for plat in platforms:
            baseline = get_baseline(plat)

            # Get last 5 completed runs for deviation analysis
            recent_runs = session.query(DbRun).filter(
                DbRun.status == 'completed',
                DbRun.platform == plat,
            ).order_by(DbRun.created_at.desc()).limit(5).all()

            deviations = []
            if baseline:
                for run in recent_runs:
                    devs = compute_deviations(run, baseline)
                    for d in devs:
                        deviations.append({
                            'run_id': run.id,
                            **asdict(d),
                        })

            result[plat] = {
                'baseline': baseline,
                'deviations': deviations,
            }

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()
