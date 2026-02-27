"""
Benchmarks service — computed metrics, baseline computation, deviation detection.

Called on pipeline completion to track per-platform performance baselines
and detect significant deviations from the 30-day rolling average.

Metrics are computed directly from DbRun rows — no intermediate snapshot table.
"""
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional

from sqlalchemy import func

from app.database import get_session
from app.models.db_run import DbRun

logger = logging.getLogger('services.benchmarks')


@dataclass
class Deviation:
    metric: str
    label: str
    run_value: float
    baseline_value: float
    pct_change: float
    severity: str        # 'notable' (>25%) or 'significant' (>50%)
    direction: str       # 'above' or 'below'


def compute_daily_metrics(platform: str, target_date: date = None) -> Optional[dict]:
    """Compute daily aggregate metrics for a platform from DbRun rows.

    Returns a dict with the same shape as the old MetricSnapshot, or None
    if no completed runs exist for the given date + platform.
    """
    try:
        session = get_session()
    except Exception as e:
        logger.error("Failed to get session: %s", e)
        return None
    try:
        if target_date is None:
            target_date = date.today()

        row = session.query(
            func.count(DbRun.id).label('runs'),
            func.avg(DbRun.profiles_found).label('avg_found'),
            func.avg(DbRun.profiles_pre_screened).label('avg_prescreened'),
            func.avg(DbRun.profiles_scored).label('avg_scored'),
            func.avg(DbRun.contacts_synced).label('avg_synced'),
            func.avg(DbRun.actual_cost).label('avg_cost'),
        ).filter(
            DbRun.status == 'completed',
            DbRun.platform == platform,
            func.date(DbRun.created_at) == target_date,
        ).first()

        if not row or row.runs == 0:
            return None

        avg_found = float(row.avg_found or 0)
        avg_prescreened = float(row.avg_prescreened or 0)
        avg_scored = float(row.avg_scored or 0)
        avg_synced = float(row.avg_synced or 0)
        avg_cost = float(row.avg_cost or 0)

        yield_rate = (avg_prescreened / avg_found * 100) if avg_found > 0 else 0.0
        funnel_conversion = (avg_synced / avg_found * 100) if avg_found > 0 else 0.0
        avg_cost_per_lead = (avg_cost / avg_synced) if avg_synced > 0 else 0.0

        # Compute auto_enroll_rate: aggregate tier_distribution across all
        # completed runs for this date+platform
        runs_with_tiers = session.query(DbRun.tier_distribution).filter(
            DbRun.status == 'completed',
            DbRun.platform == platform,
            func.date(DbRun.created_at) == target_date,
        ).all()

        total_tiered = 0
        total_auto_enroll = 0
        for (tier_dist,) in runs_with_tiers:
            if tier_dist and isinstance(tier_dist, dict):
                total_tiered += sum(tier_dist.values())
                total_auto_enroll += tier_dist.get('auto_enroll', 0)
        auto_enroll_rate = (total_auto_enroll / total_tiered * 100) if total_tiered > 0 else 0.0

        return {
            'date': target_date.isoformat(),
            'platform': platform,
            'runs_count': row.runs,
            'yield_rate': round(yield_rate, 2),
            'avg_score': round(avg_scored, 2),
            'auto_enroll_rate': round(auto_enroll_rate, 2),
            'avg_found': round(avg_found, 2),
            'avg_scored': round(avg_scored, 2),
            'avg_synced': round(avg_synced, 2),
            'funnel_conversion': round(funnel_conversion, 2),
            'avg_cost_per_lead': round(avg_cost_per_lead, 4),
        }

    except Exception as e:
        logger.error("Failed to compute daily metrics: %s", e)
        return None
    finally:
        session.close()


def get_baseline(platform: str, days: int = 30) -> Optional[dict]:
    """Compute a rolling baseline from DbRun rows over the last N days.

    Aggregates completed runs grouped by date, requires at least 3 distinct
    run-dates before returning a baseline (insufficient data otherwise).
    """
    try:
        session = get_session()
    except Exception as e:
        logger.error("Failed to get session: %s", e)
        return None
    try:
        cutoff = date.today() - timedelta(days=days)

        # Count distinct dates with completed runs
        distinct_dates = session.query(
            func.count(func.distinct(func.date(DbRun.created_at)))
        ).filter(
            DbRun.status == 'completed',
            DbRun.platform == platform,
            func.date(DbRun.created_at) >= cutoff,
        ).scalar()

        if not distinct_dates or distinct_dates < 3:
            return None

        # Aggregate across all completed runs in the window
        row = session.query(
            func.avg(DbRun.profiles_found).label('avg_found'),
            func.avg(DbRun.profiles_pre_screened).label('avg_prescreened'),
            func.avg(DbRun.profiles_scored).label('avg_scored'),
            func.avg(DbRun.contacts_synced).label('avg_synced'),
            func.avg(DbRun.actual_cost).label('avg_cost'),
        ).filter(
            DbRun.status == 'completed',
            DbRun.platform == platform,
            func.date(DbRun.created_at) >= cutoff,
        ).first()

        avg_found = float(row.avg_found or 0)
        avg_prescreened = float(row.avg_prescreened or 0)
        avg_scored = float(row.avg_scored or 0)
        avg_synced = float(row.avg_synced or 0)
        avg_cost = float(row.avg_cost or 0)

        yield_rate = (avg_prescreened / avg_found * 100) if avg_found > 0 else 0.0
        funnel_conversion = (avg_synced / avg_found * 100) if avg_found > 0 else 0.0
        avg_cost_per_lead = (avg_cost / avg_synced) if avg_synced > 0 else 0.0

        # Aggregate auto_enroll_rate across all runs in the window
        runs_with_tiers = session.query(DbRun.tier_distribution).filter(
            DbRun.status == 'completed',
            DbRun.platform == platform,
            func.date(DbRun.created_at) >= cutoff,
        ).all()

        total_tiered = 0
        total_auto_enroll = 0
        for (tier_dist,) in runs_with_tiers:
            if tier_dist and isinstance(tier_dist, dict):
                total_tiered += sum(tier_dist.values())
                total_auto_enroll += tier_dist.get('auto_enroll', 0)
        auto_enroll_rate = (total_auto_enroll / total_tiered * 100) if total_tiered > 0 else 0.0

        return {
            'days': days,
            'snapshot_count': distinct_dates,
            'yield_rate': round(yield_rate, 2),
            'avg_score': round(avg_scored, 2),
            'auto_enroll_rate': round(auto_enroll_rate, 2),
            'avg_found': round(avg_found, 2),
            'avg_scored': round(avg_scored, 2),
            'avg_synced': round(avg_synced, 2),
            'funnel_conversion': round(funnel_conversion, 2),
            'avg_cost_per_lead': round(avg_cost_per_lead, 4),
        }

    except Exception as e:
        logger.error("Failed to get baseline: %s", e)
        return None
    finally:
        session.close()


def compute_deviations(run, baseline: dict) -> List[Deviation]:
    """Compare a run's metrics to baseline. Returns flagged deviations.

    Thresholds:
    - notable: >25% deviation
    - significant: >50% deviation
    Cost-per-lead direction is inverted (higher = worse).
    """
    if not baseline:
        return []

    found = run.profiles_found or 0
    prescreened = run.profiles_pre_screened or 0
    scored = run.profiles_scored or 0
    synced = run.contacts_synced or 0
    actual_cost = run.actual_cost or 0

    run_yield = (prescreened / found * 100) if found > 0 else 0.0
    run_conversion = (synced / found * 100) if found > 0 else 0.0
    run_cpl = (actual_cost / synced) if synced > 0 else 0.0

    metrics = [
        ('yield_rate', 'Yield Rate', run_yield, False),
        ('avg_found', 'Profiles Found', float(found), False),
        ('avg_scored', 'Profiles Scored', float(scored), False),
        ('avg_synced', 'Contacts Synced', float(synced), False),
        ('funnel_conversion', 'Funnel Conversion', run_conversion, False),
        ('avg_cost_per_lead', 'Cost per Lead', run_cpl, True),  # inverted
    ]

    deviations = []
    for metric_key, label, run_value, inverted in metrics:
        baseline_value = baseline.get(metric_key, 0)
        if baseline_value == 0:
            continue

        pct_change = ((run_value - baseline_value) / baseline_value) * 100
        abs_pct = abs(pct_change)

        if abs_pct < 25:
            continue

        severity = 'significant' if abs_pct >= 50 else 'notable'

        if inverted:
            direction = 'below' if pct_change > 0 else 'above'
        else:
            direction = 'above' if pct_change > 0 else 'below'

        deviations.append(Deviation(
            metric=metric_key,
            label=label,
            run_value=round(run_value, 2),
            baseline_value=round(baseline_value, 2),
            pct_change=round(pct_change, 1),
            severity=severity,
            direction=direction,
        ))

    return deviations
