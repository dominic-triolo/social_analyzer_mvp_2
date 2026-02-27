"""
Benchmarks service — snapshot persistence, baseline computation, deviation detection.

Called on pipeline completion to track per-platform performance baselines
and detect significant deviations from the 30-day rolling average.
"""
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional

from sqlalchemy import func

from app.database import get_session
from app.models.db_run import DbRun
from app.models.metric_snapshot import MetricSnapshot

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


def persist_metric_snapshot(run) -> Optional[MetricSnapshot]:
    """Upsert a MetricSnapshot row for (today, run.platform).

    Aggregates all completed DbRun rows for that date+platform to compute
    daily snapshot metrics. Called after each successful pipeline completion.
    """
    try:
        session = get_session()
    except Exception as e:
        logger.error("Failed to get session: %s", e)
        return None
    try:
        today = date.today()
        platform = run.platform

        # Aggregate all completed runs for this date + platform
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
            func.date(DbRun.created_at) == today,
        ).first()

        if not row or row.runs == 0:
            return None

        avg_found = float(row.avg_found or 0)
        avg_prescreened = float(row.avg_prescreened or 0)
        avg_scored = float(row.avg_scored or 0)
        avg_synced = float(row.avg_synced or 0)
        avg_cost = float(row.avg_cost or 0)

        yield_rate = (avg_prescreened / avg_found * 100) if avg_found > 0 else 0.0
        auto_enroll_rate = 0.0
        funnel_conversion = (avg_synced / avg_found * 100) if avg_found > 0 else 0.0
        avg_cost_per_lead = (avg_cost / avg_synced) if avg_synced > 0 else 0.0

        # Compute auto_enroll_rate from the latest run's tier_distribution
        tier = getattr(run, 'tier_distribution', None) or {}
        tier_total = sum(tier.values()) if tier else 0
        if tier_total > 0:
            auto_enroll_rate = (tier.get('auto_enroll', 0) / tier_total) * 100

        # Compute avg_score from scored runs today
        score_row = session.query(
            func.avg(DbRun.profiles_scored).label('avg_score'),
        ).filter(
            DbRun.status == 'completed',
            DbRun.platform == platform,
            DbRun.profiles_scored > 0,
            func.date(DbRun.created_at) == today,
        ).first()
        avg_score = float(score_row.avg_score or 0) if score_row else 0.0

        # Upsert: find existing or create new
        snapshot = session.query(MetricSnapshot).filter(
            MetricSnapshot.date == today,
            MetricSnapshot.platform == platform,
        ).first()

        if snapshot:
            snapshot.runs_count = row.runs
            snapshot.yield_rate = round(yield_rate, 2)
            snapshot.avg_score = round(avg_score, 2)
            snapshot.auto_enroll_rate = round(auto_enroll_rate, 2)
            snapshot.avg_found = round(avg_found, 2)
            snapshot.avg_scored = round(avg_scored, 2)
            snapshot.avg_synced = round(avg_synced, 2)
            snapshot.funnel_conversion = round(funnel_conversion, 2)
            snapshot.avg_cost_per_lead = round(avg_cost_per_lead, 4)
        else:
            snapshot = MetricSnapshot(
                date=today,
                platform=platform,
                runs_count=row.runs,
                yield_rate=round(yield_rate, 2),
                avg_score=round(avg_score, 2),
                auto_enroll_rate=round(auto_enroll_rate, 2),
                avg_found=round(avg_found, 2),
                avg_scored=round(avg_scored, 2),
                avg_synced=round(avg_synced, 2),
                funnel_conversion=round(funnel_conversion, 2),
                avg_cost_per_lead=round(avg_cost_per_lead, 4),
            )
            session.add(snapshot)

        session.commit()
        return snapshot

    except Exception as e:
        session.rollback()
        logger.error("Failed to persist snapshot: %s", e)
        return None
    finally:
        session.close()


def get_baseline(platform: str, days: int = 30) -> Optional[dict]:
    """Average MetricSnapshot rows from the last N days for a platform.

    Returns None if fewer than 3 snapshot days exist (insufficient data).
    """
    try:
        session = get_session()
    except Exception as e:
        logger.error("Failed to get session: %s", e)
        return None
    try:
        cutoff = date.today() - timedelta(days=days)

        row = session.query(
            func.count(MetricSnapshot.id).label('count'),
            func.avg(MetricSnapshot.yield_rate).label('yield_rate'),
            func.avg(MetricSnapshot.avg_score).label('avg_score'),
            func.avg(MetricSnapshot.auto_enroll_rate).label('auto_enroll_rate'),
            func.avg(MetricSnapshot.avg_found).label('avg_found'),
            func.avg(MetricSnapshot.avg_scored).label('avg_scored'),
            func.avg(MetricSnapshot.avg_synced).label('avg_synced'),
            func.avg(MetricSnapshot.funnel_conversion).label('funnel_conversion'),
            func.avg(MetricSnapshot.avg_cost_per_lead).label('avg_cost_per_lead'),
        ).filter(
            MetricSnapshot.platform == platform,
            MetricSnapshot.date >= cutoff,
        ).first()

        if not row or row.count < 3:
            return None

        return {
            'days': days,
            'snapshot_count': row.count,
            'yield_rate': round(float(row.yield_rate or 0), 2),
            'avg_score': round(float(row.avg_score or 0), 2),
            'auto_enroll_rate': round(float(row.auto_enroll_rate or 0), 2),
            'avg_found': round(float(row.avg_found or 0), 2),
            'avg_scored': round(float(row.avg_scored or 0), 2),
            'avg_synced': round(float(row.avg_synced or 0), 2),
            'funnel_conversion': round(float(row.funnel_conversion or 0), 2),
            'avg_cost_per_lead': round(float(row.avg_cost_per_lead or 0), 4),
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
