"""Tests for app.services.benchmarks — computed metrics, baselines, deviations."""
import pytest
from datetime import date, datetime, timedelta
from unittest.mock import patch, MagicMock

from app.models.db_run import DbRun
from app.services.benchmarks import (
    compute_daily_metrics,
    get_baseline,
    compute_deviations,
    Deviation,
)


@pytest.fixture
def patch_bench_session(db_session):
    """Patch get_session where benchmarks imported it."""
    with patch('app.services.benchmarks.get_session', return_value=db_session):
        yield db_session


def _make_db_run(session, id, platform='instagram', found=100, prescreened=80,
                 enriched=70, scored=50, synced=30, cost=5.0,
                 tier_distribution=None, created_at=None):
    """Helper: insert a completed DbRun row."""
    run = DbRun(
        id=id, platform=platform, status='completed',
        profiles_found=found, profiles_pre_screened=prescreened,
        profiles_enriched=enriched, profiles_scored=scored,
        contacts_synced=synced, actual_cost=cost,
        tier_distribution=tier_distribution or {},
    )
    if created_at:
        run.created_at = created_at
    session.add(run)
    session.commit()
    return run


# ---------------------------------------------------------------------------
# compute_daily_metrics
# ---------------------------------------------------------------------------

class TestComputeDailyMetrics:

    def test_computes_metrics_for_completed_run(self, patch_bench_session):
        session = patch_bench_session
        _make_db_run(session, 'r1', found=100, prescreened=80, scored=50,
                     synced=30, cost=5.0,
                     tier_distribution={'auto_enroll': 5, 'high_priority_review': 10})

        result = compute_daily_metrics('instagram')
        assert result is not None
        assert result['platform'] == 'instagram'
        assert result['runs_count'] == 1
        assert result['avg_found'] == 100.0
        assert result['yield_rate'] == 80.0  # 80/100 * 100
        assert result['funnel_conversion'] == 30.0  # 30/100 * 100
        assert result['avg_cost_per_lead'] == round(5.0 / 30, 4)

    def test_averages_multiple_runs(self, patch_bench_session):
        session = patch_bench_session
        _make_db_run(session, 'r1', found=100, prescreened=80, scored=50, synced=30, cost=5.0)
        _make_db_run(session, 'r2', found=200, prescreened=180, scored=150, synced=130, cost=10.0)

        result = compute_daily_metrics('instagram')
        assert result['runs_count'] == 2
        assert result['avg_found'] == 150.0  # avg(100, 200)

    def test_returns_none_when_no_completed_runs(self, patch_bench_session):
        result = compute_daily_metrics('instagram')
        assert result is None

    def test_computes_yield_rate(self, patch_bench_session):
        session = patch_bench_session
        _make_db_run(session, 'r1', found=100, prescreened=40)

        result = compute_daily_metrics('instagram')
        assert result['yield_rate'] == 40.0

    def test_computes_funnel_conversion(self, patch_bench_session):
        session = patch_bench_session
        _make_db_run(session, 'r1', found=100, synced=25)

        result = compute_daily_metrics('instagram')
        assert result['funnel_conversion'] == 25.0

    def test_computes_cost_per_lead(self, patch_bench_session):
        session = patch_bench_session
        _make_db_run(session, 'r1', found=100, synced=10, cost=5.0)

        result = compute_daily_metrics('instagram')
        assert result['avg_cost_per_lead'] == 0.5

    def test_handles_zero_found(self, patch_bench_session):
        session = patch_bench_session
        _make_db_run(session, 'r1', found=0, prescreened=0, scored=0, synced=0, cost=0.0)

        result = compute_daily_metrics('instagram')
        assert result['yield_rate'] == 0.0
        assert result['funnel_conversion'] == 0.0

    def test_handles_zero_synced_for_cpl(self, patch_bench_session):
        session = patch_bench_session
        _make_db_run(session, 'r1', found=50, synced=0, cost=2.0)

        result = compute_daily_metrics('instagram')
        assert result['avg_cost_per_lead'] == 0.0

    def test_only_counts_matching_platform(self, patch_bench_session):
        session = patch_bench_session
        _make_db_run(session, 'r1', platform='instagram')

        result = compute_daily_metrics('patreon')
        assert result is None

    def test_auto_enroll_rate_aggregated_across_runs(self, patch_bench_session):
        """auto_enroll_rate should aggregate tier_distribution across all runs, not just the last."""
        session = patch_bench_session
        _make_db_run(session, 'r1',
                     tier_distribution={'auto_enroll': 5, 'high_priority_review': 15})
        _make_db_run(session, 'r2',
                     tier_distribution={'auto_enroll': 10, 'high_priority_review': 10})

        result = compute_daily_metrics('instagram')
        # Total: auto_enroll=15, total_tiered=40 → 37.5%
        assert result['auto_enroll_rate'] == 37.5

    def test_specific_target_date(self, patch_bench_session):
        session = patch_bench_session
        yesterday = datetime.now() - timedelta(days=1)
        _make_db_run(session, 'r1', created_at=yesterday)

        # Today should return None (no runs today)
        assert compute_daily_metrics('instagram') is None
        # Yesterday should find the run
        result = compute_daily_metrics('instagram', target_date=yesterday.date())
        assert result is not None

    def test_handles_exception_gracefully(self, db_session):
        with patch('app.services.benchmarks.get_session', side_effect=Exception('db down')):
            result = compute_daily_metrics('instagram')
        assert result is None


# ---------------------------------------------------------------------------
# get_baseline
# ---------------------------------------------------------------------------

class TestGetBaseline:

    def test_returns_averages_from_runs(self, patch_bench_session):
        session = patch_bench_session
        now = datetime.now()
        for i in range(5):
            _make_db_run(session, f'r{i}', found=100 + i * 10,
                         created_at=now - timedelta(days=i))

        baseline = get_baseline('instagram')
        assert baseline is not None
        assert baseline['snapshot_count'] == 5
        assert baseline['avg_found'] > 0

    def test_returns_none_for_insufficient_dates(self, patch_bench_session):
        session = patch_bench_session
        # Only 2 dates — below minimum of 3
        now = datetime.now()
        for i in range(2):
            _make_db_run(session, f'r{i}', created_at=now - timedelta(days=i))

        baseline = get_baseline('instagram')
        assert baseline is None

    def test_filters_by_platform(self, patch_bench_session):
        session = patch_bench_session
        now = datetime.now()
        for i in range(5):
            _make_db_run(session, f'r{i}', platform='instagram',
                         created_at=now - timedelta(days=i))

        assert get_baseline('instagram') is not None
        assert get_baseline('patreon') is None

    def test_respects_days_parameter(self, patch_bench_session):
        session = patch_bench_session
        now = datetime.now()
        # Runs from 40+ days ago
        for i in range(5):
            _make_db_run(session, f'r{i}',
                         created_at=now - timedelta(days=40 + i))

        assert get_baseline('instagram', days=30) is None
        assert get_baseline('instagram', days=60) is not None

    def test_returns_correct_keys(self, patch_bench_session):
        session = patch_bench_session
        now = datetime.now()
        for i in range(3):
            _make_db_run(session, f'r{i}', created_at=now - timedelta(days=i))

        baseline = get_baseline('instagram')
        expected_keys = {
            'days', 'snapshot_count', 'yield_rate', 'avg_score',
            'auto_enroll_rate', 'avg_found', 'avg_scored', 'avg_synced',
            'funnel_conversion', 'avg_cost_per_lead',
        }
        assert set(baseline.keys()) == expected_keys

    def test_auto_enroll_rate_aggregated(self, patch_bench_session):
        """auto_enroll_rate should aggregate across all runs in the window."""
        session = patch_bench_session
        now = datetime.now()
        _make_db_run(session, 'r0',
                     tier_distribution={'auto_enroll': 10, 'high_priority_review': 10},
                     created_at=now)
        _make_db_run(session, 'r1',
                     tier_distribution={'auto_enroll': 0, 'high_priority_review': 20},
                     created_at=now - timedelta(days=1))
        _make_db_run(session, 'r2',
                     tier_distribution={'auto_enroll': 5, 'high_priority_review': 5},
                     created_at=now - timedelta(days=2))

        baseline = get_baseline('instagram')
        # Total: auto_enroll=15, total_tiered=50 → 30.0%
        assert baseline['auto_enroll_rate'] == 30.0

    def test_handles_exception_gracefully(self, db_session):
        with patch('app.services.benchmarks.get_session', side_effect=Exception('db down')):
            result = get_baseline('instagram')
        assert result is None


# ---------------------------------------------------------------------------
# compute_deviations
# ---------------------------------------------------------------------------

class TestComputeDeviations:

    def test_returns_empty_when_no_baseline(self):
        run = self._make_run(found=100, scored=50, synced=30)
        assert compute_deviations(run, None) == []

    def test_detects_notable_deviation(self):
        run = self._make_run(found=130, scored=50, synced=30)
        baseline = {'avg_found': 100, 'avg_scored': 50, 'avg_synced': 30,
                     'yield_rate': 50, 'funnel_conversion': 30, 'avg_cost_per_lead': 0.5}
        devs = compute_deviations(run, baseline)
        found_dev = next((d for d in devs if d.metric == 'avg_found'), None)
        assert found_dev is not None
        assert found_dev.severity == 'notable'
        assert found_dev.direction == 'above'

    def test_detects_significant_deviation(self):
        run = self._make_run(found=200, scored=50, synced=30)
        baseline = {'avg_found': 100, 'avg_scored': 50, 'avg_synced': 30,
                     'yield_rate': 50, 'funnel_conversion': 30, 'avg_cost_per_lead': 0.5}
        devs = compute_deviations(run, baseline)
        found_dev = next((d for d in devs if d.metric == 'avg_found'), None)
        assert found_dev is not None
        assert found_dev.severity == 'significant'

    def test_below_threshold_ignored(self):
        run = self._make_run(found=110, scored=50, synced=30)
        baseline = {'avg_found': 100, 'avg_scored': 50, 'avg_synced': 30,
                     'yield_rate': 50, 'funnel_conversion': 30, 'avg_cost_per_lead': 0.5}
        devs = compute_deviations(run, baseline)
        found_dev = next((d for d in devs if d.metric == 'avg_found'), None)
        assert found_dev is None  # 10% is below 25% threshold

    def test_cost_per_lead_inverted_direction(self):
        """Higher cost-per-lead is bad, so direction should be 'below' (below expectations)."""
        run = self._make_run(found=100, scored=50, synced=20, cost=20.0)
        # run CPL = 20/20 = 1.0, baseline = 0.5, so 100% higher
        baseline = {'avg_found': 100, 'avg_scored': 50, 'avg_synced': 20,
                     'yield_rate': 50, 'funnel_conversion': 20, 'avg_cost_per_lead': 0.5}
        devs = compute_deviations(run, baseline)
        cpl_dev = next((d for d in devs if d.metric == 'avg_cost_per_lead'), None)
        assert cpl_dev is not None
        assert cpl_dev.direction == 'below'  # higher cost = worse = "below" avg performance

    def test_zero_baseline_value_skipped(self):
        run = self._make_run(found=100, scored=50, synced=30)
        baseline = {'avg_found': 0, 'avg_scored': 50, 'avg_synced': 30,
                     'yield_rate': 0, 'funnel_conversion': 30, 'avg_cost_per_lead': 0}
        devs = compute_deviations(run, baseline)
        # avg_found baseline is 0, so it should be skipped (no division by zero)
        found_dev = next((d for d in devs if d.metric == 'avg_found'), None)
        assert found_dev is None

    def test_zero_run_values_handled(self):
        run = self._make_run(found=0, scored=0, synced=0)
        baseline = {'avg_found': 100, 'avg_scored': 50, 'avg_synced': 30,
                     'yield_rate': 50, 'funnel_conversion': 30, 'avg_cost_per_lead': 0.5}
        devs = compute_deviations(run, baseline)
        # Should get deviations for found, scored, synced (all 100% below)
        found_dev = next((d for d in devs if d.metric == 'avg_found'), None)
        assert found_dev is not None
        assert found_dev.direction == 'below'
        assert found_dev.severity == 'significant'

    def test_returns_deviation_dataclass(self):
        run = self._make_run(found=200, scored=50, synced=30)
        baseline = {'avg_found': 100, 'avg_scored': 50, 'avg_synced': 30,
                     'yield_rate': 50, 'funnel_conversion': 30, 'avg_cost_per_lead': 0.5}
        devs = compute_deviations(run, baseline)
        for d in devs:
            assert isinstance(d, Deviation)
            assert hasattr(d, 'metric')
            assert hasattr(d, 'pct_change')
            assert hasattr(d, 'severity')

    def test_direction_below_for_negative_change(self):
        run = self._make_run(found=40, scored=50, synced=30)
        baseline = {'avg_found': 100, 'avg_scored': 50, 'avg_synced': 30,
                     'yield_rate': 50, 'funnel_conversion': 30, 'avg_cost_per_lead': 0.5}
        devs = compute_deviations(run, baseline)
        found_dev = next((d for d in devs if d.metric == 'avg_found'), None)
        assert found_dev is not None
        assert found_dev.direction == 'below'
        assert found_dev.pct_change < 0

    @staticmethod
    def _make_run(found=0, prescreened=None, scored=0, synced=0, cost=0.0):
        if prescreened is None:
            prescreened = int(found * 0.8)
        run = MagicMock()
        run.profiles_found = found
        run.profiles_pre_screened = prescreened
        run.profiles_scored = scored
        run.contacts_synced = synced
        run.actual_cost = cost
        return run
