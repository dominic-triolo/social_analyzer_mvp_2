"""Tests for app.services.benchmarks — snapshot persistence, baselines, deviations."""
import pytest
from datetime import date, timedelta
from unittest.mock import patch, MagicMock

from app.models.db_run import DbRun
from app.models.metric_snapshot import MetricSnapshot
from app.services.benchmarks import (
    persist_metric_snapshot,
    get_baseline,
    compute_deviations,
    Deviation,
)


@pytest.fixture
def patch_bench_session(db_session):
    """Patch get_session where benchmarks imported it."""
    with patch('app.services.benchmarks.get_session', return_value=db_session):
        yield db_session


# ---------------------------------------------------------------------------
# persist_metric_snapshot
# ---------------------------------------------------------------------------

class TestPersistMetricSnapshot:

    def test_creates_snapshot_for_completed_run(self, patch_bench_session):
        session = patch_bench_session
        run = self._make_run()
        db_run = DbRun(
            id='r1', platform='instagram', status='completed',
            profiles_found=100, profiles_pre_screened=80,
            profiles_enriched=70, profiles_scored=50,
            contacts_synced=30, actual_cost=5.0,
        )
        session.add(db_run)
        session.commit()

        result = persist_metric_snapshot(run)
        assert result is not None
        assert result.platform == 'instagram'
        assert result.date == date.today()
        assert result.runs_count == 1
        assert result.avg_found == 100.0

    def test_upserts_existing_snapshot(self, patch_bench_session):
        session = patch_bench_session
        run = self._make_run()
        # Create two completed runs for today
        for i, found in enumerate([100, 200]):
            session.add(DbRun(
                id=f'r{i}', platform='instagram', status='completed',
                profiles_found=found, profiles_pre_screened=found - 20,
                profiles_enriched=found - 30, profiles_scored=found - 50,
                contacts_synced=found - 70, actual_cost=5.0,
            ))
        session.commit()

        persist_metric_snapshot(run)
        persist_metric_snapshot(run)

        snapshots = session.query(MetricSnapshot).filter(
            MetricSnapshot.date == date.today(),
            MetricSnapshot.platform == 'instagram',
        ).all()
        assert len(snapshots) == 1
        # avg_found should be average of 100 and 200
        assert snapshots[0].avg_found == 150.0

    def test_returns_none_when_no_completed_runs(self, patch_bench_session):
        run = self._make_run()
        result = persist_metric_snapshot(run)
        assert result is None

    def test_computes_yield_rate(self, patch_bench_session):
        session = patch_bench_session
        run = self._make_run()
        session.add(DbRun(
            id='r1', platform='instagram', status='completed',
            profiles_found=100, profiles_pre_screened=40,
            profiles_enriched=35, profiles_scored=30,
            contacts_synced=20, actual_cost=3.0,
        ))
        session.commit()

        result = persist_metric_snapshot(run)
        assert result.yield_rate == 40.0  # 40/100 * 100

    def test_computes_funnel_conversion(self, patch_bench_session):
        session = patch_bench_session
        run = self._make_run()
        session.add(DbRun(
            id='r1', platform='instagram', status='completed',
            profiles_found=100, profiles_pre_screened=80,
            profiles_enriched=70, profiles_scored=50,
            contacts_synced=25, actual_cost=5.0,
        ))
        session.commit()

        result = persist_metric_snapshot(run)
        assert result.funnel_conversion == 25.0  # 25/100 * 100

    def test_computes_cost_per_lead(self, patch_bench_session):
        session = patch_bench_session
        run = self._make_run()
        session.add(DbRun(
            id='r1', platform='instagram', status='completed',
            profiles_found=100, profiles_pre_screened=80,
            profiles_enriched=70, profiles_scored=50,
            contacts_synced=10, actual_cost=5.0,
        ))
        session.commit()

        result = persist_metric_snapshot(run)
        assert result.avg_cost_per_lead == 0.5  # 5.0/10

    def test_handles_zero_found(self, patch_bench_session):
        session = patch_bench_session
        run = self._make_run()
        session.add(DbRun(
            id='r1', platform='instagram', status='completed',
            profiles_found=0, profiles_pre_screened=0,
            profiles_enriched=0, profiles_scored=0,
            contacts_synced=0, actual_cost=0.0,
        ))
        session.commit()

        result = persist_metric_snapshot(run)
        assert result.yield_rate == 0.0
        assert result.funnel_conversion == 0.0

    def test_handles_zero_synced_for_cpl(self, patch_bench_session):
        session = patch_bench_session
        run = self._make_run()
        session.add(DbRun(
            id='r1', platform='instagram', status='completed',
            profiles_found=50, profiles_pre_screened=40,
            profiles_enriched=35, profiles_scored=30,
            contacts_synced=0, actual_cost=2.0,
        ))
        session.commit()

        result = persist_metric_snapshot(run)
        assert result.avg_cost_per_lead == 0.0

    def test_only_counts_matching_platform(self, patch_bench_session):
        session = patch_bench_session
        run = self._make_run(platform='patreon')
        session.add(DbRun(
            id='r1', platform='instagram', status='completed',
            profiles_found=100, profiles_pre_screened=80,
            profiles_enriched=70, profiles_scored=50,
            contacts_synced=30, actual_cost=5.0,
        ))
        session.commit()

        result = persist_metric_snapshot(run)
        assert result is None

    def test_handles_exception_gracefully(self, db_session):
        """When get_session raises, returns None without crashing."""
        run = self._make_run()
        with patch('app.services.benchmarks.get_session', side_effect=Exception('db down')):
            result = persist_metric_snapshot(run)
        assert result is None

    @staticmethod
    def _make_run(platform='instagram'):
        run = MagicMock()
        run.platform = platform
        run.tier_distribution = {'auto_enroll': 5, 'high_priority_review': 10}
        return run


# ---------------------------------------------------------------------------
# get_baseline
# ---------------------------------------------------------------------------

class TestGetBaseline:

    def test_returns_averages_from_snapshots(self, patch_bench_session):
        session = patch_bench_session
        today = date.today()
        for i in range(5):
            session.add(MetricSnapshot(
                date=today - timedelta(days=i),
                platform='instagram',
                yield_rate=50.0 + i,
                avg_score=30.0,
                auto_enroll_rate=20.0,
                avg_found=100.0 + i * 10,
                avg_scored=50.0,
                avg_synced=25.0,
                funnel_conversion=25.0,
                avg_cost_per_lead=0.5,
                runs_count=1,
            ))
        session.commit()

        baseline = get_baseline('instagram')
        assert baseline is not None
        assert baseline['snapshot_count'] == 5
        assert baseline['avg_found'] > 0

    def test_returns_none_for_insufficient_data(self, patch_bench_session):
        session = patch_bench_session
        today = date.today()
        # Only 2 snapshots — below minimum of 3
        for i in range(2):
            session.add(MetricSnapshot(
                date=today - timedelta(days=i),
                platform='instagram',
                yield_rate=50.0,
                avg_found=100.0,
                runs_count=1,
            ))
        session.commit()

        baseline = get_baseline('instagram')
        assert baseline is None

    def test_filters_by_platform(self, patch_bench_session):
        session = patch_bench_session
        today = date.today()
        for i in range(5):
            session.add(MetricSnapshot(
                date=today - timedelta(days=i),
                platform='instagram',
                yield_rate=50.0,
                avg_found=100.0,
                runs_count=1,
            ))
        session.commit()

        assert get_baseline('instagram') is not None
        assert get_baseline('patreon') is None

    def test_respects_days_parameter(self, patch_bench_session):
        session = patch_bench_session
        today = date.today()
        # Snapshots from 40+ days ago
        for i in range(5):
            session.add(MetricSnapshot(
                date=today - timedelta(days=40 + i),
                platform='instagram',
                yield_rate=50.0,
                avg_found=100.0,
                runs_count=1,
            ))
        session.commit()

        assert get_baseline('instagram', days=30) is None
        assert get_baseline('instagram', days=60) is not None

    def test_returns_correct_keys(self, patch_bench_session):
        session = patch_bench_session
        today = date.today()
        for i in range(3):
            session.add(MetricSnapshot(
                date=today - timedelta(days=i),
                platform='instagram',
                yield_rate=50.0, avg_score=30.0,
                auto_enroll_rate=20.0,
                avg_found=100.0, avg_scored=50.0,
                avg_synced=25.0, funnel_conversion=25.0,
                avg_cost_per_lead=0.5, runs_count=1,
            ))
        session.commit()

        baseline = get_baseline('instagram')
        expected_keys = {
            'days', 'snapshot_count', 'yield_rate', 'avg_score',
            'auto_enroll_rate', 'avg_found', 'avg_scored', 'avg_synced',
            'funnel_conversion', 'avg_cost_per_lead',
        }
        assert set(baseline.keys()) == expected_keys

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
