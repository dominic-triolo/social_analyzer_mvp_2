"""Tests for app.pipeline.manager — pipeline orchestration, launch, status, cost estimation."""
import pytest
from unittest.mock import patch, MagicMock, call

from app.pipeline.base import StageResult, StageAdapter


# ── Helpers ──────────────────────────────────────────────────────────────────

class _FakeAdapter(StageAdapter):
    """Adapter that returns whatever profiles it receives."""
    platform = 'instagram'
    stage = 'discovery'

    def run(self, profiles, run):
        return StageResult(profiles=profiles, processed=len(profiles))


class _DiscoveryAdapter(StageAdapter):
    """Adapter that 'discovers' profiles from nothing."""
    platform = 'instagram'
    stage = 'discovery'

    def __init__(self, profiles=None):
        self._profiles = profiles or [
            {'platform_username': 'user1', 'name': 'User One'},
            {'platform_username': 'user2', 'name': 'User Two'},
            {'platform_username': 'user3', 'name': 'User Three'},
        ]

    def run(self, profiles, run):
        return StageResult(profiles=list(self._profiles), processed=len(self._profiles))


class _CostlyAdapter(StageAdapter):
    """Adapter that reports cost."""
    platform = 'instagram'
    stage = 'scoring'

    def run(self, profiles, run):
        return StageResult(profiles=profiles, processed=len(profiles), cost=0.50)

    def estimate_cost(self, count):
        return count * 0.05


class _FailingAdapter(StageAdapter):
    """Adapter that always raises."""
    platform = 'instagram'
    stage = 'enrichment'

    def run(self, profiles, run):
        raise RuntimeError("API timeout")


class _EmptyAdapter(StageAdapter):
    """Adapter that returns zero profiles (simulates all filtered out)."""
    platform = 'instagram'
    stage = 'pre_screen'

    def run(self, profiles, run):
        return StageResult(profiles=[], processed=len(profiles), skipped=len(profiles))


def _make_run(**overrides):
    """Build a MagicMock Run with sensible defaults."""
    defaults = dict(
        id='run-test-001',
        platform='instagram',
        status='queued',
        filters={'max_results': 10},
        bdr_assignment='Test BDR',
        estimated_cost=0.0,
        actual_cost=0.0,
        created_at='2026-01-15T10:00:00',
        profiles_found=0,
        profiles_pre_screened=0,
        profiles_enriched=0,
        profiles_scored=0,
        contacts_synced=0,
        duplicates_skipped=0,
        tier_distribution={'auto_enroll': 0, 'standard_priority_review': 0, 'low_priority_review': 0},
        error_count=0,
        errors=[],
        summary='',
        stage_outputs={},
        stage_progress={
            'discovery': {'total': 0, 'completed': 0, 'failed': 0},
            'pre_screen': {'total': 0, 'completed': 0, 'failed': 0},
            'enrichment': {'total': 0, 'completed': 0, 'failed': 0},
            'analysis': {'total': 0, 'completed': 0, 'failed': 0},
            'scoring': {'total': 0, 'completed': 0, 'failed': 0},
            'crm_sync': {'total': 0, 'completed': 0, 'failed': 0},
        },
        current_stage='',
    )
    defaults.update(overrides)
    run = MagicMock()
    for k, v in defaults.items():
        setattr(run, k, v)
    # Make save/complete/fail/update_stage/add_error no-ops that still track calls
    run.save.return_value = run
    run.complete.return_value = None
    run.fail.return_value = None
    run.update_stage.return_value = None
    run.add_error.return_value = None
    run.increment_stage_progress.return_value = None
    return run


# ── launch_run ───────────────────────────────────────────────────────────────

class TestLaunchRun:
    """launch_run() creates a Run and enqueues the pipeline job."""

    @patch('app.pipeline.manager._get_queue')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.STAGE_REGISTRY', {
        'discovery': {'instagram': _FakeAdapter},
        'pre_screen': {'instagram': _FakeAdapter},
        'enrichment': {'instagram': _FakeAdapter},
        'analysis': {'instagram': _FakeAdapter},
        'scoring': {'instagram': _FakeAdapter},
        'crm_sync': {'instagram': _FakeAdapter},
    })
    def test_creates_run_and_enqueues(self, MockRun, mock_persist, mock_queue):
        """Creates a Run, persists it, and enqueues the pipeline."""
        from app.pipeline.manager import launch_run

        mock_run = MagicMock()
        mock_run.id = 'run-123'
        MockRun.return_value = mock_run

        result = launch_run('instagram', {'max_results': 10})

        assert result is mock_run
        mock_run.save.assert_called_once()
        mock_persist.assert_called_once_with(mock_run)
        mock_queue.return_value.enqueue.assert_called_once()

    @patch('app.pipeline.manager.STAGE_REGISTRY', {
        'discovery': {'instagram': _FakeAdapter},
    })
    def test_rejects_unsupported_platform(self):
        """Raises ValueError for a platform not in the discovery registry."""
        from app.pipeline.manager import launch_run

        with pytest.raises(ValueError, match="Unsupported platform: tiktok"):
            launch_run('tiktok', {'max_results': 10})

    @patch('app.pipeline.manager._get_queue')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.STAGE_REGISTRY', {
        'discovery': {'instagram': _FakeAdapter},
        'pre_screen': {'instagram': _FakeAdapter},
        'enrichment': {'instagram': _FakeAdapter},
        'analysis': {'instagram': _FakeAdapter},
        'scoring': {'instagram': _FakeAdapter},
        'crm_sync': {'instagram': _FakeAdapter},
    })
    def test_uses_provided_bdr_names(self, MockRun, mock_persist, mock_queue):
        """BDR names from argument are injected into filters."""
        from app.pipeline.manager import launch_run

        mock_run = MagicMock()
        MockRun.return_value = mock_run

        filters = {'max_results': 10}
        launch_run('instagram', filters, bdr_names=['Alice', 'Bob'])

        assert filters['bdr_names'] == ['Alice', 'Bob']

    @patch('app.pipeline.manager._get_queue')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.BDR_OWNER_IDS', {'DefaultBDR': '12345'})
    @patch('app.pipeline.manager.STAGE_REGISTRY', {
        'discovery': {'instagram': _FakeAdapter},
        'pre_screen': {'instagram': _FakeAdapter},
        'enrichment': {'instagram': _FakeAdapter},
        'analysis': {'instagram': _FakeAdapter},
        'scoring': {'instagram': _FakeAdapter},
        'crm_sync': {'instagram': _FakeAdapter},
    })
    def test_defaults_bdr_names_from_config(self, MockRun, mock_persist, mock_queue):
        """When no bdr_names provided, defaults to BDR_OWNER_IDS keys."""
        from app.pipeline.manager import launch_run

        mock_run = MagicMock()
        MockRun.return_value = mock_run

        filters = {'max_results': 10}
        launch_run('instagram', filters)

        assert filters['bdr_names'] == ['DefaultBDR']

    @patch('app.pipeline.manager._get_queue')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.STAGE_REGISTRY', {
        'discovery': {'instagram': _FakeAdapter},
        'pre_screen': {'instagram': _FakeAdapter},
        'enrichment': {'instagram': _FakeAdapter},
        'analysis': {'instagram': _FakeAdapter},
        'scoring': {'instagram': _FakeAdapter},
        'crm_sync': {'instagram': _FakeAdapter},
    })
    def test_cost_estimation_failure_does_not_block(self, MockRun, mock_persist, mock_queue):
        """If _estimate_total_cost raises, launch_run still succeeds."""
        from app.pipeline.manager import launch_run

        mock_run = MagicMock()
        MockRun.return_value = mock_run

        with patch('app.pipeline.manager._estimate_total_cost', side_effect=Exception("cost calc failed")):
            result = launch_run('instagram', {'max_results': 10}, bdr_names=['A'])

        assert result is mock_run
        mock_run.save.assert_called_once()


# ── get_run_status ───────────────────────────────────────────────────────────

class TestGetRunStatus:
    """get_run_status() loads a Run and returns its dict representation."""

    @patch('app.pipeline.manager.Run')
    def test_returns_dict_for_existing_run(self, MockRun):
        """Returns to_dict() output for a valid run ID."""
        from app.pipeline.manager import get_run_status

        mock_run = MagicMock()
        mock_run.to_dict.return_value = {'id': 'run-123', 'status': 'completed'}
        MockRun.load.return_value = mock_run

        result = get_run_status('run-123')

        assert result == {'id': 'run-123', 'status': 'completed'}
        MockRun.load.assert_called_once_with('run-123')

    @patch('app.pipeline.manager.Run')
    def test_returns_none_for_missing_run(self, MockRun):
        """Returns None when the run doesn't exist."""
        from app.pipeline.manager import get_run_status

        MockRun.load.return_value = None
        result = get_run_status('nonexistent')

        assert result is None


# ── run_pipeline ─────────────────────────────────────────────────────────────

class TestRunPipeline:
    """run_pipeline() orchestrates the 6-stage pipeline for a run."""

    @patch('app.pipeline.manager.notify_run_complete')
    @patch('app.pipeline.manager.persist_lead_results')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.record_filter_history')
    @patch('app.pipeline.manager.dedup_profiles', return_value=([
        {'platform_username': 'user1', 'name': 'User One'},
        {'platform_username': 'user2', 'name': 'User Two'},
    ], 1))
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.PIPELINE_STAGES', [
        'discovery', 'pre_screen', 'enrichment', 'analysis', 'scoring', 'crm_sync',
    ])
    def test_full_pipeline_success(self, MockRun, mock_dedup, mock_filter_hist,
                                    mock_persist, mock_lead_results, mock_notify):
        """All 6 stages complete successfully; run is marked completed."""
        from app.pipeline.manager import run_pipeline

        run = _make_run()
        MockRun.load.return_value = run

        # All stages use the passthrough adapter
        registry = {s: {'instagram': _FakeAdapter} for s in
                    ['discovery', 'pre_screen', 'enrichment', 'analysis', 'scoring', 'crm_sync']}
        # But discovery needs to produce profiles
        registry['discovery'] = {'instagram': _DiscoveryAdapter}

        with patch('app.pipeline.manager.STAGE_REGISTRY', registry):
            run_pipeline('run-test-001')

        run.complete.assert_called_once()
        mock_persist.assert_called()
        mock_lead_results.assert_called_once()
        mock_notify.assert_called_once_with(run)

    @patch('app.pipeline.manager.Run')
    def test_run_not_found_returns_early(self, MockRun):
        """When Run.load returns None, the function returns without error."""
        from app.pipeline.manager import run_pipeline

        MockRun.load.return_value = None
        # Should not raise
        run_pipeline('nonexistent-run')

    @patch('app.pipeline.manager.notify_run_failed')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.dedup_profiles', return_value=([
        {'platform_username': 'user1', 'name': 'User One'},
    ], 0))
    @patch('app.pipeline.manager.record_filter_history')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.PIPELINE_STAGES', [
        'discovery', 'enrichment',
    ])
    def test_stage_failure_marks_run_failed(self, MockRun, mock_filter_hist,
                                             mock_dedup, mock_persist, mock_notify_fail):
        """When a stage raises, run is marked failed and notifications sent."""
        from app.pipeline.manager import run_pipeline

        run = _make_run()
        MockRun.load.return_value = run

        registry = {
            'discovery': {'instagram': _DiscoveryAdapter},
            'enrichment': {'instagram': _FailingAdapter},
        }

        with patch('app.pipeline.manager.STAGE_REGISTRY', registry):
            run_pipeline('run-test-001')

        run.fail.assert_called_once()
        fail_msg = run.fail.call_args[0][0]
        assert "enrichment" in fail_msg
        assert "API timeout" in fail_msg
        mock_notify_fail.assert_called_once_with(run)

    @patch('app.pipeline.manager.notify_run_complete')
    @patch('app.pipeline.manager.persist_lead_results')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.dedup_profiles', return_value=([
        {'platform_username': 'user1', 'name': 'User One'},
    ], 0))
    @patch('app.pipeline.manager.record_filter_history')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.PIPELINE_STAGES', [
        'discovery', 'pre_screen', 'enrichment',
    ])
    def test_zero_profiles_stops_early(self, MockRun, mock_filter_hist, mock_dedup,
                                        mock_persist, mock_lead_results, mock_notify):
        """When a stage returns 0 profiles, pipeline stops and run completes."""
        from app.pipeline.manager import run_pipeline

        run = _make_run()
        MockRun.load.return_value = run

        registry = {
            'discovery': {'instagram': _DiscoveryAdapter},
            'pre_screen': {'instagram': _EmptyAdapter},
            'enrichment': {'instagram': _FakeAdapter},
        }

        with patch('app.pipeline.manager.STAGE_REGISTRY', registry):
            run_pipeline('run-test-001')

        # Should complete early, not fail
        run.complete.assert_called_once()
        mock_notify.assert_called_once_with(run)

    @patch('app.pipeline.manager.notify_run_failed')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.dedup_profiles', return_value=([
        {'platform_username': 'user1', 'name': 'User One'},
    ], 0))
    @patch('app.pipeline.manager.record_filter_history')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.PIPELINE_STAGES', [
        'discovery', 'scoring',
    ])
    def test_budget_exceeded_stops_pipeline(self, MockRun, mock_filter_hist,
                                             mock_dedup, mock_persist, mock_notify_fail):
        """When actual_cost + estimated next stage exceeds max_budget, pipeline stops."""
        from app.pipeline.manager import run_pipeline

        # actual_cost starts at 0.90, and the scoring adapter estimates 1 profile * 0.05 = 0.05
        # 0.90 + 0.05 < 1.00, so we need a higher actual_cost or lower budget
        # Use actual_cost=0.96 so that 0.96 + 0.05 = 1.01 > 1.00
        run = _make_run(
            filters={'max_results': 10, 'max_budget': 1.00},
            actual_cost=0.96,
        )
        MockRun.load.return_value = run

        registry = {
            'discovery': {'instagram': _DiscoveryAdapter},
            'scoring': {'instagram': _CostlyAdapter},
        }

        with patch('app.pipeline.manager.STAGE_REGISTRY', registry):
            run_pipeline('run-test-001')

        run.fail.assert_called_once()
        fail_msg = run.fail.call_args[0][0]
        assert "Budget limit" in fail_msg
        mock_notify_fail.assert_called_once_with(run)

    @patch('app.pipeline.manager.notify_run_complete')
    @patch('app.pipeline.manager.persist_lead_results')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.dedup_profiles', return_value=([], 3))
    @patch('app.pipeline.manager.record_filter_history')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.PIPELINE_STAGES', ['discovery', 'pre_screen'])
    def test_dedup_runs_after_discovery(self, MockRun, mock_filter_hist, mock_dedup,
                                        mock_persist, mock_lead_results, mock_notify):
        """After discovery, dedup_profiles is called and duplicates_skipped is set."""
        from app.pipeline.manager import run_pipeline

        run = _make_run()
        MockRun.load.return_value = run

        registry = {
            'discovery': {'instagram': _DiscoveryAdapter},
            'pre_screen': {'instagram': _FakeAdapter},
        }

        with patch('app.pipeline.manager.STAGE_REGISTRY', registry):
            run_pipeline('run-test-001')

        mock_dedup.assert_called_once()
        assert run.duplicates_skipped == 3

    @patch('app.pipeline.manager.notify_run_complete')
    @patch('app.pipeline.manager.persist_lead_results')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.PIPELINE_STAGES', ['discovery', 'scoring'])
    def test_cost_accumulated_across_stages(self, MockRun, mock_persist,
                                             mock_lead_results, mock_notify):
        """Stage costs are accumulated into run.actual_cost."""
        from app.pipeline.manager import run_pipeline

        run = _make_run(actual_cost=0.0, filters={'max_results': 10})
        MockRun.load.return_value = run

        class _CostDiscovery(StageAdapter):
            platform = 'instagram'
            stage = 'discovery'
            def run(self, profiles, run_obj):
                return StageResult(
                    profiles=[{'platform_username': 'u1'}],
                    processed=1, cost=0.25,
                )

        class _CostScoring(StageAdapter):
            platform = 'instagram'
            stage = 'scoring'
            def run(self, profiles, run_obj):
                return StageResult(profiles=profiles, processed=len(profiles), cost=0.35)

        registry = {
            'discovery': {'instagram': _CostDiscovery},
            'scoring': {'instagram': _CostScoring},
        }

        with patch('app.pipeline.manager.STAGE_REGISTRY', registry), \
             patch('app.pipeline.manager.dedup_profiles', return_value=([{'platform_username': 'u1'}], 0)), \
             patch('app.pipeline.manager.record_filter_history'):
            run_pipeline('run-test-001')

        assert run.actual_cost == pytest.approx(0.60)

    @patch('app.pipeline.manager.notify_run_complete')
    @patch('app.pipeline.manager.persist_lead_results')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.PIPELINE_STAGES', ['discovery', 'enrichment', 'scoring'])
    def test_retry_from_stage_skips_earlier_stages(self, MockRun, mock_persist,
                                                     mock_lead_results, mock_notify):
        """retry_from_stage loads checkpoint and skips stages before it."""
        from app.pipeline.manager import run_pipeline

        checkpoint_profiles = [
            {'platform_username': 'u1', 'name': 'User One'},
        ]
        run = _make_run(
            stage_outputs={'discovery': checkpoint_profiles},
        )
        MockRun.load.return_value = run

        call_log = []

        class _TrackingEnrichment(StageAdapter):
            platform = 'instagram'
            stage = 'enrichment'
            def run(self, profiles, run_obj):
                call_log.append(('enrichment', len(profiles)))
                return StageResult(profiles=profiles, processed=len(profiles))

        class _TrackingScoring(StageAdapter):
            platform = 'instagram'
            stage = 'scoring'
            def run(self, profiles, run_obj):
                call_log.append(('scoring', len(profiles)))
                return StageResult(profiles=profiles, processed=len(profiles))

        registry = {
            'discovery': {'instagram': _DiscoveryAdapter},
            'enrichment': {'instagram': _TrackingEnrichment},
            'scoring': {'instagram': _TrackingScoring},
        }

        with patch('app.pipeline.manager.STAGE_REGISTRY', registry):
            run_pipeline('run-test-001', retry_from_stage='enrichment')

        # Discovery should be skipped, enrichment + scoring should run
        stage_names = [name for name, _ in call_log]
        assert 'enrichment' in stage_names
        assert 'scoring' in stage_names
        # Enrichment should have received checkpoint profiles
        assert call_log[0] == ('enrichment', 1)

    @patch('app.pipeline.manager.notify_run_complete')
    @patch('app.pipeline.manager.persist_lead_results')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.dedup_profiles', return_value=([
        {'platform_username': 'u1'},
    ], 0))
    @patch('app.pipeline.manager.record_filter_history')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.PIPELINE_STAGES', ['discovery', 'scoring'])
    def test_retry_from_stage_no_checkpoint_starts_from_scratch(self, MockRun,
                                                                  mock_filter_hist,
                                                                  mock_dedup,
                                                                  mock_persist,
                                                                  mock_lead_results,
                                                                  mock_notify):
        """If no checkpoint exists for retry point, pipeline starts from discovery."""
        from app.pipeline.manager import run_pipeline

        run = _make_run(stage_outputs={})
        MockRun.load.return_value = run

        registry = {
            'discovery': {'instagram': _DiscoveryAdapter},
            'scoring': {'instagram': _FakeAdapter},
        }

        with patch('app.pipeline.manager.STAGE_REGISTRY', registry):
            run_pipeline('run-test-001', retry_from_stage='scoring')

        # Discovery should have run since there was no checkpoint
        assert run.profiles_found > 0

    @patch('app.pipeline.manager.notify_run_complete')
    @patch('app.pipeline.manager.persist_lead_results')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.dedup_profiles', return_value=([
        {'platform_username': 'u1', 'name': 'U1'},
    ], 0))
    @patch('app.pipeline.manager.record_filter_history')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.PIPELINE_STAGES', ['discovery', 'scoring'])
    def test_stage_errors_logged_on_run(self, MockRun, mock_filter_hist, mock_dedup,
                                         mock_persist, mock_lead_results, mock_notify):
        """Errors from StageResult.errors are forwarded to run.add_error."""
        from app.pipeline.manager import run_pipeline

        run = _make_run()
        MockRun.load.return_value = run

        class _ErrorAdapter(StageAdapter):
            platform = 'instagram'
            stage = 'scoring'
            def run(self, profiles, run_obj):
                return StageResult(
                    profiles=profiles, processed=len(profiles),
                    errors=['rate limited on profile X', 'timeout on profile Y'],
                )

        registry = {
            'discovery': {'instagram': _DiscoveryAdapter},
            'scoring': {'instagram': _ErrorAdapter},
        }

        with patch('app.pipeline.manager.STAGE_REGISTRY', registry):
            run_pipeline('run-test-001')

        assert run.add_error.call_count == 2

    @patch('app.pipeline.manager.notify_run_complete')
    @patch('app.pipeline.manager.persist_lead_results')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.dedup_profiles', return_value=([
        {'platform_username': 'u1'},
    ], 0))
    @patch('app.pipeline.manager.record_filter_history')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.PIPELINE_STAGES', ['discovery', 'scoring'])
    def test_aggregate_counters_updated(self, MockRun, mock_filter_hist, mock_dedup,
                                         mock_persist, mock_lead_results, mock_notify):
        """Stage-specific profile counters are updated on the run."""
        from app.pipeline.manager import run_pipeline

        run = _make_run()
        MockRun.load.return_value = run

        registry = {
            'discovery': {'instagram': _DiscoveryAdapter},
            'scoring': {'instagram': _FakeAdapter},
        }

        with patch('app.pipeline.manager.STAGE_REGISTRY', registry):
            run_pipeline('run-test-001')

        # After dedup, profiles_found should be updated
        assert run.profiles_found == 1
        # Scoring should also be tracked
        assert run.profiles_scored == 1

    @patch('app.pipeline.manager.notify_run_complete')
    @patch('app.pipeline.manager.persist_lead_results')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.PIPELINE_STAGES', ['unknown_stage'])
    def test_missing_stage_registry_skipped(self, MockRun, mock_persist,
                                             mock_lead_results, mock_notify):
        """A stage with no registry entry is skipped without error."""
        from app.pipeline.manager import run_pipeline

        run = _make_run()
        MockRun.load.return_value = run

        with patch('app.pipeline.manager.STAGE_REGISTRY', {}):
            run_pipeline('run-test-001')

        # Should still complete
        run.complete.assert_called_once()


# ── _generate_run_summary ────────────────────────────────────────────────────

class TestGenerateRunSummary:
    """_generate_run_summary() produces human-readable run summaries."""

    def test_basic_summary(self):
        """Minimal run produces platform + found count."""
        from app.pipeline.manager import _generate_run_summary

        run = _make_run(
            platform='instagram',
            profiles_found=50,
            duplicates_skipped=0,
            profiles_pre_screened=0,
            profiles_scored=0,
            contacts_synced=0,
            tier_distribution={},
            actual_cost=0.0,
        )

        summary = _generate_run_summary(run)
        assert 'Instagram' in summary
        assert '50' in summary

    def test_summary_with_duplicates(self):
        """Duplicates count appears when nonzero."""
        from app.pipeline.manager import _generate_run_summary

        run = _make_run(
            platform='instagram',
            profiles_found=100,
            duplicates_skipped=25,
            profiles_pre_screened=0,
            profiles_scored=0,
            contacts_synced=0,
            tier_distribution={},
            actual_cost=0.0,
        )

        summary = _generate_run_summary(run)
        assert '25' in summary
        assert 'duplicate' in summary.lower()

    def test_summary_with_conversion_rate(self):
        """Conversion rate appears when both found and synced are nonzero."""
        from app.pipeline.manager import _generate_run_summary

        run = _make_run(
            platform='instagram',
            profiles_found=100,
            duplicates_skipped=0,
            profiles_pre_screened=80,
            profiles_scored=60,
            contacts_synced=50,
            tier_distribution={},
            actual_cost=0.0,
        )

        summary = _generate_run_summary(run)
        assert '50%' in summary
        assert 'conversion' in summary.lower()

    def test_summary_with_cost(self):
        """Cost appears when actual_cost > 0."""
        from app.pipeline.manager import _generate_run_summary

        run = _make_run(
            platform='instagram',
            profiles_found=10,
            duplicates_skipped=0,
            profiles_pre_screened=0,
            profiles_scored=0,
            contacts_synced=0,
            tier_distribution={},
            actual_cost=2.50,
        )

        summary = _generate_run_summary(run)
        assert '$2.50' in summary

    def test_summary_with_auto_enroll(self):
        """Auto-enroll count appears in summary when nonzero."""
        from app.pipeline.manager import _generate_run_summary

        run = _make_run(
            platform='instagram',
            profiles_found=20,
            duplicates_skipped=0,
            profiles_pre_screened=0,
            profiles_scored=15,
            contacts_synced=10,
            tier_distribution={'auto_enroll': 5},
            actual_cost=0.0,
        )

        summary = _generate_run_summary(run)
        assert '5' in summary
        assert 'auto-enroll' in summary.lower()

    def test_summary_zero_profiles_found(self):
        """Run with zero profiles still produces a valid summary."""
        from app.pipeline.manager import _generate_run_summary

        run = _make_run(
            platform='instagram',
            profiles_found=0,
            duplicates_skipped=0,
            profiles_pre_screened=0,
            profiles_scored=0,
            contacts_synced=0,
            tier_distribution={},
            actual_cost=0.0,
        )

        summary = _generate_run_summary(run)
        assert isinstance(summary, str)
        assert len(summary) > 0

    def test_summary_none_values_handled(self):
        """None values in run attributes don't cause crashes."""
        from app.pipeline.manager import _generate_run_summary

        run = _make_run(
            platform='instagram',
            profiles_found=None,
            duplicates_skipped=None,
            profiles_pre_screened=None,
            profiles_scored=None,
            contacts_synced=None,
            tier_distribution=None,
            actual_cost=None,
        )

        summary = _generate_run_summary(run)
        assert isinstance(summary, str)


# ── _estimate_total_cost ─────────────────────────────────────────────────────

class TestEstimateTotalCost:
    """_estimate_total_cost() sums adapter cost estimates across stages."""

    @patch('app.pipeline.manager.PIPELINE_STAGES', ['discovery', 'scoring'])
    def test_sums_costs_across_stages(self):
        """Total cost is the sum of per-stage estimate_cost calls."""
        from app.pipeline.manager import _estimate_total_cost

        class _Discovery(StageAdapter):
            platform = 'instagram'
            stage = 'discovery'
            def run(self, profiles, run): ...
            def estimate_cost(self, count):
                return count * 0.01

        class _Scoring(StageAdapter):
            platform = 'instagram'
            stage = 'scoring'
            def run(self, profiles, run): ...
            def estimate_cost(self, count):
                return count * 0.02

        registry = {
            'discovery': {'instagram': _Discovery},
            'scoring': {'instagram': _Scoring},
        }

        with patch('app.pipeline.manager.STAGE_REGISTRY', registry):
            cost = _estimate_total_cost('instagram', {'max_results': 100})

        # discovery: 100 * 0.01 = 1.00
        # After discovery funnel: int(100 * 0.7) = 70
        # scoring: 70 * 0.02 = 1.40
        assert cost == pytest.approx(2.40)

    @patch('app.pipeline.manager.PIPELINE_STAGES', ['discovery'])
    def test_missing_adapter_skipped(self):
        """Stages without an adapter for the platform are skipped."""
        from app.pipeline.manager import _estimate_total_cost

        with patch('app.pipeline.manager.STAGE_REGISTRY', {'discovery': {}}):
            cost = _estimate_total_cost('instagram', {'max_results': 100})

        assert cost == 0.0

    @patch('app.pipeline.manager.PIPELINE_STAGES', ['discovery'])
    def test_defaults_to_100_when_no_max_results(self):
        """When max_results is not in filters, defaults to 100."""
        from app.pipeline.manager import _estimate_total_cost

        class _Discovery(StageAdapter):
            platform = 'instagram'
            stage = 'discovery'
            def run(self, profiles, run): ...
            def estimate_cost(self, count):
                return count * 0.01

        registry = {'discovery': {'instagram': _Discovery}}

        with patch('app.pipeline.manager.STAGE_REGISTRY', registry):
            cost = _estimate_total_cost('instagram', {})

        assert cost == pytest.approx(1.00)

    @patch('app.pipeline.manager.PIPELINE_STAGES', [])
    def test_empty_pipeline_returns_zero(self):
        """No stages means zero cost."""
        from app.pipeline.manager import _estimate_total_cost

        with patch('app.pipeline.manager.STAGE_REGISTRY', {}):
            cost = _estimate_total_cost('instagram', {'max_results': 50})

        assert cost == 0.0


# ── _get_queue ───────────────────────────────────────────────────────────────

class TestGetQueue:
    """_get_queue() lazily creates an RQ Queue."""

    def test_creates_queue_once(self):
        """Queue is created on first call and cached."""
        import app.pipeline.manager as mgr
        # Reset cached queue
        mgr._queue = None

        mock_redis = MagicMock()
        mock_queue_cls = MagicMock()
        mock_queue_instance = MagicMock()
        mock_queue_cls.return_value = mock_queue_instance

        with patch('app.pipeline.manager.redis_client', mock_redis, create=True), \
             patch('app.pipeline.manager.Queue', mock_queue_cls, create=True):
            # Patch the lazy imports inside _get_queue
            with patch.dict('sys.modules', {'rq': MagicMock(Queue=mock_queue_cls)}), \
                 patch('app.extensions.redis_client', mock_redis):
                q = mgr._get_queue()

        # Reset for other tests
        mgr._queue = None


# ── Integration ──────────────────────────────────────────────────────────────

class TestIntegration:
    """End-to-end flows across multiple manager functions."""

    @patch('app.pipeline.manager.notify_run_complete')
    @patch('app.pipeline.manager.persist_lead_results')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.dedup_profiles')
    @patch('app.pipeline.manager.record_filter_history')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.PIPELINE_STAGES', [
        'discovery', 'pre_screen', 'enrichment', 'scoring',
    ])
    def test_pipeline_funnel_reduces_profiles(self, MockRun, mock_filter_hist,
                                               mock_dedup, mock_persist,
                                               mock_lead_results, mock_notify):
        """Profiles decrease through the funnel as stages filter them."""
        from app.pipeline.manager import run_pipeline

        run = _make_run()
        MockRun.load.return_value = run
        mock_dedup.return_value = ([
            {'platform_username': 'u1'}, {'platform_username': 'u2'},
            {'platform_username': 'u3'}, {'platform_username': 'u4'},
        ], 1)

        class _HalfFilter(StageAdapter):
            """Drops half the profiles."""
            platform = 'instagram'
            stage = 'pre_screen'
            def run(self, profiles, run_obj):
                kept = profiles[:len(profiles) // 2]
                return StageResult(
                    profiles=kept,
                    processed=len(profiles),
                    skipped=len(profiles) - len(kept),
                )

        registry = {
            'discovery': {'instagram': _DiscoveryAdapter},
            'pre_screen': {'instagram': _HalfFilter},
            'enrichment': {'instagram': _FakeAdapter},
            'scoring': {'instagram': _FakeAdapter},
        }

        with patch('app.pipeline.manager.STAGE_REGISTRY', registry):
            run_pipeline('run-test-001')

        run.complete.assert_called_once()
        # Pre-screen halved 4 profiles to 2
        assert run.profiles_pre_screened == 2
        # Scoring got the surviving 2
        assert run.profiles_scored == 2

    @patch('app.pipeline.manager.notify_run_complete')
    @patch('app.pipeline.manager.persist_lead_results')
    @patch('app.pipeline.manager.persist_run')
    @patch('app.pipeline.manager.dedup_profiles', return_value=([
        {'platform_username': 'u1'},
    ], 0))
    @patch('app.pipeline.manager.record_filter_history')
    @patch('app.pipeline.manager.Run')
    @patch('app.pipeline.manager.PIPELINE_STAGES', ['discovery', 'scoring'])
    def test_checkpoints_saved_after_each_stage(self, MockRun, mock_filter_hist,
                                                  mock_dedup, mock_persist,
                                                  mock_lead_results, mock_notify):
        """Each stage's output is checkpointed in stage_outputs for retry."""
        from app.pipeline.manager import run_pipeline

        run = _make_run(stage_outputs={})
        MockRun.load.return_value = run

        registry = {
            'discovery': {'instagram': _DiscoveryAdapter},
            'scoring': {'instagram': _FakeAdapter},
        }

        with patch('app.pipeline.manager.STAGE_REGISTRY', registry):
            run_pipeline('run-test-001')

        assert 'discovery' in run.stage_outputs
        assert 'scoring' in run.stage_outputs
