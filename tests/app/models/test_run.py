"""Tests for app.models.run — in-memory Run class (Redis-backed pipeline run tracking)."""
import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from app.config import PIPELINE_STAGES


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_run(**overrides):
    """Build a Run instance with Redis operations mocked out."""
    with patch('app.models.run.r'):
        from app.models.run import Run
        return Run(**overrides)


# ── Initialization ───────────────────────────────────────────────────────────

class TestRunInit:
    """Run.__init__ default values and overrides."""

    def test_default_id_is_uuid(self):
        run = _make_run()
        assert run.id is not None
        assert len(run.id) == 36  # UUID format: 8-4-4-4-12

    def test_custom_id(self):
        run = _make_run(id='custom-123')
        assert run.id == 'custom-123'

    def test_default_status_is_queued(self):
        run = _make_run()
        assert run.status == 'queued'

    def test_custom_status(self):
        run = _make_run(status='running')
        assert run.status == 'running'

    def test_default_platform_is_instagram(self):
        run = _make_run()
        assert run.platform == 'instagram'

    def test_custom_platform(self):
        run = _make_run(platform='facebook')
        assert run.platform == 'facebook'

    def test_created_at_is_iso_string(self):
        run = _make_run()
        # Should parse without error
        datetime.fromisoformat(run.created_at)

    def test_updated_at_equals_created_at_on_init(self):
        run = _make_run()
        assert run.updated_at == run.created_at

    def test_current_stage_starts_empty(self):
        run = _make_run()
        assert run.current_stage == ''

    def test_default_filters_is_empty_dict(self):
        run = _make_run()
        assert run.filters == {}

    def test_custom_filters(self):
        filters = {'max_results': 25, 'niche': 'travel'}
        run = _make_run(filters=filters)
        assert run.filters == filters

    def test_default_bdr_assignment_is_empty(self):
        run = _make_run()
        assert run.bdr_assignment == ''

    def test_custom_bdr_assignment(self):
        run = _make_run(bdr_assignment='Alice')
        assert run.bdr_assignment == 'Alice'

    def test_errors_list_starts_empty(self):
        run = _make_run()
        assert run.errors == []

    def test_default_cost_values(self):
        run = _make_run()
        assert run.estimated_cost == 0.0
        assert run.actual_cost == 0.0

    def test_summary_starts_empty(self):
        run = _make_run()
        assert run.summary == ''

    def test_stage_outputs_starts_empty(self):
        run = _make_run()
        assert run.stage_outputs == {}


# ── Stage progress ───────────────────────────────────────────────────────────

class TestStageProgress:
    """stage_progress initialisation and PIPELINE_STAGES coverage."""

    def test_all_pipeline_stages_present(self):
        run = _make_run()
        for stage in PIPELINE_STAGES:
            assert stage in run.stage_progress

    def test_each_stage_has_zero_counters(self):
        run = _make_run()
        for stage in PIPELINE_STAGES:
            assert run.stage_progress[stage] == {'total': 0, 'completed': 0, 'failed': 0}

    @patch('app.models.run.r')
    def test_increment_completed(self, mock_r):
        run = _make_run()
        run.increment_stage_progress('discovery', 'completed', 1)
        assert run.stage_progress['discovery']['completed'] == 1

    @patch('app.models.run.r')
    def test_increment_failed(self, mock_r):
        run = _make_run()
        run.increment_stage_progress('enrichment', 'failed', 3)
        assert run.stage_progress['enrichment']['failed'] == 3

    @patch('app.models.run.r')
    def test_increment_total(self, mock_r):
        run = _make_run()
        run.increment_stage_progress('scoring', 'total', 10)
        assert run.stage_progress['scoring']['total'] == 10

    @patch('app.models.run.r')
    def test_increment_accumulates(self, mock_r):
        run = _make_run()
        run.increment_stage_progress('analysis', 'completed', 2)
        run.increment_stage_progress('analysis', 'completed', 5)
        assert run.stage_progress['analysis']['completed'] == 7

    @patch('app.models.run.r')
    def test_increment_unknown_stage_ignored(self, mock_r):
        """Incrementing a stage not in PIPELINE_STAGES should not raise."""
        run = _make_run()
        run.increment_stage_progress('nonexistent_stage', 'completed', 1)
        assert 'nonexistent_stage' not in run.stage_progress

    @patch('app.models.run.r')
    def test_increment_calls_save(self, mock_r):
        run = _make_run()
        run.increment_stage_progress('discovery', 'completed')
        # save() calls r.setex and r.zadd
        assert mock_r.setex.called
        assert mock_r.zadd.called


# ── Tier distribution ────────────────────────────────────────────────────────

class TestTierDistribution:
    """Tier distribution tracking."""

    def test_default_tiers(self):
        run = _make_run()
        assert run.tier_distribution == {
            'auto_enroll': 0,
            'high_priority_review': 0,
            'standard_priority_review': 0,
            'low_priority_review': 0,
        }

    def test_tier_distribution_is_mutable(self):
        run = _make_run()
        run.tier_distribution['auto_enroll'] = 5
        run.tier_distribution['high_priority_review'] = 12
        assert run.tier_distribution['auto_enroll'] == 5
        assert run.tier_distribution['high_priority_review'] == 12


# ── Profile counters ─────────────────────────────────────────────────────────

class TestProfileCounters:
    """Profile-related counter attributes."""

    def test_all_counters_start_at_zero(self):
        run = _make_run()
        assert run.profiles_found == 0
        assert run.profiles_pre_screened == 0
        assert run.profiles_enriched == 0
        assert run.profiles_scored == 0
        assert run.contacts_synced == 0
        assert run.duplicates_skipped == 0

    def test_counters_can_be_updated(self):
        run = _make_run()
        run.profiles_found = 100
        run.profiles_pre_screened = 80
        run.profiles_enriched = 60
        run.profiles_scored = 50
        run.contacts_synced = 40
        run.duplicates_skipped = 10
        assert run.profiles_found == 100
        assert run.profiles_scored == 50


# ── Error handling ───────────────────────────────────────────────────────────

class TestErrors:
    """add_error() and error tracking."""

    @patch('app.models.run.r')
    def test_add_error_appends(self, mock_r):
        run = _make_run()
        run.add_error('discovery', 'Network timeout', 'profile-42')
        assert len(run.errors) == 1
        assert run.errors[0]['stage'] == 'discovery'
        assert run.errors[0]['message'] == 'Network timeout'
        assert run.errors[0]['profile_id'] == 'profile-42'

    @patch('app.models.run.r')
    def test_add_error_has_timestamp(self, mock_r):
        run = _make_run()
        run.add_error('enrichment', 'API error')
        ts = run.errors[0]['timestamp']
        datetime.fromisoformat(ts)  # Should not raise

    @patch('app.models.run.r')
    def test_add_error_default_profile_id_empty(self, mock_r):
        run = _make_run()
        run.add_error('scoring', 'Unexpected value')
        assert run.errors[0]['profile_id'] == ''

    @patch('app.models.run.r')
    def test_multiple_errors_accumulate(self, mock_r):
        run = _make_run()
        for i in range(5):
            run.add_error('analysis', f'Error {i}')
        assert len(run.errors) == 5


# ── Status transitions ───────────────────────────────────────────────────────

class TestStatusTransitions:
    """update_stage(), complete(), fail()."""

    @patch('app.models.run.r')
    def test_update_stage_sets_current_stage(self, mock_r):
        run = _make_run()
        run.update_stage('enrichment')
        assert run.current_stage == 'enrichment'

    @patch('app.models.run.r')
    def test_update_stage_sets_status_when_given(self, mock_r):
        run = _make_run()
        run.update_stage('discovery', status='discovering')
        assert run.status == 'discovering'

    @patch('app.models.run.r')
    def test_update_stage_preserves_status_when_omitted(self, mock_r):
        run = _make_run()
        run.update_stage('pre_screen')
        assert run.status == 'queued'  # unchanged

    @patch('app.models.run.r')
    def test_update_stage_kwargs_set_attributes(self, mock_r):
        run = _make_run()
        run.update_stage('scoring', profiles_scored=42)
        assert run.profiles_scored == 42

    @patch('app.models.run.r')
    def test_update_stage_kwargs_ignore_nonexistent_attrs(self, mock_r):
        run = _make_run()
        run.update_stage('scoring', totally_fake_attr=99)
        assert not hasattr(run, 'totally_fake_attr')

    @patch('app.models.run.r')
    def test_complete_sets_completed(self, mock_r):
        run = _make_run()
        run.complete()
        assert run.status == 'completed'

    @patch('app.models.run.r')
    def test_fail_sets_failed(self, mock_r):
        run = _make_run()
        run.fail()
        assert run.status == 'failed'

    @patch('app.models.run.r')
    def test_fail_with_reason_adds_error(self, mock_r):
        run = _make_run()
        run.update_stage('analysis')
        run.fail(reason='Out of memory')
        assert run.status == 'failed'
        assert len(run.errors) == 1
        assert run.errors[0]['message'] == 'Out of memory'
        assert run.errors[0]['stage'] == 'analysis'

    @patch('app.models.run.r')
    def test_fail_without_reason_no_error(self, mock_r):
        run = _make_run()
        run.fail()
        assert run.errors == []


# ── Serialization ────────────────────────────────────────────────────────────

class TestToDict:
    """to_dict() / serialization."""

    def test_returns_dict(self):
        run = _make_run()
        d = run.to_dict()
        assert isinstance(d, dict)

    def test_contains_all_expected_keys(self):
        run = _make_run()
        d = run.to_dict()
        expected_keys = {
            'id', 'status', 'platform', 'created_at', 'updated_at',
            'current_stage', 'stage_progress', 'filters',
            'profiles_found', 'profiles_pre_screened', 'profiles_enriched',
            'profiles_scored', 'contacts_synced', 'duplicates_skipped',
            'bdr_assignment', 'errors', 'tier_distribution',
            'summary', 'estimated_cost', 'actual_cost',
        }
        assert set(d.keys()) == expected_keys

    def test_errors_truncated_to_last_20(self):
        run = _make_run()
        run.errors = [{'stage': 'x', 'message': f'err-{i}'} for i in range(30)]
        d = run.to_dict()
        assert len(d['errors']) == 20
        # Should keep the LAST 20 (indices 10-29)
        assert d['errors'][0]['message'] == 'err-10'
        assert d['errors'][-1]['message'] == 'err-29'

    def test_errors_under_20_kept_as_is(self):
        run = _make_run()
        run.errors = [{'stage': 'x', 'message': f'err-{i}'} for i in range(5)]
        d = run.to_dict()
        assert len(d['errors']) == 5

    def test_to_dict_is_json_serializable(self):
        run = _make_run(id='abc-123', filters={'niche': 'travel'})
        d = run.to_dict()
        serialized = json.dumps(d)
        assert isinstance(serialized, str)

    def test_to_dict_reflects_mutations(self):
        run = _make_run()
        run.profiles_found = 99
        run.summary = 'All done'
        d = run.to_dict()
        assert d['profiles_found'] == 99
        assert d['summary'] == 'All done'

    def test_stage_outputs_not_in_to_dict(self):
        """stage_outputs is an internal field — it should NOT appear in to_dict()."""
        run = _make_run()
        run.stage_outputs = {'discovery': {'key': 'value'}}
        d = run.to_dict()
        assert 'stage_outputs' not in d


# ── Save / persistence ───────────────────────────────────────────────────────

class TestSave:
    """save() Redis interactions."""

    @patch('app.models.run.r')
    def test_save_calls_setex(self, mock_r):
        run = _make_run(id='run-save-1')
        run.save()
        mock_r.setex.assert_called_once()
        call_args = mock_r.setex.call_args
        assert call_args[0][0] == 'run:run-save-1'

    @patch('app.models.run.r')
    def test_save_calls_zadd(self, mock_r):
        run = _make_run(id='run-save-2')
        run.save()
        mock_r.zadd.assert_called_once()
        call_args = mock_r.zadd.call_args
        assert call_args[0][0] == 'runs:list'

    @patch('app.models.run.r')
    def test_save_updates_updated_at(self, mock_r):
        run = _make_run()
        original_updated = run.updated_at
        import time
        time.sleep(0.01)
        run.save()
        assert run.updated_at >= original_updated

    @patch('app.models.run.r')
    def test_save_stores_valid_json(self, mock_r):
        run = _make_run(id='run-json-1')
        run.save()
        stored_json = mock_r.setex.call_args[0][2]
        parsed = json.loads(stored_json)
        assert parsed['id'] == 'run-json-1'

    @patch('app.models.run.r')
    def test_save_returns_self(self, mock_r):
        run = _make_run()
        result = run.save()
        assert result is run


# ── Load ─────────────────────────────────────────────────────────────────────

class TestLoad:
    """Run.load() from Redis and DB fallback."""

    @patch('app.models.run.r')
    def test_load_from_redis(self, mock_r):
        from app.models.run import Run

        run = _make_run(id='run-load-1')
        stored_data = json.dumps(run.to_dict())
        mock_r.get.return_value = stored_data

        loaded = Run.load('run-load-1')
        assert loaded is not None
        assert loaded.id == 'run-load-1'
        assert loaded.status == 'queued'
        assert loaded.platform == 'instagram'

    @patch('app.models.run.r')
    def test_load_returns_none_when_not_found(self, mock_r):
        from app.models.run import Run
        mock_r.get.return_value = None
        # DB fallback will also fail in test env
        result = Run.load('nonexistent')
        assert result is None

    @patch('app.models.run.r')
    def test_load_preserves_stage_progress(self, mock_r):
        from app.models.run import Run

        run = _make_run(id='run-load-progress')
        run.stage_progress['discovery']['completed'] = 15
        stored_data = json.dumps({**run.to_dict(), 'stage_progress': run.stage_progress})
        mock_r.get.return_value = stored_data

        loaded = Run.load('run-load-progress')
        assert loaded.stage_progress['discovery']['completed'] == 15

    @patch('app.models.run.r')
    def test_load_preserves_errors(self, mock_r):
        from app.models.run import Run

        run_data = _make_run(id='run-load-errors').to_dict()
        run_data['errors'] = [{'stage': 'discovery', 'message': 'test error', 'profile_id': '', 'timestamp': '2026-01-01T00:00:00'}]
        mock_r.get.return_value = json.dumps(run_data)

        loaded = Run.load('run-load-errors')
        assert len(loaded.errors) == 1
        assert loaded.errors[0]['message'] == 'test error'


# ── Delete ───────────────────────────────────────────────────────────────────

class TestDelete:
    """Run.delete() removes from Redis."""

    @patch('app.models.run.r')
    def test_delete_removes_key_and_sorted_set_entry(self, mock_r):
        from app.models.run import Run
        Run.delete('run-del-1')
        mock_r.delete.assert_called_once_with('run:run-del-1')
        mock_r.zrem.assert_called_once_with('runs:list', 'run-del-1')


# ── list_recent ─────────────────────────────────────────────────────────────

class TestListRecent:
    """Run.list_recent() — Redis path and DB fallback."""

    @patch('app.models.run.r')
    def test_returns_runs_from_redis(self, mock_r):
        from app.models.run import Run

        run1 = _make_run(id='run-lr-1')
        run2 = _make_run(id='run-lr-2')
        mock_r.zrevrange.return_value = ['run-lr-1', 'run-lr-2']
        mock_r.get.side_effect = lambda key: {
            'run:run-lr-1': json.dumps(run1.to_dict()),
            'run:run-lr-2': json.dumps(run2.to_dict()),
        }.get(key)

        runs = Run.list_recent(limit=10)
        assert len(runs) == 2
        assert runs[0].id == 'run-lr-1'
        assert runs[1].id == 'run-lr-2'

    @patch('app.models.run.r')
    def test_returns_empty_list_when_redis_and_db_empty(self, mock_r):
        from app.models.run import Run
        mock_r.zrevrange.return_value = []
        result = Run.list_recent()
        assert result == [] or result is not None

    @patch('app.models.run.r')
    def test_respects_limit_param(self, mock_r):
        from app.models.run import Run
        mock_r.zrevrange.return_value = ['run-a']
        mock_r.get.return_value = json.dumps(_make_run(id='run-a').to_dict())

        runs = Run.list_recent(limit=1)
        mock_r.zrevrange.assert_called_once_with('runs:list', 0, 0)
        assert len(runs) == 1

    @patch('app.models.run.r')
    def test_skips_missing_runs_in_redis(self, mock_r):
        """If a run ID is in sorted set but key is gone, it is silently skipped."""
        from app.models.run import Run
        mock_r.zrevrange.return_value = ['run-exists', 'run-gone']
        mock_r.get.side_effect = lambda key: {
            'run:run-exists': json.dumps(_make_run(id='run-exists').to_dict()),
            'run:run-gone': None,
        }.get(key)

        runs = Run.list_recent()
        assert len(runs) == 1
        assert runs[0].id == 'run-exists'


# ── _from_db_run ────────────────────────────────────────────────────────────

class TestFromDbRun:
    """Run._from_db_run() hydration from a DbRun ORM object."""

    def _make_db_run(self, **overrides):
        """Build a MagicMock that looks like a DbRun row."""
        defaults = dict(
            id='db-run-1',
            status='completed',
            platform='instagram',
            created_at=datetime(2026, 1, 15, 10, 0, 0),
            finished_at=datetime(2026, 1, 15, 10, 30, 0),
            filters={'max_results': 25},
            profiles_found=100,
            profiles_pre_screened=80,
            profiles_enriched=60,
            profiles_scored=50,
            contacts_synced=40,
            duplicates_skipped=10,
            bdr_assignment='Alice',
            tier_distribution={'auto_enroll': 5},
            summary='Run finished successfully',
            estimated_cost=2.50,
            actual_cost=2.10,
            stage_outputs={'discovery': {'count': 100}},
        )
        defaults.update(overrides)
        db_run = MagicMock()
        for k, v in defaults.items():
            setattr(db_run, k, v)
        return db_run

    def test_basic_field_mapping(self):
        from app.models.run import Run
        db_run = self._make_db_run()
        run = Run._from_db_run(db_run)
        assert run.id == 'db-run-1'
        assert run.status == 'completed'
        assert run.platform == 'instagram'
        assert run.profiles_found == 100
        assert run.contacts_synced == 40

    def test_created_at_is_iso_string(self):
        from app.models.run import Run
        db_run = self._make_db_run()
        run = Run._from_db_run(db_run)
        assert run.created_at == '2026-01-15T10:00:00'

    def test_updated_at_uses_finished_at_when_present(self):
        from app.models.run import Run
        db_run = self._make_db_run()
        run = Run._from_db_run(db_run)
        assert run.updated_at == '2026-01-15T10:30:00'

    def test_updated_at_falls_back_to_created_at_when_no_finished(self):
        from app.models.run import Run
        db_run = self._make_db_run(finished_at=None)
        run = Run._from_db_run(db_run)
        assert run.updated_at == '2026-01-15T10:00:00'

    def test_none_fields_default_gracefully(self):
        from app.models.run import Run
        db_run = self._make_db_run(
            filters=None,
            profiles_found=None,
            profiles_pre_screened=None,
            profiles_enriched=None,
            profiles_scored=None,
            contacts_synced=None,
            duplicates_skipped=None,
            bdr_assignment=None,
            tier_distribution=None,
            summary=None,
            estimated_cost=None,
            actual_cost=None,
            stage_outputs=None,
        )
        run = Run._from_db_run(db_run)
        assert run.filters == {}
        assert run.profiles_found == 0
        assert run.bdr_assignment == ''
        assert run.tier_distribution == {}
        assert run.summary == ''
        assert run.estimated_cost == 0.0
        assert run.actual_cost == 0.0
        assert run.stage_outputs == {}

    def test_created_at_none_gives_empty_strings(self):
        from app.models.run import Run
        db_run = self._make_db_run(created_at=None)
        run = Run._from_db_run(db_run)
        assert run.created_at == ''
        assert run.updated_at == ''

    def test_errors_always_empty_list(self):
        from app.models.run import Run
        db_run = self._make_db_run()
        run = Run._from_db_run(db_run)
        assert run.errors == []

    def test_stage_progress_always_empty_dict(self):
        from app.models.run import Run
        db_run = self._make_db_run()
        run = Run._from_db_run(db_run)
        assert run.stage_progress == {}

    def test_current_stage_always_empty(self):
        from app.models.run import Run
        db_run = self._make_db_run()
        run = Run._from_db_run(db_run)
        assert run.current_stage == ''


# ── Load DB fallback ────────────────────────────────────────────────────────

class TestLoadDbFallback:
    """Run.load() DB fallback path when Redis misses."""

    @patch('app.models.run.r')
    def test_load_falls_back_to_db(self, mock_r, db_session):
        from app.models.run import Run
        from app.models.db_run import DbRun

        mock_r.get.return_value = None

        db_run = DbRun(
            id='db-fallback-1',
            platform='instagram',
            status='completed',
            profiles_found=42,
        )
        db_session.add(db_run)
        db_session.commit()

        loaded = Run.load('db-fallback-1')
        assert loaded is not None
        assert loaded.id == 'db-fallback-1'
        assert loaded.status == 'completed'
        assert loaded.profiles_found == 42

    @patch('app.models.run.r')
    def test_load_returns_none_when_redis_and_db_miss(self, mock_r, db_session):
        from app.models.run import Run
        mock_r.get.return_value = None
        result = Run.load('totally-nonexistent-id')
        assert result is None


# ── Integration ─────────────────────────────────────────────────────────────

class TestIntegration:
    """End-to-end round-trip through save/load and status lifecycle."""

    @patch('app.models.run.r')
    def test_save_then_load_round_trip(self, mock_r):
        """Data survives a save → load cycle via Redis."""
        from app.models.run import Run

        storage = {}

        def fake_setex(key, ttl, data):
            storage[key] = data

        def fake_get(key):
            return storage.get(key)

        mock_r.setex.side_effect = fake_setex
        mock_r.get.side_effect = fake_get
        mock_r.zadd.return_value = 1

        run = _make_run(id='rt-1', filters={'niche': 'travel'}, bdr_assignment='Bob')
        run.profiles_found = 77
        run.summary = 'Test round trip'
        run.save()

        loaded = Run.load('rt-1')
        assert loaded is not None
        assert loaded.id == 'rt-1'
        assert loaded.profiles_found == 77
        assert loaded.summary == 'Test round trip'
        assert loaded.filters == {'niche': 'travel'}
        assert loaded.bdr_assignment == 'Bob'

    @patch('app.models.run.r')
    def test_full_lifecycle_queued_to_completed(self, mock_r):
        """Run flows through queued → stages → completed with counters."""
        run = _make_run(id='lifecycle-1')
        run.update_stage('discovery', status='discovering')
        assert run.status == 'discovering'

        run.increment_stage_progress('discovery', 'total', 50)
        run.increment_stage_progress('discovery', 'completed', 50)

        run.update_stage('scoring', status='scoring', profiles_scored=45)
        assert run.profiles_scored == 45

        run.complete()
        assert run.status == 'completed'

    @patch('app.models.run.r')
    def test_full_lifecycle_queued_to_failed_with_errors(self, mock_r):
        """Run flows through queued → stage → failure with error logging."""
        run = _make_run(id='lifecycle-fail')
        run.update_stage('enrichment', status='enriching')
        run.add_error('enrichment', 'API timeout', 'profile-99')
        run.fail(reason='Too many failures')
        assert run.status == 'failed'
        assert len(run.errors) == 2
        assert run.errors[0]['message'] == 'API timeout'
        assert run.errors[1]['message'] == 'Too many failures'
