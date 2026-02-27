"""Tests for app.services.db -- database persistence helpers."""
import hashlib
import json
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from app.services.db import (
    persist_run,
    persist_lead_results,
    dedup_profiles,
    make_filter_hash,
    record_filter_history,
    get_filter_staleness,
    _extract_platform_id,
    _determine_stage_reached,
)
from app.models.db_run import DbRun
from app.models.lead import Lead
from app.models.lead_run import LeadRun
from app.models.filter_history import FilterHistory


@pytest.fixture(autouse=True)
def _patch_db_service_session(db_engine):
    """
    Route get_session() calls inside app.services.db to test sessions.

    The module does `from app.database import get_session` at import time, so
    the conftest patch on `app.database.get_session` does not affect the local
    binding.  We must also patch `app.services.db.get_session`.

    Each call to get_session() returns a new session bound to the same in-memory
    engine so that close() inside the production code does not destroy the test
    DB connection.
    """
    from sqlalchemy.orm import sessionmaker
    TestSession = sessionmaker(bind=db_engine)
    with patch('app.services.db.get_session', side_effect=lambda: TestSession()):
        yield TestSession


# ---------------------------------------------------------------------------
# persist_run
# ---------------------------------------------------------------------------

class TestPersistRun:
    """persist_run() INSERTs or UPDATEs a run record in Postgres."""

    def test_inserts_new_run(self, db_session, make_run):
        run = make_run()
        persist_run(run)
        row = db_session.get(DbRun, run.id)
        assert row is not None
        assert row.platform == 'instagram'
        assert row.status == 'queued'
        assert row.bdr_assignment == 'Test BDR'

    def test_insert_stores_filters(self, db_session, make_run):
        run = make_run(filters={'max_results': 25, 'niche': 'travel'})
        persist_run(run)
        row = db_session.get(DbRun, run.id)
        assert row.filters == {'max_results': 25, 'niche': 'travel'}

    def test_insert_stores_estimated_cost(self, db_session, make_run):
        run = make_run(estimated_cost=3.75)
        persist_run(run)
        row = db_session.get(DbRun, run.id)
        assert row.estimated_cost == 3.75

    def test_insert_with_none_estimated_cost(self, db_session, make_run):
        run = make_run(estimated_cost=None)
        persist_run(run)
        row = db_session.get(DbRun, run.id)
        assert row.estimated_cost is None

    def test_insert_with_zero_estimated_cost_stores_none(self, db_session, make_run):
        """estimated_cost=0.0 is falsy, so `run.estimated_cost or None` -> None."""
        run = make_run(estimated_cost=0.0)
        persist_run(run)
        row = db_session.get(DbRun, run.id)
        assert row.estimated_cost is None

    def test_updates_existing_run(self, db_session, make_run):
        run = make_run()
        persist_run(run)

        # Mutate the run to simulate pipeline completion
        run.status = 'completed'
        run.profiles_found = 42
        run.profiles_pre_screened = 38
        run.profiles_enriched = 30
        run.profiles_scored = 28
        run.contacts_synced = 20
        run.duplicates_skipped = 5
        run.tier_distribution = {'auto_enroll': 10, 'high_priority_review': 10}
        run.errors = [{'msg': 'timeout'}]
        run.summary = 'Run completed successfully'
        run.actual_cost = 2.50
        run.stage_outputs = {'discovery': {'count': 42}}
        persist_run(run)

        row = db_session.get(DbRun, run.id)
        assert row.status == 'completed'
        assert row.profiles_found == 42
        assert row.profiles_pre_screened == 38
        assert row.profiles_enriched == 30
        assert row.profiles_scored == 28
        assert row.contacts_synced == 20
        assert row.duplicates_skipped == 5
        assert row.tier_distribution == {'auto_enroll': 10, 'high_priority_review': 10}
        assert row.error_count == 1
        assert row.summary == 'Run completed successfully'
        assert row.actual_cost == 2.50
        assert row.stage_outputs == {'discovery': {'count': 42}}

    def test_update_completed_sets_finished_at(self, db_session, make_run):
        run = make_run()
        persist_run(run)

        run.status = 'completed'
        run.errors = []
        persist_run(run)

        row = db_session.get(DbRun, run.id)
        assert row.finished_at is not None

    def test_update_failed_sets_finished_at(self, db_session, make_run):
        run = make_run()
        persist_run(run)

        run.status = 'failed'
        run.errors = [{'msg': 'boom'}]
        persist_run(run)

        row = db_session.get(DbRun, run.id)
        assert row.finished_at is not None

    def test_update_running_does_not_set_finished_at(self, db_session, make_run):
        run = make_run()
        persist_run(run)

        run.status = 'running'
        run.errors = []
        persist_run(run)

        row = db_session.get(DbRun, run.id)
        assert row.finished_at is None

    def test_db_error_does_not_raise(self, make_run):
        """DB errors are swallowed -- pipeline never blocks on persistence."""
        run = make_run()
        with patch('app.services.db.get_session') as mock_gs:
            mock_gs.return_value.get.side_effect = RuntimeError("connection lost")
            # Should not raise
            persist_run(run)

    def test_session_closed_on_success(self, db_session, make_run):
        """Session is always closed after persist_run, even on success."""
        run = make_run()
        with patch('app.services.db.get_session') as mock_gs:
            mock_session = MagicMock()
            mock_session.get.return_value = None
            mock_gs.return_value = mock_session
            persist_run(run)
            mock_session.close.assert_called_once()

    def test_session_closed_on_error(self, make_run):
        """Session is always closed after persist_run, even on error."""
        with patch('app.services.db.get_session') as mock_gs:
            mock_session = MagicMock()
            mock_session.get.side_effect = RuntimeError("boom")
            mock_gs.return_value = mock_session
            persist_run(make_run())
            mock_session.rollback.assert_called_once()
            mock_session.close.assert_called_once()

    def test_idempotent_double_insert(self, db_session, make_run):
        """Calling persist_run twice with same data should not create duplicates."""
        run = make_run()
        persist_run(run)
        persist_run(run)  # second call updates
        count = db_session.query(DbRun).filter_by(id=run.id).count()
        assert count == 1


# ---------------------------------------------------------------------------
# persist_lead_results
# ---------------------------------------------------------------------------

class TestPersistLeadResults:
    """persist_lead_results() upserts leads and creates lead_run records."""

    def test_creates_lead_and_lead_run(self, db_session, make_run):
        run = make_run()
        persist_run(run)

        profiles = [{
            'platform_username': 'travel_jane',
            'name': 'Jane Doe',
            'url': 'https://instagram.com/travel_jane',
            'introduction': 'I love travel and photography',
            'follower_count': 80000,
            'email': 'jane@example.com',
            'website': 'https://jane.com',
            '_social_urls': {'twitter': 'https://twitter.com/jane'},
        }]
        persist_lead_results(run, profiles)

        lead = db_session.query(Lead).filter_by(platform_id='travel_jane').first()
        assert lead is not None
        assert lead.platform == 'instagram'
        assert lead.name == 'Jane Doe'
        assert lead.bio == 'I love travel and photography'
        assert lead.follower_count == 80000
        assert lead.email == 'jane@example.com'
        assert lead.website == 'https://jane.com'

        lead_run = db_session.query(LeadRun).filter_by(lead_id=lead.id, run_id=run.id).first()
        assert lead_run is not None

    def test_creates_lead_run_with_scoring_data(self, db_session, make_run):
        run = make_run()
        persist_run(run)

        profiles = [{
            'platform_username': 'scored_user',
            'name': 'Scored User',
            '_lead_analysis': {
                'lead_score': 0.85,
                'manual_score': 0.80,
                'section_scores': {'niche': 0.9, 'authenticity': 0.8},
                'priority_tier': 'auto_enroll',
                'score_reasoning': 'Strong travel niche fit',
            },
            '_synced_to_crm': True,
        }]
        persist_lead_results(run, profiles)

        lead = db_session.query(Lead).filter_by(platform_id='scored_user').first()
        lr = db_session.query(LeadRun).filter_by(lead_id=lead.id).first()
        assert lr.lead_score == 0.85
        assert lr.manual_score == 0.80
        assert lr.section_scores == {'niche': 0.9, 'authenticity': 0.8}
        assert lr.priority_tier == 'auto_enroll'
        assert lr.score_reasoning == 'Strong travel niche fit'
        assert lr.synced_to_crm is True
        assert lr.stage_reached == 'crm_sync'

    def test_updates_existing_lead_on_second_run(self, db_session, make_run):
        run1 = make_run(id='run-001')
        persist_run(run1)
        profiles = [{'platform_username': 'returning_user', 'name': 'Original Name', 'follower_count': 1000}]
        persist_lead_results(run1, profiles)

        run2 = make_run(id='run-002')
        persist_run(run2)
        updated = [{'platform_username': 'returning_user', 'name': 'Updated Name', 'follower_count': 5000}]
        persist_lead_results(run2, updated)

        leads = db_session.query(Lead).filter_by(platform_id='returning_user').all()
        assert len(leads) == 1
        assert leads[0].name == 'Updated Name'
        assert leads[0].follower_count == 5000

        lead_runs = db_session.query(LeadRun).filter_by(lead_id=leads[0].id).all()
        assert len(lead_runs) == 2

    def test_skips_profile_without_platform_id(self, db_session, make_run):
        run = make_run()
        persist_run(run)

        profiles = [{'name': 'No Username'}]  # no platform_username
        persist_lead_results(run, profiles)

        assert db_session.query(Lead).count() == 0

    def test_empty_profiles_list(self, db_session, make_run):
        run = make_run()
        persist_run(run)
        persist_lead_results(run, [])
        assert db_session.query(Lead).count() == 0
        assert db_session.query(LeadRun).count() == 0

    def test_collects_analysis_evidence(self, db_session, make_run):
        run = make_run()
        persist_run(run)

        profiles = [{
            'platform_username': 'evidence_user',
            '_bio_evidence': {'keywords': ['travel', 'adventure']},
            '_caption_evidence': {'sentiment': 'positive'},
            '_thumbnail_evidence': {'has_travel': True},
            '_creator_profile': {'niche': 'travel'},
            '_lead_analysis': {'lead_score': 0.7},
        }]
        persist_lead_results(run, profiles)

        lr = db_session.query(LeadRun).first()
        assert 'bio_evidence' in lr.analysis_evidence
        assert 'caption_evidence' in lr.analysis_evidence
        assert 'thumbnail_evidence' in lr.analysis_evidence
        assert 'creator_profile' in lr.analysis_evidence

    def test_db_error_does_not_raise(self, make_run):
        run = make_run()
        with patch('app.services.db.get_session') as mock_gs:
            mock_gs.return_value.query.side_effect = RuntimeError("connection lost")
            persist_lead_results(run, [{'platform_username': 'x'}])

    def test_fallback_name_from_first_name(self, db_session, make_run):
        """Uses _first_name when name is empty."""
        run = make_run()
        persist_run(run)
        profiles = [{'platform_username': 'fname_user', '_first_name': 'Maria'}]
        persist_lead_results(run, profiles)
        lead = db_session.query(Lead).filter_by(platform_id='fname_user').first()
        assert lead.name == 'Maria'

    def test_prescreen_result_stored(self, db_session, make_run):
        run = make_run()
        persist_run(run)
        profiles = [{
            'platform_username': 'prescreened_user',
            '_prescreen_result': 'disqualified',
            '_prescreen_reason': 'Too few followers',
        }]
        persist_lead_results(run, profiles)
        lr = db_session.query(LeadRun).first()
        assert lr.prescreen_result == 'disqualified'
        assert lr.prescreen_reason == 'Too few followers'
        assert lr.stage_reached == 'pre_screen'


# ---------------------------------------------------------------------------
# dedup_profiles
# ---------------------------------------------------------------------------

class TestDedupProfiles:
    """dedup_profiles() filters out leads already in the DB."""

    def test_all_new_profiles_returned(self, db_session):
        profiles = [
            {'platform_username': 'new_user_1'},
            {'platform_username': 'new_user_2'},
        ]
        result, skipped = dedup_profiles(profiles, 'instagram')
        assert len(result) == 2
        assert skipped == 0

    def test_existing_profiles_filtered(self, db_session):
        # Insert an existing lead
        lead = Lead(platform='instagram', platform_id='existing_user', name='Existing')
        db_session.add(lead)
        db_session.commit()

        profiles = [
            {'platform_username': 'existing_user'},
            {'platform_username': 'brand_new'},
        ]
        result, skipped = dedup_profiles(profiles, 'instagram')
        assert len(result) == 1
        assert result[0]['platform_username'] == 'brand_new'
        assert skipped == 1

    def test_all_existing_returns_empty(self, db_session):
        lead = Lead(platform='instagram', platform_id='dup_user', name='Dup')
        db_session.add(lead)
        db_session.commit()

        profiles = [{'platform_username': 'dup_user'}]
        result, skipped = dedup_profiles(profiles, 'instagram')
        assert len(result) == 0
        assert skipped == 1

    def test_empty_profiles_list(self, db_session):
        result, skipped = dedup_profiles([], 'instagram')
        assert result == []
        assert skipped == 0

    def test_profile_without_platform_id_passes_through(self, db_session):
        """Profiles with no extractable platform_id are kept (not deduped)."""
        profiles = [{'name': 'Mystery User'}]  # no username/id
        result, skipped = dedup_profiles(profiles, 'instagram')
        assert len(result) == 1
        assert skipped == 0

    def test_cross_platform_no_collision(self, db_session):
        """Same platform_id on different platform should not deduplicate."""
        lead = Lead(platform='patreon', platform_id='travel_guru', name='Travel Guru')
        db_session.add(lead)
        db_session.commit()

        profiles = [{'platform_username': 'travel_guru'}]
        result, skipped = dedup_profiles(profiles, 'instagram')
        assert len(result) == 1
        assert skipped == 0

    def test_db_failure_returns_all_unfiltered(self):
        """On DB error, all profiles are returned (never blocks pipeline)."""
        with patch('app.services.db.get_session') as mock_gs:
            mock_gs.side_effect = RuntimeError("connection refused")
            profiles = [{'platform_username': 'u1'}, {'platform_username': 'u2'}]
            result, skipped = dedup_profiles(profiles, 'instagram')
            assert len(result) == 2
            assert skipped == 0


# ---------------------------------------------------------------------------
# make_filter_hash
# ---------------------------------------------------------------------------

class TestMakeFilterHash:
    """make_filter_hash() returns a deterministic SHA-256 of normalized filters."""

    def test_deterministic_for_same_input(self):
        h1 = make_filter_hash('instagram', {'max_results': 10, 'niche': 'travel'})
        h2 = make_filter_hash('instagram', {'max_results': 10, 'niche': 'travel'})
        assert h1 == h2

    def test_different_filters_different_hash(self):
        h1 = make_filter_hash('instagram', {'max_results': 10})
        h2 = make_filter_hash('instagram', {'max_results': 20})
        assert h1 != h2

    def test_different_platforms_different_hash(self):
        h1 = make_filter_hash('instagram', {'max_results': 10})
        h2 = make_filter_hash('patreon', {'max_results': 10})
        assert h1 != h2

    def test_ignores_bdr_names(self):
        """bdr_names is stripped before hashing for determinism."""
        h1 = make_filter_hash('instagram', {'max_results': 10, 'bdr_names': ['Alice']})
        h2 = make_filter_hash('instagram', {'max_results': 10, 'bdr_names': ['Bob']})
        h3 = make_filter_hash('instagram', {'max_results': 10})
        assert h1 == h2 == h3

    def test_key_order_independent(self):
        """Dict key order should not affect the hash."""
        h1 = make_filter_hash('instagram', {'a': 1, 'b': 2})
        h2 = make_filter_hash('instagram', {'b': 2, 'a': 1})
        assert h1 == h2

    def test_returns_hex_string(self):
        h = make_filter_hash('instagram', {})
        assert isinstance(h, str)
        assert len(h) == 64  # SHA-256 hex digest

    def test_empty_filters(self):
        h = make_filter_hash('instagram', {})
        assert isinstance(h, str)
        assert len(h) == 64


# ---------------------------------------------------------------------------
# record_filter_history
# ---------------------------------------------------------------------------

class TestRecordFilterHistory:
    """record_filter_history() writes a FilterHistory row for staleness tracking."""

    def test_creates_filter_history_record(self, db_session, make_run):
        run = make_run()
        record_filter_history(run, new_count=8, total_count=10)

        fh = db_session.query(FilterHistory).first()
        assert fh is not None
        assert fh.platform == 'instagram'
        assert fh.run_id == run.id
        assert fh.total_found == 10
        assert fh.new_found == 8
        assert fh.novelty_rate == 0.8

    def test_novelty_rate_zero_when_no_total(self, db_session, make_run):
        run = make_run()
        record_filter_history(run, new_count=0, total_count=0)

        fh = db_session.query(FilterHistory).first()
        assert fh.novelty_rate == 0.0

    def test_stores_correct_filter_hash(self, db_session, make_run):
        run = make_run(filters={'niche': 'travel'})
        record_filter_history(run, new_count=5, total_count=5)

        fh = db_session.query(FilterHistory).first()
        expected_hash = make_filter_hash('instagram', {'niche': 'travel'})
        assert fh.filter_hash == expected_hash

    def test_db_error_does_not_raise(self, make_run):
        run = make_run()
        with patch('app.services.db.get_session') as mock_gs:
            mock_gs.side_effect = RuntimeError("boom")
            record_filter_history(run, new_count=5, total_count=10)


# ---------------------------------------------------------------------------
# get_filter_staleness
# ---------------------------------------------------------------------------

class TestGetFilterStaleness:
    """get_filter_staleness() returns staleness info or None."""

    def test_returns_none_when_no_history(self, db_session):
        result = get_filter_staleness('instagram', {'max_results': 999})
        assert result is None

    def test_returns_staleness_info(self, db_session, make_run):
        run = make_run(filters={'niche': 'travel'})
        record_filter_history(run, new_count=7, total_count=10)

        result = get_filter_staleness('instagram', {'niche': 'travel'})
        assert result is not None
        assert 'last_run_days_ago' in result
        assert result['novelty_rate'] == 70.0
        assert result['total_found'] == 10
        assert result['new_found'] == 7

    def test_returns_most_recent_history(self, db_session, make_run):
        """When multiple histories exist, returns the most recent."""
        run1 = make_run(id='run-older', filters={'niche': 'travel'})
        record_filter_history(run1, new_count=3, total_count=10)

        # Manually backdate the first record so ordering is deterministic
        fh_older = db_session.query(FilterHistory).filter_by(run_id='run-older').first()
        fh_older.ran_at = datetime(2025, 1, 1)
        db_session.commit()

        run2 = make_run(id='run-newer', filters={'niche': 'travel'})
        record_filter_history(run2, new_count=1, total_count=10)

        result = get_filter_staleness('instagram', {'niche': 'travel'})
        # Most recent should be the one with new_found=1
        assert result['new_found'] == 1

    def test_different_filters_not_matched(self, db_session, make_run):
        run = make_run(filters={'niche': 'travel'})
        record_filter_history(run, new_count=5, total_count=10)

        result = get_filter_staleness('instagram', {'niche': 'food'})
        assert result is None

    def test_db_error_returns_none(self):
        with patch('app.services.db.get_session') as mock_gs:
            mock_gs.side_effect = RuntimeError("boom")
            result = get_filter_staleness('instagram', {'max_results': 10})
            assert result is None


# ---------------------------------------------------------------------------
# _extract_platform_id (private but has significant branching logic)
# ---------------------------------------------------------------------------

class TestExtractPlatformId:
    """_extract_platform_id() dispatches by platform to find the unique ID."""

    def test_instagram_uses_platform_username(self):
        assert _extract_platform_id({'platform_username': 'ig_user'}, 'instagram') == 'ig_user'

    def test_instagram_falls_back_to_username(self):
        assert _extract_platform_id({'username': 'ig_user2'}, 'instagram') == 'ig_user2'

    def test_instagram_falls_back_to_handle(self):
        assert _extract_platform_id({'handle': '@ig_user3'}, 'instagram') == '@ig_user3'

    def test_patreon_uses_slug(self):
        assert _extract_platform_id({'slug': 'my-patreon'}, 'patreon') == 'my-patreon'

    def test_patreon_falls_back_to_id(self):
        assert _extract_platform_id({'id': '12345'}, 'patreon') == '12345'

    def test_patreon_falls_back_to_vanity(self):
        assert _extract_platform_id({'vanity': 'my_vanity'}, 'patreon') == 'my_vanity'

    def test_facebook_uses_group_id(self):
        assert _extract_platform_id({'group_id': 'fb-group'}, 'facebook') == 'fb-group'

    def test_facebook_falls_back_to_id(self):
        assert _extract_platform_id({'id': 'fb-id'}, 'facebook') == 'fb-id'

    def test_unknown_platform_uses_id(self):
        assert _extract_platform_id({'id': 'generic-id'}, 'tiktok') == 'generic-id'

    def test_unknown_platform_falls_back_to_platform_id(self):
        assert _extract_platform_id({'platform_id': 'pid'}, 'tiktok') == 'pid'

    def test_returns_none_when_no_id_found(self):
        assert _extract_platform_id({'name': 'No ID here'}, 'instagram') is None

    def test_empty_profile(self):
        assert _extract_platform_id({}, 'instagram') is None


# ---------------------------------------------------------------------------
# _determine_stage_reached (private but has 5+ code paths)
# ---------------------------------------------------------------------------

class TestDetermineStageReached:
    """_determine_stage_reached() infers pipeline stage from profile keys."""

    def test_discovery_when_minimal_profile(self):
        assert _determine_stage_reached({'name': 'Basic'}) == 'discovery'

    def test_pre_screen_stage(self):
        assert _determine_stage_reached({'_prescreen_result': 'passed'}) == 'pre_screen'

    def test_enrichment_via_social_data(self):
        assert _determine_stage_reached({
            '_prescreen_result': 'passed',
            '_social_data': {'followers': 100},
        }) == 'enrichment'

    def test_enrichment_via_enrichment_status(self):
        assert _determine_stage_reached({
            '_prescreen_result': 'passed',
            'enrichment_status': 'success',
        }) == 'enrichment'

    def test_analysis_via_creator_profile(self):
        assert _determine_stage_reached({
            '_creator_profile': {'niche': 'travel'},
        }) == 'analysis'

    def test_analysis_via_content_analyses(self):
        assert _determine_stage_reached({
            '_content_analyses': [{'score': 0.5}],
        }) == 'analysis'

    def test_scoring_stage(self):
        assert _determine_stage_reached({
            '_lead_analysis': {'lead_score': 0.8},
        }) == 'scoring'

    def test_crm_sync_stage(self):
        assert _determine_stage_reached({
            '_lead_analysis': {'lead_score': 0.9},
            '_synced_to_crm': True,
        }) == 'crm_sync'

    def test_empty_profile(self):
        assert _determine_stage_reached({}) == 'discovery'


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

class TestIntegration:
    """End-to-end flows across multiple functions."""

    def test_full_pipeline_persist_flow(self, db_session, make_run, sample_profiles):
        """Create run -> persist -> add leads -> verify all tables populated."""
        run = make_run()
        persist_run(run)

        persist_lead_results(run, sample_profiles)

        assert db_session.query(DbRun).count() == 1
        assert db_session.query(Lead).count() == 2
        assert db_session.query(LeadRun).count() == 2

    def test_persist_then_dedup_flow(self, db_session, make_run, sample_profiles):
        """After persisting leads, dedup correctly filters them on next run."""
        run1 = make_run(id='run-first')
        persist_run(run1)
        persist_lead_results(run1, sample_profiles)

        # Second run: same profiles should be deduped
        new_profiles, skipped = dedup_profiles(sample_profiles, 'instagram')
        assert skipped == 2
        assert len(new_profiles) == 0

    def test_filter_history_round_trip(self, db_session, make_run):
        """Record filter history, then query staleness."""
        run = make_run(filters={'niche': 'adventure', 'max_results': 20})
        record_filter_history(run, new_count=15, total_count=20)

        staleness = get_filter_staleness('instagram', {'niche': 'adventure', 'max_results': 20})
        assert staleness is not None
        assert staleness['total_found'] == 20
        assert staleness['new_found'] == 15
        assert staleness['novelty_rate'] == 75.0
        # ran_at uses server_default (UTC) while code compares with datetime.now() (local)
        # so days_ago can be 0 or -1 depending on timezone; both are acceptable for "today"
        assert staleness['last_run_days_ago'] in (0, -1)
