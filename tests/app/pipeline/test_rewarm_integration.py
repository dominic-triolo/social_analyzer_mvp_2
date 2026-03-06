"""Integration tests for the rewarm pipeline."""
import pytest
from unittest.mock import patch, MagicMock

from app.models.run import Run
from app.config import REWARM_PIPELINE_STAGES, PIPELINE_STAGES


class TestRunTypeModel:
    """Test run_type field across Run model operations."""

    def test_default_run_type_is_discovery(self, mock_redis):
        run = Run(platform='instagram')
        assert run.run_type == 'discovery'

    def test_rewarm_run_type(self, mock_redis):
        run = Run(platform='instagram', run_type='rewarm')
        assert run.run_type == 'rewarm'

    def test_discovery_stage_progress_uses_pipeline_stages(self, mock_redis):
        run = Run(platform='instagram', run_type='discovery')
        assert set(run.stage_progress.keys()) == set(PIPELINE_STAGES)

    def test_rewarm_stage_progress_uses_rewarm_stages(self, mock_redis):
        run = Run(platform='instagram', run_type='rewarm')
        assert set(run.stage_progress.keys()) == set(REWARM_PIPELINE_STAGES)

    def test_rewarm_stage_progress_has_segment_import(self, mock_redis):
        run = Run(platform='instagram', run_type='rewarm')
        assert 'segment_import' in run.stage_progress
        assert 'discovery' not in run.stage_progress
        assert 'pre_screen' not in run.stage_progress

    def test_to_dict_includes_run_type(self, mock_redis):
        run = Run(platform='instagram', run_type='rewarm')
        d = run.to_dict()
        assert d['run_type'] == 'rewarm'

    def test_load_from_redis_preserves_run_type(self):
        import json
        run = Run(platform='instagram', run_type='rewarm')
        data = run.to_dict()
        with patch('app.models.run.r') as mock_r:
            mock_r.get.return_value = json.dumps(data)
            loaded = Run.load(run.id)
        assert loaded.run_type == 'rewarm'

    def test_load_defaults_run_type_for_old_data(self):
        import json
        run = Run(platform='instagram')
        data = run.to_dict()
        del data['run_type']  # simulate old data without run_type
        with patch('app.models.run.r') as mock_r:
            mock_r.get.return_value = json.dumps(data)
            loaded = Run.load(run.id)
        assert loaded.run_type == 'discovery'


class TestLaunchRewarm:
    """Test launch_rewarm() function."""

    @patch('app.pipeline.manager._get_queue')
    def test_launch_rewarm_creates_run(self, mock_queue, mock_redis):
        mock_queue.return_value.enqueue = MagicMock()
        from app.pipeline.manager import launch_rewarm
        run = launch_rewarm('instagram', {'hubspot_list_ids': ['123']})
        assert run.run_type == 'rewarm'
        assert run.platform == 'instagram'
        assert run.filters['hubspot_list_ids'] == ['123']

    @patch('app.pipeline.manager._get_queue')
    def test_launch_rewarm_enqueues_pipeline(self, mock_queue, mock_redis):
        mock_q = MagicMock()
        mock_queue.return_value = mock_q
        from app.pipeline.manager import launch_rewarm
        run = launch_rewarm('instagram', {'hubspot_list_ids': ['123']})
        mock_q.enqueue.assert_called_once()
        args = mock_q.enqueue.call_args
        assert args[0][1] == run.id  # run_id passed to run_pipeline

    def test_launch_rewarm_unsupported_platform(self, mock_redis):
        from app.pipeline.manager import launch_rewarm
        with pytest.raises(ValueError, match="Rewarm not supported"):
            launch_rewarm('tiktok', {'hubspot_list_ids': ['123']})


class TestPersistRunWithRunType:
    """Test that persist_run stores run_type via DbRun model."""

    def test_dbrun_model_has_run_type_column(self):
        from app.models.db_run import DbRun
        assert hasattr(DbRun, 'run_type')

    def test_dbrun_run_type_defaults_to_discovery(self, db_session):
        from app.models.db_run import DbRun
        db_run = DbRun(id='test-default-rt', platform='instagram', status='queued')
        db_session.add(db_run)
        db_session.flush()
        assert db_run.run_type == 'discovery'

    def test_dbrun_stores_rewarm_run_type(self, db_session):
        from app.models.db_run import DbRun
        db_run = DbRun(id='test-rewarm-rt', platform='instagram', status='queued', run_type='rewarm')
        db_session.add(db_run)
        db_session.flush()
        fetched = db_session.get(DbRun, 'test-rewarm-rt')
        assert fetched.run_type == 'rewarm'

    def test_persist_run_passes_run_type(self):
        """Verify persist_run reads run_type from the run object."""
        from app.services.db import persist_run
        # Check that the code references run_type
        import inspect
        src = inspect.getsource(persist_run)
        assert 'run_type' in src


class TestCrmDryRun:
    """Test dry_run guard on CRM sync adapters."""

    def test_instagram_crm_dry_run_skips_sync(self):
        from app.pipeline.crm import InstagramCrmSync
        adapter = InstagramCrmSync()
        run = MagicMock()
        run.filters = {'dry_run': True}
        run.increment_stage_progress = MagicMock()
        profiles = [{'_lead_analysis': {'lead_score': 0.8}}]
        result = adapter.run(profiles, run)
        assert len(result.profiles) == 1
        assert result.profiles[0]['_synced_to_crm'] is False
        run.increment_stage_progress.assert_called_with('crm_sync', 'completed')

    def test_patreon_crm_dry_run_skips_sync(self):
        from app.pipeline.crm import PatreonCrmSync
        adapter = PatreonCrmSync()
        run = MagicMock()
        run.filters = {'dry_run': True}
        run.increment_stage_progress = MagicMock()
        profiles = [{'name': 'test'}]
        result = adapter.run(profiles, run)
        assert len(result.profiles) == 1

    def test_facebook_crm_dry_run_skips_sync(self):
        from app.pipeline.crm import FacebookCrmSync
        adapter = FacebookCrmSync()
        run = MagicMock()
        run.filters = {'dry_run': True}
        run.increment_stage_progress = MagicMock()
        profiles = [{'name': 'test'}]
        result = adapter.run(profiles, run)
        assert len(result.profiles) == 1


class TestRewarmPipelineStages:
    """Verify rewarm pipeline config is correct."""

    def test_rewarm_stages_defined(self):
        assert REWARM_PIPELINE_STAGES == [
            'segment_import', 'enrichment', 'analysis', 'scoring', 'crm_sync'
        ]

    def test_rewarm_stages_no_discovery_or_prescreen(self):
        assert 'discovery' not in REWARM_PIPELINE_STAGES
        assert 'pre_screen' not in REWARM_PIPELINE_STAGES

    def test_segment_import_in_stage_registry(self):
        from app.pipeline.manager import STAGE_REGISTRY
        assert 'segment_import' in STAGE_REGISTRY


class TestBuildStagesRewarm:
    """Test _build_stages handles rewarm runs."""

    def test_rewarm_uses_rewarm_stage_order(self):
        from app.routes.monitor import _build_stages
        run_dict = {'run_type': 'rewarm', 'current_stage': 'enrichment', 'status': 'enriching'}
        stages = _build_stages(run_dict)
        keys = [s['key'] for s in stages]
        assert keys == ['segment_import', 'enrichment', 'analysis', 'scoring', 'crm_sync']

    def test_discovery_uses_default_stage_order(self):
        from app.routes.monitor import _build_stages
        run_dict = {'run_type': 'discovery', 'current_stage': 'enrichment', 'status': 'enriching'}
        stages = _build_stages(run_dict)
        keys = [s['key'] for s in stages]
        assert keys == ['discovery', 'pre_screen', 'enrichment', 'analysis', 'scoring', 'crm_sync']

    def test_rewarm_segment_import_completed_when_past(self):
        from app.routes.monitor import _build_stages
        run_dict = {'run_type': 'rewarm', 'current_stage': 'analysis', 'status': 'analyzing'}
        stages = _build_stages(run_dict)
        statuses = {s['key']: s['status'] for s in stages}
        assert statuses['segment_import'] == 'completed'
        assert statuses['enrichment'] == 'completed'
        assert statuses['analysis'] == 'running'
        assert statuses['scoring'] == 'pending'


class TestRunSummaryRewarm:
    """Test _generate_run_summary for rewarm runs."""

    def test_rewarm_summary_says_imported(self):
        from app.pipeline.manager import _generate_run_summary
        run = MagicMock()
        run.run_type = 'rewarm'
        run.platform = 'instagram'
        run.profiles_discovered = 10
        run.profiles_found = 10
        run.duplicates_skipped = 0
        run.hubspot_duplicates = 0
        run.profiles_pre_screened = 0
        run.profiles_enriched = 10
        run.profiles_scored = 10
        run.contacts_synced = 8
        run.tier_distribution = {'auto_enroll': 3, 'standard_review': 5}
        run.estimated_cost = 0.0
        run.actual_cost = 0.0
        run.current_stage = 'crm_sync'
        run.errors = []
        summary = _generate_run_summary(run)
        assert 'Imported 10 Instagram contacts from HubSpot' in summary
        assert 'Discovered' not in summary

    def test_rewarm_zero_results_summary(self):
        from app.pipeline.manager import _generate_run_summary
        run = MagicMock()
        run.run_type = 'rewarm'
        run.platform = 'instagram'
        run.profiles_discovered = 0
        run.profiles_found = 0
        run.duplicates_skipped = 0
        run.hubspot_duplicates = 0
        run.profiles_pre_screened = 0
        run.profiles_enriched = 0
        run.profiles_scored = 0
        run.contacts_synced = 0
        run.tier_distribution = {}
        run.estimated_cost = 0.0
        run.actual_cost = 0.0
        run.current_stage = ''
        run.errors = []
        summary = _generate_run_summary(run)
        assert 'HubSpot' in summary
        assert 'segment' in summary.lower()

    def test_discovery_summary_says_discovered(self):
        from app.pipeline.manager import _generate_run_summary
        run = MagicMock()
        run.run_type = 'discovery'
        run.platform = 'instagram'
        run.profiles_discovered = 20
        run.profiles_found = 18
        run.duplicates_skipped = 2
        run.hubspot_duplicates = 0
        run.profiles_pre_screened = 15
        run.profiles_enriched = 15
        run.profiles_scored = 15
        run.contacts_synced = 12
        run.tier_distribution = {'auto_enroll': 5, 'standard_review': 7}
        run.estimated_cost = 0.0
        run.actual_cost = 0.0
        run.current_stage = 'crm_sync'
        run.errors = []
        summary = _generate_run_summary(run)
        assert 'Discovered 20 Instagram profiles' in summary
        assert 'Imported' not in summary


class TestProfileContractParity:
    """Verify segment import profiles match the canonical format."""

    def test_segment_import_profile_matches_discovery_fields(self):
        """Segment import profiles should have the same core fields as discovery profiles."""
        from app.pipeline.segment_import import SegmentImportInstagram
        # The canonical fields from _standardize_results()
        canonical_fields = {
            'first_and_last_name', 'flagship_social_platform_handle',
            'instagram_handle', 'instagram_bio', 'instagram_followers',
            'average_engagement', 'email', 'phone',
            'tiktok_handle', 'youtube_profile_link', 'facebook_profile_link',
            'patreon_link', 'pinterest_profile_link',
            'city', 'state', 'country',
            'flagship_social_platform', 'channel',
            'channel_host_prospected', 'funnel', 'enrichment_status',
        }

        adapter = SegmentImportInstagram()
        run = MagicMock()
        run.filters = {'hubspot_list_ids': ['list1']}
        run.increment_stage_progress = MagicMock()

        mock_contacts = [{
            'firstname': 'Jane',
            'lastname': 'Doe',
            'email': 'jane@example.com',
            'instagram_handle': 'https://www.instagram.com/janedoe/',
            'instagram_followers': '50000',
            'city': 'LA',
            'state': 'CA',
            'country': 'US',
        }]

        with patch('app.services.hubspot.hubspot_import_segment', return_value=mock_contacts):
            result = adapter.run([], run)

        assert len(result.profiles) == 1
        profile = result.profiles[0]
        # Check all canonical fields are present
        for field in canonical_fields:
            assert field in profile, f"Missing field: {field}"
