"""Tests for app.pipeline.crm — CRM sync stage adapters."""
import pytest
from unittest.mock import patch, MagicMock, call

from app.config import BDR_OWNER_IDS
from app.pipeline.base import StageResult, StageAdapter
from app.pipeline.crm import (
    InstagramCrmSync,
    PatreonCrmSync,
    FacebookCrmSync,
    ADAPTERS,
)


# ── ADAPTERS registry ──────────────────────────────────────────────────────

class TestAdaptersRegistry:
    """The module-level ADAPTERS dict maps platform names to CRM sync classes."""

    def test_registry_contains_instagram(self):
        assert ADAPTERS['instagram'] is InstagramCrmSync

    def test_registry_contains_patreon(self):
        assert ADAPTERS['patreon'] is PatreonCrmSync

    def test_registry_contains_facebook(self):
        assert ADAPTERS['facebook'] is FacebookCrmSync

    def test_registry_has_exactly_three_entries(self):
        assert len(ADAPTERS) == 3


# ── InstagramCrmSync ───────────────────────────────────────────────────────

class TestInstagramCrmSync:
    """InstagramCrmSync pushes each profile individually to HubSpot."""

    def test_is_stage_adapter_subclass(self):
        assert issubclass(InstagramCrmSync, StageAdapter)

    def test_class_attributes(self):
        adapter = InstagramCrmSync()
        assert adapter.platform == 'instagram'
        assert adapter.stage == 'crm_sync'
        assert 'HubSpot' in adapter.apis

    def test_estimate_cost_always_zero(self):
        adapter = InstagramCrmSync()
        assert adapter.estimate_cost(0) == 0.0
        assert adapter.estimate_cost(100) == 0.0
        assert adapter.estimate_cost(9999) == 0.0

    @patch('app.pipeline.crm.send_to_hubspot')
    def test_run_syncs_profiles_to_hubspot(self, mock_send, make_run):
        """Each profile with a contact_id is sent to HubSpot."""
        profiles = [
            {
                'contact_id': 'c-001',
                '_lead_analysis': {'lead_score': 0.85, 'section_scores': {'a': 1}, 'score_reasoning': 'Good'},
                '_creator_profile': {'name': 'Creator A'},
                '_content_analyses': [{'id': 1}],
                '_first_name': 'Alice',
            },
            {
                'contact_id': 'c-002',
                '_lead_analysis': {'lead_score': 0.60, 'section_scores': {}, 'score_reasoning': 'OK'},
                '_creator_profile': {},
                '_content_analyses': [],
                '_first_name': 'Bob',
            },
        ]
        run = make_run(platform='instagram')

        adapter = InstagramCrmSync()
        result = adapter.run(profiles, run)

        assert isinstance(result, StageResult)
        assert len(result.profiles) == 2
        assert result.processed == 2
        assert result.failed == 0
        assert result.errors == []
        assert mock_send.call_count == 2

    @patch('app.pipeline.crm.send_to_hubspot')
    def test_run_uses_id_fallback_for_contact_id(self, mock_send, make_run):
        """Falls back to 'id' field when 'contact_id' is not present."""
        profiles = [
            {'id': 'fallback-id', '_lead_analysis': {'lead_score': 0.5}},
        ]
        run = make_run(platform='instagram')

        adapter = InstagramCrmSync()
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        # First positional arg to send_to_hubspot is the contact_id
        assert mock_send.call_args_list[0][0][0] == 'fallback-id'

    @patch('app.pipeline.crm.send_to_hubspot')
    def test_run_skips_profile_without_contact_id(self, mock_send, make_run):
        """Profiles with no contact_id or id are skipped with an error."""
        profiles = [
            {'_lead_analysis': {'lead_score': 0.9}},
        ]
        run = make_run(platform='instagram')

        adapter = InstagramCrmSync()
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 0
        assert result.processed == 1
        assert result.failed == 1
        assert 'No contact_id' in result.errors[0]
        mock_send.assert_not_called()

    @patch('app.pipeline.crm.send_to_hubspot')
    def test_run_skips_profile_with_empty_contact_id(self, mock_send, make_run):
        """Empty string contact_id is treated as missing."""
        profiles = [
            {'contact_id': '', 'id': '', '_lead_analysis': {}},
        ]
        run = make_run(platform='instagram')

        adapter = InstagramCrmSync()
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 0
        assert result.failed == 1
        mock_send.assert_not_called()

    @patch('app.pipeline.crm.send_to_hubspot')
    def test_run_handles_hubspot_error_gracefully(self, mock_send, make_run):
        """HubSpot API errors are caught and recorded, not raised."""
        mock_send.side_effect = Exception("HubSpot 429 rate limit")

        profiles = [
            {'contact_id': 'c-err', '_lead_analysis': {'lead_score': 0.5}},
        ]
        run = make_run(platform='instagram')

        adapter = InstagramCrmSync()
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 0
        assert result.failed == 1
        assert 'c-err' in result.errors[0]
        assert 'rate limit' in result.errors[0]

    @patch('app.pipeline.crm.send_to_hubspot')
    def test_run_increments_progress_on_success(self, mock_send, make_run):
        """Each synced profile triggers a 'completed' progress increment."""
        profiles = [
            {'contact_id': 'c-1', '_lead_analysis': {}},
            {'contact_id': 'c-2', '_lead_analysis': {}},
        ]
        run = make_run(platform='instagram')

        adapter = InstagramCrmSync()
        adapter.run(profiles, run)

        completed_calls = [
            c for c in run.increment_stage_progress.call_args_list
            if c == call('crm_sync', 'completed')
        ]
        assert len(completed_calls) == 2

    @patch('app.pipeline.crm.send_to_hubspot')
    def test_run_increments_progress_on_failure(self, mock_send, make_run):
        """Failed syncs trigger a 'failed' progress increment."""
        mock_send.side_effect = Exception("API error")

        profiles = [{'contact_id': 'c-fail', '_lead_analysis': {}}]
        run = make_run(platform='instagram')

        adapter = InstagramCrmSync()
        adapter.run(profiles, run)

        run.increment_stage_progress.assert_called_once_with('crm_sync', 'failed')

    @patch('app.pipeline.crm.send_to_hubspot')
    def test_run_updates_run_contacts_synced(self, mock_send, make_run):
        """run.contacts_synced is set to the number of successfully synced profiles."""
        profiles = [
            {'contact_id': 'c-1', '_lead_analysis': {}},
            {'contact_id': 'c-2', '_lead_analysis': {}},
        ]
        run = make_run(platform='instagram')

        adapter = InstagramCrmSync()
        adapter.run(profiles, run)

        assert run.contacts_synced == 2
        run.save.assert_called_once()

    @patch('app.pipeline.crm.send_to_hubspot')
    def test_run_saves_run_after_sync(self, mock_send, make_run):
        """run.save() is called after processing all profiles."""
        profiles = [{'contact_id': 'c-1', '_lead_analysis': {}}]
        run = make_run(platform='instagram')

        adapter = InstagramCrmSync()
        adapter.run(profiles, run)

        run.save.assert_called_once()

    @patch('app.pipeline.crm.send_to_hubspot')
    def test_run_empty_profiles(self, mock_send, make_run):
        """Empty input produces empty output and zero synced count."""
        run = make_run(platform='instagram')

        adapter = InstagramCrmSync()
        result = adapter.run([], run)

        assert result.profiles == []
        assert result.processed == 0
        assert result.failed == 0
        assert run.contacts_synced == 0
        mock_send.assert_not_called()

    @patch('app.pipeline.crm.send_to_hubspot')
    def test_run_passes_correct_args_to_hubspot(self, mock_send, make_run):
        """send_to_hubspot receives the correct arguments from the profile."""
        profile = {
            'contact_id': 'c-args',
            '_lead_analysis': {
                'lead_score': 0.92,
                'section_scores': {'engagement': 0.8, 'reach': 0.9},
                'score_reasoning': 'Excellent engagement',
            },
            '_creator_profile': {'name': 'Test Creator', 'bio': 'Travel'},
            '_content_analyses': [{'type': 'image', 'score': 0.9}],
            '_first_name': 'Testy',
        }
        run = make_run(platform='instagram')

        adapter = InstagramCrmSync()
        adapter.run([profile], run)

        mock_send.assert_called_once_with(
            'c-args',
            0.92,
            {'engagement': 0.8, 'reach': 0.9},
            'Excellent engagement',
            {'name': 'Test Creator', 'bio': 'Travel'},
            [{'type': 'image', 'score': 0.9}],
            profile['_lead_analysis'],
            first_name='Testy',
        )

    @patch('app.pipeline.crm.send_to_hubspot')
    def test_run_defaults_missing_analysis_fields(self, mock_send, make_run):
        """Missing _lead_analysis sub-fields default to safe values."""
        profile = {
            'contact_id': 'c-defaults',
            '_lead_analysis': {},  # No lead_score, section_scores, etc.
        }
        run = make_run(platform='instagram')

        adapter = InstagramCrmSync()
        adapter.run([profile], run)

        args, kwargs = mock_send.call_args
        assert args[0] == 'c-defaults'
        assert args[1] == 0       # lead_score default
        assert args[2] == {}      # section_scores default
        assert args[3] == ''      # score_reasoning default

    @patch('app.pipeline.crm.send_to_hubspot')
    def test_run_defaults_first_name_to_there(self, mock_send, make_run):
        """Missing _first_name defaults to 'there'."""
        profile = {'contact_id': 'c-name', '_lead_analysis': {}}
        run = make_run(platform='instagram')

        adapter = InstagramCrmSync()
        adapter.run([profile], run)

        _, kwargs = mock_send.call_args
        assert kwargs['first_name'] == 'there'

    @patch('app.pipeline.crm.send_to_hubspot')
    def test_run_mixed_success_and_failure(self, mock_send, make_run):
        """Mix of successful and failed syncs is tracked correctly."""
        mock_send.side_effect = [None, Exception("fail"), None]

        profiles = [
            {'contact_id': 'ok-1', '_lead_analysis': {}},
            {'contact_id': 'fail-1', '_lead_analysis': {}},
            {'contact_id': 'ok-2', '_lead_analysis': {}},
        ]
        run = make_run(platform='instagram')

        adapter = InstagramCrmSync()
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 2
        assert result.processed == 3
        assert result.failed == 1
        assert run.contacts_synced == 2


# ── PatreonCrmSync ─────────────────────────────────────────────────────────

class TestPatreonCrmSync:
    """PatreonCrmSync standardizes, assigns BDR, and batch-imports to HubSpot."""

    def test_is_stage_adapter_subclass(self):
        assert issubclass(PatreonCrmSync, StageAdapter)

    def test_class_attributes(self):
        adapter = PatreonCrmSync()
        assert adapter.platform == 'patreon'
        assert adapter.stage == 'crm_sync'
        assert 'HubSpot' in adapter.apis

    def test_estimate_cost_always_zero(self):
        adapter = PatreonCrmSync()
        assert adapter.estimate_cost(0) == 0.0
        assert adapter.estimate_cost(100) == 0.0

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_patreon_profiles')
    def test_run_full_pipeline(self, mock_standardize, mock_bdr, mock_import, make_run):
        """Runs standardize -> BDR assign -> HubSpot import in order."""
        standardized = [{'name': 'S1', 'email': 's1@test.com'}]
        bdr_assigned = [{'name': 'S1', 'email': 's1@test.com', 'bdr_': '12345'}]

        mock_standardize.return_value = standardized
        mock_bdr.return_value = bdr_assigned
        mock_import.return_value = {'created': 1, 'skipped': 0, 'errors': []}

        profiles = [{'name': 'Creator A'}]
        run = make_run(id='run-p1', platform='patreon', filters={})

        adapter = PatreonCrmSync()
        result = adapter.run(profiles, run)

        assert isinstance(result, StageResult)
        assert result.profiles == bdr_assigned
        assert result.processed == 1
        assert result.skipped == 0
        assert result.meta == {'created': 1, 'skipped': 0, 'errors': []}

        mock_standardize.assert_called_once_with(profiles)
        mock_bdr.assert_called_once_with(standardized, list(BDR_OWNER_IDS.keys()))
        mock_import.assert_called_once_with(bdr_assigned, 'run-p1')

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_patreon_profiles')
    def test_run_empty_profiles_short_circuits(self, mock_std, mock_bdr, mock_import, make_run):
        """Empty input returns immediately without calling any service."""
        run = make_run(platform='patreon', filters={})

        adapter = PatreonCrmSync()
        result = adapter.run([], run)

        assert result.profiles == []
        assert result.processed == 0
        mock_std.assert_not_called()
        mock_bdr.assert_not_called()
        mock_import.assert_not_called()

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_patreon_profiles')
    def test_run_uses_custom_bdr_names(self, mock_std, mock_bdr, mock_import, make_run):
        """bdr_names from run.filters overrides the default BDR list."""
        mock_std.return_value = [{'name': 'X'}]
        mock_bdr.return_value = [{'name': 'X', 'bdr_': '99999'}]
        mock_import.return_value = {'created': 1, 'skipped': 0}

        custom_bdrs = ['Alice Smith', 'Bob Jones']
        run = make_run(
            id='run-custom',
            platform='patreon',
            filters={'bdr_names': custom_bdrs},
        )

        adapter = PatreonCrmSync()
        adapter.run([{'name': 'Creator'}], run)

        mock_bdr.assert_called_once_with(mock_std.return_value, custom_bdrs)

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_patreon_profiles')
    def test_run_defaults_bdr_names_to_all_bdr_owners(self, mock_std, mock_bdr, mock_import, make_run):
        """When bdr_names is not in filters, uses all BDR_OWNER_IDS keys."""
        mock_std.return_value = [{'name': 'X'}]
        mock_bdr.return_value = [{'name': 'X'}]
        mock_import.return_value = {'created': 1, 'skipped': 0}

        run = make_run(id='run-default', platform='patreon', filters={})

        adapter = PatreonCrmSync()
        adapter.run([{'name': 'Creator'}], run)

        expected_bdrs = list(BDR_OWNER_IDS.keys())
        mock_bdr.assert_called_once_with(mock_std.return_value, expected_bdrs)

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_patreon_profiles')
    def test_run_updates_run_contacts_synced(self, mock_std, mock_bdr, mock_import, make_run):
        """run.contacts_synced and run.duplicates_skipped are set from import results."""
        mock_std.return_value = []
        mock_bdr.return_value = []
        mock_import.return_value = {'created': 5, 'skipped': 3}

        run = make_run(id='run-counts', platform='patreon', filters={})

        adapter = PatreonCrmSync()
        adapter.run([{'name': 'X'}], run)

        assert run.contacts_synced == 5
        assert run.duplicates_skipped == 3
        run.save.assert_called_once()

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_patreon_profiles')
    def test_run_skipped_in_result(self, mock_std, mock_bdr, mock_import, make_run):
        """StageResult.skipped reflects the skipped count from import."""
        mock_std.return_value = []
        mock_bdr.return_value = []
        mock_import.return_value = {'created': 2, 'skipped': 8}

        run = make_run(id='run-skip', platform='patreon', filters={})

        adapter = PatreonCrmSync()
        result = adapter.run([{'name': 'X'}, {'name': 'Y'}], run)

        assert result.skipped == 8
        assert result.processed == 2

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_patreon_profiles')
    def test_run_import_results_in_meta(self, mock_std, mock_bdr, mock_import, make_run):
        """Full import_results dict is stored in StageResult.meta."""
        import_data = {'created': 3, 'skipped': 1, 'errors': ['dup@test.com']}
        mock_std.return_value = []
        mock_bdr.return_value = []
        mock_import.return_value = import_data

        run = make_run(id='run-meta', platform='patreon', filters={})

        adapter = PatreonCrmSync()
        result = adapter.run([{'name': 'X'}], run)

        assert result.meta == import_data


# ── FacebookCrmSync ────────────────────────────────────────────────────────

class TestFacebookCrmSync:
    """FacebookCrmSync standardizes FB groups, assigns BDR, and batch-imports."""

    def test_is_stage_adapter_subclass(self):
        assert issubclass(FacebookCrmSync, StageAdapter)

    def test_class_attributes(self):
        adapter = FacebookCrmSync()
        assert adapter.platform == 'facebook'
        assert adapter.stage == 'crm_sync'
        assert 'HubSpot' in adapter.apis

    def test_estimate_cost_always_zero(self):
        adapter = FacebookCrmSync()
        assert adapter.estimate_cost(0) == 0.0
        assert adapter.estimate_cost(100) == 0.0

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_facebook_profiles')
    def test_run_full_pipeline(self, mock_standardize, mock_bdr, mock_import, make_run):
        """Runs standardize_facebook_profiles -> BDR assign -> HubSpot import."""
        standardized = [{'group_name': 'G1', 'email': 'admin@g1.com'}]
        bdr_assigned = [{'group_name': 'G1', 'email': 'admin@g1.com', 'bdr_': '12345'}]

        mock_standardize.return_value = standardized
        mock_bdr.return_value = bdr_assigned
        mock_import.return_value = {'created': 1, 'skipped': 0, 'errors': []}

        profiles = [{'group_name': 'Travel Group'}]
        run = make_run(id='run-fb1', platform='facebook', filters={})

        adapter = FacebookCrmSync()
        result = adapter.run(profiles, run)

        assert isinstance(result, StageResult)
        assert result.profiles == bdr_assigned
        assert result.processed == 1
        assert result.skipped == 0

        mock_standardize.assert_called_once_with(profiles)
        mock_bdr.assert_called_once_with(standardized, list(BDR_OWNER_IDS.keys()))
        mock_import.assert_called_once_with(bdr_assigned, 'run-fb1')

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_facebook_profiles')
    def test_run_empty_profiles_short_circuits(self, mock_std, mock_bdr, mock_import, make_run):
        """Empty input returns immediately without calling any service."""
        run = make_run(platform='facebook', filters={})

        adapter = FacebookCrmSync()
        result = adapter.run([], run)

        assert result.profiles == []
        assert result.processed == 0
        mock_std.assert_not_called()
        mock_bdr.assert_not_called()
        mock_import.assert_not_called()

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_facebook_profiles')
    def test_run_uses_custom_bdr_names(self, mock_std, mock_bdr, mock_import, make_run):
        """bdr_names from run.filters overrides the default BDR list."""
        mock_std.return_value = [{'group_name': 'G'}]
        mock_bdr.return_value = [{'group_name': 'G', 'bdr_': '99999'}]
        mock_import.return_value = {'created': 1, 'skipped': 0}

        custom_bdrs = ['Custom BDR']
        run = make_run(
            id='run-fb-custom',
            platform='facebook',
            filters={'bdr_names': custom_bdrs},
        )

        adapter = FacebookCrmSync()
        adapter.run([{'group_name': 'Group'}], run)

        mock_bdr.assert_called_once_with(mock_std.return_value, custom_bdrs)

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_facebook_profiles')
    def test_run_defaults_bdr_names_to_all_bdr_owners(self, mock_std, mock_bdr, mock_import, make_run):
        """When bdr_names is not in filters, uses all BDR_OWNER_IDS keys."""
        mock_std.return_value = []
        mock_bdr.return_value = []
        mock_import.return_value = {'created': 0, 'skipped': 0}

        run = make_run(id='run-fb-default', platform='facebook', filters={})

        adapter = FacebookCrmSync()
        adapter.run([{'group_name': 'G'}], run)

        expected_bdrs = list(BDR_OWNER_IDS.keys())
        mock_bdr.assert_called_once_with(mock_std.return_value, expected_bdrs)

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_facebook_profiles')
    def test_run_updates_run_contacts_synced(self, mock_std, mock_bdr, mock_import, make_run):
        """run.contacts_synced and run.duplicates_skipped are set from import results."""
        mock_std.return_value = []
        mock_bdr.return_value = []
        mock_import.return_value = {'created': 10, 'skipped': 2}

        run = make_run(id='run-fb-counts', platform='facebook', filters={})

        adapter = FacebookCrmSync()
        adapter.run([{'group_name': 'G'}], run)

        assert run.contacts_synced == 10
        assert run.duplicates_skipped == 2
        run.save.assert_called_once()

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_facebook_profiles')
    def test_run_skipped_in_result(self, mock_std, mock_bdr, mock_import, make_run):
        """StageResult.skipped reflects the skipped count from import."""
        mock_std.return_value = []
        mock_bdr.return_value = []
        mock_import.return_value = {'created': 1, 'skipped': 5}

        run = make_run(id='run-fb-skip', platform='facebook', filters={})

        adapter = FacebookCrmSync()
        result = adapter.run([{'group_name': 'G'}], run)

        assert result.skipped == 5

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_facebook_profiles')
    def test_run_import_results_in_meta(self, mock_std, mock_bdr, mock_import, make_run):
        """Full import_results dict is stored in StageResult.meta."""
        import_data = {'created': 4, 'skipped': 0, 'errors': []}
        mock_std.return_value = []
        mock_bdr.return_value = []
        mock_import.return_value = import_data

        run = make_run(id='run-fb-meta', platform='facebook', filters={})

        adapter = FacebookCrmSync()
        result = adapter.run([{'group_name': 'G'}], run)

        assert result.meta == import_data

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_facebook_profiles')
    def test_run_uses_standardize_facebook_not_patreon(self, mock_std, mock_bdr, mock_import, make_run):
        """FacebookCrmSync calls standardize_facebook_profiles, not patreon variant."""
        mock_std.return_value = []
        mock_bdr.return_value = []
        mock_import.return_value = {'created': 0, 'skipped': 0}

        run = make_run(id='run-fb-std', platform='facebook', filters={})

        adapter = FacebookCrmSync()
        adapter.run([{'group_name': 'G'}], run)

        mock_std.assert_called_once()


# ── Integration ─────────────────────────────────────────────────────────────

class TestIntegration:
    """Cross-cutting checks across all CRM sync adapters."""

    def test_all_adapters_are_stage_adapter_subclasses(self):
        """Every adapter in the registry is a StageAdapter subclass."""
        for platform, cls in ADAPTERS.items():
            adapter = cls()
            assert isinstance(adapter, StageAdapter)
            assert adapter.platform == platform
            assert adapter.stage == 'crm_sync'

    def test_all_adapters_have_zero_cost(self):
        """All CRM sync adapters have zero estimated cost (HubSpot free-tier)."""
        for platform, cls in ADAPTERS.items():
            adapter = cls()
            assert adapter.estimate_cost(100) == 0.0

    @patch('app.pipeline.crm.import_profiles_to_hubspot')
    @patch('app.pipeline.crm.assign_bdr_round_robin')
    @patch('app.pipeline.crm.standardize_patreon_profiles')
    @patch('app.pipeline.crm.standardize_facebook_profiles')
    def test_patreon_and_facebook_use_different_standardizers(
        self, mock_fb_std, mock_pt_std, mock_bdr, mock_import, make_run
    ):
        """Patreon uses standardize_patreon_profiles, Facebook uses standardize_facebook_profiles."""
        mock_pt_std.return_value = []
        mock_fb_std.return_value = []
        mock_bdr.return_value = []
        mock_import.return_value = {'created': 0, 'skipped': 0}

        patreon = PatreonCrmSync()
        patreon.run(
            [{'name': 'P'}],
            make_run(id='r-pt', platform='patreon', filters={}),
        )

        facebook = FacebookCrmSync()
        facebook.run(
            [{'group_name': 'F'}],
            make_run(id='r-fb', platform='facebook', filters={}),
        )

        mock_pt_std.assert_called_once_with([{'name': 'P'}])
        mock_fb_std.assert_called_once_with([{'group_name': 'F'}])
