"""Tests for app.pipeline.segment_import — HubSpot segment import adapter."""
import pytest
from unittest.mock import patch, MagicMock, call

from app.pipeline.base import StageResult, StageAdapter
from app.pipeline.segment_import import (
    SegmentImportInstagram,
    ADAPTERS,
    _extract_ig_username,
)


# ── ADAPTERS registry ──────────────────────────────────────────────────────


class TestAdaptersRegistry:
    """The module-level ADAPTERS dict maps platform names to adapter classes."""

    def test_registry_contains_instagram(self):
        assert ADAPTERS['instagram'] is SegmentImportInstagram

    def test_registry_has_exactly_one_entry(self):
        assert len(ADAPTERS) == 1


# ── _extract_ig_username helper ────────────────────────────────────────────


class TestExtractIgUsername:
    """Helper that parses Instagram usernames from URLs and raw handles."""

    def test_full_url_with_trailing_slash(self):
        assert _extract_ig_username('https://www.instagram.com/wanderlust_maya/') == 'wanderlust_maya'

    def test_full_url_without_trailing_slash(self):
        assert _extract_ig_username('https://www.instagram.com/wanderlust_maya') == 'wanderlust_maya'

    def test_http_url(self):
        assert _extract_ig_username('http://instagram.com/some_user/') == 'some_user'

    def test_url_with_query_params(self):
        assert _extract_ig_username('https://instagram.com/creator123/?hl=en') == 'creator123'

    def test_url_no_www(self):
        assert _extract_ig_username('https://instagram.com/creator_abc') == 'creator_abc'

    def test_at_sign_handle(self):
        assert _extract_ig_username('@creator_handle') == 'creator_handle'

    def test_raw_username(self):
        assert _extract_ig_username('simple_user') == 'simple_user'

    def test_empty_string_returns_none(self):
        assert _extract_ig_username('') is None

    def test_none_returns_none(self):
        assert _extract_ig_username(None) is None

    def test_whitespace_only_returns_none(self):
        assert _extract_ig_username('   ') is None

    def test_at_sign_only_returns_none(self):
        assert _extract_ig_username('@') is None

    def test_instagram_com_root_returns_none(self):
        assert _extract_ig_username('https://www.instagram.com/') is None

    def test_strips_leading_trailing_whitespace(self):
        assert _extract_ig_username('  https://www.instagram.com/padded_user/  ') == 'padded_user'


# ── SegmentImportInstagram ─────────────────────────────────────────────────


class TestSegmentImportInstagram:
    """SegmentImportInstagram imports contacts from HubSpot lists."""

    def test_is_stage_adapter_subclass(self):
        assert issubclass(SegmentImportInstagram, StageAdapter)

    def test_class_attributes(self):
        adapter = SegmentImportInstagram()
        assert adapter.platform == 'instagram'
        assert adapter.stage == 'segment_import'
        assert 'HubSpot' in adapter.apis

    # ── Successful imports ──────────────────────────────────────────────

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_imports_contacts_from_single_list(self, mock_import, make_run):
        """Contacts from a single HubSpot list are converted to profiles."""
        mock_import.return_value = [
            {
                'firstname': 'Maya',
                'lastname': 'Chen',
                'email': 'maya@example.com',
                'instagram_handle': 'https://www.instagram.com/wanderlust_maya/',
                'city': 'Denver',
                'state': 'CO',
                'country': 'US',
                'instagram_followers': 87000,
            },
        ]

        adapter = SegmentImportInstagram()
        run = make_run(
            run_type='rewarm',
            filters={'hubspot_list_ids': ['list-1']},
        )
        result = adapter.run([], run)

        assert isinstance(result, StageResult)
        assert len(result.profiles) == 1
        assert result.processed == 1
        assert result.skipped == 0

        p = result.profiles[0]
        assert p['first_and_last_name'] == 'Maya Chen'
        assert p['flagship_social_platform_handle'] == 'wanderlust_maya'
        assert p['instagram_handle'] == 'https://www.instagram.com/wanderlust_maya/'
        assert p['instagram_bio'] == ''
        assert p['instagram_followers'] == 87000
        assert p['average_engagement'] == 0
        assert p['email'] == 'maya@example.com'
        assert p['phone'] is None
        assert p['tiktok_handle'] is None
        assert p['youtube_profile_link'] is None
        assert p['facebook_profile_link'] is None
        assert p['patreon_link'] is None
        assert p['pinterest_profile_link'] is None
        assert p['city'] == 'Denver'
        assert p['state'] == 'CO'
        assert p['country'] == 'US'
        assert p['flagship_social_platform'] == 'instagram'
        assert p['channel'] == 'Outbound'
        assert p['channel_host_prospected'] == 'HubSpot Rewarm'
        assert p['funnel'] == 'Creator'
        assert p['enrichment_status'] == 'pending'

        mock_import.assert_called_once_with('list-1', 'instagram')

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_imports_from_multiple_lists(self, mock_import, make_run):
        """Contacts from multiple HubSpot lists are all imported."""
        mock_import.side_effect = [
            [
                {'firstname': 'Maya', 'lastname': 'Chen', 'email': 'maya@ex.com',
                 'instagram_handle': 'https://instagram.com/maya_c/', 'instagram_followers': 5000},
            ],
            [
                {'firstname': 'Jorge', 'lastname': 'R', 'email': 'jorge@ex.com',
                 'instagram_handle': 'https://instagram.com/jorge_r/', 'instagram_followers': 10000},
                {'firstname': 'Li', 'lastname': 'Wei', 'email': 'li@ex.com',
                 'instagram_handle': '@li_wei', 'instagram_followers': 3000},
            ],
        ]

        adapter = SegmentImportInstagram()
        run = make_run(
            run_type='rewarm',
            filters={'hubspot_list_ids': ['list-a', 'list-b']},
        )
        result = adapter.run([], run)

        assert len(result.profiles) == 3
        assert result.processed == 3
        assert mock_import.call_count == 2
        mock_import.assert_any_call('list-a', 'instagram')
        mock_import.assert_any_call('list-b', 'instagram')

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_increments_progress_per_profile(self, mock_import, make_run):
        """Each imported profile triggers a progress increment."""
        mock_import.return_value = [
            {'firstname': 'A', 'lastname': 'B', 'instagram_handle': '@user1', 'instagram_followers': 100},
            {'firstname': 'C', 'lastname': 'D', 'instagram_handle': '@user2', 'instagram_followers': 200},
        ]

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['list-1']})
        adapter.run([], run)

        assert run.increment_stage_progress.call_count == 2
        run.increment_stage_progress.assert_any_call('segment_import', 'completed')

    # ── Username extraction from handles ─────────────────────────────────

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_extracts_username_from_full_url(self, mock_import, make_run):
        """Full Instagram URLs are parsed to extract the username."""
        mock_import.return_value = [
            {'firstname': 'A', 'lastname': 'B',
             'instagram_handle': 'https://www.instagram.com/some_creator/',
             'instagram_followers': 5000},
        ]

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['list-1']})
        result = adapter.run([], run)

        assert result.profiles[0]['flagship_social_platform_handle'] == 'some_creator'
        assert result.profiles[0]['instagram_handle'] == 'https://www.instagram.com/some_creator/'

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_extracts_username_from_at_handle(self, mock_import, make_run):
        """@-prefixed handles are normalized to plain username."""
        mock_import.return_value = [
            {'firstname': 'A', 'lastname': 'B',
             'instagram_handle': '@at_user',
             'instagram_followers': 1000},
        ]

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['list-1']})
        result = adapter.run([], run)

        assert result.profiles[0]['flagship_social_platform_handle'] == 'at_user'

    # ── Skipping contacts without IG handles ─────────────────────────────

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_skips_contacts_without_ig_handle(self, mock_import, make_run):
        """Contacts with empty/missing instagram_handle are skipped."""
        mock_import.return_value = [
            {'firstname': 'No', 'lastname': 'Handle', 'email': 'no@ex.com',
             'instagram_handle': '', 'instagram_followers': 0},
            {'firstname': 'Good', 'lastname': 'User', 'email': 'good@ex.com',
             'instagram_handle': 'https://instagram.com/good_user/', 'instagram_followers': 5000},
        ]

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['list-1']})
        result = adapter.run([], run)

        assert len(result.profiles) == 1
        assert result.skipped == 1
        assert result.processed == 2
        assert result.profiles[0]['flagship_social_platform_handle'] == 'good_user'

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_skips_contacts_with_missing_ig_handle_key(self, mock_import, make_run):
        """Contacts that don't have the instagram_handle key at all are skipped."""
        mock_import.return_value = [
            {'firstname': 'No', 'lastname': 'Key', 'email': 'no@ex.com'},
        ]

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['list-1']})
        result = adapter.run([], run)

        assert len(result.profiles) == 0
        assert result.skipped == 1

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_all_contacts_skipped(self, mock_import, make_run):
        """When all contacts lack IG handles, result has zero profiles."""
        mock_import.return_value = [
            {'firstname': 'A', 'lastname': 'B', 'instagram_handle': ''},
            {'firstname': 'C', 'lastname': 'D', 'instagram_handle': None},
        ]

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['list-1']})
        result = adapter.run([], run)

        assert len(result.profiles) == 0
        assert result.skipped == 2
        assert result.processed == 2

    # ── Empty / missing list IDs ─────────────────────────────────────────

    def test_run_returns_empty_when_no_list_ids(self, make_run):
        """Returns empty result when hubspot_list_ids is not in filters."""
        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={})
        result = adapter.run([], run)

        assert result.profiles == []
        assert result.processed == 0

    def test_run_returns_empty_when_list_ids_empty(self, make_run):
        """Returns empty result when hubspot_list_ids is an empty list."""
        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': []})
        result = adapter.run([], run)

        assert result.profiles == []
        assert result.processed == 0

    def test_run_handles_none_filters(self, make_run):
        """Returns empty result when run.filters is None."""
        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters=None)
        result = adapter.run([], run)

        assert result.profiles == []
        assert result.processed == 0

    # ── Name handling ────────────────────────────────────────────────────

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_concatenates_first_and_last_name(self, mock_import, make_run):
        """First and last name are joined with a space."""
        mock_import.return_value = [
            {'firstname': 'Jane', 'lastname': 'Doe',
             'instagram_handle': '@janedoe', 'instagram_followers': 1000},
        ]

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['list-1']})
        result = adapter.run([], run)

        assert result.profiles[0]['first_and_last_name'] == 'Jane Doe'

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_handles_missing_first_name(self, mock_import, make_run):
        """Missing firstname still produces a valid name from lastname."""
        mock_import.return_value = [
            {'lastname': 'Only',
             'instagram_handle': '@only_last', 'instagram_followers': 500},
        ]

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['list-1']})
        result = adapter.run([], run)

        assert result.profiles[0]['first_and_last_name'] == 'Only'

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_handles_missing_last_name(self, mock_import, make_run):
        """Missing lastname still produces a valid name from firstname."""
        mock_import.return_value = [
            {'firstname': 'Solo',
             'instagram_handle': '@solo_first', 'instagram_followers': 500},
        ]

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['list-1']})
        result = adapter.run([], run)

        assert result.profiles[0]['first_and_last_name'] == 'Solo'

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_handles_none_name_fields(self, mock_import, make_run):
        """None values for firstname/lastname produce empty string name."""
        mock_import.return_value = [
            {'firstname': None, 'lastname': None,
             'instagram_handle': '@anon', 'instagram_followers': 100},
        ]

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['list-1']})
        result = adapter.run([], run)

        assert result.profiles[0]['first_and_last_name'] == ''

    # ── Follower count handling ──────────────────────────────────────────

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_converts_follower_count_to_int(self, mock_import, make_run):
        """String follower counts from HubSpot are converted to int."""
        mock_import.return_value = [
            {'firstname': 'A', 'lastname': 'B',
             'instagram_handle': '@user', 'instagram_followers': '12500'},
        ]

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['list-1']})
        result = adapter.run([], run)

        assert result.profiles[0]['instagram_followers'] == 12500

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_handles_none_follower_count(self, mock_import, make_run):
        """None follower count defaults to 0."""
        mock_import.return_value = [
            {'firstname': 'A', 'lastname': 'B',
             'instagram_handle': '@user', 'instagram_followers': None},
        ]

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['list-1']})
        result = adapter.run([], run)

        assert result.profiles[0]['instagram_followers'] == 0

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_handles_missing_follower_count(self, mock_import, make_run):
        """Missing follower count defaults to 0."""
        mock_import.return_value = [
            {'firstname': 'A', 'lastname': 'B', 'instagram_handle': '@user'},
        ]

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['list-1']})
        result = adapter.run([], run)

        assert result.profiles[0]['instagram_followers'] == 0

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_handles_non_numeric_follower_count(self, mock_import, make_run):
        """Non-numeric follower count defaults to 0."""
        mock_import.return_value = [
            {'firstname': 'A', 'lastname': 'B',
             'instagram_handle': '@user', 'instagram_followers': 'not-a-number'},
        ]

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['list-1']})
        result = adapter.run([], run)

        assert result.profiles[0]['instagram_followers'] == 0

    # ── Error handling ───────────────────────────────────────────────────

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_handles_api_exception(self, mock_import, make_run):
        """API exceptions for a list are caught and recorded as errors."""
        mock_import.side_effect = Exception("HubSpot API error")

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['bad-list']})
        result = adapter.run([], run)

        assert result.profiles == []
        assert len(result.errors) == 1
        assert 'bad-list' in result.errors[0]

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_continues_after_failed_list(self, mock_import, make_run):
        """If one list fails, subsequent lists are still processed."""
        mock_import.side_effect = [
            Exception("API error"),
            [
                {'firstname': 'Good', 'lastname': 'User',
                 'instagram_handle': '@good_user', 'instagram_followers': 5000},
            ],
        ]

        adapter = SegmentImportInstagram()
        run = make_run(
            run_type='rewarm',
            filters={'hubspot_list_ids': ['bad-list', 'good-list']},
        )
        result = adapter.run([], run)

        assert len(result.profiles) == 1
        assert len(result.errors) == 1
        assert result.profiles[0]['flagship_social_platform_handle'] == 'good_user'

    @patch('app.services.hubspot.hubspot_import_segment', create=True)
    def test_run_empty_contact_list(self, mock_import, make_run):
        """Empty contact list from HubSpot results in zero profiles."""
        mock_import.return_value = []

        adapter = SegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['empty-list']})
        result = adapter.run([], run)

        assert result.profiles == []
        assert result.processed == 0


# ── MockSegmentImportInstagram ─────────────────────────────────────────────


class TestMockSegmentImportInstagram:
    """Mock adapter for segment_import stage."""

    def test_is_in_mock_registry(self):
        from app.pipeline.mock_adapters import MOCK_STAGE_REGISTRY
        assert 'segment_import' in MOCK_STAGE_REGISTRY
        assert 'instagram' in MOCK_STAGE_REGISTRY['segment_import']

    def test_is_stage_adapter_subclass(self):
        from app.pipeline.mock_adapters import MockSegmentImportInstagram
        assert issubclass(MockSegmentImportInstagram, StageAdapter)

    def test_class_attributes(self):
        from app.pipeline.mock_adapters import MockSegmentImportInstagram
        adapter = MockSegmentImportInstagram()
        assert adapter.platform == 'instagram'
        assert adapter.stage == 'segment_import'

    @patch('app.pipeline.mock_adapters._simulate_delay')
    def test_run_returns_profiles(self, mock_delay, make_run):
        """Mock adapter returns between 5 and 10 profiles."""
        from app.pipeline.mock_adapters import MockSegmentImportInstagram

        adapter = MockSegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['mock-list']})
        result = adapter.run([], run)

        assert isinstance(result, StageResult)
        assert 5 <= len(result.profiles) <= 10
        assert result.processed == len(result.profiles)

    @patch('app.pipeline.mock_adapters._simulate_delay')
    def test_run_profiles_have_canonical_fields(self, mock_delay, make_run):
        """Each mock profile has the canonical field set."""
        from app.pipeline.mock_adapters import MockSegmentImportInstagram

        adapter = MockSegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['mock-list']})
        result = adapter.run([], run)

        required_keys = {
            'first_and_last_name', 'flagship_social_platform_handle',
            'instagram_handle', 'instagram_bio', 'instagram_followers',
            'average_engagement', 'email', 'phone', 'tiktok_handle',
            'youtube_profile_link', 'facebook_profile_link', 'patreon_link',
            'pinterest_profile_link', 'city', 'state', 'country',
            'flagship_social_platform', 'channel', 'channel_host_prospected',
            'funnel', 'enrichment_status',
        }

        for profile in result.profiles:
            for key in required_keys:
                assert key in profile, f"Missing key: {key}"

    @patch('app.pipeline.mock_adapters._simulate_delay')
    def test_run_profiles_have_rewarm_channel(self, mock_delay, make_run):
        """Mock profiles have 'HubSpot Rewarm' as channel_host_prospected."""
        from app.pipeline.mock_adapters import MockSegmentImportInstagram

        adapter = MockSegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['mock-list']})
        result = adapter.run([], run)

        for profile in result.profiles:
            assert profile['channel_host_prospected'] == 'HubSpot Rewarm'

    @patch('app.pipeline.mock_adapters._simulate_delay')
    def test_run_increments_progress(self, mock_delay, make_run):
        """Mock adapter calls increment_stage_progress for each profile."""
        from app.pipeline.mock_adapters import MockSegmentImportInstagram

        adapter = MockSegmentImportInstagram()
        run = make_run(run_type='rewarm', filters={'hubspot_list_ids': ['mock-list']})
        result = adapter.run([], run)

        assert run.increment_stage_progress.call_count == len(result.profiles)
        run.increment_stage_progress.assert_any_call('segment_import', 'completed')
