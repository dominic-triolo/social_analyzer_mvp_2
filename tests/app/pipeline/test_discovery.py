"""Tests for app.pipeline.discovery — platform discovery adapters."""
import pytest
from unittest.mock import patch, MagicMock, call

from app.pipeline.base import StageResult, StageAdapter
from app.pipeline.discovery import (
    InstagramDiscovery,
    PatreonDiscovery,
    FacebookDiscovery,
    ADAPTERS,
)


# ── ADAPTERS registry ──────────────────────────────────────────────────────

class TestAdaptersRegistry:
    """The module-level ADAPTERS dict maps platform names to adapter classes."""

    def test_registry_contains_instagram(self):
        assert ADAPTERS['instagram'] is InstagramDiscovery

    def test_registry_contains_patreon(self):
        assert ADAPTERS['patreon'] is PatreonDiscovery

    def test_registry_contains_facebook(self):
        assert ADAPTERS['facebook'] is FacebookDiscovery

    def test_registry_has_exactly_three_entries(self):
        assert len(ADAPTERS) == 3


# ── InstagramDiscovery ──────────────────────────────────────────────────────

class TestInstagramDiscovery:
    """InstagramDiscovery calls InsightIQ API and returns StageResult."""

    def test_is_stage_adapter_subclass(self):
        assert issubclass(InstagramDiscovery, StageAdapter)

    def test_class_attributes(self):
        adapter = InstagramDiscovery()
        assert adapter.platform == 'instagram'
        assert adapter.stage == 'discovery'
        assert 'InsightIQ' in adapter.apis

    def test_estimate_cost(self):
        adapter = InstagramDiscovery()
        assert adapter.estimate_cost(100) == pytest.approx(2.0)
        assert adapter.estimate_cost(0) == 0.0
        assert adapter.estimate_cost(1) == pytest.approx(0.02)

    @patch('app.pipeline.discovery.INSIGHTIQ_CLIENT_ID', 'test-id')
    @patch('app.pipeline.discovery.INSIGHTIQ_SECRET', 'test-secret')
    @patch('app.services.insightiq.InsightIQDiscovery')
    def test_run_returns_profiles_from_insightiq(self, MockClient, make_run):
        """Successful discovery returns profiles from InsightIQ API."""
        mock_instance = MockClient.return_value
        mock_instance.search_profiles.return_value = [
            {'platform_username': 'creator_a', 'follower_count': 5000},
            {'platform_username': 'creator_b', 'follower_count': 10000},
        ]

        adapter = InstagramDiscovery()
        run = make_run(platform='instagram', filters={'max_results': 10})
        result = adapter.run([], run)

        assert isinstance(result, StageResult)
        assert len(result.profiles) == 2
        assert result.processed == 2
        MockClient.assert_called_once_with('test-id', 'test-secret')
        mock_instance.search_profiles.assert_called_once_with(
            platform='instagram', user_filters={'max_results': 10}
        )

    @patch('app.pipeline.discovery.INSIGHTIQ_CLIENT_ID', 'test-id')
    @patch('app.pipeline.discovery.INSIGHTIQ_SECRET', 'test-secret')
    @patch('app.services.insightiq.InsightIQDiscovery')
    def test_run_empty_results(self, MockClient, make_run):
        """Returns empty profiles list when API finds nothing."""
        mock_instance = MockClient.return_value
        mock_instance.search_profiles.return_value = []

        adapter = InstagramDiscovery()
        run = make_run(platform='instagram', filters={'max_results': 10})
        result = adapter.run([], run)

        assert result.profiles == []
        assert result.processed == 0

    @patch('app.pipeline.discovery.INSIGHTIQ_CLIENT_ID', None)
    @patch('app.pipeline.discovery.INSIGHTIQ_SECRET', 'test-secret')
    def test_run_raises_when_client_id_missing(self, make_run):
        """Raises ValueError when INSIGHTIQ_CLIENT_ID is not set."""
        adapter = InstagramDiscovery()
        run = make_run(platform='instagram')
        with pytest.raises(ValueError, match="INSIGHTIQ_CLIENT_ID"):
            adapter.run([], run)

    @patch('app.pipeline.discovery.INSIGHTIQ_CLIENT_ID', 'test-id')
    @patch('app.pipeline.discovery.INSIGHTIQ_SECRET', None)
    def test_run_raises_when_secret_missing(self, make_run):
        """Raises ValueError when INSIGHTIQ_SECRET is not set."""
        adapter = InstagramDiscovery()
        run = make_run(platform='instagram')
        with pytest.raises(ValueError, match="INSIGHTIQ_SECRET"):
            adapter.run([], run)

    @patch('app.pipeline.discovery.INSIGHTIQ_CLIENT_ID', 'test-id')
    @patch('app.pipeline.discovery.INSIGHTIQ_SECRET', 'test-secret')
    @patch('app.services.insightiq.InsightIQDiscovery')
    def test_run_passes_filters_to_client(self, MockClient, make_run):
        """Filters from run.filters are forwarded to the API client."""
        mock_instance = MockClient.return_value
        mock_instance.search_profiles.return_value = []

        filters = {'max_results': 50, 'min_followers': 1000, 'interests': ['travel']}
        adapter = InstagramDiscovery()
        run = make_run(platform='instagram', filters=filters)
        adapter.run([], run)

        mock_instance.search_profiles.assert_called_once_with(
            platform='instagram', user_filters=filters
        )

    @patch('app.pipeline.discovery.INSIGHTIQ_CLIENT_ID', 'test-id')
    @patch('app.pipeline.discovery.INSIGHTIQ_SECRET', 'test-secret')
    @patch('app.services.insightiq.InsightIQDiscovery')
    def test_run_handles_none_filters(self, MockClient, make_run):
        """When run.filters is None, defaults to empty dict."""
        mock_instance = MockClient.return_value
        mock_instance.search_profiles.return_value = []

        adapter = InstagramDiscovery()
        run = make_run(platform='instagram', filters=None)
        adapter.run([], run)

        mock_instance.search_profiles.assert_called_once_with(
            platform='instagram', user_filters={}
        )

    @patch('app.pipeline.discovery.INSIGHTIQ_CLIENT_ID', 'test-id')
    @patch('app.pipeline.discovery.INSIGHTIQ_SECRET', 'test-secret')
    @patch('app.services.insightiq.InsightIQDiscovery')
    def test_run_raises_on_invalid_lookalike_type(self, MockClient, make_run):
        """Raises ValueError when lookalike_type is not 'creator' or 'audience'."""
        adapter = InstagramDiscovery()
        run = make_run(
            platform='instagram',
            filters={'lookalike_type': 'invalid', 'lookalike_username': 'someone'},
        )
        with pytest.raises(ValueError, match="lookalike_type must be"):
            adapter.run([], run)

    @patch('app.pipeline.discovery.INSIGHTIQ_CLIENT_ID', 'test-id')
    @patch('app.pipeline.discovery.INSIGHTIQ_SECRET', 'test-secret')
    @patch('app.services.insightiq.InsightIQDiscovery')
    def test_run_raises_when_lookalike_type_set_without_username(self, MockClient, make_run):
        """Raises ValueError when lookalike_type is set but lookalike_username is missing."""
        adapter = InstagramDiscovery()
        run = make_run(
            platform='instagram',
            filters={'lookalike_type': 'creator', 'lookalike_username': ''},
        )
        with pytest.raises(ValueError, match="lookalike_username required"):
            adapter.run([], run)

    @patch('app.pipeline.discovery.INSIGHTIQ_CLIENT_ID', 'test-id')
    @patch('app.pipeline.discovery.INSIGHTIQ_SECRET', 'test-secret')
    @patch('app.services.insightiq.InsightIQDiscovery')
    def test_run_accepts_valid_lookalike_creator(self, MockClient, make_run):
        """lookalike_type='creator' with a username passes validation."""
        mock_instance = MockClient.return_value
        mock_instance.search_profiles.return_value = [{'username': 'x'}]

        adapter = InstagramDiscovery()
        run = make_run(
            platform='instagram',
            filters={'lookalike_type': 'creator', 'lookalike_username': 'travel_guru'},
        )
        result = adapter.run([], run)
        assert len(result.profiles) == 1

    @patch('app.pipeline.discovery.INSIGHTIQ_CLIENT_ID', 'test-id')
    @patch('app.pipeline.discovery.INSIGHTIQ_SECRET', 'test-secret')
    @patch('app.services.insightiq.InsightIQDiscovery')
    def test_run_accepts_valid_lookalike_audience(self, MockClient, make_run):
        """lookalike_type='audience' with a username passes validation."""
        mock_instance = MockClient.return_value
        mock_instance.search_profiles.return_value = []

        adapter = InstagramDiscovery()
        run = make_run(
            platform='instagram',
            filters={'lookalike_type': 'audience', 'lookalike_username': 'some_creator'},
        )
        result = adapter.run([], run)
        assert result.profiles == []

    @patch('app.pipeline.discovery.INSIGHTIQ_CLIENT_ID', 'test-id')
    @patch('app.pipeline.discovery.INSIGHTIQ_SECRET', 'test-secret')
    @patch('app.services.insightiq.InsightIQDiscovery')
    def test_run_strips_whitespace_from_lookalike_username(self, MockClient, make_run):
        """Whitespace-only lookalike_username is treated as empty."""
        adapter = InstagramDiscovery()
        run = make_run(
            platform='instagram',
            filters={'lookalike_type': 'creator', 'lookalike_username': '   '},
        )
        with pytest.raises(ValueError, match="lookalike_username required"):
            adapter.run([], run)


# ── PatreonDiscovery ────────────────────────────────────────────────────────

class TestPatreonDiscovery:
    """PatreonDiscovery calls Apify Patreon scraper."""

    def test_is_stage_adapter_subclass(self):
        assert issubclass(PatreonDiscovery, StageAdapter)

    def test_class_attributes(self):
        adapter = PatreonDiscovery()
        assert adapter.platform == 'patreon'
        assert adapter.stage == 'discovery'
        assert 'Apify' in adapter.apis

    def test_estimate_cost(self):
        adapter = PatreonDiscovery()
        assert adapter.estimate_cost(100) == pytest.approx(1.0)
        assert adapter.estimate_cost(0) == 0.0

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_returns_profiles(self, MockApify, make_run):
        """Successful run returns normalized profiles from Apify."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-123'}

        raw_items = [
            {
                'name': 'Creator A',
                'url': 'https://patreon.com/creator_a',
                'instagram': 'https://instagram.com/creator_a',
                'youtube': 'https://youtube.com/creator_a',
            },
            {
                'name': 'Creator B',
                'url': 'https://patreon.com/creator_b',
            },
        ]
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter(raw_items)
        mock_apify.dataset.return_value = mock_dataset

        adapter = PatreonDiscovery()
        run = make_run(
            platform='patreon',
            filters={'search_keywords': ['travel'], 'max_results': 50},
        )
        result = adapter.run([], run)

        assert isinstance(result, StageResult)
        assert len(result.profiles) == 2
        assert result.processed == 2

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_normalizes_social_urls(self, MockApify, make_run):
        """Social URL fields are normalized using setdefault."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-123'}

        raw_items = [{
            'name': 'Creator',
            'instagram': 'https://instagram.com/test',
            'youtube': None,
            'twitter': 'https://twitter.com/test',
        }]
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter(raw_items)
        mock_apify.dataset.return_value = mock_dataset

        adapter = PatreonDiscovery()
        run = make_run(
            platform='patreon',
            filters={'search_keywords': ['travel']},
        )
        result = adapter.run([], run)

        profile = result.profiles[0]
        assert profile['instagram_url'] == 'https://instagram.com/test'
        assert profile['youtube_url'] is None
        assert profile['twitter_url'] == 'https://twitter.com/test'
        assert profile.get('facebook_url') is None
        assert profile.get('tiktok_url') is None
        assert profile.get('twitch_url') is None

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_setdefault_does_not_overwrite_existing_keys(self, MockApify, make_run):
        """If profile already has instagram_url, setdefault keeps it."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-123'}

        raw_items = [{
            'name': 'Creator',
            'instagram': 'https://instagram.com/old',
            'instagram_url': 'https://instagram.com/already_set',
        }]
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter(raw_items)
        mock_apify.dataset.return_value = mock_dataset

        adapter = PatreonDiscovery()
        run = make_run(
            platform='patreon',
            filters={'search_keywords': ['art']},
        )
        result = adapter.run([], run)

        # setdefault should not overwrite already_set
        assert result.profiles[0]['instagram_url'] == 'https://instagram.com/already_set'

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', None)
    def test_run_raises_when_token_missing(self, make_run):
        """Raises ValueError when APIFY_API_TOKEN is not set."""
        adapter = PatreonDiscovery()
        run = make_run(platform='patreon', filters={'search_keywords': ['travel']})
        with pytest.raises(ValueError, match="APIFY_API_TOKEN"):
            adapter.run([], run)

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    def test_run_raises_when_search_keywords_missing(self, make_run):
        """Raises ValueError when search_keywords is not provided."""
        adapter = PatreonDiscovery()
        run = make_run(platform='patreon', filters={})
        with pytest.raises(ValueError, match="search_keywords required"):
            adapter.run([], run)

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    def test_run_raises_when_search_keywords_empty(self, make_run):
        """Raises ValueError when search_keywords is an empty list."""
        adapter = PatreonDiscovery()
        run = make_run(platform='patreon', filters={'search_keywords': []})
        with pytest.raises(ValueError, match="search_keywords required"):
            adapter.run([], run)

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_appends_location_to_queries(self, MockApify, make_run):
        """Search queries include the location suffix."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-123'}
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter([])
        mock_apify.dataset.return_value = mock_dataset

        adapter = PatreonDiscovery()
        run = make_run(
            platform='patreon',
            filters={'search_keywords': ['travel'], 'location': 'Mexico'},
        )
        adapter.run([], run)

        call_args = mock_actor.call.call_args[1]['run_input']
        assert call_args['searchQueries'] == ['travel Mexico']

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_defaults_location_to_united_states(self, MockApify, make_run):
        """When location is not specified, defaults to 'United States'."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-123'}
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter([])
        mock_apify.dataset.return_value = mock_dataset

        adapter = PatreonDiscovery()
        run = make_run(
            platform='patreon',
            filters={'search_keywords': ['yoga']},
        )
        adapter.run([], run)

        call_args = mock_actor.call.call_args[1]['run_input']
        assert call_args['searchQueries'] == ['yoga United States']

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_handles_none_filters(self, MockApify, make_run):
        """When run.filters is None, raises because search_keywords are required."""
        adapter = PatreonDiscovery()
        run = make_run(platform='patreon', filters=None)
        with pytest.raises(ValueError, match="search_keywords required"):
            adapter.run([], run)

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_empty_apify_response(self, MockApify, make_run):
        """Returns empty profiles when Apify returns no items."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-123'}
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter([])
        mock_apify.dataset.return_value = mock_dataset

        adapter = PatreonDiscovery()
        run = make_run(
            platform='patreon',
            filters={'search_keywords': ['obscure_topic']},
        )
        result = adapter.run([], run)

        assert result.profiles == []
        assert result.processed == 0

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_multiple_keywords_generate_multiple_queries(self, MockApify, make_run):
        """Multiple search keywords produce multiple search queries."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-123'}
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter([])
        mock_apify.dataset.return_value = mock_dataset

        adapter = PatreonDiscovery()
        run = make_run(
            platform='patreon',
            filters={'search_keywords': ['travel', 'adventure'], 'location': 'Canada'},
        )
        adapter.run([], run)

        call_args = mock_actor.call.call_args[1]['run_input']
        assert call_args['searchQueries'] == ['travel Canada', 'adventure Canada']


# ── FacebookDiscovery ───────────────────────────────────────────────────────

class TestFacebookDiscovery:
    """FacebookDiscovery scrapes Google for Facebook Groups."""

    def test_is_stage_adapter_subclass(self):
        assert issubclass(FacebookDiscovery, StageAdapter)

    def test_class_attributes(self):
        adapter = FacebookDiscovery()
        assert adapter.platform == 'facebook'
        assert adapter.stage == 'discovery'
        assert 'Apify' in adapter.apis

    def test_estimate_cost(self):
        adapter = FacebookDiscovery()
        assert adapter.estimate_cost(100) == pytest.approx(1.0)
        assert adapter.estimate_cost(0) == 0.0

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_returns_groups(self, MockApify, make_run):
        """Successful run returns normalized group profiles."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-456'}

        items = [{
            'organicResults': [
                {
                    'url': 'https://www.facebook.com/groups/travelenthusiasts/?ref=share',
                    'title': 'Travel Enthusiasts | Facebook',
                    'description': 'A group for people who love to travel. 5.2K members.',
                },
                {
                    'url': 'https://www.facebook.com/groups/hikers/',
                    'title': 'Hikers - Facebook',
                    'description': 'Group for hikers. 1,200 members. 10 posts a month.',
                },
            ]
        }]
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter(items)
        mock_apify.dataset.return_value = mock_dataset

        adapter = FacebookDiscovery()
        run = make_run(
            platform='facebook',
            filters={'keywords': ['travel'], 'max_results': 100},
        )
        result = adapter.run([], run)

        assert isinstance(result, StageResult)
        assert len(result.profiles) == 2
        assert result.processed == 2

        # Check first profile structure
        p0 = result.profiles[0]
        assert p0['group_name'] == 'Travel Enthusiasts'
        assert 'facebook.com/groups/travelenthusiasts' in p0['group_url']
        assert p0['facebook_url'] == p0['group_url']
        assert p0['instagram_url'] is None
        assert p0['youtube_url'] is None

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_deduplicates_groups_by_url(self, MockApify, make_run):
        """Duplicate group URLs across search results are deduplicated."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-456'}

        same_group = {
            'url': 'https://www.facebook.com/groups/travel/',
            'title': 'Travel Group | Facebook',
            'description': 'A travel group.',
        }
        items = [
            {'organicResults': [same_group, same_group]},
            {'organicResults': [same_group]},
        ]
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter(items)
        mock_apify.dataset.return_value = mock_dataset

        adapter = FacebookDiscovery()
        run = make_run(
            platform='facebook',
            filters={'keywords': ['travel'], 'max_results': 100},
        )
        result = adapter.run([], run)

        assert len(result.profiles) == 1

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_respects_max_results(self, MockApify, make_run):
        """Stops collecting groups once max_results is reached."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-456'}

        results = [
            {
                'url': f'https://www.facebook.com/groups/group{i}/',
                'title': f'Group {i} | Facebook',
                'description': f'Group {i} desc',
            }
            for i in range(20)
        ]
        items = [{'organicResults': results}]
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter(items)
        mock_apify.dataset.return_value = mock_dataset

        adapter = FacebookDiscovery()
        run = make_run(
            platform='facebook',
            filters={'keywords': ['test'], 'max_results': 5},
        )
        result = adapter.run([], run)

        assert len(result.profiles) == 5

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_skips_non_facebook_group_urls(self, MockApify, make_run):
        """URLs that don't contain 'facebook.com/groups/' are skipped."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-456'}

        items = [{
            'organicResults': [
                {
                    'url': 'https://www.facebook.com/somepage',
                    'title': 'Not a group',
                    'description': 'Regular page.',
                },
                {
                    'url': 'https://www.facebook.com/groups/realgroup/',
                    'title': 'Real Group | Facebook',
                    'description': 'Actual group.',
                },
                {
                    'url': 'https://www.example.com/groups/fake/',
                    'title': 'Fake',
                    'description': 'Not Facebook.',
                },
            ]
        }]
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter(items)
        mock_apify.dataset.return_value = mock_dataset

        adapter = FacebookDiscovery()
        run = make_run(
            platform='facebook',
            filters={'keywords': ['test'], 'max_results': 100},
        )
        result = adapter.run([], run)

        assert len(result.profiles) == 1
        assert 'realgroup' in result.profiles[0]['group_url']

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', None)
    def test_run_raises_when_token_missing(self, make_run):
        """Raises ValueError when APIFY_API_TOKEN is not set."""
        adapter = FacebookDiscovery()
        run = make_run(platform='facebook', filters={'keywords': ['travel']})
        with pytest.raises(ValueError, match="APIFY_API_TOKEN"):
            adapter.run([], run)

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    def test_run_raises_when_keywords_missing(self, make_run):
        """Raises ValueError when keywords filter is not provided."""
        adapter = FacebookDiscovery()
        run = make_run(platform='facebook', filters={})
        with pytest.raises(ValueError, match="keywords required"):
            adapter.run([], run)

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    def test_run_raises_when_keywords_empty(self, make_run):
        """Raises ValueError when keywords list is empty."""
        adapter = FacebookDiscovery()
        run = make_run(platform='facebook', filters={'keywords': []})
        with pytest.raises(ValueError, match="keywords required"):
            adapter.run([], run)

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_adds_visibility_suffix_for_public(self, MockApify, make_run):
        """visibility='public' adds 'public group' suffix to queries."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-456'}
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter([])
        mock_apify.dataset.return_value = mock_dataset

        adapter = FacebookDiscovery()
        run = make_run(
            platform='facebook',
            filters={'keywords': ['travel'], 'visibility': 'public'},
        )
        adapter.run([], run)

        call_args = mock_actor.call.call_args[1]['run_input']
        queries = call_args['queries']
        # All queries should contain 'public group'
        for q in queries.split('\n'):
            assert '"public group"' in q

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_adds_visibility_suffix_for_private(self, MockApify, make_run):
        """visibility='private' adds 'private group' suffix to queries."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-456'}
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter([])
        mock_apify.dataset.return_value = mock_dataset

        adapter = FacebookDiscovery()
        run = make_run(
            platform='facebook',
            filters={'keywords': ['travel'], 'visibility': 'private'},
        )
        adapter.run([], run)

        call_args = mock_actor.call.call_args[1]['run_input']
        queries = call_args['queries']
        for q in queries.split('\n'):
            assert '"private group"' in q

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_no_visibility_suffix_for_all(self, MockApify, make_run):
        """visibility='all' does not add any suffix to queries."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-456'}
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter([])
        mock_apify.dataset.return_value = mock_dataset

        adapter = FacebookDiscovery()
        run = make_run(
            platform='facebook',
            filters={'keywords': ['travel'], 'visibility': 'all'},
        )
        adapter.run([], run)

        call_args = mock_actor.call.call_args[1]['run_input']
        queries = call_args['queries']
        for q in queries.split('\n'):
            assert '"public group"' not in q
            assert '"private group"' not in q

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_limits_google_queries_to_fifteen(self, MockApify, make_run):
        """Google queries are capped at 15 regardless of keyword count."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-456'}
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter([])
        mock_apify.dataset.return_value = mock_dataset

        adapter = FacebookDiscovery()
        # 10 keywords * 3 query patterns = 30 queries, should be capped at 15
        run = make_run(
            platform='facebook',
            filters={'keywords': [f'kw{i}' for i in range(10)]},
        )
        adapter.run([], run)

        call_args = mock_actor.call.call_args[1]['run_input']
        queries = call_args['queries'].split('\n')
        assert len(queries) == 15

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_strips_facebook_suffix_from_title(self, MockApify, make_run):
        """Title cleaning removes ' | Facebook' and ' - Facebook' suffixes."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-456'}

        items = [{
            'organicResults': [
                {
                    'url': 'https://www.facebook.com/groups/group1/',
                    'title': 'Travel Lovers | Facebook',
                    'description': 'Desc',
                },
                {
                    'url': 'https://www.facebook.com/groups/group2/',
                    'title': 'Adventure Group - Facebook',
                    'description': 'Desc',
                },
            ]
        }]
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter(items)
        mock_apify.dataset.return_value = mock_dataset

        adapter = FacebookDiscovery()
        run = make_run(
            platform='facebook',
            filters={'keywords': ['travel'], 'max_results': 100},
        )
        result = adapter.run([], run)

        assert result.profiles[0]['group_name'] == 'Travel Lovers'
        assert result.profiles[1]['group_name'] == 'Adventure Group'

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_truncates_description_to_2000_chars(self, MockApify, make_run):
        """Description is truncated to 2000 characters."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-456'}

        long_desc = 'x' * 3000
        items = [{
            'organicResults': [{
                'url': 'https://www.facebook.com/groups/testgroup/',
                'title': 'Test Group',
                'description': long_desc,
            }]
        }]
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter(items)
        mock_apify.dataset.return_value = mock_dataset

        adapter = FacebookDiscovery()
        run = make_run(
            platform='facebook',
            filters={'keywords': ['test'], 'max_results': 100},
        )
        result = adapter.run([], run)

        assert len(result.profiles[0]['description']) == 2000

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_handles_empty_organic_results(self, MockApify, make_run):
        """Items with no organicResults return zero profiles."""
        mock_apify = MockApify.return_value
        mock_actor = MagicMock()
        mock_apify.actor.return_value = mock_actor
        mock_actor.call.return_value = {'defaultDatasetId': 'ds-456'}

        items = [
            {'organicResults': []},
            {},  # no organicResults key at all
        ]
        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter(items)
        mock_apify.dataset.return_value = mock_dataset

        adapter = FacebookDiscovery()
        run = make_run(
            platform='facebook',
            filters={'keywords': ['empty'], 'max_results': 100},
        )
        result = adapter.run([], run)

        assert result.profiles == []
        assert result.processed == 0

    @patch('app.pipeline.discovery.APIFY_API_TOKEN', 'test-token')
    @patch('apify_client.ApifyClient')
    def test_run_handles_none_filters(self, MockApify, make_run):
        """When run.filters is None, raises because keywords are required."""
        adapter = FacebookDiscovery()
        run = make_run(platform='facebook', filters=None)
        with pytest.raises(ValueError, match="keywords required"):
            adapter.run([], run)


# ── Integration ─────────────────────────────────────────────────────────────

class TestIntegration:
    """Cross-cutting checks across all discovery adapters."""

    def test_all_adapters_return_stage_result(self, make_run):
        """Every adapter in the registry can be instantiated."""
        for platform, cls in ADAPTERS.items():
            adapter = cls()
            assert isinstance(adapter, StageAdapter)
            assert adapter.platform == platform
            assert adapter.stage == 'discovery'

    def test_all_adapters_have_estimate_cost(self):
        """Every adapter implements estimate_cost with non-negative result."""
        for platform, cls in ADAPTERS.items():
            adapter = cls()
            cost = adapter.estimate_cost(100)
            assert cost >= 0.0, f"{platform} estimate_cost returned negative"
