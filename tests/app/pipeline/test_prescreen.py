"""Tests for app.pipeline.prescreen — quick disqualification stage."""
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock
from PIL import Image

from app.pipeline.prescreen import (
    check_post_frequency,
    create_profile_snapshot,
    check_for_travel_experience,
    pre_screen_profile,
    InstagramPrescreen,
    PatreonPrescreen,
    FacebookPrescreen,
    ADAPTERS,
)
from app.pipeline.base import StageResult


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def recent_content_items():
    """Content items with recent, evenly-spaced publish dates."""
    now = datetime.now(timezone.utc)
    return [
        {
            'published_at': (now - timedelta(days=i * 5)).isoformat(),
            'is_pinned': False,
            'thumbnail_url': None,
            'description': f'Post {i} description',
            'title': f'Post {i}',
        }
        for i in range(6)
    ]


@pytest.fixture
def stale_content_items():
    """Content items where the most recent post is older than 6 weeks."""
    old_date = datetime.now(timezone.utc) - timedelta(weeks=10)
    return [
        {
            'published_at': (old_date - timedelta(days=i * 3)).isoformat(),
            'is_pinned': False,
        }
        for i in range(5)
    ]


@pytest.fixture
def gapped_content_items():
    """Content items with a gap longer than 6 weeks between consecutive posts."""
    now = datetime.now(timezone.utc)
    return [
        {'published_at': (now - timedelta(days=1)).isoformat(), 'is_pinned': False},
        {'published_at': (now - timedelta(days=3)).isoformat(), 'is_pinned': False},
        # Big gap: next post is 50 days before the one above
        {'published_at': (now - timedelta(days=53)).isoformat(), 'is_pinned': False},
        {'published_at': (now - timedelta(days=56)).isoformat(), 'is_pinned': False},
    ]


@pytest.fixture
def profile_data_basic():
    """Minimal profile data dict for snapshot/pre-screen tests."""
    return {
        'username': 'test_creator',
        'bio': 'Yoga teacher and mindful living advocate',
        'follower_count': 45000,
        'image_url': '',
    }


@pytest.fixture
def mock_run(make_run):
    """A Run mock configured for prescreen tests."""
    return make_run(
        platform='instagram',
        filters={'max_results': 20},
    )


# ── check_post_frequency ─────────────────────────────────────────────────────

class TestCheckPostFrequency:
    """check_post_frequency() decides if a profile posts too infrequently."""

    def test_recent_evenly_spaced_posts_pass(self, recent_content_items):
        """Active profiles with regular posting should not be disqualified."""
        should_disqualify, reason = check_post_frequency(recent_content_items)
        assert should_disqualify is False
        assert reason == ""

    def test_empty_list_disqualifies(self):
        """No content items at all means disqualification."""
        should_disqualify, reason = check_post_frequency([])
        assert should_disqualify is True
        assert "No non-pinned posts" in reason

    def test_only_pinned_posts_disqualifies(self):
        """All pinned posts means no organic posting activity."""
        items = [
            {'published_at': datetime.now(timezone.utc).isoformat(), 'is_pinned': True},
            {'published_at': datetime.now(timezone.utc).isoformat(), 'is_pinned': True},
        ]
        should_disqualify, reason = check_post_frequency(items)
        assert should_disqualify is True
        assert "No non-pinned posts" in reason

    def test_stale_most_recent_post_disqualifies(self, stale_content_items):
        """Most recent post older than 6 weeks triggers disqualification."""
        should_disqualify, reason = check_post_frequency(stale_content_items)
        assert should_disqualify is True
        assert "days old" in reason

    def test_gap_between_posts_disqualifies(self, gapped_content_items):
        """A gap longer than 6 weeks between consecutive posts triggers disqualification."""
        should_disqualify, reason = check_post_frequency(gapped_content_items)
        assert should_disqualify is True
        assert "Gap of" in reason

    def test_no_valid_dates_disqualifies(self):
        """Items without published_at fields should disqualify."""
        items = [
            {'is_pinned': False},
            {'is_pinned': False, 'published_at': None},
        ]
        should_disqualify, reason = check_post_frequency(items)
        assert should_disqualify is True
        assert "No valid publish dates" in reason

    def test_single_recent_post_passes(self):
        """A single recent post with no gap should pass."""
        items = [
            {'published_at': datetime.now(timezone.utc).isoformat(), 'is_pinned': False},
        ]
        should_disqualify, reason = check_post_frequency(items)
        assert should_disqualify is False

    def test_pinned_posts_are_excluded_from_check(self):
        """Pinned posts should be filtered out; only non-pinned dates matter."""
        now = datetime.now(timezone.utc)
        items = [
            {'published_at': (now - timedelta(weeks=20)).isoformat(), 'is_pinned': True},
            {'published_at': now.isoformat(), 'is_pinned': False},
            {'published_at': (now - timedelta(days=10)).isoformat(), 'is_pinned': False},
        ]
        should_disqualify, reason = check_post_frequency(items)
        assert should_disqualify is False

    def test_z_suffix_iso_dates_work_with_utc_aware_comparison(self):
        """Dates ending in 'Z' become offset-aware and compare correctly against UTC now."""
        now = datetime.now(timezone.utc)
        items = [
            {'published_at': (now - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ'), 'is_pinned': False},
            {'published_at': (now - timedelta(days=5)).strftime('%Y-%m-%dT%H:%M:%SZ'), 'is_pinned': False},
        ]
        should_disqualify, reason = check_post_frequency(items)
        assert should_disqualify is False

    def test_exactly_six_weeks_gap_does_not_disqualify(self):
        """A gap of exactly 42 days is not greater than 6 weeks, so it passes."""
        now = datetime.now(timezone.utc)
        items = [
            {'published_at': now.isoformat(), 'is_pinned': False},
            {'published_at': (now - timedelta(weeks=6)).isoformat(), 'is_pinned': False},
        ]
        should_disqualify, reason = check_post_frequency(items)
        assert should_disqualify is False

    def test_naive_timestamps_without_timezone_pass(self):
        """Timestamps without timezone info (naive) should be treated as UTC and not crash."""
        now = datetime.now(timezone.utc)
        items = [
            {'published_at': now.strftime('%Y-%m-%dT%H:%M:%S'), 'is_pinned': False},
            {'published_at': (now - timedelta(days=5)).strftime('%Y-%m-%dT%H:%M:%S'), 'is_pinned': False},
        ]
        should_disqualify, reason = check_post_frequency(items)
        assert should_disqualify is False

    def test_mixed_naive_and_aware_timestamps_pass(self):
        """Mix of naive and tz-aware timestamps should compare without error."""
        now = datetime.now(timezone.utc)
        items = [
            {'published_at': now.strftime('%Y-%m-%dT%H:%M:%SZ'), 'is_pinned': False},
            {'published_at': (now - timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%S'), 'is_pinned': False},
            {'published_at': (now - timedelta(days=7)).isoformat(), 'is_pinned': False},
        ]
        should_disqualify, reason = check_post_frequency(items)
        assert should_disqualify is False


# ── create_profile_snapshot ───────────────────────────────────────────────────

class TestCreateProfileSnapshot:
    """create_profile_snapshot() builds a composite PIL image."""

    def test_returns_image_with_expected_dimensions(self, profile_data_basic, recent_content_items):
        """Snapshot should be 1200x1600 RGB image."""
        img = create_profile_snapshot(profile_data_basic, recent_content_items)
        assert isinstance(img, Image.Image)
        assert img.size == (1200, 1600)
        assert img.mode == 'RGB'

    def test_empty_content_items_still_produces_image(self, profile_data_basic):
        """Even with no content, the snapshot renders header/bio."""
        img = create_profile_snapshot(profile_data_basic, [])
        assert isinstance(img, Image.Image)
        assert img.size == (1200, 1600)

    def test_missing_username_defaults_to_unknown(self, recent_content_items):
        """Profile data without username should fall back to 'Unknown'."""
        img = create_profile_snapshot({}, recent_content_items)
        assert isinstance(img, Image.Image)

    def test_follower_count_zero_handled(self, recent_content_items):
        """Follower count of 0 should not crash the renderer."""
        profile_data = {'username': 'test', 'follower_count': 0, 'bio': 'Test'}
        img = create_profile_snapshot(profile_data, recent_content_items)
        assert isinstance(img, Image.Image)

    def test_long_bio_truncated_gracefully(self, recent_content_items):
        """Very long bios should render without crashing."""
        profile_data = {
            'username': 'verbose_creator',
            'bio': 'word ' * 500,
            'follower_count': 10000,
        }
        img = create_profile_snapshot(profile_data, recent_content_items)
        assert isinstance(img, Image.Image)

    @patch('app.pipeline.prescreen.requests.get')
    def test_thumbnail_http_error_renders_error_box(self, mock_get, profile_data_basic):
        """Failed thumbnail downloads should render an error placeholder."""
        mock_get.side_effect = Exception("Connection timeout")
        items = [{'thumbnail_url': 'https://example.com/img.jpg', 'is_pinned': False}]
        img = create_profile_snapshot(profile_data_basic, items)
        assert isinstance(img, Image.Image)


# ── check_for_travel_experience ───────────────────────────────────────────────

class TestCheckForTravelExperience:
    """check_for_travel_experience() detects travel/retreat marketing signals."""

    def test_travel_keyword_in_bio_returns_true(self):
        """Bio mentioning a retreat should flag as travel experience."""
        assert check_for_travel_experience("Join my yoga retreat in Bali", []) is True

    def test_no_travel_keywords_returns_false(self):
        """Generic bio with no travel signals returns False."""
        assert check_for_travel_experience("I love cats and coffee", []) is False

    def test_travel_keyword_case_insensitive(self):
        """Keywords should match regardless of case."""
        assert check_for_travel_experience("BOOK NOW for our RETREAT", []) is True

    def test_content_with_travel_and_booking_indicators_returns_true(self):
        """Content items with both travel keyword + booking indicator flag."""
        items = [
            {'description': 'Join our exclusive trip — spots limited!', 'title': ''},
        ]
        assert check_for_travel_experience("no travel here", items) is True

    def test_content_with_travel_but_no_booking_indicator_returns_false(self):
        """Travel keyword in content without booking language is not enough."""
        items = [
            {'description': 'Beautiful trip to the mountains', 'title': ''},
        ]
        # Bio has no travel keywords, so it falls through to content check.
        # Content has 'trip' but no booking indicator, so returns False.
        assert check_for_travel_experience("I love cats and coffee", items) is False

    def test_empty_bio_and_content_returns_false(self):
        """Empty inputs should return False without errors."""
        assert check_for_travel_experience("", []) is False

    def test_only_checks_first_10_content_items(self):
        """Travel check should only inspect the first 10 items."""
        safe_items = [
            {'description': 'Normal post', 'title': ''}
            for _ in range(10)
        ]
        travel_item = {'description': 'Join our trip — book now!', 'title': ''}
        items = safe_items + [travel_item]
        # Bio has no travel keywords; the 11th item should not be checked
        assert check_for_travel_experience("I love cats and coffee", items) is False

    def test_title_field_checked_for_travel_keywords(self):
        """Travel keywords in 'title' (not just description) should be detected."""
        items = [
            {'description': '', 'title': 'Exclusive retreat — register now'},
        ]
        assert check_for_travel_experience("no travel here", items) is True

    def test_missing_description_and_title_does_not_crash(self):
        """Items without description/title fields should not raise."""
        items = [{'id': 123}]
        # Bio must not contain travel keywords to test the content path
        assert check_for_travel_experience("I love cats and coffee", items) is False


# ── pre_screen_profile ────────────────────────────────────────────────────────

class TestPreScreenProfile:
    """pre_screen_profile() sends a snapshot to GPT-4o for quick screening."""

    @patch('app.pipeline.prescreen.client')
    def test_reject_decision_returned(self, mock_client, profile_data_basic):
        """A 'reject' response from GPT-4o is parsed and returned."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = '{"decision": "reject", "reasoning": "Brand account", "selected_content_indices": []}'
        mock_client.chat.completions.create.return_value = mock_response

        img = Image.new('RGB', (1200, 1600), 'white')
        result = pre_screen_profile(img, profile_data_basic)

        assert result['decision'] == 'reject'
        assert 'Brand account' in result['reasoning']

    @patch('app.pipeline.prescreen.client')
    def test_continue_decision_with_indices(self, mock_client, profile_data_basic):
        """A 'continue' response includes selected content indices."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = '{"decision": "continue", "reasoning": "Good travel creator", "selected_content_indices": [0, 3, 7]}'
        mock_client.chat.completions.create.return_value = mock_response

        img = Image.new('RGB', (1200, 1600), 'white')
        result = pre_screen_profile(img, profile_data_basic)

        assert result['decision'] == 'continue'
        assert result['selected_content_indices'] == [0, 3, 7]

    @patch('app.pipeline.prescreen.client')
    def test_sends_image_as_base64(self, mock_client, profile_data_basic):
        """The image is base64-encoded and sent as an image_url content part."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = '{"decision": "continue", "reasoning": "OK", "selected_content_indices": [0]}'
        mock_client.chat.completions.create.return_value = mock_response

        img = Image.new('RGB', (100, 100), 'white')
        pre_screen_profile(img, profile_data_basic)

        call_kwargs = mock_client.chat.completions.create.call_args[1]
        user_msg = call_kwargs['messages'][1]
        image_part = user_msg['content'][1]
        assert image_part['type'] == 'image_url'
        assert image_part['image_url']['url'].startswith('data:image/png;base64,')

    @patch('app.pipeline.prescreen.client')
    def test_uses_gpt4o_model(self, mock_client, profile_data_basic):
        """The call should target the gpt-4o model."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = '{"decision": "continue", "reasoning": "OK", "selected_content_indices": []}'
        mock_client.chat.completions.create.return_value = mock_response

        img = Image.new('RGB', (100, 100), 'white')
        pre_screen_profile(img, profile_data_basic)

        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert call_kwargs['model'] == 'gpt-4o'

    @patch('app.pipeline.prescreen.client')
    def test_missing_username_defaults_to_unknown(self, mock_client):
        """Profile without username should use 'Unknown' in the prompt."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = '{"decision": "continue", "reasoning": "OK", "selected_content_indices": []}'
        mock_client.chat.completions.create.return_value = mock_response

        img = Image.new('RGB', (100, 100), 'white')
        result = pre_screen_profile(img, {})

        call_kwargs = mock_client.chat.completions.create.call_args[1]
        user_text = call_kwargs['messages'][1]['content'][0]['text']
        assert '@Unknown' in user_text


# ── InstagramPrescreen adapter ────────────────────────────────────────────────

class TestInstagramPrescreen:
    """InstagramPrescreen.run() orchestrates fetch + frequency check + GPT screen."""

    def test_metadata(self):
        """Adapter has correct platform, stage, and description."""
        adapter = InstagramPrescreen()
        assert adapter.platform == 'instagram'
        assert adapter.stage == 'pre_screen'
        assert 'GPT-4o' in adapter.description

    def test_estimate_cost_scales_with_count(self):
        """Cost estimate is 0.05 per profile."""
        adapter = InstagramPrescreen()
        assert adapter.estimate_cost(10) == pytest.approx(0.50)
        assert adapter.estimate_cost(0) == pytest.approx(0.0)

    @patch('app.pipeline.prescreen.pre_screen_profile')
    @patch('app.pipeline.prescreen.create_profile_snapshot')
    @patch('app.pipeline.prescreen.check_post_frequency', return_value=(False, ''))
    @patch('app.pipeline.prescreen.check_for_travel_experience', return_value=False)
    @patch('app.services.insightiq.filter_content_items', return_value=[{'id': 1}])
    @patch('app.services.insightiq.fetch_social_content')
    def test_profile_passes_all_checks(
        self, mock_fetch, mock_filter, mock_travel, mock_freq,
        mock_snapshot, mock_prescreen, mock_run,
    ):
        """Profile that passes frequency + GPT screen ends up in result.profiles."""
        mock_fetch.return_value = {
            'data': [{'id': 1, 'profile': {'platform_username': 'creator', 'follower_count': 50000, 'image_url': ''}}],
        }
        mock_filter.return_value = [{'id': 1}]
        mock_snapshot.return_value = Image.new('RGB', (100, 100), 'white')
        mock_prescreen.return_value = {
            'decision': 'continue',
            'reasoning': 'Looks good',
            'selected_content_indices': [0, 1, 2],
        }

        adapter = InstagramPrescreen()
        profiles = [{'url': 'https://instagram.com/creator', 'bio': 'Hello', 'follower_count': 50000}]
        result = adapter.run(profiles, mock_run)

        assert isinstance(result, StageResult)
        assert len(result.profiles) == 1
        assert result.processed == 1
        assert result.failed == 0
        assert '_content_items' in result.profiles[0]
        assert '_selected_indices' in result.profiles[0]
        assert result.meta == {}  # No filtered profiles

    @patch('app.pipeline.prescreen.check_post_frequency', return_value=(True, 'No recent posts'))
    @patch('app.services.insightiq.filter_content_items', return_value=[{'id': 1}])
    @patch('app.services.insightiq.fetch_social_content')
    def test_frequency_disqualified_profile_skipped(
        self, mock_fetch, mock_filter, mock_freq, mock_run,
    ):
        """Profile failing frequency check is skipped, not passed."""
        mock_fetch.return_value = {'data': [{'id': 1}]}

        adapter = InstagramPrescreen()
        profiles = [{'url': 'https://instagram.com/stale', 'bio': '', 'follower_count': 1000}]
        result = adapter.run(profiles, mock_run)

        assert len(result.profiles) == 0
        assert result.skipped == 1
        assert profiles[0]['_prescreen_result'] == 'disqualified'
        assert profiles[0]['_prescreen_score'] == 0.15
        assert len(result.meta['filtered']) == 1
        assert result.meta['filtered'][0]['type'] == 'disqualified'
        assert result.meta['filtered'][0]['reason'] == 'No recent posts'

    @patch('app.pipeline.prescreen.pre_screen_profile')
    @patch('app.pipeline.prescreen.create_profile_snapshot')
    @patch('app.pipeline.prescreen.check_post_frequency', return_value=(False, ''))
    @patch('app.services.insightiq.filter_content_items', return_value=[{'id': 1}])
    @patch('app.services.insightiq.fetch_social_content')
    def test_gpt_rejected_profile_skipped(
        self, mock_fetch, mock_filter, mock_freq, mock_snapshot,
        mock_prescreen, mock_run,
    ):
        """Profile rejected by GPT-4o vision is skipped."""
        mock_fetch.return_value = {
            'data': [{'id': 1, 'profile': {'platform_username': 'bad', 'follower_count': 100, 'image_url': ''}}],
        }
        mock_snapshot.return_value = Image.new('RGB', (100, 100), 'white')
        mock_prescreen.return_value = {
            'decision': 'reject',
            'reasoning': 'Meme account',
        }

        adapter = InstagramPrescreen()
        profiles = [{'url': 'https://instagram.com/memes', 'bio': '', 'follower_count': 100}]
        result = adapter.run(profiles, mock_run)

        assert len(result.profiles) == 0
        assert result.skipped == 1
        assert profiles[0]['_prescreen_result'] == 'rejected'
        assert profiles[0]['_prescreen_score'] == 0.20
        assert len(result.meta['filtered']) == 1
        assert result.meta['filtered'][0]['type'] == 'rejected'
        assert result.meta['filtered'][0]['reason'] == 'Meme account'

    @patch('app.services.insightiq.fetch_social_content')
    def test_no_content_items_skipped(self, mock_fetch, mock_run):
        """Profile with no content data from InsightIQ is skipped."""
        mock_fetch.return_value = {'data': []}

        adapter = InstagramPrescreen()
        profiles = [{'url': 'https://instagram.com/empty', 'bio': '', 'follower_count': 0}]
        result = adapter.run(profiles, mock_run)

        assert len(result.profiles) == 0
        assert result.skipped == 1
        assert len(result.meta['filtered']) == 1
        assert result.meta['filtered'][0]['type'] == 'no_content'

    @patch('app.services.insightiq.fetch_social_content')
    def test_api_error_recorded(self, mock_fetch, mock_run):
        """InsightIQ API errors are caught and added to errors list."""
        mock_fetch.side_effect = Exception("API timeout")

        adapter = InstagramPrescreen()
        profiles = [{'url': 'https://instagram.com/timeout', 'bio': '', 'follower_count': 0}]
        result = adapter.run(profiles, mock_run)

        assert len(result.profiles) == 0
        assert result.failed == 1
        assert len(result.errors) == 1
        assert 'API timeout' in result.errors[0]

    @patch('app.services.insightiq.filter_content_items', return_value=[])
    @patch('app.services.insightiq.fetch_social_content')
    def test_empty_filtered_items_skipped(self, mock_fetch, mock_filter, mock_run):
        """All items filtered out (e.g., all Stories) leads to skip."""
        mock_fetch.return_value = {'data': [{'type': 'STORY'}]}

        adapter = InstagramPrescreen()
        profiles = [{'url': 'https://instagram.com/stories_only', 'bio': '', 'follower_count': 500}]
        result = adapter.run(profiles, mock_run)

        assert len(result.profiles) == 0
        assert result.skipped == 1
        assert result.meta['filtered'][0]['type'] == 'no_content'

    @patch('app.pipeline.prescreen.pre_screen_profile')
    @patch('app.pipeline.prescreen.create_profile_snapshot')
    @patch('app.pipeline.prescreen.check_post_frequency', return_value=(False, ''))
    @patch('app.pipeline.prescreen.check_for_travel_experience', return_value=True)
    @patch('app.services.insightiq.filter_content_items', return_value=[{'id': 1}])
    @patch('app.services.insightiq.fetch_social_content')
    def test_travel_experience_flag_attached(
        self, mock_fetch, mock_filter, mock_travel, mock_freq,
        mock_snapshot, mock_prescreen, mock_run,
    ):
        """Profiles passing screen get _has_travel_experience attached."""
        mock_fetch.return_value = {
            'data': [{'id': 1, 'profile': {'platform_username': 'traveler', 'follower_count': 80000, 'image_url': ''}}],
        }
        mock_snapshot.return_value = Image.new('RGB', (100, 100), 'white')
        mock_prescreen.return_value = {
            'decision': 'continue',
            'reasoning': 'Travel creator',
            'selected_content_indices': [0],
        }

        adapter = InstagramPrescreen()
        profiles = [{'url': 'https://instagram.com/traveler', 'bio': 'Join my retreat', 'follower_count': 80000}]
        result = adapter.run(profiles, mock_run)

        assert result.profiles[0]['_has_travel_experience'] is True


# ── PatreonPrescreen adapter ─────────────────────────────────────────────────

class TestPatreonPrescreen:
    """PatreonPrescreen.run() filters by NSFW, patron count, and post count."""

    def test_metadata(self):
        adapter = PatreonPrescreen()
        assert adapter.platform == 'patreon'
        assert adapter.stage == 'pre_screen'

    def test_estimate_cost_is_zero(self):
        """Patreon prescreen has no API cost."""
        adapter = PatreonPrescreen()
        assert adapter.estimate_cost(100) == 0.0

    def test_nsfw_profiles_filtered(self, make_run):
        """NSFW profiles are always skipped."""
        run = make_run(filters={})
        adapter = PatreonPrescreen()
        profiles = [
            {'name': 'Safe Creator', 'is_nsfw': 0, 'patron_count': 100, 'post_count': 50},
            {'name': 'NSFW Creator', 'is_nsfw': 1, 'patron_count': 500, 'post_count': 100},
        ]
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        assert result.profiles[0]['name'] == 'Safe Creator'
        assert result.skipped == 1

    def test_below_min_patrons_filtered(self, make_run):
        """Profiles below minimum patron count are skipped."""
        run = make_run(filters={'min_patrons': 100})
        adapter = PatreonPrescreen()
        profiles = [
            {'name': 'Small', 'patron_count': 50, 'post_count': 20},
            {'name': 'Big', 'patron_count': 200, 'post_count': 20},
        ]
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        assert result.profiles[0]['name'] == 'Big'

    def test_above_max_patrons_filtered(self, make_run):
        """Profiles above maximum patron count are skipped."""
        run = make_run(filters={'max_patrons': 1000})
        adapter = PatreonPrescreen()
        profiles = [
            {'name': 'Medium', 'patron_count': 500, 'post_count': 20},
            {'name': 'Huge', 'patron_count': 5000, 'post_count': 20},
        ]
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        assert result.profiles[0]['name'] == 'Medium'

    def test_below_min_posts_filtered(self, make_run):
        """Profiles below minimum post count are skipped."""
        run = make_run(filters={'min_posts': 10})
        adapter = PatreonPrescreen()
        profiles = [
            {'name': 'Active', 'post_count': 25},
            {'name': 'Inactive', 'post_count': 3},
        ]
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        assert result.profiles[0]['name'] == 'Active'

    def test_no_filters_passes_all_non_nsfw(self, make_run):
        """Without filters, all non-NSFW profiles pass."""
        run = make_run(filters={})
        adapter = PatreonPrescreen()
        profiles = [
            {'name': 'A', 'patron_count': 0, 'post_count': 0},
            {'name': 'B', 'patron_count': 10000, 'post_count': 500},
        ]
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 2
        assert result.processed == 2
        assert result.skipped == 0

    def test_total_members_fallback_for_patron_count(self, make_run):
        """'total_members' is used as fallback when 'patron_count' is absent."""
        run = make_run(filters={'min_patrons': 50})
        adapter = PatreonPrescreen()
        profiles = [
            {'name': 'Alt Field', 'total_members': 100, 'post_count': 10},
        ]
        result = adapter.run(profiles, run)
        assert len(result.profiles) == 1

    def test_posts_count_fallback(self, make_run):
        """'posts_count' is used as fallback when 'post_count' is absent."""
        run = make_run(filters={'min_posts': 5})
        adapter = PatreonPrescreen()
        profiles = [
            {'name': 'Alt Posts', 'posts_count': 20},
        ]
        result = adapter.run(profiles, run)
        assert len(result.profiles) == 1

    def test_empty_profiles_returns_empty(self, make_run):
        """Empty input produces empty output with zero counts."""
        run = make_run(filters={})
        adapter = PatreonPrescreen()
        result = adapter.run([], run)

        assert len(result.profiles) == 0
        assert result.processed == 0
        assert result.skipped == 0

    def test_none_patron_count_treated_as_zero(self, make_run):
        """None patron_count should not crash; treated as 0."""
        run = make_run(filters={'min_patrons': 10})
        adapter = PatreonPrescreen()
        profiles = [{'name': 'None Count', 'patron_count': None}]
        result = adapter.run(profiles, run)
        assert len(result.profiles) == 0
        assert result.skipped == 1


# ── FacebookPrescreen adapter ────────────────────────────────────────────────

class TestFacebookPrescreen:
    """FacebookPrescreen.run() filters by member count, visibility, and posts/month."""

    def test_metadata(self):
        adapter = FacebookPrescreen()
        assert adapter.platform == 'facebook'
        assert adapter.stage == 'pre_screen'

    def test_estimate_cost_is_zero(self):
        adapter = FacebookPrescreen()
        assert adapter.estimate_cost(50) == 0.0

    def test_below_min_members_filtered(self, make_run):
        """Groups below minimum member count are skipped."""
        run = make_run(filters={'min_members': 500})
        adapter = FacebookPrescreen()
        profiles = [
            {'name': 'Small Group', 'member_count': 100},
            {'name': 'Big Group', 'member_count': 1000},
        ]
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        assert result.profiles[0]['name'] == 'Big Group'

    def test_above_max_members_filtered(self, make_run):
        """Groups above maximum member count are skipped."""
        run = make_run(filters={'max_members': 5000})
        adapter = FacebookPrescreen()
        profiles = [
            {'name': 'Medium Group', 'member_count': 2000},
            {'name': 'Huge Group', 'member_count': 50000},
        ]
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        assert result.profiles[0]['name'] == 'Medium Group'

    def test_zero_member_count_not_filtered_by_min(self, make_run):
        """member_count=0 is NOT filtered by min_members (handles unknown counts)."""
        run = make_run(filters={'min_members': 500})
        adapter = FacebookPrescreen()
        profiles = [{'name': 'Unknown Size', 'member_count': 0}]
        result = adapter.run(profiles, run)

        # The code checks `0 < mc < min_members`, so mc=0 passes
        assert len(result.profiles) == 1

    def test_public_visibility_filters_private_groups(self, make_run):
        """When visibility='public', private groups are skipped."""
        run = make_run(filters={'visibility': 'public'})
        adapter = FacebookPrescreen()
        profiles = [
            {'name': 'Open Group', '_search_title': 'Yoga Public Group', '_search_snippet': ''},
            {'name': 'Closed Group', '_search_title': 'Hiking Private Group', '_search_snippet': ''},
        ]
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        assert result.profiles[0]['name'] == 'Open Group'

    def test_private_visibility_filters_public_groups(self, make_run):
        """When visibility='private', public groups are skipped."""
        run = make_run(filters={'visibility': 'private'})
        adapter = FacebookPrescreen()
        profiles = [
            {'name': 'Secret Group', '_search_title': 'Private Group for moms', '_search_snippet': ''},
            {'name': 'Open Group', '_search_title': 'Public Group for hiking', '_search_snippet': ''},
        ]
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        assert result.profiles[0]['name'] == 'Secret Group'

    def test_visibility_all_passes_both(self, make_run):
        """When visibility='all', both public and private groups pass."""
        run = make_run(filters={'visibility': 'all'})
        adapter = FacebookPrescreen()
        profiles = [
            {'name': 'Public', '_search_title': 'Public Group', '_search_snippet': ''},
            {'name': 'Private', '_search_title': 'Private Group', '_search_snippet': ''},
        ]
        result = adapter.run(profiles, run)
        assert len(result.profiles) == 2

    def test_below_min_posts_per_month_filtered(self, make_run):
        """Groups below minimum posts/month are skipped."""
        run = make_run(filters={'min_posts_per_month': 10})
        adapter = FacebookPrescreen()
        profiles = [
            {'name': 'Active', 'member_count': 1000, 'posts_per_month': 30},
            {'name': 'Dead', 'member_count': 1000, 'posts_per_month': 2},
        ]
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        assert result.profiles[0]['name'] == 'Active'

    def test_none_posts_per_month_not_filtered(self, make_run):
        """If posts_per_month is None (unknown), the profile is not filtered."""
        run = make_run(filters={'min_posts_per_month': 10})
        adapter = FacebookPrescreen()
        profiles = [
            {'name': 'Unknown Activity', 'member_count': 1000, 'posts_per_month': None},
        ]
        result = adapter.run(profiles, run)
        assert len(result.profiles) == 1

    def test_no_filters_passes_all(self, make_run):
        """Without filters, all profiles pass."""
        run = make_run(filters={})
        adapter = FacebookPrescreen()
        profiles = [
            {'name': 'A', 'member_count': 10},
            {'name': 'B', 'member_count': 100000},
        ]
        result = adapter.run(profiles, run)
        assert len(result.profiles) == 2

    def test_empty_profiles_returns_empty(self, make_run):
        """Empty input produces empty result."""
        run = make_run(filters={'min_members': 100})
        adapter = FacebookPrescreen()
        result = adapter.run([], run)

        assert len(result.profiles) == 0
        assert result.processed == 0

    def test_combined_filters_applied(self, make_run):
        """Multiple filters are applied together."""
        run = make_run(filters={
            'min_members': 100,
            'max_members': 10000,
            'visibility': 'public',
            'min_posts_per_month': 5,
        })
        adapter = FacebookPrescreen()
        profiles = [
            # Passes all filters
            {'name': 'Perfect', 'member_count': 500, 'posts_per_month': 20,
             '_search_title': 'Public Group', '_search_snippet': ''},
            # Fails min_members
            {'name': 'Tiny', 'member_count': 10, 'posts_per_month': 20,
             '_search_title': 'Public Group', '_search_snippet': ''},
            # Fails visibility
            {'name': 'Hidden', 'member_count': 500, 'posts_per_month': 20,
             '_search_title': 'Private Group', '_search_snippet': ''},
            # Fails posts_per_month
            {'name': 'Dead', 'member_count': 500, 'posts_per_month': 1,
             '_search_title': 'Public Group', '_search_snippet': ''},
        ]
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        assert result.profiles[0]['name'] == 'Perfect'


# ── ADAPTERS registry ─────────────────────────────────────────────────────────

class TestAdaptersRegistry:
    """The ADAPTERS dict maps platform names to adapter classes."""

    def test_instagram_registered(self):
        assert ADAPTERS['instagram'] is InstagramPrescreen

    def test_patreon_registered(self):
        assert ADAPTERS['patreon'] is PatreonPrescreen

    def test_facebook_registered(self):
        assert ADAPTERS['facebook'] is FacebookPrescreen

    def test_all_adapters_are_stage_adapters(self):
        from app.pipeline.base import StageAdapter
        for platform, cls in ADAPTERS.items():
            assert issubclass(cls, StageAdapter), f"{platform} adapter is not a StageAdapter"


# ── Integration ───────────────────────────────────────────────────────────────

class TestIntegration:
    """Cross-function integration tests for the prescreen module."""

    def test_frequency_check_feeds_into_instagram_adapter(self, make_run):
        """End-to-end: content with a large gap causes the adapter to skip."""
        now = datetime.now(timezone.utc)
        content_with_gap = [
            {'published_at': now.isoformat(), 'is_pinned': False, 'type': 'POST'},
            {'published_at': (now - timedelta(days=60)).isoformat(), 'is_pinned': False, 'type': 'POST'},
        ]

        with patch('app.services.insightiq.fetch_social_content') as mock_fetch, \
             patch('app.services.insightiq.filter_content_items', return_value=content_with_gap):
            mock_fetch.return_value = {'data': content_with_gap}
            run = make_run(platform='instagram', filters={})

            adapter = InstagramPrescreen()
            profiles = [{'url': 'https://instagram.com/gappy', 'bio': '', 'follower_count': 1000}]
            result = adapter.run(profiles, run)

            assert len(result.profiles) == 0
            assert result.skipped == 1

    @patch('app.pipeline.prescreen.pre_screen_profile')
    @patch('app.pipeline.prescreen.create_profile_snapshot')
    @patch('app.pipeline.prescreen.check_for_travel_experience', return_value=False)
    @patch('app.services.insightiq.filter_content_items', return_value=[{'id': 1}])
    @patch('app.services.insightiq.fetch_social_content')
    def test_mixed_pass_and_filter_tracks_all_reasons(
        self, mock_fetch, mock_filter, mock_travel,
        mock_snapshot, mock_prescreen, make_run,
    ):
        """Multiple profiles: some pass, some filtered — meta tracks all filter reasons."""
        # Profile 1: no content → filtered
        # Profile 2: disqualified by frequency → filtered
        # Profile 3: passes all checks
        mock_fetch.side_effect = [
            {'data': []},  # no content
            {'data': [{'id': 1}]},  # has content, will be disqualified
            {'data': [{'id': 1, 'profile': {'platform_username': 'good', 'follower_count': 5000, 'image_url': ''}}]},
        ]
        mock_snapshot.return_value = Image.new('RGB', (100, 100), 'white')
        mock_prescreen.return_value = {
            'decision': 'continue', 'reasoning': 'OK',
            'selected_content_indices': [0],
        }

        run = make_run(platform='instagram', filters={})
        adapter = InstagramPrescreen()

        with patch('app.pipeline.prescreen.check_post_frequency') as mock_freq:
            # First call for profile 2 → disqualify, second call for profile 3 → pass
            mock_freq.side_effect = [(True, 'Stale posts'), (False, '')]
            profiles = [
                {'url': 'https://instagram.com/empty', 'bio': '', 'follower_count': 0},
                {'url': 'https://instagram.com/stale', 'bio': '', 'follower_count': 1000},
                {'url': 'https://instagram.com/good', 'bio': 'Hello', 'follower_count': 5000},
            ]
            result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        assert result.skipped == 2
        assert len(result.meta['filtered']) == 2
        assert result.meta['filtered'][0]['type'] == 'no_content'
        assert result.meta['filtered'][1]['type'] == 'disqualified'
        assert result.meta['filtered'][1]['reason'] == 'Stale posts'

    def test_patreon_combined_nsfw_and_patron_filter(self, make_run):
        """NSFW and patron count filters work together correctly."""
        run = make_run(filters={'min_patrons': 50, 'max_patrons': 5000})
        adapter = PatreonPrescreen()
        profiles = [
            {'name': 'Good', 'is_nsfw': 0, 'patron_count': 200, 'post_count': 10},
            {'name': 'NSFW Good Count', 'is_nsfw': 1, 'patron_count': 200, 'post_count': 10},
            {'name': 'Clean Low Count', 'is_nsfw': 0, 'patron_count': 5, 'post_count': 10},
            {'name': 'Clean High Count', 'is_nsfw': 0, 'patron_count': 10000, 'post_count': 10},
        ]
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        assert result.profiles[0]['name'] == 'Good'
