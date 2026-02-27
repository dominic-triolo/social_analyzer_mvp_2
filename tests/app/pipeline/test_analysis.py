"""Tests for app.pipeline.analysis — deep content analysis per platform."""
import json
import pytest
from unittest.mock import patch, MagicMock, call

from app.pipeline.analysis import (
    analyze_thumbnail_evidence,
    analyze_selected_content,
    gather_evidence,
    InstagramAnalysis,
    PatreonAnalysis,
    FacebookAnalysis,
    ADAPTERS,
)
from app.pipeline.base import StageResult


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_openai_response(content_dict):
    """Build a MagicMock that mimics an OpenAI chat completion response."""
    mock_response = MagicMock()
    mock_response.choices[0].message.content = json.dumps(content_dict)
    return mock_response


def _make_openai_client(**response_overrides):
    """Build a mock OpenAI client that returns a configurable JSON response."""
    default_response = {
        "creator_visibility": {"visible_in_content": True, "frequency": "most", "confidence": 0.9},
        "niche_consistency": {"consistent_theme": True, "niche_description": "Travel photography", "confidence": 0.85},
        "event_promotion": {"evidence_found": False, "post_count": 0, "confidence": 0.3},
        "audience_engagement_cues": {"invitational_language": True, "post_count": 2, "confidence": 0.6},
    }
    default_response.update(response_overrides)
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = _make_openai_response(default_response)
    return mock_client


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_openai_client():
    """Factory that returns a mock OpenAI client with configurable response."""
    return _make_openai_client


@pytest.fixture
def sample_content_items():
    """12 content items resembling Instagram posts from the pipeline."""
    items = []
    for i in range(12):
        items.append({
            'format': 'IMAGE' if i % 3 != 0 else 'VIDEO',
            'media_url': f'https://cdn.instagram.com/media_{i}.jpg',
            'thumbnail_url': f'https://cdn.instagram.com/thumb_{i}.jpg',
            'description': f'Check out this amazing post #{i}! #travel #adventure',
            'title': f'Post {i}',
            'is_pinned': i == 0,
            'likes_and_views_disabled': i == 11,
            'engagement': {
                'like_count': 200 + i * 50,
                'comment_count': 20 + i * 5,
            },
            'content_group_media': [
                {'media_url': f'https://cdn.instagram.com/carousel_{i}_0.jpg'},
            ],
        })
    return items


@pytest.fixture
def sample_engagement_data():
    """Engagement data for 12 posts."""
    return [
        {'is_pinned': False, 'likes_and_views_disabled': False,
         'engagement': {'like_count': 200, 'comment_count': 20}},
        {'is_pinned': False, 'likes_and_views_disabled': False,
         'engagement': {'like_count': 50, 'comment_count': 5}},
        {'is_pinned': True, 'likes_and_views_disabled': False,
         'engagement': {'like_count': 300, 'comment_count': 30}},
        {'is_pinned': False, 'likes_and_views_disabled': True,
         'engagement': {'like_count': 0, 'comment_count': 0}},
    ]


@pytest.fixture
def ig_profile(sample_content_items):
    """Instagram profile dict with content items ready for analysis."""
    return {
        'profile_url': 'https://instagram.com/travel_creator',
        'url': 'https://instagram.com/travel_creator',
        'contact_id': 'contact-ig-001',
        'id': 'contact-ig-001',
        'bio': 'Travel photographer | Retreat host | DM for collabs',
        '_content_items': sample_content_items,
        '_selected_indices': [1, 2, 3],
    }


@pytest.fixture
def patreon_profile():
    """Patreon profile dict ready for analysis."""
    return {
        'creator_name': 'Adventure Creators',
        'name': 'Adventure Creators',
        'about': 'We help aspiring travel photographers build their dream careers.',
        'summary': 'Travel photography community',
        'description': 'Join our Patreon for exclusive travel tips',
        'patron_count': 850,
        'total_members': 850,
        'post_count': 120,
        'total_posts': 120,
        'tiers': [
            {'name': 'Explorer', 'price': 5},
            {'name': 'Adventurer', 'price': 15},
            {'name': 'VIP Traveler', 'price': 50},
        ],
        'instagram_url': 'https://instagram.com/adventure_creators',
        'youtube_url': 'https://youtube.com/adventurecreators',
        'email': 'hello@adventurecreators.com',
        'personal_website': 'https://adventurecreators.com',
    }


@pytest.fixture
def fb_profile():
    """Facebook group profile dict ready for analysis."""
    return {
        'group_name': 'Wanderlust Women Travel Club',
        'description': 'A community for women who love to explore the world together.',
        'member_count': 12500,
        'posts_per_month': 45,
        'creator_name': 'Jane Explorer',
        'email': 'jane@wanderlustwomen.com',
        'personal_website': 'https://wanderlustwomen.com',
    }


# ── analyze_thumbnail_evidence ───────────────────────────────────────────────

class TestAnalyzeThumbnailEvidence:
    """analyze_thumbnail_evidence() extracts structured evidence from a thumbnail grid."""

    def test_empty_thumbnail_urls_returns_defaults(self):
        """No thumbnails returns a default dict with all keys zeroed out."""
        result = analyze_thumbnail_evidence([], [], 'contact-001')

        assert result['creator_visibility']['visible_in_content'] is False
        assert result['creator_visibility']['frequency'] == 'none'
        assert result['niche_consistency']['consistent_theme'] is False
        assert result['event_promotion']['evidence_found'] is False
        assert result['engagement_metrics']['posts_above_threshold'] == 0

    def test_calls_openai_with_grid_image(self, mock_openai_client):
        """Sends the thumbnail grid URL to GPT-4o for vision analysis."""
        thumb_urls = ['https://cdn.example.com/thumb1.jpg', 'https://cdn.example.com/thumb2.jpg']
        engagement_data = [
            {'is_pinned': False, 'likes_and_views_disabled': False,
             'engagement': {'like_count': 200, 'comment_count': 20}},
            {'is_pinned': False, 'likes_and_views_disabled': False,
             'engagement': {'like_count': 50, 'comment_count': 5}},
        ]

        mock_client = mock_openai_client()
        with patch('app.pipeline.analysis.client', mock_client), \
             patch('app.pipeline.analysis.create_thumbnail_grid', return_value='https://r2.example.com/grid.jpg'):
            result = analyze_thumbnail_evidence(thumb_urls, engagement_data, 'contact-001')

        mock_client.chat.completions.create.assert_called_once()
        assert 'creator_visibility' in result
        assert 'niche_consistency' in result
        assert 'engagement_metrics' in result

    def test_engagement_metrics_counted_correctly(self, mock_openai_client):
        """Above/below threshold and hidden posts are counted from engagement data."""
        engagement_data = [
            # Above threshold: likes >= 150 AND comments >= 15
            {'is_pinned': False, 'likes_and_views_disabled': False,
             'engagement': {'like_count': 200, 'comment_count': 20}},
            # Below threshold
            {'is_pinned': False, 'likes_and_views_disabled': False,
             'engagement': {'like_count': 50, 'comment_count': 5}},
            # Hidden
            {'is_pinned': False, 'likes_and_views_disabled': True,
             'engagement': {}},
            # Pinned -- should be skipped entirely
            {'is_pinned': True, 'likes_and_views_disabled': False,
             'engagement': {'like_count': 300, 'comment_count': 30}},
        ]

        with patch('app.pipeline.analysis.client', mock_openai_client()), \
             patch('app.pipeline.analysis.create_thumbnail_grid', return_value='https://r2.example.com/grid.jpg'):
            result = analyze_thumbnail_evidence(
                ['https://thumb1.jpg'], engagement_data, 'contact-001'
            )

        metrics = result['engagement_metrics']
        assert metrics['posts_above_threshold'] == 1
        assert metrics['posts_below_threshold'] == 1
        assert metrics['posts_hidden'] == 1
        assert metrics['posts_analyzed'] == 4

    def test_none_like_count_treated_as_zero(self, mock_openai_client):
        """None values in engagement data default to 0."""
        engagement_data = [
            {'is_pinned': False, 'likes_and_views_disabled': False,
             'engagement': {'like_count': None, 'comment_count': None}},
        ]

        with patch('app.pipeline.analysis.client', mock_openai_client()), \
             patch('app.pipeline.analysis.create_thumbnail_grid', return_value='https://r2.example.com/grid.jpg'):
            result = analyze_thumbnail_evidence(
                ['https://thumb1.jpg'], engagement_data, 'contact-001'
            )

        assert result['engagement_metrics']['posts_below_threshold'] == 1

    def test_boundary_engagement_values(self, mock_openai_client):
        """Exactly at threshold (150 likes, 15 comments) counts as above."""
        engagement_data = [
            {'is_pinned': False, 'likes_and_views_disabled': False,
             'engagement': {'like_count': 150, 'comment_count': 15}},
        ]

        with patch('app.pipeline.analysis.client', mock_openai_client()), \
             patch('app.pipeline.analysis.create_thumbnail_grid', return_value='https://r2.example.com/grid.jpg'):
            result = analyze_thumbnail_evidence(
                ['https://thumb1.jpg'], engagement_data, 'contact-001'
            )

        assert result['engagement_metrics']['posts_above_threshold'] == 1

    def test_just_below_threshold(self, mock_openai_client):
        """149 likes or 14 comments counts as below threshold."""
        engagement_data = [
            {'is_pinned': False, 'likes_and_views_disabled': False,
             'engagement': {'like_count': 149, 'comment_count': 15}},
            {'is_pinned': False, 'likes_and_views_disabled': False,
             'engagement': {'like_count': 150, 'comment_count': 14}},
        ]

        with patch('app.pipeline.analysis.client', mock_openai_client()), \
             patch('app.pipeline.analysis.create_thumbnail_grid', return_value='https://r2.example.com/grid.jpg'):
            result = analyze_thumbnail_evidence(
                ['https://thumb1.jpg'], engagement_data, 'contact-001'
            )

        assert result['engagement_metrics']['posts_below_threshold'] == 2
        assert result['engagement_metrics']['posts_above_threshold'] == 0


# ── analyze_selected_content ─────────────────────────────────────────────────

class TestAnalyzeSelectedContent:
    """analyze_selected_content() analyzes up to 3 selected content items."""

    def test_analyzes_up_to_3_items(self):
        """Only the first 3 selected indices are analyzed."""
        items = [
            {'format': 'IMAGE', 'media_url': f'https://cdn.example.com/img_{i}.jpg',
             'description': f'Post {i}', 'is_pinned': False,
             'likes_and_views_disabled': False, 'engagement': {}}
            for i in range(5)
        ]
        selected = [0, 1, 2, 3, 4]

        with patch('app.pipeline.analysis.rehost_media_on_r2', side_effect=lambda url, cid, fmt: url), \
             patch('app.pipeline.analysis.analyze_content_item', return_value={
                 'type': 'IMAGE', 'summary': 'Test analysis', 'url': 'test'}):
            result = analyze_selected_content(items, selected, 'contact-001')

        assert len(result) == 3

    def test_skips_out_of_range_indices(self):
        """Indices beyond the filtered_items list are skipped."""
        items = [
            {'format': 'IMAGE', 'media_url': 'https://cdn.example.com/img.jpg',
             'description': '', 'is_pinned': False,
             'likes_and_views_disabled': False, 'engagement': {}}
        ]
        selected = [0, 5, 10]

        with patch('app.pipeline.analysis.rehost_media_on_r2', side_effect=lambda url, cid, fmt: url), \
             patch('app.pipeline.analysis.analyze_content_item', return_value={
                 'type': 'IMAGE', 'summary': 'Test', 'url': 'test'}):
            result = analyze_selected_content(items, selected, 'contact-001')

        assert len(result) == 1

    def test_video_format_uses_media_url(self):
        """VIDEO items pass media_url with format VIDEO to rehost."""
        items = [
            {'format': 'VIDEO', 'media_url': 'https://cdn.example.com/vid.mp4',
             'thumbnail_url': 'https://cdn.example.com/thumb.jpg',
             'description': 'Video post', 'is_pinned': False,
             'likes_and_views_disabled': False, 'engagement': {}}
        ]

        with patch('app.pipeline.analysis.rehost_media_on_r2', return_value='https://r2/vid.mp4') as mock_rehost, \
             patch('app.pipeline.analysis.analyze_content_item', return_value={
                 'type': 'VIDEO', 'summary': 'Video analysis', 'url': 'test'}), \
             patch('app.pipeline.analysis.requests') as mock_requests:
            mock_requests.head.return_value = MagicMock(
                headers={'content-length': '1000000'})
            result = analyze_selected_content(items, [0], 'contact-001')

        mock_rehost.assert_called_once_with(
            'https://cdn.example.com/vid.mp4', 'contact-001', 'VIDEO')
        assert len(result) == 1

    def test_collection_format_uses_first_carousel_image(self):
        """COLLECTION items use the first content_group_media URL."""
        items = [
            {'format': 'COLLECTION',
             'media_url': 'https://cdn.example.com/main.jpg',
             'thumbnail_url': 'https://cdn.example.com/thumb.jpg',
             'content_group_media': [
                 {'media_url': 'https://cdn.example.com/carousel_0.jpg'},
                 {'media_url': 'https://cdn.example.com/carousel_1.jpg'},
             ],
             'description': 'Carousel', 'is_pinned': False,
             'likes_and_views_disabled': False, 'engagement': {}}
        ]

        with patch('app.pipeline.analysis.rehost_media_on_r2', return_value='https://r2/img.jpg') as mock_rehost, \
             patch('app.pipeline.analysis.analyze_content_item', return_value={
                 'type': 'IMAGE', 'summary': 'Carousel analysis', 'url': 'test'}):
            result = analyze_selected_content(items, [0], 'contact-001')

        mock_rehost.assert_called_once_with(
            'https://cdn.example.com/carousel_0.jpg', 'contact-001', 'IMAGE')

    def test_collection_without_group_media_falls_back_to_thumbnail(self):
        """COLLECTION with empty content_group_media uses thumbnail_url."""
        items = [
            {'format': 'COLLECTION',
             'media_url': 'https://cdn.example.com/main.jpg',
             'thumbnail_url': 'https://cdn.example.com/thumb.jpg',
             'content_group_media': [],
             'description': '', 'is_pinned': False,
             'likes_and_views_disabled': False, 'engagement': {}}
        ]

        with patch('app.pipeline.analysis.rehost_media_on_r2', return_value='https://r2/img.jpg') as mock_rehost, \
             patch('app.pipeline.analysis.analyze_content_item', return_value={
                 'type': 'IMAGE', 'summary': 'Test', 'url': 'test'}):
            result = analyze_selected_content(items, [0], 'contact-001')

        mock_rehost.assert_called_once_with(
            'https://cdn.example.com/thumb.jpg', 'contact-001', 'IMAGE')

    def test_skips_item_without_media_url(self):
        """Items with no media_url at all are skipped."""
        items = [
            {'format': 'IMAGE', 'media_url': None, 'thumbnail_url': None,
             'description': '', 'is_pinned': False,
             'likes_and_views_disabled': False, 'engagement': {}}
        ]

        result = analyze_selected_content(items, [0], 'contact-001')
        assert len(result) == 0

    def test_strips_trailing_dots_from_media_url(self):
        """Trailing dots on media URLs are stripped before processing."""
        items = [
            {'format': 'IMAGE',
             'media_url': 'https://cdn.example.com/img.jpg...',
             'description': '', 'is_pinned': False,
             'likes_and_views_disabled': False, 'engagement': {}}
        ]

        with patch('app.pipeline.analysis.rehost_media_on_r2', return_value='https://r2/img.jpg') as mock_rehost, \
             patch('app.pipeline.analysis.analyze_content_item', return_value={
                 'type': 'IMAGE', 'summary': 'Test', 'url': 'test'}):
            analyze_selected_content(items, [0], 'contact-001')

        mock_rehost.assert_called_once_with(
            'https://cdn.example.com/img.jpg', 'contact-001', 'IMAGE')

    def test_skips_oversized_videos(self):
        """Videos larger than 25MB are skipped."""
        items = [
            {'format': 'VIDEO', 'media_url': 'https://cdn.example.com/big.mp4',
             'description': '', 'is_pinned': False,
             'likes_and_views_disabled': False, 'engagement': {}}
        ]

        with patch('app.pipeline.analysis.requests') as mock_requests:
            mock_requests.head.return_value = MagicMock(
                headers={'content-length': str(30 * 1024 * 1024)})
            result = analyze_selected_content(items, [0], 'contact-001')

        assert len(result) == 0

    def test_video_size_check_failure_continues_anyway(self):
        """When HEAD request fails, video analysis is still attempted."""
        items = [
            {'format': 'VIDEO', 'media_url': 'https://cdn.example.com/vid.mp4',
             'description': '', 'is_pinned': False,
             'likes_and_views_disabled': False, 'engagement': {}}
        ]

        with patch('app.pipeline.analysis.requests') as mock_requests, \
             patch('app.pipeline.analysis.rehost_media_on_r2', return_value='https://r2/vid.mp4'), \
             patch('app.pipeline.analysis.analyze_content_item', return_value={
                 'type': 'VIDEO', 'summary': 'Test', 'url': 'test'}):
            mock_requests.head.side_effect = Exception("Connection timeout")
            result = analyze_selected_content(items, [0], 'contact-001')

        assert len(result) == 1

    def test_analysis_error_on_one_item_continues_to_next(self):
        """If analyze_content_item fails for one item, the rest are still processed."""
        items = [
            {'format': 'IMAGE', 'media_url': f'https://cdn.example.com/img_{i}.jpg',
             'description': f'Post {i}', 'is_pinned': False,
             'likes_and_views_disabled': False, 'engagement': {}}
            for i in range(3)
        ]

        call_count = [0]

        def rehost_side_effect(url, cid, fmt):
            call_count[0] += 1
            if call_count[0] == 2:
                raise Exception("R2 upload failed")
            return url

        with patch('app.pipeline.analysis.rehost_media_on_r2', side_effect=rehost_side_effect), \
             patch('app.pipeline.analysis.analyze_content_item', return_value={
                 'type': 'IMAGE', 'summary': 'Test', 'url': 'test'}):
            result = analyze_selected_content(items, [0, 1, 2], 'contact-001')

        assert len(result) == 2

    def test_attaches_metadata_to_analysis(self):
        """Each analysis result includes description, is_pinned, engagement from original item."""
        items = [
            {'format': 'IMAGE', 'media_url': 'https://cdn.example.com/img.jpg',
             'description': 'Amazing sunset', 'is_pinned': True,
             'likes_and_views_disabled': True,
             'engagement': {'like_count': 500, 'comment_count': 50}}
        ]

        with patch('app.pipeline.analysis.rehost_media_on_r2', return_value='https://r2/img.jpg'), \
             patch('app.pipeline.analysis.analyze_content_item', return_value={
                 'type': 'IMAGE', 'summary': 'Sunset photo', 'url': 'test'}):
            result = analyze_selected_content(items, [0], 'contact-001')

        assert result[0]['description'] == 'Amazing sunset'
        assert result[0]['is_pinned'] is True
        assert result[0]['likes_and_views_disabled'] is True
        assert result[0]['engagement'] == {'like_count': 500, 'comment_count': 50}

    def test_empty_selected_indices(self):
        """Empty selected_indices list returns empty results."""
        items = [{'format': 'IMAGE', 'media_url': 'https://example.com/img.jpg'}]
        result = analyze_selected_content(items, [], 'contact-001')
        assert result == []


# ── gather_evidence ──────────────────────────────────────────────────────────

class TestGatherEvidence:
    """gather_evidence() collects bio, caption, and thumbnail evidence."""

    def test_returns_three_evidence_dicts(self, sample_content_items, mock_openai_client):
        """Returns a tuple of (bio_evidence, caption_evidence, thumbnail_evidence)."""
        with patch('app.pipeline.analysis.analyze_bio_evidence', return_value={'bio': 'data'}) as mock_bio, \
             patch('app.pipeline.analysis.analyze_caption_evidence', return_value={'caption': 'data'}) as mock_cap, \
             patch('app.pipeline.analysis.analyze_thumbnail_evidence', return_value={'thumb': 'data'}) as mock_thumb:

            bio_ev, cap_ev, thumb_ev = gather_evidence(
                sample_content_items, 'Travel photographer', 'contact-001'
            )

        assert bio_ev == {'bio': 'data'}
        assert cap_ev == {'caption': 'data'}
        assert thumb_ev == {'thumb': 'data'}

    def test_limits_to_12_items(self, mock_openai_client):
        """Only the first 12 items are used for evidence gathering."""
        items = [
            {'thumbnail_url': f'https://thumb_{i}.jpg',
             'description': f'Caption {i}',
             'is_pinned': False, 'likes_and_views_disabled': False,
             'engagement': {}}
            for i in range(20)
        ]

        with patch('app.pipeline.analysis.analyze_bio_evidence', return_value={}), \
             patch('app.pipeline.analysis.analyze_caption_evidence', return_value={}) as mock_cap, \
             patch('app.pipeline.analysis.analyze_thumbnail_evidence', return_value={}) as mock_thumb:

            gather_evidence(items, 'Bio text', 'contact-001')

        # Caption evidence should receive at most 12 captions
        captions_arg = mock_cap.call_args[0][0]
        assert len(captions_arg) == 12

        # Thumbnail evidence should receive at most 12 URLs
        thumb_urls_arg = mock_thumb.call_args[0][0]
        assert len(thumb_urls_arg) == 12

    def test_truncates_captions_to_500_chars(self):
        """Long captions are truncated to 500 characters."""
        long_caption = 'A' * 1000
        items = [
            {'thumbnail_url': 'https://thumb.jpg',
             'description': long_caption,
             'is_pinned': False, 'likes_and_views_disabled': False,
             'engagement': {}}
        ]

        with patch('app.pipeline.analysis.analyze_bio_evidence', return_value={}), \
             patch('app.pipeline.analysis.analyze_caption_evidence', return_value={}) as mock_cap, \
             patch('app.pipeline.analysis.analyze_thumbnail_evidence', return_value={}):

            gather_evidence(items, 'Bio', 'contact-001')

        captions_arg = mock_cap.call_args[0][0]
        assert len(captions_arg[0]) == 500

    def test_uses_title_as_caption_fallback(self):
        """Falls back to title when description is empty."""
        items = [
            {'thumbnail_url': 'https://thumb.jpg',
             'description': '',
             'title': 'My cool title',
             'is_pinned': False, 'likes_and_views_disabled': False,
             'engagement': {}}
        ]

        with patch('app.pipeline.analysis.analyze_bio_evidence', return_value={}), \
             patch('app.pipeline.analysis.analyze_caption_evidence', return_value={}) as mock_cap, \
             patch('app.pipeline.analysis.analyze_thumbnail_evidence', return_value={}):

            gather_evidence(items, 'Bio', 'contact-001')

        captions_arg = mock_cap.call_args[0][0]
        assert captions_arg[0] == 'My cool title'

    def test_empty_items_list(self):
        """Empty items list calls evidence functions with empty data."""
        with patch('app.pipeline.analysis.analyze_bio_evidence', return_value={'empty': True}) as mock_bio, \
             patch('app.pipeline.analysis.analyze_caption_evidence', return_value={'empty': True}) as mock_cap, \
             patch('app.pipeline.analysis.analyze_thumbnail_evidence', return_value={'empty': True}) as mock_thumb:

            bio_ev, cap_ev, thumb_ev = gather_evidence([], 'Bio text', 'contact-001')

        mock_bio.assert_called_once_with('Bio text')
        mock_cap.assert_called_once_with([])
        mock_thumb.assert_called_once_with([], [], 'contact-001')

    def test_skips_items_without_thumbnail_url(self):
        """Items missing thumbnail_url are excluded from thumbnail list but still provide captions."""
        items = [
            {'thumbnail_url': None, 'description': 'Caption A',
             'is_pinned': False, 'likes_and_views_disabled': False, 'engagement': {}},
            {'thumbnail_url': 'https://thumb.jpg', 'description': 'Caption B',
             'is_pinned': False, 'likes_and_views_disabled': False, 'engagement': {}},
        ]

        with patch('app.pipeline.analysis.analyze_bio_evidence', return_value={}), \
             patch('app.pipeline.analysis.analyze_caption_evidence', return_value={}) as mock_cap, \
             patch('app.pipeline.analysis.analyze_thumbnail_evidence', return_value={}) as mock_thumb:

            gather_evidence(items, 'Bio', 'contact-001')

        thumb_urls_arg = mock_thumb.call_args[0][0]
        assert len(thumb_urls_arg) == 1
        captions_arg = mock_cap.call_args[0][0]
        assert len(captions_arg) == 2


# ── InstagramAnalysis adapter ────────────────────────────────────────────────

class TestInstagramAnalysis:
    """InstagramAnalysis.run() orchestrates IG content analysis."""

    @pytest.fixture
    def adapter(self):
        return InstagramAnalysis()

    def test_platform_and_stage_metadata(self, adapter):
        """Adapter has correct platform/stage identifiers."""
        assert adapter.platform == 'instagram'
        assert adapter.stage == 'analysis'

    def test_estimate_cost(self, adapter):
        """Cost estimate is $0.15 per profile."""
        assert adapter.estimate_cost(10) == 1.5
        assert adapter.estimate_cost(0) == 0.0

    def test_successful_analysis_attaches_all_evidence_keys(self, adapter, make_run, ig_profile):
        """Successful analysis populates _content_analyses, _bio_evidence, etc."""
        run = make_run()

        with patch('app.pipeline.analysis.analyze_selected_content', return_value=[
                 {'type': 'IMAGE', 'summary': 'Travel photo', 'url': 'test'}]), \
             patch('app.pipeline.analysis.gather_evidence', return_value=(
                 {'bio': 'evidence'}, {'caption': 'evidence'}, {'thumb': 'evidence'})), \
             patch('app.pipeline.analysis.generate_creator_profile', return_value={
                 'primary_category': 'Travel', 'content_types': 'photos'}):

            result = adapter.run([ig_profile], run)

        assert isinstance(result, StageResult)
        assert len(result.profiles) == 1
        assert result.failed == 0
        assert result.processed == 1

        profile = result.profiles[0]
        assert profile['_content_analyses'] == [{'type': 'IMAGE', 'summary': 'Travel photo', 'url': 'test'}]
        assert profile['_bio_evidence'] == {'bio': 'evidence'}
        assert profile['_caption_evidence'] == {'caption': 'evidence'}
        assert profile['_thumbnail_evidence'] == {'thumb': 'evidence'}
        assert profile['_creator_profile'] == {'primary_category': 'Travel', 'content_types': 'photos'}

    def test_profile_without_content_items_rejected(self, adapter, make_run):
        """Profile with no _content_items is skipped with an error."""
        run = make_run()
        profile = {
            'profile_url': 'https://instagram.com/empty_creator',
            'bio': 'No content',
        }

        result = adapter.run([profile], run)

        assert len(result.profiles) == 0
        assert result.failed == 1
        assert 'No content' in result.errors[0]

    def test_empty_content_analyses_skips_profile(self, adapter, make_run, ig_profile):
        """When analyze_selected_content returns empty, profile is skipped."""
        run = make_run()

        with patch('app.pipeline.analysis.analyze_selected_content', return_value=[]):
            result = adapter.run([ig_profile], run)

        assert len(result.profiles) == 0
        assert result.failed == 1
        assert 'Could not analyze content' in result.errors[0]

    def test_uses_default_selected_indices_when_missing(self, adapter, make_run, ig_profile):
        """Falls back to [0, 1, 2] when _selected_indices not present."""
        run = make_run()
        del ig_profile['_selected_indices']

        with patch('app.pipeline.analysis.analyze_selected_content', return_value=[
                 {'type': 'IMAGE', 'summary': 'Test', 'url': 'test'}]) as mock_analyze, \
             patch('app.pipeline.analysis.gather_evidence', return_value=({}, {}, {})), \
             patch('app.pipeline.analysis.generate_creator_profile', return_value={}):

            adapter.run([ig_profile], run)

        called_indices = mock_analyze.call_args[0][1]
        assert called_indices == [0, 1, 2]

    def test_error_on_one_profile_doesnt_block_others(self, adapter, make_run, ig_profile):
        """Exception on one profile does not prevent others from being analyzed."""
        run = make_run()
        good_profile = dict(ig_profile)
        bad_profile = {
            'profile_url': 'https://instagram.com/bad_creator',
            'contact_id': 'bad-001',
            'bio': 'Bad',
            '_content_items': [{'format': 'IMAGE', 'media_url': 'https://example.com/img.jpg'}],
        }

        call_count = [0]

        def analyze_side_effect(items, indices, cid):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("API timeout")
            return [{'type': 'IMAGE', 'summary': 'OK', 'url': 'test'}]

        with patch('app.pipeline.analysis.analyze_selected_content', side_effect=analyze_side_effect), \
             patch('app.pipeline.analysis.gather_evidence', return_value=({}, {}, {})), \
             patch('app.pipeline.analysis.generate_creator_profile', return_value={}):

            result = adapter.run([bad_profile, good_profile], run)

        assert len(result.profiles) == 1
        assert result.failed == 1
        assert len(result.errors) == 1

    def test_increments_stage_progress_on_success(self, adapter, make_run, ig_profile):
        """Calls run.increment_stage_progress('analysis', 'completed') on success."""
        run = make_run()

        with patch('app.pipeline.analysis.analyze_selected_content', return_value=[
                 {'type': 'IMAGE', 'summary': 'Test', 'url': 'test'}]), \
             patch('app.pipeline.analysis.gather_evidence', return_value=({}, {}, {})), \
             patch('app.pipeline.analysis.generate_creator_profile', return_value={}):

            adapter.run([ig_profile], run)

        run.increment_stage_progress.assert_any_call('analysis', 'completed')

    def test_increments_stage_progress_on_failure(self, adapter, make_run):
        """Calls run.increment_stage_progress('analysis', 'failed') on exception."""
        run = make_run()
        profile = {
            'profile_url': 'https://instagram.com/fail',
            'bio': '',
            '_content_items': [{'format': 'IMAGE'}],
        }

        with patch('app.pipeline.analysis.analyze_selected_content', side_effect=Exception("boom")):
            adapter.run([profile], run)

        run.increment_stage_progress.assert_any_call('analysis', 'failed')

    def test_empty_profiles_list(self, adapter, make_run):
        """Empty input list returns empty result with zero counts."""
        run = make_run()
        result = adapter.run([], run)

        assert result.profiles == []
        assert result.processed == 0
        assert result.failed == 0
        assert result.errors == []

    def test_uses_url_fallback_when_profile_url_missing(self, adapter, make_run, ig_profile):
        """Falls back to 'url' key when 'profile_url' is absent."""
        run = make_run()
        del ig_profile['profile_url']

        with patch('app.pipeline.analysis.analyze_selected_content', return_value=[
                 {'type': 'IMAGE', 'summary': 'Test', 'url': 'test'}]), \
             patch('app.pipeline.analysis.gather_evidence', return_value=({}, {}, {})), \
             patch('app.pipeline.analysis.generate_creator_profile', return_value={}):

            result = adapter.run([ig_profile], run)

        assert len(result.profiles) == 1


# ── PatreonAnalysis adapter ──────────────────────────────────────────────────

class TestPatreonAnalysis:
    """PatreonAnalysis.run() evaluates Patreon creators via GPT-4o text analysis."""

    @pytest.fixture
    def adapter(self):
        return PatreonAnalysis()

    @pytest.fixture
    def patreon_openai_response(self):
        """Standard GPT-4o response for Patreon analysis."""
        return {
            'niche_description': 'Travel photography education',
            'audience_type': 'identity',
            'community_signals': ['discord', 'newsletter'],
            'monetization_sophistication': 'high',
            'event_evidence': True,
            'authenticity_score': 0.85,
            'overall_assessment': 'Strong travel host fit with active community.',
        }

    def test_platform_and_stage_metadata(self, adapter):
        assert adapter.platform == 'patreon'
        assert adapter.stage == 'analysis'

    def test_estimate_cost(self, adapter):
        assert adapter.estimate_cost(10) == 1.0
        assert adapter.estimate_cost(0) == 0.0

    def test_successful_analysis_attaches_all_keys(self, adapter, make_run, patreon_profile, patreon_openai_response):
        """Successful Patreon analysis populates all required evidence keys."""
        run = make_run(platform='patreon')
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(patreon_openai_response)

        with patch('app.pipeline.analysis.client', mock_client):
            result = adapter.run([patreon_profile], run)

        assert len(result.profiles) == 1
        assert result.failed == 0

        profile = result.profiles[0]
        assert '_creator_profile' in profile
        assert '_analysis_result' in profile
        assert '_bio_evidence' in profile
        assert '_caption_evidence' in profile
        assert '_thumbnail_evidence' in profile
        assert '_content_analyses' in profile
        assert profile['_content_analyses'] == []  # No visual content for Patreon

    def test_creator_profile_shape(self, adapter, make_run, patreon_profile, patreon_openai_response):
        """_creator_profile contains niche info derived from analysis."""
        run = make_run(platform='patreon')
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(patreon_openai_response)

        with patch('app.pipeline.analysis.client', mock_client):
            result = adapter.run([patreon_profile], run)

        cp = result.profiles[0]['_creator_profile']
        assert cp['primary_category'] == 'Travel photography education'
        assert cp['audience_type'] == 'identity'
        assert cp['content_types'] == 'Patreon posts'
        assert cp['creator_presence'] == 'text-based'

    def test_bio_evidence_from_analysis(self, adapter, make_run, patreon_profile, patreon_openai_response):
        """_bio_evidence reflects GPT-4o analysis of community and monetization signals."""
        run = make_run(platform='patreon')
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(patreon_openai_response)

        with patch('app.pipeline.analysis.client', mock_client):
            result = adapter.run([patreon_profile], run)

        bio_ev = result.profiles[0]['_bio_evidence']
        assert bio_ev['niche_signals']['niche_identified'] is True
        assert bio_ev['in_person_events']['evidence_found'] is True
        assert bio_ev['community_platforms']['platforms'] == ['discord', 'newsletter']
        assert bio_ev['monetization']['evidence_found'] is True

    def test_caption_evidence_from_analysis(self, adapter, make_run, patreon_profile, patreon_openai_response):
        """_caption_evidence reflects event and community signal counts."""
        run = make_run(platform='patreon')
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(patreon_openai_response)

        with patch('app.pipeline.analysis.client', mock_client):
            result = adapter.run([patreon_profile], run)

        cap_ev = result.profiles[0]['_caption_evidence']
        assert cap_ev['in_person_events']['mention_count'] == 1  # event_evidence=True
        assert cap_ev['community_platforms']['mention_count'] == 2  # len(community_signals)
        assert cap_ev['authenticity_vulnerability']['degree'] == 0.85

    def test_thumbnail_evidence_placeholder(self, adapter, make_run, patreon_profile, patreon_openai_response):
        """_thumbnail_evidence is a placeholder with no visual content data."""
        run = make_run(platform='patreon')
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(patreon_openai_response)

        with patch('app.pipeline.analysis.client', mock_client):
            result = adapter.run([patreon_profile], run)

        thumb_ev = result.profiles[0]['_thumbnail_evidence']
        assert thumb_ev['creator_visibility']['visible_in_content'] is False
        assert thumb_ev['engagement_metrics']['posts_analyzed'] == 0

    def test_uses_about_then_summary_then_description_for_bio(self, adapter, make_run, patreon_openai_response):
        """Bio field falls through about -> summary -> description."""
        run = make_run(platform='patreon')
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(patreon_openai_response)

        # Profile with no 'about' field
        profile = {
            'creator_name': 'Test',
            'summary': 'My summary',
            'patron_count': 100,
            'post_count': 10,
            'tiers': [],
        }

        with patch('app.pipeline.analysis.client', mock_client):
            result = adapter.run([profile], run)

        prompt_content = mock_client.chat.completions.create.call_args[1]['messages'][0]['content']
        assert 'My summary' in prompt_content

    def test_error_handling_doesnt_crash(self, adapter, make_run):
        """API error on a profile produces an error entry, not a crash."""
        run = make_run(platform='patreon')
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = Exception("OpenAI rate limit")

        with patch('app.pipeline.analysis.client', mock_client):
            result = adapter.run([{'creator_name': 'Failing Creator'}], run)

        assert result.failed == 1
        assert len(result.errors) == 1
        assert 'Failing Creator' in result.errors[0]
        assert result.profiles == []

    def test_empty_profiles_list(self, adapter, make_run):
        run = make_run(platform='patreon')
        result = adapter.run([], run)
        assert result.profiles == []
        assert result.processed == 0
        assert result.failed == 0

    def test_low_monetization_sets_no_monetization_evidence(self, adapter, make_run, patreon_profile):
        """When monetization_sophistication is 'low', monetization evidence is False."""
        run = make_run(platform='patreon')
        response = {
            'niche_description': 'Casual blogging',
            'audience_type': 'interest',
            'community_signals': [],
            'monetization_sophistication': 'low',
            'event_evidence': False,
            'authenticity_score': 0.3,
            'overall_assessment': 'Weak fit.',
        }
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(response)

        with patch('app.pipeline.analysis.client', mock_client):
            result = adapter.run([patreon_profile], run)

        bio_ev = result.profiles[0]['_bio_evidence']
        assert bio_ev['monetization']['evidence_found'] is False


# ── FacebookAnalysis adapter ─────────────────────────────────────────────────

class TestFacebookAnalysis:
    """FacebookAnalysis.run() evaluates Facebook groups via GPT-4o text analysis."""

    @pytest.fixture
    def adapter(self):
        return FacebookAnalysis()

    @pytest.fixture
    def fb_openai_response(self):
        """Standard GPT-4o response for Facebook group analysis."""
        return {
            'niche_description': 'Women-focused travel community',
            'community_health': 'healthy',
            'travel_relevance': 'high',
            'admin_identifiable': True,
            'engagement_level': 'high',
            'overall_assessment': 'Active travel group with engaged admin.',
        }

    def test_platform_and_stage_metadata(self, adapter):
        assert adapter.platform == 'facebook'
        assert adapter.stage == 'analysis'

    def test_estimate_cost(self, adapter):
        assert adapter.estimate_cost(10) == 1.0
        assert adapter.estimate_cost(0) == 0.0

    def test_successful_analysis_attaches_all_keys(self, adapter, make_run, fb_profile, fb_openai_response):
        """Successful FB analysis populates all required evidence keys."""
        run = make_run(platform='facebook')
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(fb_openai_response)

        with patch('app.pipeline.analysis.client', mock_client):
            result = adapter.run([fb_profile], run)

        assert len(result.profiles) == 1
        assert result.failed == 0

        profile = result.profiles[0]
        assert '_creator_profile' in profile
        assert '_analysis_result' in profile
        assert '_bio_evidence' in profile
        assert '_caption_evidence' in profile
        assert '_thumbnail_evidence' in profile
        assert '_content_analyses' in profile
        assert profile['_content_analyses'] == []

    def test_creator_profile_shape(self, adapter, make_run, fb_profile, fb_openai_response):
        """_creator_profile reflects group analysis."""
        run = make_run(platform='facebook')
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(fb_openai_response)

        with patch('app.pipeline.analysis.client', mock_client):
            result = adapter.run([fb_profile], run)

        cp = result.profiles[0]['_creator_profile']
        assert cp['primary_category'] == 'Women-focused travel community'
        assert cp['content_types'] == 'Group posts'
        assert cp['creator_presence'] == 'group admin'
        assert cp['community_health'] == 'healthy'

    def test_bio_evidence_always_includes_facebook_group_platform(self, adapter, make_run, fb_profile, fb_openai_response):
        """_bio_evidence always lists facebook_group as a community platform."""
        run = make_run(platform='facebook')
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(fb_openai_response)

        with patch('app.pipeline.analysis.client', mock_client):
            result = adapter.run([fb_profile], run)

        bio_ev = result.profiles[0]['_bio_evidence']
        assert 'facebook_group' in bio_ev['community_platforms']['platforms']
        assert bio_ev['community_platforms']['evidence_found'] is True

    def test_thumbnail_evidence_placeholder(self, adapter, make_run, fb_profile, fb_openai_response):
        """_thumbnail_evidence is placeholder with no visual data."""
        run = make_run(platform='facebook')
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(fb_openai_response)

        with patch('app.pipeline.analysis.client', mock_client):
            result = adapter.run([fb_profile], run)

        thumb_ev = result.profiles[0]['_thumbnail_evidence']
        assert thumb_ev['creator_visibility']['visible_in_content'] is False
        assert thumb_ev['event_promotion']['evidence_found'] is False
        assert thumb_ev['engagement_metrics']['posts_analyzed'] == 0

    def test_error_handling_doesnt_crash(self, adapter, make_run):
        """API error produces an error entry, not a crash."""
        run = make_run(platform='facebook')
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = Exception("OpenAI error")

        with patch('app.pipeline.analysis.client', mock_client):
            result = adapter.run([{'group_name': 'Failing Group'}], run)

        assert result.failed == 1
        assert 'Failing Group' in result.errors[0]
        assert result.profiles == []

    def test_empty_profiles_list(self, adapter, make_run):
        run = make_run(platform='facebook')
        result = adapter.run([], run)
        assert result.profiles == []
        assert result.processed == 0
        assert result.failed == 0

    def test_missing_optional_fields_dont_crash(self, adapter, make_run, fb_openai_response):
        """Profile with minimal fields still analyzes successfully."""
        run = make_run(platform='facebook')
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(fb_openai_response)

        minimal_profile = {'group_name': 'Minimal Group'}

        with patch('app.pipeline.analysis.client', mock_client):
            result = adapter.run([minimal_profile], run)

        assert len(result.profiles) == 1

    def test_increments_stage_progress_on_success(self, adapter, make_run, fb_profile, fb_openai_response):
        run = make_run(platform='facebook')
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(fb_openai_response)

        with patch('app.pipeline.analysis.client', mock_client):
            adapter.run([fb_profile], run)

        run.increment_stage_progress.assert_any_call('analysis', 'completed')

    def test_increments_stage_progress_on_failure(self, adapter, make_run):
        run = make_run(platform='facebook')
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = Exception("boom")

        with patch('app.pipeline.analysis.client', mock_client):
            adapter.run([{'group_name': 'Fail'}], run)

        run.increment_stage_progress.assert_any_call('analysis', 'failed')


# ── ADAPTERS registry ────────────────────────────────────────────────────────

class TestAdapterRegistry:
    """ADAPTERS dict maps platform names to the correct adapter classes."""

    def test_all_platforms_registered(self):
        assert set(ADAPTERS.keys()) == {'instagram', 'patreon', 'facebook'}

    def test_instagram_adapter_class(self):
        assert ADAPTERS['instagram'] is InstagramAnalysis

    def test_patreon_adapter_class(self):
        assert ADAPTERS['patreon'] is PatreonAnalysis

    def test_facebook_adapter_class(self):
        assert ADAPTERS['facebook'] is FacebookAnalysis


# ── Integration ──────────────────────────────────────────────────────────────

class TestIntegration:
    """Cross-function flows that verify data shape compatibility between stages."""

    def test_ig_analysis_output_shape_matches_scoring_input(self, make_run, ig_profile):
        """Instagram analysis produces the evidence keys that the scoring stage expects."""
        run = make_run()

        with patch('app.pipeline.analysis.analyze_selected_content', return_value=[
                 {'type': 'IMAGE', 'summary': 'Travel photo', 'url': 'test'}]), \
             patch('app.pipeline.analysis.gather_evidence', return_value=(
                 {'niche_signals': {}, 'in_person_events': {}, 'community_platforms': {}, 'monetization': {}},
                 {'in_person_events': {}, 'community_platforms': {}, 'audience_engagement': {}, 'authenticity_vulnerability': {}},
                 {'creator_visibility': {}, 'niche_consistency': {}, 'event_promotion': {}, 'engagement_metrics': {}})), \
             patch('app.pipeline.analysis.generate_creator_profile', return_value={
                 'primary_category': 'Travel', 'content_types': 'reels'}):

            result = InstagramAnalysis().run([ig_profile], run)

        profile = result.profiles[0]
        # These keys are required by the scoring stage
        required_keys = ['_content_analyses', '_bio_evidence', '_caption_evidence',
                         '_thumbnail_evidence', '_creator_profile']
        for key in required_keys:
            assert key in profile, f"Missing required key: {key}"

    def test_patreon_analysis_output_compatible_with_scoring(self, make_run, patreon_profile):
        """Patreon analysis produces all evidence keys needed for scoring."""
        run = make_run(platform='patreon')
        response = {
            'niche_description': 'Education',
            'audience_type': 'identity',
            'community_signals': ['discord'],
            'monetization_sophistication': 'high',
            'event_evidence': True,
            'authenticity_score': 0.8,
            'overall_assessment': 'Good fit.',
        }
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(response)

        with patch('app.pipeline.analysis.client', mock_client):
            result = PatreonAnalysis().run([patreon_profile], run)

        profile = result.profiles[0]
        required_keys = ['_content_analyses', '_bio_evidence', '_caption_evidence',
                         '_thumbnail_evidence', '_creator_profile']
        for key in required_keys:
            assert key in profile, f"Missing required key: {key}"

        # Verify nested structure matches scoring expectations
        assert 'niche_signals' in profile['_bio_evidence']
        assert 'in_person_events' in profile['_caption_evidence']
        assert 'engagement_metrics' in profile['_thumbnail_evidence']

    def test_facebook_analysis_output_compatible_with_scoring(self, make_run, fb_profile):
        """Facebook analysis produces all evidence keys needed for scoring."""
        run = make_run(platform='facebook')
        response = {
            'niche_description': 'Travel group',
            'community_health': 'healthy',
            'travel_relevance': 'high',
            'admin_identifiable': True,
            'engagement_level': 'high',
            'overall_assessment': 'Good fit.',
        }
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_openai_response(response)

        with patch('app.pipeline.analysis.client', mock_client):
            result = FacebookAnalysis().run([fb_profile], run)

        profile = result.profiles[0]
        required_keys = ['_content_analyses', '_bio_evidence', '_caption_evidence',
                         '_thumbnail_evidence', '_creator_profile']
        for key in required_keys:
            assert key in profile, f"Missing required key: {key}"

        assert 'engagement_metrics' in profile['_thumbnail_evidence']
        assert 'niche_signals' in profile['_bio_evidence']
