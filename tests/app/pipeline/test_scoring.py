"""Tests for app.pipeline.scoring — evidence-based scoring + tier assignment."""
import json
import pytest
from unittest.mock import patch, MagicMock

from app.pipeline.scoring import (
    load_scoring_config,
    load_category_examples,
    format_category_examples,
    calculate_engagement_penalties,
    generate_evidence_based_score,
    InstagramScoring,
    PatreonScoring,
    FacebookScoring,
    _default_config,
)
from app.pipeline.base import StageResult


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def reset_config_cache():
    """Reset module-level caches between tests so each test starts clean."""
    import app.pipeline.scoring as mod
    mod._scoring_config = None
    mod.CATEGORY_EXAMPLES = None
    yield
    mod._scoring_config = None
    mod.CATEGORY_EXAMPLES = None


@pytest.fixture
def default_config():
    """Return a fresh copy of the default config dict."""
    return _default_config()


def _make_openai_mock(**overrides):
    """Build a MagicMock that stands in for the OpenAI client.

    The real client may be None when OPENAI_API_KEY is unset, so we patch
    the whole `client` object at module level rather than traversing into
    `client.chat.completions.create`.
    """
    scores = {
        'niche_and_audience_identity': 0.7,
        'creator_authenticity_and_presence': 0.8,
        'monetization_and_business_mindset': 0.6,
        'community_infrastructure': 0.5,
        'engagement_and_connection': 0.4,
        'score_reasoning': 'Strong travel niche with active community.',
    }
    scores.update(overrides)

    mock_response = MagicMock()
    mock_response.choices[0].message.content = json.dumps(scores)

    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_response
    return mock_client


@pytest.fixture
def mock_openai():
    """Fixture returning the _make_openai_mock factory."""
    return _make_openai_mock


@pytest.fixture
def evidence_inputs():
    """Minimal evidence dicts for calling generate_evidence_based_score."""
    return dict(
        bio_evidence={
            'niche_signals': {'niche_identified': True, 'niche_description': 'Travel'},
            'in_person_events': {'evidence_found': True, 'event_types': ['retreats']},
            'community_platforms': {'evidence_found': True, 'platforms': ['newsletter']},
            'monetization': {'evidence_found': True, 'types': ['courses']},
        },
        caption_evidence={
            'in_person_events': {'mention_count': 3},
            'community_platforms': {'mention_count': 2},
            'audience_engagement': {'question_count': 4},
            'authenticity_vulnerability': {'degree': 0.7, 'post_count': 5},
        },
        thumbnail_evidence={
            'creator_visibility': {'frequency': 'high'},
            'niche_consistency': {'consistent_theme': True, 'niche_description': 'Travel'},
            'event_promotion': {'post_count': 2},
            'audience_engagement_cues': {'post_count': 3},
            'engagement_metrics': {
                'posts_above_threshold': 5,
                'posts_below_threshold': 2,
                'posts_hidden': 1,
            },
        },
        content_analyses=[
            {'type': 'reel', 'summary': 'Travel vlog from Bali', 'shows_pov': True,
             'shows_authenticity': True, 'shows_vulnerability': False},
        ],
        creator_profile={
            'primary_category': 'Travel',
            'content_types': 'reels, carousels',
            'creator_presence': 'high',
        },
        follower_count=80000,
    )


@pytest.fixture
def make_scored_profile():
    """Factory for a profile dict with all the _evidence keys an adapter expects."""
    def _make(**overrides):
        defaults = dict(
            profile_url='https://instagram.com/test_creator',
            url='https://instagram.com/test_creator',
            contact_id='contact-001',
            id='contact-001',
            bio='Adventure photographer',
            follower_count=80000,
            _bio_evidence={
                'niche_signals': {'niche_identified': True, 'niche_description': 'Travel'},
                'in_person_events': {'evidence_found': False, 'event_types': []},
                'community_platforms': {'evidence_found': False, 'platforms': []},
                'monetization': {'evidence_found': False, 'types': []},
            },
            _caption_evidence={
                'in_person_events': {'mention_count': 0},
                'community_platforms': {'mention_count': 0},
                'audience_engagement': {'question_count': 0},
                'authenticity_vulnerability': {'degree': 0.0, 'post_count': 0},
            },
            _thumbnail_evidence={
                'creator_visibility': {'frequency': 'low'},
                'niche_consistency': {'consistent_theme': False, 'niche_description': ''},
                'event_promotion': {'post_count': 0},
                'audience_engagement_cues': {'post_count': 0},
                'engagement_metrics': {
                    'posts_above_threshold': 0,
                    'posts_below_threshold': 0,
                    'posts_hidden': 0,
                },
            },
            _content_analyses=[],
            _creator_profile={'primary_category': 'Travel', 'content_types': 'reels'},
            _social_data={'data': [{'profile': {'platform_username': 'test', 'full_name': 'Test Creator', 'introduction': ''}}]},
            _has_travel_experience=False,
        )
        defaults.update(overrides)
        return defaults
    return _make


# ── load_scoring_config ──────────────────────────────────────────────────────

class TestLoadScoringConfig:
    """load_scoring_config() loads YAML with cache and fallback."""

    def test_loads_yaml_when_file_exists(self, tmp_path):
        """Reads from YAML file when present."""
        yaml_content = "version: 'test-1.0'\nweights:\n  niche_and_audience_identity: 0.40\n"
        config_file = tmp_path / 'scoring_config.yaml'
        config_file.write_text(yaml_content)

        with patch('app.pipeline.scoring.os.path.join', return_value=str(config_file)):
            cfg = load_scoring_config()

        assert cfg['version'] == 'test-1.0'
        assert cfg['weights']['niche_and_audience_identity'] == 0.40

    def test_falls_back_to_defaults_when_yaml_missing(self):
        """Returns hardcoded defaults when YAML file doesn't exist."""
        with patch('app.pipeline.scoring.os.path.join', return_value='/nonexistent/path.yaml'):
            cfg = load_scoring_config()

        assert cfg['version'] == 'default'
        assert cfg['weights']['niche_and_audience_identity'] == 0.30

    def test_caches_config_after_first_load(self, tmp_path):
        """Second call returns cached result without reading file again."""
        yaml_content = "version: 'cached'\n"
        config_file = tmp_path / 'scoring_config.yaml'
        config_file.write_text(yaml_content)

        with patch('app.pipeline.scoring.os.path.join', return_value=str(config_file)):
            first = load_scoring_config()
            config_file.write_text("version: 'new'\n")
            second = load_scoring_config()

        assert first is second
        assert second['version'] == 'cached'

    def test_falls_back_on_yaml_parse_error(self, tmp_path):
        """Malformed YAML triggers fallback to defaults."""
        config_file = tmp_path / 'scoring_config.yaml'
        config_file.write_text("invalid: yaml: content: [[[")

        with patch('app.pipeline.scoring.os.path.join', return_value=str(config_file)):
            with patch('yaml.safe_load', side_effect=Exception("parse error")):
                cfg = load_scoring_config()

        assert cfg['version'] == 'default'


# ── load_category_examples ───────────────────────────────────────────────────

class TestLoadCategoryExamples:
    """load_category_examples() loads JSON file with cache and graceful fallback."""

    def test_loads_json_when_file_exists(self, tmp_path):
        """Reads category examples from JSON file."""
        examples = {'Travel': {'good_fits': [{'handle': 'adventurer'}], 'bad_fits': []}}
        json_file = tmp_path / 'category_examples.json'
        json_file.write_text(json.dumps(examples))

        with patch('app.pipeline.scoring.os.path.join', return_value=str(json_file)):
            result = load_category_examples()

        assert 'Travel' in result
        assert result['Travel']['good_fits'][0]['handle'] == 'adventurer'

    def test_returns_empty_dict_when_file_missing(self):
        """Returns {} when JSON file doesn't exist."""
        with patch('app.pipeline.scoring.os.path.join', return_value='/nonexistent/path.json'):
            result = load_category_examples()

        assert result == {}

    def test_caches_after_first_load(self, tmp_path):
        """Second call returns cached result."""
        json_file = tmp_path / 'category_examples.json'
        json_file.write_text('{"Fitness": {}}')

        with patch('app.pipeline.scoring.os.path.join', return_value=str(json_file)):
            first = load_category_examples()
            second = load_category_examples()

        assert first is second


# ── format_category_examples ─────────────────────────────────────────────────

class TestFormatCategoryExamples:
    """format_category_examples() renders category examples as prompt text."""

    def test_returns_empty_string_for_unknown_category(self):
        """Unknown category produces empty string."""
        with patch('app.pipeline.scoring.load_category_examples', return_value={}):
            result = format_category_examples('NonExistent')
        assert result == ""

    def test_formats_good_and_bad_fits(self):
        """Includes both good and bad fit sections for a known category."""
        examples = {
            'Travel': {
                'good_fits': [
                    {'handle': 'explorer', 'niche': 'Adventure', 'why': 'Active community', 'trip_concept': 'Hiking in Patagonia'},
                ],
                'bad_fits': [
                    {'handle': 'spammer', 'niche': 'Promo', 'why': 'No real audience'},
                ],
            }
        }
        with patch('app.pipeline.scoring.load_category_examples', return_value=examples):
            result = format_category_examples('Travel')

        assert '@explorer' in result
        assert 'Adventure' in result
        assert 'Hiking in Patagonia' in result
        assert '@spammer' in result
        assert 'GOOD FIT EXAMPLES' in result
        assert 'BAD FIT EXAMPLES' in result
        assert 'CRITICAL PATTERNS' in result

    def test_handles_missing_optional_fields(self):
        """Works when good/bad fit entries lack optional fields like niche, why, trip_concept."""
        examples = {
            'Fitness': {
                'good_fits': [{'handle': 'fit_pro'}],
                'bad_fits': [{'handle': 'gym_bot'}],
            }
        }
        with patch('app.pipeline.scoring.load_category_examples', return_value=examples):
            result = format_category_examples('Fitness')

        assert '@fit_pro' in result
        assert '@gym_bot' in result

    def test_empty_good_and_bad_fits(self):
        """Category exists but has no examples in either list."""
        examples = {'Empty': {'good_fits': [], 'bad_fits': []}}
        with patch('app.pipeline.scoring.load_category_examples', return_value=examples):
            result = format_category_examples('Empty')

        assert 'GOOD FIT EXAMPLES' in result
        assert 'CRITICAL PATTERNS' in result


# ── calculate_engagement_penalties ───────────────────────────────────────────

class TestCalculateEngagementPenalties:
    """calculate_engagement_penalties() computes penalties from content analyses."""

    @pytest.fixture(autouse=True)
    def use_default_config(self):
        """Ensure default config is loaded for penalty calculations."""
        with patch('app.pipeline.scoring.load_scoring_config', return_value=_default_config()):
            yield

    def test_no_posts_returns_zero_penalties(self):
        """Empty content list means no penalties."""
        result = calculate_engagement_penalties([])
        assert result['total_penalty'] == 0.0
        assert result['hidden_count'] == 0
        assert result['low_engagement_count'] == 0

    def test_hidden_engagement_posts_penalized(self):
        """Posts with likes_and_views_disabled incur hidden penalty."""
        posts = [
            {'likes_and_views_disabled': True},
            {'likes_and_views_disabled': True},
        ]
        result = calculate_engagement_penalties(posts)
        assert result['hidden_count'] == 2
        assert result['hidden_engagement_penalty'] == 2 * 0.05

    def test_low_engagement_posts_penalized(self):
        """Posts below like/comment thresholds incur low engagement penalty."""
        posts = [
            {'engagement': {'like_count': 50, 'comment_count': 2}},
            {'engagement': {'like_count': 100, 'comment_count': 5}},
        ]
        result = calculate_engagement_penalties(posts)
        assert result['low_engagement_count'] == 2
        assert result['low_engagement_penalty'] == 2 * 0.03

    def test_pinned_posts_skipped(self):
        """Pinned posts are excluded from penalty calculations."""
        posts = [
            {'is_pinned': True, 'likes_and_views_disabled': True},
            {'engagement': {'like_count': 50, 'comment_count': 2}},
        ]
        result = calculate_engagement_penalties(posts)
        assert result['hidden_count'] == 0
        assert result['low_engagement_count'] == 1

    def test_hidden_penalty_capped(self):
        """Hidden engagement penalty doesn't exceed the configured cap (0.15)."""
        posts = [{'likes_and_views_disabled': True} for _ in range(10)]
        result = calculate_engagement_penalties(posts)
        assert result['hidden_engagement_penalty'] == 0.15

    def test_low_engagement_penalty_capped(self):
        """Low engagement penalty doesn't exceed the configured cap (0.15)."""
        posts = [{'engagement': {'like_count': 0, 'comment_count': 0}} for _ in range(20)]
        result = calculate_engagement_penalties(posts)
        assert result['low_engagement_penalty'] == 0.15

    def test_total_penalty_capped(self):
        """Combined penalties don't exceed the total cap (0.20)."""
        posts = (
            [{'likes_and_views_disabled': True} for _ in range(10)] +
            [{'engagement': {'like_count': 0, 'comment_count': 0}} for _ in range(10)]
        )
        result = calculate_engagement_penalties(posts)
        assert result['total_penalty'] == 0.20

    def test_posts_above_threshold_not_penalized(self):
        """Posts above like/comment thresholds incur no penalty."""
        posts = [
            {'engagement': {'like_count': 500, 'comment_count': 30}},
            {'engagement': {'like_count': 200, 'comment_count': 15}},
        ]
        result = calculate_engagement_penalties(posts)
        assert result['low_engagement_count'] == 0
        assert result['total_penalty'] == 0.0

    def test_none_engagement_values_treated_as_zero(self):
        """None values in like_count/comment_count default to 0."""
        posts = [{'engagement': {'like_count': None, 'comment_count': None}}]
        result = calculate_engagement_penalties(posts)
        assert result['low_engagement_count'] == 1


# ── generate_evidence_based_score ────────────────────────────────────────────

class TestGenerateEvidenceBasedScore:
    """generate_evidence_based_score() orchestrates OpenAI scoring + adjustments."""

    @pytest.fixture(autouse=True)
    def use_default_config(self):
        """Ensure default config for deterministic test results."""
        with patch('app.pipeline.scoring.load_scoring_config', return_value=_default_config()):
            yield

    @pytest.fixture(autouse=True)
    def stub_category_examples(self):
        """Prevent file I/O for category examples."""
        with patch('app.pipeline.scoring.load_category_examples', return_value={}):
            yield

    def test_happy_path_returns_all_expected_keys(self, evidence_inputs, mock_openai):
        """Result dict contains all scoring keys."""
        with patch('app.pipeline.scoring.client', mock_openai()):
            result = generate_evidence_based_score(**evidence_inputs)

        expected_keys = {
            'section_scores', 'manual_score', 'lead_score', 'follower_boost',
            'engagement_adjustment', 'category_penalty', 'priority_tier',
            'expected_precision', 'score_reasoning',
        }
        assert expected_keys == set(result.keys())

    def test_weighted_score_calculation(self, evidence_inputs, mock_openai):
        """Manual score equals weighted sum of section scores."""
        with patch('app.pipeline.scoring.client', mock_openai()):
            result = generate_evidence_based_score(**evidence_inputs)

        # niche=0.7*0.30 + auth=0.8*0.30 + monet=0.6*0.20 + comm=0.5*0.15 + eng=0.4*0.05
        # = 0.21 + 0.24 + 0.12 + 0.075 + 0.02 = 0.665
        assert abs(result['manual_score'] - 0.665) < 0.001

    def test_category_penalty_applied_for_entertainment(self, evidence_inputs, mock_openai):
        """Entertainment category gets -0.10 penalty."""
        evidence_inputs['creator_profile']['primary_category'] = 'Entertainment'
        with patch('app.pipeline.scoring.client', mock_openai()):
            result = generate_evidence_based_score(**evidence_inputs)

        assert result['category_penalty'] == -0.10
        assert abs(result['manual_score'] - 0.565) < 0.001

    def test_no_category_penalty_for_travel(self, evidence_inputs, mock_openai):
        """Travel category gets no penalty."""
        with patch('app.pipeline.scoring.client', mock_openai()):
            result = generate_evidence_based_score(**evidence_inputs)

        assert result['category_penalty'] == 0.0

    def test_follower_boost_100k(self, evidence_inputs, mock_openai):
        """100k+ followers get +0.15 boost."""
        evidence_inputs['follower_count'] = 150000
        with patch('app.pipeline.scoring.client', mock_openai()):
            result = generate_evidence_based_score(**evidence_inputs)

        assert result['follower_boost'] == 0.15

    def test_follower_boost_75k(self, evidence_inputs, mock_openai):
        """75k+ followers get +0.10 boost."""
        evidence_inputs['follower_count'] = 80000
        with patch('app.pipeline.scoring.client', mock_openai()):
            result = generate_evidence_based_score(**evidence_inputs)

        assert result['follower_boost'] == 0.10

    def test_follower_boost_50k(self, evidence_inputs, mock_openai):
        """50k+ followers get +0.05 boost."""
        evidence_inputs['follower_count'] = 55000
        with patch('app.pipeline.scoring.client', mock_openai()):
            result = generate_evidence_based_score(**evidence_inputs)

        assert result['follower_boost'] == 0.05

    def test_no_follower_boost_below_50k(self, evidence_inputs, mock_openai):
        """Below 50k followers gets no boost."""
        evidence_inputs['follower_count'] = 30000
        with patch('app.pipeline.scoring.client', mock_openai()):
            result = generate_evidence_based_score(**evidence_inputs)

        assert result['follower_boost'] == 0.0

    def test_engagement_adjustment_clamped(self, evidence_inputs, mock_openai):
        """Engagement adjustment is clamped to [-0.20, 0.20]."""
        evidence_inputs['thumbnail_evidence']['engagement_metrics'] = {
            'posts_above_threshold': 100,
            'posts_below_threshold': 0,
            'posts_hidden': 0,
        }
        with patch('app.pipeline.scoring.client', mock_openai()):
            result = generate_evidence_based_score(**evidence_inputs)

        assert result['engagement_adjustment'] == 0.20

    def test_negative_engagement_adjustment(self, evidence_inputs, mock_openai):
        """Many below-threshold posts yield negative adjustment."""
        evidence_inputs['thumbnail_evidence']['engagement_metrics'] = {
            'posts_above_threshold': 0,
            'posts_below_threshold': 10,
            'posts_hidden': 0,
        }
        with patch('app.pipeline.scoring.client', mock_openai()):
            result = generate_evidence_based_score(**evidence_inputs)

        assert result['engagement_adjustment'] == -0.20

    def test_lead_score_clamped_0_to_1(self, evidence_inputs, mock_openai):
        """Final lead_score never goes below 0.0 or above 1.0."""
        with patch('app.pipeline.scoring.client', mock_openai(
            niche_and_audience_identity=0.0,
            creator_authenticity_and_presence=0.0,
            monetization_and_business_mindset=0.0,
            community_infrastructure=0.0,
            engagement_and_connection=0.0,
        )):
            evidence_inputs['thumbnail_evidence']['engagement_metrics'] = {
                'posts_above_threshold': 0, 'posts_below_threshold': 50, 'posts_hidden': 10,
            }
            evidence_inputs['follower_count'] = 0
            result = generate_evidence_based_score(**evidence_inputs)

        assert result['lead_score'] >= 0.0

    def test_tier_auto_enroll_by_manual_score(self, evidence_inputs, mock_openai):
        """Manual score >= 0.65 -> auto_enroll tier."""
        with patch('app.pipeline.scoring.client', mock_openai()):
            result = generate_evidence_based_score(**evidence_inputs)

        assert result['priority_tier'] == 'auto_enroll'

    def test_tier_auto_enroll_by_full_score(self, evidence_inputs, mock_openai):
        """Manual score < 0.65 but full_score >= 0.80 -> auto_enroll."""
        with patch('app.pipeline.scoring.client', mock_openai(
            niche_and_audience_identity=0.6,
            creator_authenticity_and_presence=0.6,
            monetization_and_business_mindset=0.5,
            community_infrastructure=0.5,
            engagement_and_connection=0.4,
        )):
            evidence_inputs['follower_count'] = 150000
            evidence_inputs['thumbnail_evidence']['engagement_metrics'] = {
                'posts_above_threshold': 10, 'posts_below_threshold': 0, 'posts_hidden': 0,
            }
            result = generate_evidence_based_score(**evidence_inputs)

        assert result['priority_tier'] == 'auto_enroll'

    def test_tier_standard_priority_review(self, evidence_inputs, mock_openai):
        """Full score >= 0.25 but below auto_enroll -> standard_priority_review."""
        with patch('app.pipeline.scoring.client', mock_openai(
            niche_and_audience_identity=0.3,
            creator_authenticity_and_presence=0.3,
            monetization_and_business_mindset=0.2,
            community_infrastructure=0.2,
            engagement_and_connection=0.1,
        )):
            evidence_inputs['follower_count'] = 10000
            evidence_inputs['thumbnail_evidence']['engagement_metrics'] = {
                'posts_above_threshold': 0, 'posts_below_threshold': 0, 'posts_hidden': 0,
            }
            result = generate_evidence_based_score(**evidence_inputs)

        assert result['priority_tier'] == 'standard_priority_review'

    def test_tier_low_priority_review(self, evidence_inputs, mock_openai):
        """Full score < 0.25 -> low_priority_review."""
        with patch('app.pipeline.scoring.client', mock_openai(
            niche_and_audience_identity=0.1,
            creator_authenticity_and_presence=0.1,
            monetization_and_business_mindset=0.1,
            community_infrastructure=0.1,
            engagement_and_connection=0.1,
        )):
            evidence_inputs['follower_count'] = 5000
            evidence_inputs['thumbnail_evidence']['engagement_metrics'] = {
                'posts_above_threshold': 0, 'posts_below_threshold': 5, 'posts_hidden': 2,
            }
            result = generate_evidence_based_score(**evidence_inputs)

        assert result['priority_tier'] == 'low_priority_review'
        assert result['expected_precision'] == 0.0

    def test_score_reasoning_includes_adjustments(self, evidence_inputs, mock_openai):
        """Reasoning string includes follower boost and engagement info when present."""
        evidence_inputs['follower_count'] = 100000
        with patch('app.pipeline.scoring.client', mock_openai()):
            result = generate_evidence_based_score(**evidence_inputs)

        assert 'Follower boost' in result['score_reasoning']
        assert 'TIER' in result['score_reasoning']


# ── InstagramScoring adapter ─────────────────────────────────────────────────

class TestInstagramScoring:
    """InstagramScoring.run() scores profiles and handles errors."""

    @pytest.fixture
    def adapter(self):
        return InstagramScoring()

    def test_scores_profile_successfully(self, adapter, make_run, make_scored_profile, mock_openai):
        """Successful scoring populates _lead_analysis and _first_name on profile."""
        run = make_run()
        profile = make_scored_profile()

        with patch('app.pipeline.scoring.client', mock_openai()), \
             patch('app.pipeline.scoring.load_scoring_config', return_value=_default_config()), \
             patch('app.pipeline.scoring.load_category_examples', return_value={}), \
             patch('app.services.r2.save_analysis_cache'), \
             patch('app.services.openai_client.extract_first_names_from_instagram_profile', return_value='Test'):

            result = adapter.run([profile], run)

        assert isinstance(result, StageResult)
        assert len(result.profiles) == 1
        assert result.failed == 0
        assert '_lead_analysis' in result.profiles[0]
        assert '_first_name' in result.profiles[0]
        assert result.profiles[0]['_first_name'] == 'Test'

    def test_error_on_single_profile_doesnt_block_others(self, adapter, make_run, make_scored_profile, mock_openai):
        """One failing profile doesn't stop the rest from being scored."""
        run = make_run()
        good_profile = make_scored_profile(contact_id='good')
        bad_profile = make_scored_profile(contact_id='bad')
        del bad_profile['_bio_evidence']

        with patch('app.pipeline.scoring.client', mock_openai()), \
             patch('app.pipeline.scoring.load_scoring_config', return_value=_default_config()), \
             patch('app.pipeline.scoring.load_category_examples', return_value={}), \
             patch('app.services.r2.save_analysis_cache'), \
             patch('app.services.openai_client.extract_first_names_from_instagram_profile', return_value='Good'):

            result = adapter.run([bad_profile, good_profile], run)

        assert len(result.profiles) == 1
        assert result.failed == 1
        assert len(result.errors) == 1

    def test_travel_experience_floor_boost(self, adapter, make_run, make_scored_profile, mock_openai):
        """Profile with travel experience gets boosted to floor if score is below it."""
        run = make_run()
        profile = make_scored_profile(_has_travel_experience=True)

        with patch('app.pipeline.scoring.client', mock_openai(
            niche_and_audience_identity=0.2,
            creator_authenticity_and_presence=0.2,
            monetization_and_business_mindset=0.1,
            community_infrastructure=0.1,
            engagement_and_connection=0.1,
        )), \
             patch('app.pipeline.scoring.load_scoring_config', return_value=_default_config()), \
             patch('app.pipeline.scoring.load_category_examples', return_value={}), \
             patch('app.services.r2.save_analysis_cache'), \
             patch('app.services.openai_client.extract_first_names_from_instagram_profile', return_value='Trav'):

            result = adapter.run([profile], run)

        assert result.profiles[0]['_lead_analysis']['lead_score'] == 0.50
        assert 'TRAVEL EXPERIENCE BOOST' in result.profiles[0]['_lead_analysis']['score_reasoning']

    def test_empty_profiles_returns_empty_result(self, adapter, make_run):
        """No profiles in -> no profiles out, zero errors."""
        run = make_run()
        result = adapter.run([], run)
        assert result.profiles == []
        assert result.processed == 0
        assert result.failed == 0

    def test_updates_tier_distribution_on_run(self, adapter, make_run, make_scored_profile, mock_openai):
        """Run's tier_distribution gets incremented for each scored profile."""
        run = make_run()
        profile = make_scored_profile()

        with patch('app.pipeline.scoring.client', mock_openai()), \
             patch('app.pipeline.scoring.load_scoring_config', return_value=_default_config()), \
             patch('app.pipeline.scoring.load_category_examples', return_value={}), \
             patch('app.services.r2.save_analysis_cache'), \
             patch('app.services.openai_client.extract_first_names_from_instagram_profile', return_value='X'):

            adapter.run([profile], run)

        assert run.tier_distribution['auto_enroll'] == 1

    def test_estimate_cost(self, adapter):
        """Cost estimate is $0.02 per profile."""
        assert adapter.estimate_cost(100) == 2.0
        assert adapter.estimate_cost(0) == 0.0

    def test_caches_analysis_to_r2(self, adapter, make_run, make_scored_profile, mock_openai):
        """Saves analysis cache to R2 when contact_id is present."""
        run = make_run()
        profile = make_scored_profile(contact_id='cache-test')

        with patch('app.pipeline.scoring.client', mock_openai()), \
             patch('app.pipeline.scoring.load_scoring_config', return_value=_default_config()), \
             patch('app.pipeline.scoring.load_category_examples', return_value={}), \
             patch('app.services.r2.save_analysis_cache') as mock_cache, \
             patch('app.services.openai_client.extract_first_names_from_instagram_profile', return_value='C'):

            adapter.run([profile], run)

        mock_cache.assert_called_once()
        cache_data = mock_cache.call_args[0][1]
        assert cache_data['contact_id'] == 'cache-test'


# ── PatreonScoring adapter ───────────────────────────────────────────────────

class TestPatreonScoring:
    """PatreonScoring.run() uses patron_count as follower proxy."""

    @pytest.fixture
    def adapter(self):
        return PatreonScoring()

    def test_scores_profile_with_patron_count(self, adapter, make_run, mock_openai):
        """Uses patron_count as follower_count for scoring."""
        run = make_run(platform='patreon')
        profile = {
            'creator_name': 'Patreon Creator',
            'patron_count': 500,
            '_bio_evidence': {},
            '_caption_evidence': {},
            '_thumbnail_evidence': {'engagement_metrics': {}},
            '_content_analyses': [],
            '_creator_profile': {'primary_category': 'Education'},
        }

        with patch('app.pipeline.scoring.client', mock_openai()), \
             patch('app.pipeline.scoring.load_scoring_config', return_value=_default_config()), \
             patch('app.pipeline.scoring.load_category_examples', return_value={}):

            result = adapter.run([profile], run)

        assert len(result.profiles) == 1
        assert '_lead_analysis' in result.profiles[0]
        assert result.failed == 0

    def test_falls_back_to_total_members(self, adapter, make_run, mock_openai):
        """Uses total_members when patron_count is missing."""
        run = make_run(platform='patreon')
        profile = {
            'creator_name': 'Creator X',
            'total_members': 1000,
            '_bio_evidence': {},
            '_caption_evidence': {},
            '_thumbnail_evidence': {'engagement_metrics': {}},
            '_content_analyses': [],
            '_creator_profile': {'primary_category': 'Travel'},
        }

        with patch('app.pipeline.scoring.client', mock_openai()), \
             patch('app.pipeline.scoring.load_scoring_config', return_value=_default_config()), \
             patch('app.pipeline.scoring.load_category_examples', return_value={}):

            result = adapter.run([profile], run)

        assert len(result.profiles) == 1

    def test_error_doesnt_block_pipeline(self, adapter, make_run):
        """API error on one profile doesn't crash the adapter."""
        run = make_run(platform='patreon')
        profile = {'creator_name': 'Bad Profile'}

        result = adapter.run([profile], run)

        assert result.failed == 1
        assert len(result.errors) == 1
        assert result.profiles == []

    def test_estimate_cost(self, adapter):
        assert adapter.estimate_cost(50) == 1.0


# ── FacebookScoring adapter ──────────────────────────────────────────────────

class TestFacebookScoring:
    """FacebookScoring.run() uses member_count as follower proxy."""

    @pytest.fixture
    def adapter(self):
        return FacebookScoring()

    def test_scores_group_profile(self, adapter, make_run, mock_openai):
        """Scores a Facebook group using member_count."""
        run = make_run(platform='facebook')
        profile = {
            'group_name': 'Travel Lovers',
            'member_count': 5000,
            '_bio_evidence': {},
            '_caption_evidence': {},
            '_thumbnail_evidence': {'engagement_metrics': {}},
            '_content_analyses': [],
            '_creator_profile': {'primary_category': 'Travel'},
        }

        with patch('app.pipeline.scoring.client', mock_openai()), \
             patch('app.pipeline.scoring.load_scoring_config', return_value=_default_config()), \
             patch('app.pipeline.scoring.load_category_examples', return_value={}):

            result = adapter.run([profile], run)

        assert len(result.profiles) == 1
        assert result.failed == 0

    def test_error_handling(self, adapter, make_run):
        """Missing evidence keys cause error, not crash."""
        run = make_run(platform='facebook')
        profile = {'group_name': 'Bad Group'}

        result = adapter.run([profile], run)

        assert result.failed == 1
        assert result.profiles == []

    def test_estimate_cost(self, adapter):
        assert adapter.estimate_cost(200) == 4.0


# ── Integration ──────────────────────────────────────────────────────────────

class TestIntegration:
    """Cross-function flows that catch data shape mismatches."""

    def test_config_feeds_into_scoring(self, evidence_inputs, mock_openai):
        """load_scoring_config() output is consumed correctly by generate_evidence_based_score()."""
        with patch('app.pipeline.scoring.load_scoring_config', return_value=_default_config()), \
             patch('app.pipeline.scoring.load_category_examples', return_value={}), \
             patch('app.pipeline.scoring.client', mock_openai()):

            result = generate_evidence_based_score(**evidence_inputs)

        assert 0.0 <= result['lead_score'] <= 1.0
        assert result['priority_tier'] in ('auto_enroll', 'standard_priority_review', 'low_priority_review')
        assert all(dim in result['section_scores'] for dim in [
            'niche_and_audience_identity', 'creator_authenticity_and_presence',
            'monetization_and_business_mindset', 'community_infrastructure',
            'engagement_and_connection',
        ])

    def test_adapter_uses_generate_score_output_shape(self, make_run, make_scored_profile, mock_openai):
        """InstagramScoring adapter correctly consumes generate_evidence_based_score output."""
        adapter = InstagramScoring()
        run = make_run()
        profile = make_scored_profile(follower_count=100000, _has_travel_experience=True)

        with patch('app.pipeline.scoring.client', mock_openai()), \
             patch('app.pipeline.scoring.load_scoring_config', return_value=_default_config()), \
             patch('app.pipeline.scoring.load_category_examples', return_value={}), \
             patch('app.services.r2.save_analysis_cache'), \
             patch('app.services.openai_client.extract_first_names_from_instagram_profile', return_value='Integ'):

            result = adapter.run([profile], run)

        analysis = result.profiles[0]['_lead_analysis']
        assert 'lead_score' in analysis
        assert 'priority_tier' in analysis
        assert 'section_scores' in analysis
        assert result.profiles[0]['_first_name'] == 'Integ'
