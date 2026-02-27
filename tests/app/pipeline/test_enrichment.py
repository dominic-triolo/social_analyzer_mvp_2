"""Tests for app.pipeline.enrichment — enrichment stage adapters."""
import pytest
from unittest.mock import patch, MagicMock

from app.pipeline.base import StageResult, StageAdapter
from app.pipeline.enrichment import (
    InstagramEnrichment,
    PatreonEnrichment,
    FacebookEnrichment,
    ADAPTERS,
)


# ── ADAPTERS registry ──────────────────────────────────────────────────────

class TestAdaptersRegistry:
    """The module-level ADAPTERS dict maps platform names to enrichment classes."""

    def test_registry_contains_instagram(self):
        assert ADAPTERS['instagram'] is InstagramEnrichment

    def test_registry_contains_patreon(self):
        assert ADAPTERS['patreon'] is PatreonEnrichment

    def test_registry_contains_facebook(self):
        assert ADAPTERS['facebook'] is FacebookEnrichment

    def test_registry_has_exactly_three_entries(self):
        assert len(ADAPTERS) == 3


# ── InstagramEnrichment ────────────────────────────────────────────────────

class TestInstagramEnrichment:
    """InstagramEnrichment filters profiles based on _content_items presence."""

    def test_is_stage_adapter_subclass(self):
        assert issubclass(InstagramEnrichment, StageAdapter)

    def test_class_attributes(self):
        adapter = InstagramEnrichment()
        assert adapter.platform == 'instagram'
        assert adapter.stage == 'enrichment'
        assert adapter.apis == []

    def test_estimate_cost_always_zero(self):
        """Passthrough stage has zero cost."""
        adapter = InstagramEnrichment()
        assert adapter.estimate_cost(0) == 0.0
        assert adapter.estimate_cost(100) == 0.0
        assert adapter.estimate_cost(9999) == 0.0

    def test_run_profiles_with_content_pass_through(self, make_run):
        """Profiles that have _content_items are kept."""
        adapter = InstagramEnrichment()
        profiles = [
            {'url': 'https://instagram.com/a', '_content_items': [{'id': 1}]},
            {'url': 'https://instagram.com/b', '_content_items': [{'id': 2}, {'id': 3}]},
        ]
        run = make_run(platform='instagram')
        result = adapter.run(profiles, run)

        assert isinstance(result, StageResult)
        assert len(result.profiles) == 2
        assert result.processed == 2
        assert result.failed == 0
        assert result.errors == []

    def test_run_profiles_without_content_rejected(self, make_run):
        """Profiles missing _content_items are filtered out with an error."""
        adapter = InstagramEnrichment()
        profiles = [
            {'url': 'https://instagram.com/no_content'},
        ]
        run = make_run(platform='instagram')
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 0
        assert result.processed == 1
        assert result.failed == 1
        assert len(result.errors) == 1
        assert 'no_content' in result.errors[0]

    def test_run_profiles_with_empty_content_rejected(self, make_run):
        """Profiles with empty _content_items list are filtered out."""
        adapter = InstagramEnrichment()
        profiles = [
            {'url': 'https://instagram.com/empty', '_content_items': []},
        ]
        run = make_run(platform='instagram')
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 0
        assert result.failed == 1

    def test_run_profiles_with_none_content_rejected(self, make_run):
        """Profiles with _content_items=None are filtered out."""
        adapter = InstagramEnrichment()
        profiles = [
            {'url': 'https://instagram.com/none', '_content_items': None},
        ]
        run = make_run(platform='instagram')
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 0
        assert result.failed == 1

    def test_run_mixed_profiles(self, make_run):
        """Mix of enriched and unenriched profiles is split correctly."""
        adapter = InstagramEnrichment()
        profiles = [
            {'url': 'https://instagram.com/good', '_content_items': [{'id': 1}]},
            {'url': 'https://instagram.com/bad'},
            {'url': 'https://instagram.com/also_good', '_content_items': [{'id': 2}]},
            {'url': 'https://instagram.com/also_bad', '_content_items': []},
        ]
        run = make_run(platform='instagram')
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 2
        assert result.processed == 4
        assert result.failed == 2
        assert len(result.errors) == 2

    def test_run_empty_profiles_list(self, make_run):
        """Empty input produces empty output with zero counts."""
        adapter = InstagramEnrichment()
        run = make_run(platform='instagram')
        result = adapter.run([], run)

        assert result.profiles == []
        assert result.processed == 0
        assert result.failed == 0
        assert result.errors == []

    def test_run_calls_increment_stage_progress(self, make_run):
        """Each enriched profile triggers a progress increment."""
        adapter = InstagramEnrichment()
        profiles = [
            {'url': 'a', '_content_items': [{'id': 1}]},
            {'url': 'b', '_content_items': [{'id': 2}]},
        ]
        run = make_run(platform='instagram')
        result = adapter.run(profiles, run)

        assert run.increment_stage_progress.call_count == 2
        run.increment_stage_progress.assert_any_call('enrichment', 'completed')

    def test_run_does_not_increment_progress_for_rejected(self, make_run):
        """Rejected profiles do not trigger progress increment."""
        adapter = InstagramEnrichment()
        profiles = [
            {'url': 'no_content'},
        ]
        run = make_run(platform='instagram')
        adapter.run(profiles, run)

        run.increment_stage_progress.assert_not_called()

    def test_run_error_message_includes_url(self, make_run):
        """Error message for missing content includes the profile URL."""
        adapter = InstagramEnrichment()
        profiles = [
            {'url': 'https://instagram.com/specific_user'},
        ]
        run = make_run(platform='instagram')
        result = adapter.run(profiles, run)

        assert 'specific_user' in result.errors[0]

    def test_run_error_message_shows_unknown_when_no_url(self, make_run):
        """Error message falls back to 'unknown' when profile has no url key."""
        adapter = InstagramEnrichment()
        profiles = [
            {'name': 'No URL Profile'},
        ]
        run = make_run(platform='instagram')
        result = adapter.run(profiles, run)

        assert 'unknown' in result.errors[0]

    def test_run_preserves_profile_data(self, make_run):
        """Enriched profiles retain all original fields."""
        adapter = InstagramEnrichment()
        original = {
            'url': 'https://instagram.com/test',
            'name': 'Test User',
            'follower_count': 50000,
            'bio': 'Travel lover',
            '_content_items': [{'id': 1, 'type': 'image'}],
            'custom_field': 'preserved',
        }
        run = make_run(platform='instagram')
        result = adapter.run([original], run)

        assert result.profiles[0] == original
        assert result.profiles[0]['custom_field'] == 'preserved'


# ── PatreonEnrichment ──────────────────────────────────────────────────────

class TestPatreonEnrichment:
    """PatreonEnrichment delegates to the full 11-step pipeline."""

    def test_is_stage_adapter_subclass(self):
        assert issubclass(PatreonEnrichment, StageAdapter)

    def test_class_attributes(self):
        adapter = PatreonEnrichment()
        assert adapter.platform == 'patreon'
        assert adapter.stage == 'enrichment'
        assert 'Apify' in adapter.apis
        assert 'Apollo' in adapter.apis

    def test_estimate_cost(self):
        adapter = PatreonEnrichment()
        assert adapter.estimate_cost(100) == pytest.approx(5.0)
        assert adapter.estimate_cost(0) == 0.0
        assert adapter.estimate_cost(1) == pytest.approx(0.05)

    @patch('app.services.apify.enrich_profiles_full_pipeline')
    def test_run_delegates_to_full_pipeline(self, mock_pipeline, make_run):
        """Calls enrich_profiles_full_pipeline with correct arguments."""
        enriched_profiles = [
            {'name': 'Creator A', 'email': 'a@example.com'},
            {'name': 'Creator B', 'email': 'b@example.com'},
        ]
        mock_pipeline.return_value = enriched_profiles

        adapter = PatreonEnrichment()
        profiles = [{'name': 'Creator A'}, {'name': 'Creator B'}]
        run = make_run(id='run-123', platform='patreon')
        result = adapter.run(profiles, run)

        assert isinstance(result, StageResult)
        assert result.profiles == enriched_profiles
        assert result.processed == 2
        assert result.meta == {'enrichment_steps': 11}
        mock_pipeline.assert_called_once_with(profiles, 'run-123', platform='patreon')

    @patch('app.services.apify.enrich_profiles_full_pipeline')
    def test_run_empty_profiles_short_circuits(self, mock_pipeline, make_run):
        """Empty input returns immediately without calling the pipeline."""
        adapter = PatreonEnrichment()
        run = make_run(platform='patreon')
        result = adapter.run([], run)

        assert result.profiles == []
        assert result.processed == 0
        mock_pipeline.assert_not_called()

    @patch('app.services.apify.enrich_profiles_full_pipeline')
    def test_run_pipeline_returns_fewer_profiles(self, mock_pipeline, make_run):
        """Pipeline may filter out profiles; processed count reflects input size."""
        mock_pipeline.return_value = [{'name': 'Survivor'}]

        adapter = PatreonEnrichment()
        profiles = [{'name': 'One'}, {'name': 'Two'}, {'name': 'Three'}]
        run = make_run(id='run-456', platform='patreon')
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        assert result.processed == 3

    @patch('app.services.apify.enrich_profiles_full_pipeline')
    def test_run_pipeline_returns_empty(self, mock_pipeline, make_run):
        """Pipeline returning empty list is handled gracefully."""
        mock_pipeline.return_value = []

        adapter = PatreonEnrichment()
        profiles = [{'name': 'Creator'}]
        run = make_run(id='run-789', platform='patreon')
        result = adapter.run(profiles, run)

        assert result.profiles == []
        assert result.processed == 1

    @patch('app.services.apify.enrich_profiles_full_pipeline')
    def test_run_passes_patreon_platform(self, mock_pipeline, make_run):
        """Platform argument is 'patreon' for PatreonEnrichment."""
        mock_pipeline.return_value = []

        adapter = PatreonEnrichment()
        run = make_run(id='run-001', platform='patreon')
        adapter.run([{'name': 'X'}], run)

        _, kwargs = mock_pipeline.call_args
        assert kwargs['platform'] == 'patreon'

    @patch('app.services.apify.enrich_profiles_full_pipeline')
    def test_run_single_profile(self, mock_pipeline, make_run):
        """Single profile is enriched correctly (boundary case)."""
        mock_pipeline.return_value = [{'name': 'Solo', 'email': 'solo@test.com'}]

        adapter = PatreonEnrichment()
        run = make_run(id='run-solo', platform='patreon')
        result = adapter.run([{'name': 'Solo'}], run)

        assert len(result.profiles) == 1
        assert result.processed == 1


# ── FacebookEnrichment ─────────────────────────────────────────────────────

class TestFacebookEnrichment:
    """FacebookEnrichment delegates to the same 11-step pipeline with facebook_groups platform."""

    def test_is_stage_adapter_subclass(self):
        assert issubclass(FacebookEnrichment, StageAdapter)

    def test_class_attributes(self):
        adapter = FacebookEnrichment()
        assert adapter.platform == 'facebook'
        assert adapter.stage == 'enrichment'
        assert 'Apify' in adapter.apis
        assert 'Apollo' in adapter.apis

    def test_estimate_cost(self):
        adapter = FacebookEnrichment()
        assert adapter.estimate_cost(100) == pytest.approx(5.0)
        assert adapter.estimate_cost(0) == 0.0
        assert adapter.estimate_cost(1) == pytest.approx(0.05)

    @patch('app.services.apify.enrich_profiles_full_pipeline')
    def test_run_delegates_to_full_pipeline(self, mock_pipeline, make_run):
        """Calls enrich_profiles_full_pipeline with 'facebook_groups' platform."""
        enriched_profiles = [
            {'group_name': 'Travel Group', 'email': 'admin@example.com'},
        ]
        mock_pipeline.return_value = enriched_profiles

        adapter = FacebookEnrichment()
        profiles = [{'group_name': 'Travel Group'}]
        run = make_run(id='run-fb-1', platform='facebook')
        result = adapter.run(profiles, run)

        assert isinstance(result, StageResult)
        assert result.profiles == enriched_profiles
        assert result.processed == 1
        assert result.meta == {'enrichment_steps': 11}
        mock_pipeline.assert_called_once_with(profiles, 'run-fb-1', platform='facebook_groups')

    @patch('app.services.apify.enrich_profiles_full_pipeline')
    def test_run_empty_profiles_short_circuits(self, mock_pipeline, make_run):
        """Empty input returns immediately without calling the pipeline."""
        adapter = FacebookEnrichment()
        run = make_run(platform='facebook')
        result = adapter.run([], run)

        assert result.profiles == []
        assert result.processed == 0
        mock_pipeline.assert_not_called()

    @patch('app.services.apify.enrich_profiles_full_pipeline')
    def test_run_passes_facebook_groups_platform(self, mock_pipeline, make_run):
        """Platform argument is 'facebook_groups' (not 'facebook')."""
        mock_pipeline.return_value = []

        adapter = FacebookEnrichment()
        run = make_run(id='run-fb-2', platform='facebook')
        adapter.run([{'group_name': 'X'}], run)

        _, kwargs = mock_pipeline.call_args
        assert kwargs['platform'] == 'facebook_groups'

    @patch('app.services.apify.enrich_profiles_full_pipeline')
    def test_run_pipeline_returns_fewer_profiles(self, mock_pipeline, make_run):
        """Pipeline may filter out profiles; processed count reflects input size."""
        mock_pipeline.return_value = [{'group_name': 'Survivor'}]

        adapter = FacebookEnrichment()
        profiles = [{'group_name': 'A'}, {'group_name': 'B'}, {'group_name': 'C'}]
        run = make_run(id='run-fb-3', platform='facebook')
        result = adapter.run(profiles, run)

        assert len(result.profiles) == 1
        assert result.processed == 3

    @patch('app.services.apify.enrich_profiles_full_pipeline')
    def test_run_pipeline_returns_empty(self, mock_pipeline, make_run):
        """Pipeline returning empty list is handled gracefully."""
        mock_pipeline.return_value = []

        adapter = FacebookEnrichment()
        profiles = [{'group_name': 'Group'}]
        run = make_run(id='run-fb-4', platform='facebook')
        result = adapter.run(profiles, run)

        assert result.profiles == []
        assert result.processed == 1

    @patch('app.services.apify.enrich_profiles_full_pipeline')
    def test_run_single_profile(self, mock_pipeline, make_run):
        """Single profile boundary case."""
        mock_pipeline.return_value = [{'group_name': 'Solo Group', 'email': 'admin@solo.com'}]

        adapter = FacebookEnrichment()
        run = make_run(id='run-fb-solo', platform='facebook')
        result = adapter.run([{'group_name': 'Solo Group'}], run)

        assert len(result.profiles) == 1
        assert result.processed == 1


# ── Integration ─────────────────────────────────────────────────────────────

class TestIntegration:
    """Cross-cutting checks across all enrichment adapters."""

    def test_all_adapters_are_stage_adapter_subclasses(self):
        """Every adapter in the registry is a StageAdapter subclass."""
        for platform, cls in ADAPTERS.items():
            adapter = cls()
            assert isinstance(adapter, StageAdapter)
            assert adapter.platform == platform
            assert adapter.stage == 'enrichment'

    def test_all_adapters_have_estimate_cost(self):
        """Every adapter implements estimate_cost with non-negative result."""
        for platform, cls in ADAPTERS.items():
            adapter = cls()
            cost = adapter.estimate_cost(100)
            assert cost >= 0.0, f"{platform} estimate_cost returned negative"

    @patch('app.services.apify.enrich_profiles_full_pipeline')
    def test_patreon_and_facebook_use_different_platform_strings(self, mock_pipeline, make_run):
        """Patreon uses 'patreon' platform, Facebook uses 'facebook_groups'."""
        mock_pipeline.return_value = []

        patreon = PatreonEnrichment()
        patreon.run([{'name': 'X'}], make_run(id='r1', platform='patreon'))

        facebook = FacebookEnrichment()
        facebook.run([{'name': 'X'}], make_run(id='r2', platform='facebook'))

        calls = mock_pipeline.call_args_list
        assert calls[0][1]['platform'] == 'patreon'
        assert calls[1][1]['platform'] == 'facebook_groups'
