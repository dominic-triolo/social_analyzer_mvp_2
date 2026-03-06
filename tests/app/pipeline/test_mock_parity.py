"""Contract tests — verify mock adapters match real InsightIQ field formats.

These prevent the mock-vs-production drift that caused deploy-time bugs
where `db.py` fallback chains masked missing fields.
"""
import pytest
from unittest.mock import MagicMock

from app.pipeline.mock_adapters import (
    MockInstagramDiscovery,
    MockInstagramPrescreen,
    MockInstagramEnrichment,
    MockInstagramAnalysis,
    MockInstagramScoring,
    MockInstagramCrmSync,
)

# Exact keys from InsightIQDiscovery._standardize_results() (insightiq.py:412-434)
INSIGHTIQ_DISCOVERY_KEYS = {
    'first_and_last_name',
    'flagship_social_platform_handle',
    'instagram_handle',
    'instagram_bio',
    'instagram_followers',
    'average_engagement',
    'email',
    'phone',
    'tiktok_handle',
    'youtube_profile_link',
    'facebook_profile_link',
    'patreon_link',
    'pinterest_profile_link',
    'city',
    'state',
    'country',
    'flagship_social_platform',
    'channel',
    'channel_host_prospected',
    'funnel',
    'enrichment_status',
}


def _make_run(**overrides):
    defaults = dict(
        id='parity-test-001',
        platform='instagram',
        status='running',
        filters={'max_results': 3},
        tier_distribution={'auto_enroll': 0, 'standard_review': 0},
    )
    defaults.update(overrides)
    run = MagicMock()
    for k, v in defaults.items():
        setattr(run, k, v)
    return run


class TestDiscoveryParity:
    """Mock discovery output must contain exactly the InsightIQ keys."""

    def test_discovery_profiles_have_insightiq_keys(self):
        run = _make_run()
        adapter = MockInstagramDiscovery()
        result = adapter.run([], run)

        assert len(result.profiles) > 0
        for profile in result.profiles:
            profile_keys = {k for k in profile.keys() if not k.startswith('_')}
            assert profile_keys == INSIGHTIQ_DISCOVERY_KEYS, (
                f"Key mismatch.\n"
                f"  Missing: {INSIGHTIQ_DISCOVERY_KEYS - profile_keys}\n"
                f"  Extra:   {profile_keys - INSIGHTIQ_DISCOVERY_KEYS}"
            )

    def test_discovery_profiles_have_no_old_keys(self):
        """Old mock keys must not appear in output."""
        old_keys = {'platform_username', 'name', 'bio', 'follower_count',
                     'contact_id', 'id', 'url', 'profile_url', 'primary_category'}
        run = _make_run()
        result = MockInstagramDiscovery().run([], run)

        for profile in result.profiles:
            found_old = old_keys & profile.keys()
            assert not found_old, f"Old keys still present: {found_old}"

    def test_instagram_handle_is_full_url(self):
        run = _make_run()
        result = MockInstagramDiscovery().run([], run)
        for profile in result.profiles:
            assert profile['instagram_handle'].startswith('https://www.instagram.com/')
            assert profile['instagram_handle'].endswith('/')

    def test_flagship_platform_is_instagram(self):
        run = _make_run()
        result = MockInstagramDiscovery().run([], run)
        for profile in result.profiles:
            assert profile['flagship_social_platform'] == 'instagram'
            assert profile['channel'] == 'Outbound'
            assert profile['funnel'] == 'Creator'


class TestPrescreenParity:
    """Prescreen content items must use InsightIQ content API format."""

    def test_content_item_types_are_uppercase(self):
        run = _make_run()
        profiles = MockInstagramDiscovery().run([], run).profiles
        result = MockInstagramPrescreen().run(profiles, run)

        valid_types = {'IMAGE', 'VIDEO', 'COLLECTION'}
        for profile in result.profiles:
            for item in profile.get('_content_items', []):
                assert item['type'] in valid_types, (
                    f"Content type '{item['type']}' not in {valid_types}"
                )

    def test_content_items_have_required_fields(self):
        run = _make_run()
        profiles = MockInstagramDiscovery().run([], run).profiles
        result = MockInstagramPrescreen().run(profiles, run)

        required = {'type', 'url', 'thumbnail_url', 'description', 'title',
                     'published_at', 'engagement'}
        for profile in result.profiles:
            for item in profile.get('_content_items', []):
                missing = required - item.keys()
                assert not missing, f"Content item missing fields: {missing}"


class TestEndToEndParity:
    """Mock pipeline end-to-end: profiles flow through all 6 stages."""

    def test_full_mock_pipeline(self):
        """Run all 6 mock stages and verify profile structure at the end."""
        run = _make_run()

        # Stage 1: Discovery
        profiles = MockInstagramDiscovery().run([], run).profiles
        assert len(profiles) > 0

        # Stage 2: Prescreen
        profiles = MockInstagramPrescreen().run(profiles, run).profiles

        # Stage 3: Enrichment
        profiles = MockInstagramEnrichment().run(profiles, run).profiles

        # Stage 4: Analysis
        profiles = MockInstagramAnalysis().run(profiles, run).profiles

        # Stage 5: Scoring
        profiles = MockInstagramScoring().run(profiles, run).profiles

        # Stage 6: CRM Sync
        profiles = MockInstagramCrmSync().run(profiles, run).profiles

        assert len(profiles) > 0

        for profile in profiles:
            # InsightIQ discovery keys still present
            assert 'first_and_last_name' in profile
            assert 'flagship_social_platform_handle' in profile
            assert 'instagram_followers' in profile
            assert 'instagram_bio' in profile
            assert 'instagram_handle' in profile

            # Old keys should NOT be present
            assert 'platform_username' not in profile
            assert 'follower_count' not in profile

            # Pipeline-added keys present
            assert '_lead_analysis' in profile
            assert '_synced_to_crm' in profile
            assert profile['_synced_to_crm'] is True

    def test_platform_id_extractable_from_mock_profiles(self):
        """db.py _extract_platform_id must resolve a value from mock profiles."""
        from app.services.db import _extract_platform_id

        run = _make_run()
        profiles = MockInstagramDiscovery().run([], run).profiles

        for profile in profiles:
            pid = _extract_platform_id(profile, 'instagram')
            assert pid, f"Could not extract platform_id from {list(profile.keys())[:5]}"
            # Should extract the username from instagram_handle URL
            assert len(pid) > 0
