"""Tests for InsightIQ normaliser helpers — interests and hashtags.

Covers:
  - _normalize_interests: exact match, alias mapping, case-insensitive, invalid drop
  - _normalize_hashtags: string→dict, dict passthrough, # stripping
"""
import pytest

from app.services.insightiq import (
    _normalize_interests,
    _normalize_hashtags,
    VALID_INTERESTS,
    INTEREST_ALIASES,
)


# ── _normalize_interests ──────────────────────────────────────────────────

class TestNormalizeInterests:
    """Map user-provided interest strings to valid InsightIQ enum values."""

    def test_exact_match_passthrough(self):
        result = _normalize_interests(['Travel, Tourism & Aviation'])
        assert result == ['Travel, Tourism & Aviation']

    def test_multiple_exact_matches(self):
        result = _normalize_interests(['Sports', 'Music', 'Gaming'])
        assert result == ['Sports', 'Music', 'Gaming']

    def test_alias_maps_travel(self):
        result = _normalize_interests(['Travel'])
        assert result == ['Travel, Tourism & Aviation']

    def test_alias_maps_food_and_drink(self):
        result = _normalize_interests(['Food & Drink'])
        assert result == ['Restaurants, Food & Grocery']

    def test_alias_maps_fashion(self):
        result = _normalize_interests(['Fashion'])
        assert result == ['Clothes, Shoes, Handbags & Accessories']

    def test_alias_maps_beauty(self):
        result = _normalize_interests(['Beauty'])
        assert result == ['Beauty & Cosmetics']

    def test_alias_case_insensitive(self):
        result = _normalize_interests(['travel'])
        assert result == ['Travel, Tourism & Aviation']

    def test_exact_match_case_insensitive(self):
        result = _normalize_interests(['fitness & yoga'])
        assert result == ['Fitness & Yoga']

    def test_invalid_interest_dropped(self):
        result = _normalize_interests(['Nonexistent Category'])
        assert result == []

    def test_mixed_valid_and_invalid(self):
        result = _normalize_interests(['Travel', 'Bogus', 'Music'])
        assert result == ['Travel, Tourism & Aviation', 'Music']

    def test_deduplicates(self):
        """If alias and exact both resolve to same value, deduplicated."""
        result = _normalize_interests(['Travel', 'Travel, Tourism & Aviation'])
        assert result == ['Travel, Tourism & Aviation']

    def test_empty_list(self):
        assert _normalize_interests([]) == []

    def test_none_input(self):
        assert _normalize_interests(None) == []

    def test_non_list_input(self):
        assert _normalize_interests('Travel') == []

    def test_strips_whitespace(self):
        result = _normalize_interests(['  Sports  '])
        assert result == ['Sports']

    def test_skips_empty_strings(self):
        result = _normalize_interests(['', '  ', 'Music'])
        assert result == ['Music']

    def test_all_alias_keys_resolve_to_valid_interests(self):
        """Every alias value must be in VALID_INTERESTS."""
        for alias, full_name in INTEREST_ALIASES.items():
            assert full_name in VALID_INTERESTS, (
                f"Alias {alias!r} → {full_name!r} is not in VALID_INTERESTS"
            )

    def test_real_api_error_case_travel(self):
        """The exact input that caused the prod error: ['Travel', 'Food & Drink']."""
        result = _normalize_interests(['Travel', 'Food & Drink'])
        assert result == ['Travel, Tourism & Aviation', 'Restaurants, Food & Grocery']
        for interest in result:
            assert interest in VALID_INTERESTS


# ── _normalize_hashtags ───────────────────────────────────────────────────

class TestNormalizeHashtags:
    """Ensure hashtags are in InsightIQ dict format."""

    def test_strings_to_dicts(self):
        result = _normalize_hashtags(['travel', 'hiking'])
        assert result == [{'name': 'travel'}, {'name': 'hiking'}]

    def test_strips_hash_prefix(self):
        result = _normalize_hashtags(['#grouptravel', '#travelwithme'])
        assert result == [{'name': 'grouptravel'}, {'name': 'travelwithme'}]

    def test_dict_passthrough(self):
        result = _normalize_hashtags([{'name': 'travel'}])
        assert result == [{'name': 'travel'}]

    def test_mixed_strings_and_dicts(self):
        result = _normalize_hashtags(['#adventure', {'name': 'hiking'}])
        assert result == [{'name': 'adventure'}, {'name': 'hiking'}]

    def test_empty_list(self):
        assert _normalize_hashtags([]) == []

    def test_none_input(self):
        assert _normalize_hashtags(None) == []

    def test_non_list_input(self):
        assert _normalize_hashtags('#travel') == []

    def test_skips_empty_strings(self):
        result = _normalize_hashtags(['', '  ', 'travel'])
        assert result == [{'name': 'travel'}]

    def test_strips_whitespace(self):
        result = _normalize_hashtags(['  #travel  '])
        assert result == [{'name': 'travel'}]

    def test_real_api_error_case(self):
        """The exact input that caused the prod error: ['#grouptravel', '#travelwithme']."""
        result = _normalize_hashtags(['#grouptravel', '#travelwithme'])
        for tag in result:
            assert isinstance(tag, dict)
            assert 'name' in tag
            assert not tag['name'].startswith('#')
