"""Tests for POST /api/keyword-suggestions — AI keyword suggestion endpoint."""
import json
import pytest
from unittest.mock import patch, MagicMock


def _mock_anthropic_response(text):
    """Build a mock Anthropic messages.create() response."""
    msg = MagicMock()
    block = MagicMock()
    block.text = text
    msg.content = [block]
    return msg


# ---------------------------------------------------------------------------
# Happy paths — one per platform
# ---------------------------------------------------------------------------

class TestKeywordSuggestionsInstagram:
    """POST /api/keyword-suggestions with platform=instagram."""

    @patch('app.extensions.anthropic_client')
    def test_returns_suggestions(self, mock_client, client):
        mock_client.messages.create.return_value = _mock_anthropic_response(
            "#adventuretravel\nbackpacking tips\n#wanderlust\ndigital nomad"
        )
        resp = client.post('/api/keyword-suggestions',
                           json={'platform': 'instagram', 'keywords': ['travel']})
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'suggestions' in data
        assert len(data['suggestions']) == 4
        assert '#adventuretravel' in data['suggestions']


class TestKeywordSuggestionsPatreon:
    """POST /api/keyword-suggestions with platform=patreon."""

    @patch('app.extensions.anthropic_client')
    def test_returns_suggestions(self, mock_client, client):
        mock_client.messages.create.return_value = _mock_anthropic_response(
            "travel vlog\nbackpacking\nvan life\ntravel photography"
        )
        resp = client.post('/api/keyword-suggestions',
                           json={'platform': 'patreon', 'keywords': ['travel creator']})
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data['suggestions']) == 4
        # Verify platform-specific prompt was used
        call_kwargs = mock_client.messages.create.call_args[1]
        assert 'Patreon' in call_kwargs['system']


class TestKeywordSuggestionsFacebook:
    """POST /api/keyword-suggestions with platform=facebook."""

    @patch('app.extensions.anthropic_client')
    def test_returns_suggestions(self, mock_client, client):
        mock_client.messages.create.return_value = _mock_anthropic_response(
            "travel community\nbackpackers group\nbudget travel tips"
        )
        resp = client.post('/api/keyword-suggestions',
                           json={'platform': 'facebook', 'keywords': ['hiking']})
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data['suggestions']) == 3
        call_kwargs = mock_client.messages.create.call_args[1]
        assert 'Facebook' in call_kwargs['system']


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

class TestKeywordSuggestionsErrors:
    """Error handling for /api/keyword-suggestions."""

    def test_400_when_no_keywords(self, client):
        with patch('app.extensions.anthropic_client', new=MagicMock()):
            resp = client.post('/api/keyword-suggestions',
                               json={'platform': 'instagram', 'keywords': []})
            assert resp.status_code == 400
            assert 'error' in resp.get_json()

    def test_503_when_client_unavailable(self, client):
        with patch('app.extensions.anthropic_client', new=None):
            resp = client.post('/api/keyword-suggestions',
                               json={'platform': 'instagram', 'keywords': ['travel']})
            assert resp.status_code == 503
            assert 'unavailable' in resp.get_json()['error'].lower()

    @patch('app.extensions.anthropic_client')
    def test_500_on_api_error(self, mock_client, client):
        mock_client.messages.create.side_effect = Exception("API timeout")
        resp = client.post('/api/keyword-suggestions',
                           json={'platform': 'instagram', 'keywords': ['travel']})
        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class TestKeywordSuggestionsDedup:
    """Suggestions are deduplicated against user's existing keywords."""

    @patch('app.extensions.anthropic_client')
    def test_removes_duplicates_case_insensitive(self, mock_client, client):
        mock_client.messages.create.return_value = _mock_anthropic_response(
            "Travel\nadventure\nHIKING\nnew suggestion"
        )
        resp = client.post('/api/keyword-suggestions',
                           json={'platform': 'instagram', 'keywords': ['travel', 'hiking']})
        assert resp.status_code == 200
        data = resp.get_json()
        suggestions_lower = [s.lower() for s in data['suggestions']]
        assert 'travel' not in suggestions_lower
        assert 'hiking' not in suggestions_lower
        assert 'adventure' in suggestions_lower
        assert 'new suggestion' in suggestions_lower
