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


def _mock_ollama_response(text):
    """Build a mock Ollama /api/chat JSON response."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"message": {"content": text}}
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# Happy paths — Anthropic (production)
# ---------------------------------------------------------------------------

class TestKeywordSuggestionsInstagram:
    """POST /api/keyword-suggestions with platform=instagram (Anthropic)."""

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
    """POST /api/keyword-suggestions with platform=patreon (Anthropic)."""

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
    """POST /api/keyword-suggestions with platform=facebook (Anthropic)."""

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
# Happy paths — Ollama fallback (local dev)
# ---------------------------------------------------------------------------

class TestKeywordSuggestionsOllama:
    """POST /api/keyword-suggestions falls back to Ollama when no Anthropic key."""

    def test_ollama_fallback_returns_suggestions(self, client):
        """When anthropic_client is None, endpoint calls Ollama."""
        with patch('app.extensions.anthropic_client', new=None), \
             patch('app.routes.discovery._call_ollama') as mock_ollama:
            mock_ollama.return_value = "#solotravel\nbudget backpacking\n#digitalnomad\ntravel tips"
            resp = client.post('/api/keyword-suggestions',
                               json={'platform': 'instagram', 'keywords': ['travel']})
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data['suggestions']) == 4
        assert '#solotravel' in data['suggestions']
        mock_ollama.assert_called_once()

    def test_ollama_deduplicates(self, client):
        """Ollama suggestions are deduplicated the same way as Anthropic."""
        with patch('app.extensions.anthropic_client', new=None), \
             patch('app.routes.discovery._call_ollama') as mock_ollama:
            mock_ollama.return_value = "Travel\nnew idea\nHiking\nexplore"
            resp = client.post('/api/keyword-suggestions',
                               json={'platform': 'instagram', 'keywords': ['travel', 'hiking']})
        assert resp.status_code == 200
        suggestions_lower = [s.lower() for s in resp.get_json()['suggestions']]
        assert 'travel' not in suggestions_lower
        assert 'hiking' not in suggestions_lower
        assert 'new idea' in suggestions_lower

    def test_ollama_error_returns_500(self, client):
        """Ollama connection failure returns 500."""
        with patch('app.extensions.anthropic_client', new=None), \
             patch('app.routes.discovery._call_ollama', side_effect=Exception("Connection refused")):
            resp = client.post('/api/keyword-suggestions',
                               json={'platform': 'instagram', 'keywords': ['travel']})
        assert resp.status_code == 500


class TestCallOllama:
    """Unit tests for _call_ollama helper."""

    @patch('app.routes.discovery.http_requests')
    def test_posts_to_ollama_api(self, mock_http, client):
        """Verify correct Ollama API call shape."""
        mock_http.post.return_value = _mock_ollama_response("suggestion 1\nsuggestion 2")
        from app.routes.discovery import _call_ollama
        result = _call_ollama("system prompt", "user input")
        assert result == "suggestion 1\nsuggestion 2"
        call_args = mock_http.post.call_args
        payload = call_args[1]['json']
        assert payload['messages'][0]['role'] == 'system'
        assert payload['messages'][1]['role'] == 'user'
        assert payload['stream'] is False


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
