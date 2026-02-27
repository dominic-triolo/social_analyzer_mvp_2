"""Tests for /api/health and /api/health/<service>/reset endpoints."""
import pytest
from unittest.mock import patch, MagicMock


class TestApiHealth:
    """GET /api/health returns circuit breaker states."""

    def test_returns_200(self, client):
        resp = client.get('/api/health')
        assert resp.status_code == 200

    def test_returns_services_dict(self, client):
        resp = client.get('/api/health')
        data = resp.get_json()
        assert 'services' in data
        # init_breakers is called in create_app, so all 6 should be present
        assert 'insightiq' in data['services']
        assert 'openai' in data['services']
        assert 'apify' in data['services']

    def test_service_has_expected_fields(self, client):
        resp = client.get('/api/health')
        data = resp.get_json()
        svc = data['services']['insightiq']
        assert 'name' in svc
        assert 'state' in svc
        assert 'failure_count' in svc
        assert 'failure_threshold' in svc
        assert 'total_success' in svc
        assert 'total_failure' in svc


class TestResetCircuit:
    """POST /api/health/<service>/reset resets a circuit breaker."""

    def test_reset_known_service(self, client):
        resp = client.post('/api/health/insightiq/reset')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['ok'] is True

    def test_reset_unknown_service_404(self, client):
        resp = client.post('/api/health/nonexistent/reset')
        assert resp.status_code == 404


class TestApiHealthPartial:
    """GET /partials/api-health returns HTML for the dashboard."""

    def test_returns_200(self, client):
        resp = client.get('/partials/api-health')
        assert resp.status_code == 200
        assert resp.content_type.startswith('text/html')
