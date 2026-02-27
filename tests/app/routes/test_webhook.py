"""Tests for app.routes.webhook — deprecated webhook endpoints."""


class TestHandleWebhookAsync:
    """POST /webhook/async returns 410 Gone."""

    def test_returns_410(self, client):
        resp = client.post('/webhook/async', json={'profile_url': 'test'})
        assert resp.status_code == 410

    def test_returns_deprecated_message(self, client):
        resp = client.post('/webhook/async', json={})
        assert resp.json['status'] == 'deprecated'
        assert 'POST /api/runs' in resp.json['message']

    def test_get_method_not_allowed(self, client):
        resp = client.get('/webhook/async')
        assert resp.status_code == 405


class TestEnrichWebhook:
    """POST /api/webhook/enrich returns 410 Gone."""

    def test_returns_410(self, client):
        resp = client.post('/api/webhook/enrich', json={})
        assert resp.status_code == 410

    def test_returns_deprecated_message(self, client):
        resp = client.post('/api/webhook/enrich', json={})
        assert 'deprecated' in resp.json['status']


class TestCheckTaskStatus:
    """GET /webhook/status/<task_id> returns 410 Gone."""

    def test_returns_410(self, client):
        resp = client.get('/webhook/status/abc-123')
        assert resp.status_code == 410

    def test_returns_deprecated_message(self, client):
        resp = client.get('/webhook/status/abc-123')
        assert '/runs/' in resp.json['message']
