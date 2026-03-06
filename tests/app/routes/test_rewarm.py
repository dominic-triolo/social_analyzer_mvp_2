"""Tests for app.routes.rewarm — Rewarm UI, DB-backed segment API, sync endpoint, rewarm launch API."""
import pytest
from unittest.mock import patch, MagicMock

from app.models.hubspot_list import HubSpotList
from app.models.app_config import AppConfig


# ---------------------------------------------------------------------------
# GET /rewarm
# ---------------------------------------------------------------------------

class TestRewarmPage:
    """GET /rewarm renders the rewarm UI."""

    def test_returns_200(self, client):
        resp = client.get('/rewarm')
        assert resp.status_code == 200

    def test_renders_html(self, client):
        resp = client.get('/rewarm')
        assert resp.content_type.startswith('text/html')

    def test_contains_page_title(self, client):
        html = client.get('/rewarm').data.decode()
        assert 'New Rewarm Run' in html

    def test_contains_platform_selector(self, client):
        html = client.get('/rewarm').data.decode()
        assert 'platform-selector' in html

    def test_contains_segment_search(self, client):
        html = client.get('/rewarm').data.decode()
        assert 'segment-search-input' in html

    def test_contains_refresh_button(self, client):
        html = client.get('/rewarm').data.decode()
        assert 'refresh-btn' in html

    def test_contains_sync_bar(self, client):
        html = client.get('/rewarm').data.decode()
        assert 'sync-bar' in html


# ---------------------------------------------------------------------------
# GET /api/rewarm/segments
# ---------------------------------------------------------------------------

class TestGetSegments:
    """GET /api/rewarm/segments — mock or DB."""

    @patch('app.routes.rewarm.HUBSPOT_API_KEY', None)
    def test_mock_returns_segments(self, client):
        data = client.get('/api/rewarm/segments').json
        assert len(data['segments']) > 0
        assert data['synced_at'] is None

    @patch('app.routes.rewarm.HUBSPOT_API_KEY', None)
    def test_mock_segments_have_required_fields(self, client):
        for seg in client.get('/api/rewarm/segments').json['segments']:
            assert 'id' in seg
            assert 'name' in seg
            assert 'size' in seg

    @patch('app.routes.rewarm.HUBSPOT_API_KEY', 'test-key')
    def test_needs_sync_when_db_empty(self, client, db_session):
        data = client.get('/api/rewarm/segments').json
        assert data['needs_sync'] is True
        assert data['segments'] == []

    @patch('app.routes.rewarm.HUBSPOT_API_KEY', 'test-key')
    def test_returns_segments_from_db(self, client, db_session):
        db_session.add(HubSpotList(list_id='1', name='Test List', size=42, processing_type='MANUAL'))
        db_session.add(AppConfig(key='hubspot_lists_synced_at', value='2026-03-06T12:00:00Z'))
        db_session.flush()

        data = client.get('/api/rewarm/segments').json
        assert len(data['segments']) == 1
        assert data['segments'][0] == {'id': '1', 'name': 'Test List', 'size': 42, 'processing_type': 'MANUAL'}
        assert data['synced_at'] == '2026-03-06T12:00:00Z'

    @patch('app.routes.rewarm.HUBSPOT_API_KEY', 'test-key')
    def test_segments_ordered_by_name(self, client, db_session):
        db_session.add(HubSpotList(list_id='2', name='Zebra', size=10))
        db_session.add(HubSpotList(list_id='1', name='Alpha', size=20))
        db_session.add(AppConfig(key='hubspot_lists_synced_at', value='2026-03-06T12:00:00Z'))
        db_session.flush()

        names = [s['name'] for s in client.get('/api/rewarm/segments').json['segments']]
        assert names == ['Alpha', 'Zebra']


# ---------------------------------------------------------------------------
# POST /api/rewarm/segments/sync
# ---------------------------------------------------------------------------

class TestSyncSegments:
    """POST /api/rewarm/segments/sync — triggers refresh, returns segments."""

    @patch('app.routes.rewarm.HUBSPOT_API_KEY', None)
    def test_mock_returns_segments(self, client):
        data = client.post('/api/rewarm/segments/sync').json
        assert len(data['segments']) > 0
        assert data['synced_at'] is None

    @patch('app.services.hubspot.sync_hubspot_lists_to_db')
    @patch('app.routes.rewarm.HUBSPOT_API_KEY', 'test-key')
    def test_syncs_and_returns_segments(self, mock_sync, client, db_session):
        # sync_hubspot_lists_to_db populates the DB; simulate that
        def do_sync():
            db_session.add(HubSpotList(list_id='1', name='Synced List', size=99))
            db_session.add(AppConfig(key='hubspot_lists_synced_at', value='2026-03-06T14:00:00Z'))
            db_session.flush()
            return {'count': 1, 'synced_at': '2026-03-06T14:00:00Z'}
        mock_sync.side_effect = lambda: do_sync()

        data = client.post('/api/rewarm/segments/sync').json
        assert data['segments'][0]['name'] == 'Synced List'
        assert data['synced_at'] == '2026-03-06T14:00:00Z'
        mock_sync.assert_called_once()

    @patch('app.services.hubspot.sync_hubspot_lists_to_db')
    @patch('app.routes.rewarm.HUBSPOT_API_KEY', 'test-key')
    def test_returns_500_on_error(self, mock_sync, client):
        mock_sync.side_effect = Exception("API timeout")

        resp = client.post('/api/rewarm/segments/sync')
        assert resp.status_code == 500
        assert 'API timeout' in resp.json['error']


# ---------------------------------------------------------------------------
# POST /api/rewarm
# ---------------------------------------------------------------------------

class TestLaunchRewarm:
    """POST /api/rewarm launches a rewarm run."""

    @patch('app.pipeline.manager.launch_rewarm')
    def test_valid_request_returns_202(self, mock_launch, client):
        mock_run = MagicMock()
        mock_run.to_dict.return_value = {
            'id': 'run-rewarm-001', 'platform': 'instagram',
            'run_type': 'rewarm', 'status': 'queued',
        }
        mock_launch.return_value = mock_run

        resp = client.post('/api/rewarm', json={
            'platform': 'instagram', 'hubspot_list_ids': ['123', '456'], 'dry_run': True,
        })
        assert resp.status_code == 202
        assert resp.json['id'] == 'run-rewarm-001'

    @patch('app.pipeline.manager.launch_rewarm')
    def test_passes_filters(self, mock_launch, client):
        mock_run = MagicMock()
        mock_run.to_dict.return_value = {'id': 'test'}
        mock_launch.return_value = mock_run

        client.post('/api/rewarm', json={
            'platform': 'instagram', 'hubspot_list_ids': ['123'], 'dry_run': False,
        })
        mock_launch.assert_called_once_with('instagram', {
            'hubspot_list_ids': ['123'], 'dry_run': False,
        })

    def test_missing_list_ids_returns_400(self, client):
        resp = client.post('/api/rewarm', json={'hubspot_list_ids': []})
        assert resp.status_code == 400

    def test_empty_body_returns_400(self, client):
        assert client.post('/api/rewarm', json={}).status_code == 400

    @patch('app.pipeline.manager.launch_rewarm')
    def test_defaults_platform_to_instagram(self, mock_launch, client):
        mock_run = MagicMock()
        mock_run.to_dict.return_value = {'id': 'test'}
        mock_launch.return_value = mock_run
        client.post('/api/rewarm', json={'hubspot_list_ids': ['123']})
        assert mock_launch.call_args[0][0] == 'instagram'

    @patch('app.pipeline.manager.launch_rewarm')
    def test_defaults_dry_run_to_true(self, mock_launch, client):
        mock_run = MagicMock()
        mock_run.to_dict.return_value = {'id': 'test'}
        mock_launch.return_value = mock_run
        client.post('/api/rewarm', json={'hubspot_list_ids': ['123']})
        assert mock_launch.call_args[0][1]['dry_run'] is True

    @patch('app.pipeline.manager.launch_rewarm')
    def test_value_error_returns_400(self, mock_launch, client):
        mock_launch.side_effect = ValueError("Unsupported platform")
        resp = client.post('/api/rewarm', json={'platform': 'x', 'hubspot_list_ids': ['1']})
        assert resp.status_code == 400

    @patch('app.pipeline.manager.launch_rewarm')
    def test_unexpected_error_returns_500(self, mock_launch, client):
        mock_launch.side_effect = RuntimeError("Redis down")
        resp = client.post('/api/rewarm', json={'hubspot_list_ids': ['1']})
        assert resp.status_code == 500
