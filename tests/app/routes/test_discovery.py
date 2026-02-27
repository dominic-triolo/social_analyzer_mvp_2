"""Tests for app.routes.discovery — Discovery UI, presets API, pipeline preview, staleness check."""
import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime


@pytest.fixture
def patch_discovery_session(db_session):
    """Patch get_session where the discovery routes imported it."""
    with patch('app.routes.discovery.get_session', return_value=db_session):
        yield db_session


# ---------------------------------------------------------------------------
# /discovery
# ---------------------------------------------------------------------------

class TestDiscoveryPage:
    """GET /discovery renders the discovery UI."""

    def test_returns_200(self, client):
        resp = client.get('/discovery')
        assert resp.status_code == 200

    def test_renders_html(self, client):
        resp = client.get('/discovery')
        assert resp.content_type.startswith('text/html')


# ---------------------------------------------------------------------------
# /partials/pipeline-preview
# ---------------------------------------------------------------------------

class TestPipelinePreviewPartial:
    """GET /partials/pipeline-preview renders the pipeline stage diagram."""

    @patch('app.pipeline.base.get_pipeline_info')
    @patch('app.pipeline.manager.STAGE_REGISTRY', new={'discovery': {}})
    def test_returns_200_default_platform(self, mock_info, client):
        mock_info.return_value = {
            'instagram': {
                'discovery': {'description': 'Find profiles', 'apis': ['Apify'], 'est': 2},
                'enrichment': {'description': 'Enrich data', 'apis': ['Apify'], 'est': 3},
            }
        }
        resp = client.get('/partials/pipeline-preview')
        assert resp.status_code == 200

    @patch('app.pipeline.base.get_pipeline_info')
    @patch('app.pipeline.manager.STAGE_REGISTRY', new={'discovery': {}})
    def test_respects_platform_param(self, mock_info, client):
        mock_info.return_value = {
            'patreon': {
                'discovery': {'description': 'Patreon search', 'apis': ['Patreon API'], 'est': 1},
            }
        }
        resp = client.get('/partials/pipeline-preview?platform=patreon')
        assert resp.status_code == 200

    @patch('app.pipeline.base.get_pipeline_info')
    @patch('app.pipeline.manager.STAGE_REGISTRY', new={'discovery': {}})
    def test_unknown_platform_renders_empty(self, mock_info, client):
        mock_info.return_value = {}
        resp = client.get('/partials/pipeline-preview?platform=unknown')
        assert resp.status_code == 200

    @patch('app.pipeline.base.get_pipeline_info')
    @patch('app.pipeline.manager.STAGE_REGISTRY', new={'discovery': {}})
    def test_stages_with_no_est_renders(self, mock_info, client):
        mock_info.return_value = {
            'instagram': {
                'discovery': {'description': 'Find', 'apis': [], 'est': None},
            }
        }
        resp = client.get('/partials/pipeline-preview')
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GET /api/presets
# ---------------------------------------------------------------------------

class TestListPresets:
    """GET /api/presets returns saved filter presets."""

    def test_returns_empty_list_when_no_presets(self, client, patch_discovery_session):
        resp = client.get('/api/presets')
        assert resp.status_code == 200
        assert resp.json == []

    def test_returns_presets_as_json(self, client, patch_discovery_session):
        from app.models.preset import Preset
        session = patch_discovery_session
        p = Preset(name='My Preset', platform='instagram', filters={'max_results': 10})
        session.add(p)
        session.commit()

        resp = client.get('/api/presets')
        assert resp.status_code == 200
        data = resp.json
        assert len(data) == 1
        assert data[0]['name'] == 'My Preset'
        assert data[0]['platform'] == 'instagram'
        assert data[0]['filters'] == {'max_results': 10}

    def test_filters_by_platform(self, client, patch_discovery_session):
        from app.models.preset import Preset
        session = patch_discovery_session
        session.add(Preset(name='IG Preset', platform='instagram', filters={}))
        session.add(Preset(name='Pat Preset', platform='patreon', filters={}))
        session.commit()

        resp = client.get('/api/presets?platform=instagram')
        data = resp.json
        assert len(data) == 1
        assert data[0]['name'] == 'IG Preset'

    def test_returns_all_when_no_platform_filter(self, client, patch_discovery_session):
        from app.models.preset import Preset
        session = patch_discovery_session
        session.add(Preset(name='IG', platform='instagram', filters={}))
        session.add(Preset(name='Pat', platform='patreon', filters={}))
        session.commit()

        resp = client.get('/api/presets')
        assert len(resp.json) == 2

    def test_includes_id_and_created_at(self, client, patch_discovery_session):
        from app.models.preset import Preset
        session = patch_discovery_session
        session.add(Preset(name='Test', platform='instagram', filters={}))
        session.commit()

        data = client.get('/api/presets').json
        assert 'id' in data[0]
        assert 'created_at' in data[0]


# ---------------------------------------------------------------------------
# POST /api/presets
# ---------------------------------------------------------------------------

class TestCreatePreset:
    """POST /api/presets saves a new filter preset."""

    def test_creates_preset_returns_201(self, client, patch_discovery_session):
        resp = client.post('/api/presets', json={
            'name': 'Travel Creators',
            'platform': 'instagram',
            'filters': {'max_results': 25},
        })
        assert resp.status_code == 201
        data = resp.json
        assert data['name'] == 'Travel Creators'
        assert data['platform'] == 'instagram'
        assert data['filters'] == {'max_results': 25}
        assert 'id' in data

    def test_preset_persisted_in_database(self, client, patch_discovery_session):
        client.post('/api/presets', json={
            'name': 'Persisted',
            'platform': 'instagram',
            'filters': {},
        })
        from app.models.preset import Preset
        session = patch_discovery_session
        presets = session.query(Preset).all()
        assert len(presets) == 1
        assert presets[0].name == 'Persisted'

    def test_missing_name_returns_400(self, client, patch_discovery_session):
        resp = client.post('/api/presets', json={
            'platform': 'instagram',
            'filters': {},
        })
        assert resp.status_code == 400
        assert 'Name is required' in resp.json['error']

    def test_empty_name_returns_400(self, client, patch_discovery_session):
        resp = client.post('/api/presets', json={
            'name': '   ',
            'platform': 'instagram',
            'filters': {},
        })
        assert resp.status_code == 400

    def test_missing_platform_returns_400(self, client, patch_discovery_session):
        resp = client.post('/api/presets', json={
            'name': 'No Platform',
            'filters': {},
        })
        assert resp.status_code == 400
        assert 'Platform is required' in resp.json['error']

    def test_empty_body_returns_400(self, client, patch_discovery_session):
        resp = client.post('/api/presets', json={})
        assert resp.status_code == 400

    def test_defaults_filters_to_empty_dict(self, client, patch_discovery_session):
        resp = client.post('/api/presets', json={
            'name': 'Minimal',
            'platform': 'instagram',
        })
        assert resp.status_code == 201
        assert resp.json['filters'] == {}

    def test_name_is_stripped_of_whitespace(self, client, patch_discovery_session):
        resp = client.post('/api/presets', json={
            'name': '  Padded Name  ',
            'platform': 'instagram',
            'filters': {},
        })
        assert resp.status_code == 201
        assert resp.json['name'] == 'Padded Name'


# ---------------------------------------------------------------------------
# DELETE /api/presets/<id>
# ---------------------------------------------------------------------------

class TestDeletePreset:
    """DELETE /api/presets/<id> removes a preset."""

    def test_deletes_existing_preset(self, client, patch_discovery_session):
        from app.models.preset import Preset
        session = patch_discovery_session
        p = Preset(name='To Delete', platform='instagram', filters={})
        session.add(p)
        session.commit()
        preset_id = p.id

        resp = client.delete(f'/api/presets/{preset_id}')
        assert resp.status_code == 200
        assert resp.json['ok'] is True

    def test_nonexistent_preset_returns_404(self, client, patch_discovery_session):
        resp = client.delete('/api/presets/99999')
        assert resp.status_code == 404
        assert 'not found' in resp.json['error'].lower()


# ---------------------------------------------------------------------------
# GET /api/filter-staleness
# ---------------------------------------------------------------------------

class TestFilterStaleness:
    """GET /api/filter-staleness checks if filters have been run before."""

    @patch('app.services.db.get_filter_staleness')
    def test_not_stale_when_no_history(self, mock_staleness, client):
        mock_staleness.return_value = None

        resp = client.get('/api/filter-staleness?platform=instagram&filters={}')
        assert resp.status_code == 200
        assert resp.json['stale'] is False

    @patch('app.services.db.get_filter_staleness')
    def test_stale_when_novelty_below_20(self, mock_staleness, client):
        mock_staleness.return_value = {
            'novelty_rate': 10,
            'last_run_days_ago': 2,
            'total_found': 100,
            'new_found': 10,
        }

        resp = client.get('/api/filter-staleness?platform=instagram&filters={}')
        assert resp.status_code == 200
        data = resp.json
        assert data['stale'] is True
        assert data['novelty_rate'] == 10
        assert data['last_run_days_ago'] == 2
        assert data['total_found'] == 100
        assert data['new_found'] == 10

    @patch('app.services.db.get_filter_staleness')
    def test_not_stale_when_novelty_above_20(self, mock_staleness, client):
        mock_staleness.return_value = {
            'novelty_rate': 80,
            'last_run_days_ago': 1,
            'total_found': 50,
            'new_found': 40,
        }

        resp = client.get('/api/filter-staleness?platform=instagram&filters={}')
        data = resp.json
        assert data['stale'] is False
        assert data['novelty_rate'] == 80

    @patch('app.services.db.get_filter_staleness')
    def test_stale_boundary_at_exactly_20(self, mock_staleness, client):
        """Novelty rate of exactly 20 is NOT stale (< 20 threshold)."""
        mock_staleness.return_value = {
            'novelty_rate': 20,
            'last_run_days_ago': 3,
            'total_found': 50,
            'new_found': 10,
        }

        resp = client.get('/api/filter-staleness?platform=instagram&filters={}')
        assert resp.json['stale'] is False

    @patch('app.services.db.get_filter_staleness')
    def test_defaults_to_instagram_platform(self, mock_staleness, client):
        mock_staleness.return_value = None
        client.get('/api/filter-staleness')
        mock_staleness.assert_called_once_with('instagram', {})

    @patch('app.services.db.get_filter_staleness')
    def test_parses_filters_json_from_query_string(self, mock_staleness, client):
        mock_staleness.return_value = None
        filters = json.dumps({'max_results': 50, 'niche': 'travel'})
        client.get(f'/api/filter-staleness?platform=patreon&filters={filters}')
        mock_staleness.assert_called_once_with('patreon', {'max_results': 50, 'niche': 'travel'})

    def test_malformed_json_returns_not_stale(self, client):
        """Bad JSON in filters param should not crash — returns not stale."""
        resp = client.get('/api/filter-staleness?filters=NOT_JSON')
        assert resp.status_code == 200
        assert resp.json['stale'] is False

    @patch('app.services.db.get_filter_staleness')
    def test_service_error_returns_not_stale(self, mock_staleness, client):
        """If get_filter_staleness raises, the endpoint still returns gracefully."""
        mock_staleness.side_effect = Exception("DB timeout")
        resp = client.get('/api/filter-staleness?platform=instagram&filters={}')
        assert resp.status_code == 200
        assert resp.json['stale'] is False


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

class TestDiscoveryIntegration:
    """Cross-endpoint flow: create preset, list it, delete it."""

    def test_create_list_delete_preset_lifecycle(self, client, patch_discovery_session):
        # Create
        create_resp = client.post('/api/presets', json={
            'name': 'Lifecycle Test',
            'platform': 'instagram',
            'filters': {'max_results': 5},
        })
        assert create_resp.status_code == 201
        preset_id = create_resp.json['id']

        # List and verify it appears
        list_resp = client.get('/api/presets')
        names = [p['name'] for p in list_resp.json]
        assert 'Lifecycle Test' in names

        # Delete
        delete_resp = client.delete(f'/api/presets/{preset_id}')
        assert delete_resp.status_code == 200

        # Verify it is gone
        list_resp2 = client.get('/api/presets')
        ids = [p['id'] for p in list_resp2.json]
        assert preset_id not in ids

    def test_create_then_filter_by_platform(self, client, patch_discovery_session):
        client.post('/api/presets', json={
            'name': 'IG One', 'platform': 'instagram', 'filters': {},
        })
        client.post('/api/presets', json={
            'name': 'Pat One', 'platform': 'patreon', 'filters': {},
        })

        ig_resp = client.get('/api/presets?platform=instagram')
        assert len(ig_resp.json) == 1
        assert ig_resp.json[0]['name'] == 'IG One'

        pat_resp = client.get('/api/presets?platform=patreon')
        assert len(pat_resp.json) == 1
        assert pat_resp.json[0]['name'] == 'Pat One'
