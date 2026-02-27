"""Tests for app.routes.monitor — Run views, pipeline API, HTMX partials, SSE stream."""
import pytest
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_run_mock(**overrides):
    """Build a Run-like mock with to_dict support."""
    defaults = dict(
        id='run-001', status='queued', platform='instagram',
        current_stage='', filters={'max_results': 10},
        bdr_assignment='Test BDR', profiles_found=0,
        contacts_synced=0, stage_progress={}, errors=[],
        tier_distribution={}, created_at='2026-01-01T00:00:00',
        updated_at='2026-01-01T00:00:00', stage_outputs={},
    )
    defaults.update(overrides)
    run = MagicMock()
    for k, v in defaults.items():
        setattr(run, k, v)
    run.to_dict.return_value = defaults
    return run


# ---------------------------------------------------------------------------
# _build_stages (helper, but has significant branching logic)
# ---------------------------------------------------------------------------

class TestBuildStages:
    """_build_stages computes stage statuses from a run dict."""

    def test_all_pending_when_no_current_stage(self):
        from app.routes.monitor import _build_stages
        stages = _build_stages({'current_stage': '', 'status': 'queued'})
        assert all(s['status'] == 'pending' for s in stages)
        assert len(stages) == 6

    def test_current_stage_is_running(self):
        from app.routes.monitor import _build_stages
        stages = _build_stages({'current_stage': 'enrichment', 'status': 'running'})
        by_key = {s['key']: s['status'] for s in stages}
        assert by_key['discovery'] == 'completed'
        assert by_key['pre_screen'] == 'completed'
        assert by_key['enrichment'] == 'running'
        assert by_key['analysis'] == 'pending'
        assert by_key['scoring'] == 'pending'
        assert by_key['crm_sync'] == 'pending'

    def test_completed_run_all_stages_completed(self):
        from app.routes.monitor import _build_stages
        stages = _build_stages({'current_stage': 'crm_sync', 'status': 'completed'})
        assert all(s['status'] == 'completed' for s in stages)

    def test_failed_at_specific_stage(self):
        from app.routes.monitor import _build_stages
        stages = _build_stages({'current_stage': 'scoring', 'status': 'failed'})
        by_key = {s['key']: s['status'] for s in stages}
        assert by_key['discovery'] == 'completed'
        assert by_key['pre_screen'] == 'completed'
        assert by_key['enrichment'] == 'completed'
        assert by_key['analysis'] == 'completed'
        assert by_key['scoring'] == 'failed'
        assert by_key['crm_sync'] == 'pending'

    def test_failed_at_first_stage(self):
        from app.routes.monitor import _build_stages
        stages = _build_stages({'current_stage': 'discovery', 'status': 'failed'})
        by_key = {s['key']: s['status'] for s in stages}
        assert by_key['discovery'] == 'failed'
        assert by_key['pre_screen'] == 'pending'


# ---------------------------------------------------------------------------
# GET /runs
# ---------------------------------------------------------------------------

class TestRunsList:
    """GET /runs renders the runs list page."""

    @patch('app.routes.monitor.Run')
    def test_returns_200(self, mock_run_cls, client):
        mock_run_cls.list_recent.return_value = []
        resp = client.get('/runs')
        assert resp.status_code == 200

    @patch('app.routes.monitor.Run')
    def test_passes_runs_to_template(self, mock_run_cls, client):
        run = _make_run_mock()
        mock_run_cls.list_recent.return_value = [run]
        resp = client.get('/runs')
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GET /runs/<run_id>
# ---------------------------------------------------------------------------

class TestRunDetail:
    """GET /runs/<run_id> renders a single run detail page."""

    @patch('app.routes.monitor.Run')
    def test_returns_200_for_existing_run(self, mock_run_cls, client):
        mock_run_cls.load.return_value = _make_run_mock()
        resp = client.get('/runs/run-001')
        assert resp.status_code == 200

    @patch('app.routes.monitor.Run')
    def test_returns_404_for_missing_run(self, mock_run_cls, client):
        mock_run_cls.load.return_value = None
        resp = client.get('/runs/nonexistent')
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/runs
# ---------------------------------------------------------------------------

class TestCreateRun:
    """POST /api/runs creates a new pipeline run."""

    @patch('app.routes.monitor.launch_run')
    def test_returns_202_on_valid_request(self, mock_launch, client):
        mock_launch.return_value = _make_run_mock(status='queued')
        resp = client.post('/api/runs', json={
            'platform': 'instagram',
            'filters': {'max_results': 10},
        })
        assert resp.status_code == 202
        assert resp.json['status'] == 'queued'

    @patch('app.routes.monitor.launch_run')
    def test_defaults_platform_to_instagram(self, mock_launch, client):
        mock_launch.return_value = _make_run_mock()
        client.post('/api/runs', json={})
        mock_launch.assert_called_once_with(platform='instagram', filters={}, bdr_names=None)

    @patch('app.routes.monitor.launch_run')
    def test_passes_bdr_names(self, mock_launch, client):
        mock_launch.return_value = _make_run_mock()
        client.post('/api/runs', json={
            'platform': 'instagram',
            'bdr_names': ['Alice', 'Bob'],
        })
        mock_launch.assert_called_once_with(
            platform='instagram', filters={}, bdr_names=['Alice', 'Bob'],
        )

    def test_rejects_unsupported_platform(self, client):
        resp = client.post('/api/runs', json={'platform': 'tiktok'})
        assert resp.status_code == 400
        assert 'Unsupported platform' in resp.json['error']

    def test_accepts_patreon(self, client):
        with patch('app.routes.monitor.launch_run') as mock_launch:
            mock_launch.return_value = _make_run_mock(platform='patreon')
            resp = client.post('/api/runs', json={'platform': 'patreon'})
        assert resp.status_code == 202

    def test_accepts_facebook(self, client):
        with patch('app.routes.monitor.launch_run') as mock_launch:
            mock_launch.return_value = _make_run_mock(platform='facebook')
            resp = client.post('/api/runs', json={'platform': 'facebook'})
        assert resp.status_code == 202

    @patch('app.routes.monitor.launch_run')
    def test_launch_error_returns_500(self, mock_launch, client):
        mock_launch.side_effect = Exception("Queue full")
        resp = client.post('/api/runs', json={'platform': 'instagram'})
        assert resp.status_code == 500
        assert 'Queue full' in resp.json['error']

    def test_empty_body_defaults_to_instagram(self, client):
        with patch('app.routes.monitor.launch_run') as mock_launch:
            mock_launch.return_value = _make_run_mock()
            resp = client.post('/api/runs', json={})
        assert resp.status_code == 202


# ---------------------------------------------------------------------------
# GET /api/runs
# ---------------------------------------------------------------------------

class TestListRuns:
    """GET /api/runs returns recent runs as JSON."""

    @patch('app.routes.monitor.Run')
    def test_returns_list_of_runs(self, mock_run_cls, client):
        run = _make_run_mock()
        mock_run_cls.list_recent.return_value = [run]
        resp = client.get('/api/runs')
        assert resp.status_code == 200
        assert isinstance(resp.json, list)
        assert len(resp.json) == 1

    @patch('app.routes.monitor.Run')
    def test_empty_list_when_no_runs(self, mock_run_cls, client):
        mock_run_cls.list_recent.return_value = []
        resp = client.get('/api/runs')
        assert resp.json == []

    @patch('app.routes.monitor.Run')
    def test_respects_limit_param(self, mock_run_cls, client):
        mock_run_cls.list_recent.return_value = []
        client.get('/api/runs?limit=5')
        mock_run_cls.list_recent.assert_called_once_with(limit=5)

    @patch('app.routes.monitor.Run')
    def test_default_limit_is_20(self, mock_run_cls, client):
        mock_run_cls.list_recent.return_value = []
        client.get('/api/runs')
        mock_run_cls.list_recent.assert_called_once_with(limit=20)


# ---------------------------------------------------------------------------
# GET /api/runs/<run_id>
# ---------------------------------------------------------------------------

class TestGetRun:
    """GET /api/runs/<run_id> returns a single run's status."""

    @patch('app.routes.monitor.get_run_status')
    def test_returns_run_status(self, mock_status, client):
        mock_status.return_value = {'id': 'run-001', 'status': 'running'}
        resp = client.get('/api/runs/run-001')
        assert resp.status_code == 200
        assert resp.json['id'] == 'run-001'

    @patch('app.routes.monitor.get_run_status')
    def test_returns_404_when_not_found(self, mock_status, client):
        mock_status.return_value = None
        resp = client.get('/api/runs/nonexistent')
        assert resp.status_code == 404
        assert 'not found' in resp.json['error'].lower()


# ---------------------------------------------------------------------------
# POST /api/runs/<run_id>/retry
# ---------------------------------------------------------------------------

class TestRetryRun:
    """POST /api/runs/<run_id>/retry creates a new run from a checkpoint."""

    @patch('app.pipeline.manager._get_queue')
    @patch('app.services.db.persist_run')
    @patch('app.routes.monitor.Run')
    def test_returns_202_on_valid_retry(self, mock_run_cls, mock_persist, mock_queue, client, db_session):
        original = _make_run_mock(id='orig-001')
        mock_run_cls.load.return_value = original
        # The new Run() created inside the handler
        new_run = _make_run_mock(id='new-001', status='queued')
        mock_run_cls.return_value = new_run

        mock_q = MagicMock()
        mock_queue.return_value = mock_q

        resp = client.post('/api/runs/orig-001/retry', json={'from_stage': 'enrichment'})
        assert resp.status_code == 202

    @patch('app.routes.monitor.Run')
    def test_returns_404_for_missing_original(self, mock_run_cls, client):
        mock_run_cls.load.return_value = None
        resp = client.post('/api/runs/nonexistent/retry', json={})
        assert resp.status_code == 404

    @patch('app.routes.monitor.Run')
    def test_rejects_invalid_stage(self, mock_run_cls, client):
        mock_run_cls.load.return_value = _make_run_mock()
        resp = client.post('/api/runs/run-001/retry', json={'from_stage': 'not_a_stage'})
        assert resp.status_code == 400
        assert 'Invalid stage' in resp.json['error']

    @patch('app.pipeline.manager._get_queue')
    @patch('app.services.db.persist_run')
    @patch('app.routes.monitor.Run')
    def test_retry_without_from_stage(self, mock_run_cls, mock_persist, mock_queue, client, db_session):
        """Retry with no from_stage restarts the full pipeline."""
        original = _make_run_mock()
        mock_run_cls.load.return_value = original
        new_run = _make_run_mock(id='new-002')
        mock_run_cls.return_value = new_run

        mock_q = MagicMock()
        mock_queue.return_value = mock_q

        resp = client.post('/api/runs/run-001/retry', json={})
        assert resp.status_code == 202


# ---------------------------------------------------------------------------
# GET /partials/run-detail/<run_id>
# ---------------------------------------------------------------------------

class TestRunDetailPartial:
    """GET /partials/run-detail/<run_id> renders HTMX partial for run detail."""

    @patch('app.routes.monitor.Run')
    def test_returns_200_for_existing_run(self, mock_run_cls, client):
        mock_run_cls.load.return_value = _make_run_mock(
            current_stage='enrichment', status='running',
        )
        resp = client.get('/partials/run-detail/run-001')
        assert resp.status_code == 200

    @patch('app.routes.monitor.Run')
    def test_returns_404_for_missing_run(self, mock_run_cls, client):
        mock_run_cls.load.return_value = None
        resp = client.get('/partials/run-detail/nonexistent')
        assert resp.status_code == 404
        assert b'Run not found' in resp.data

    @patch('app.routes.monitor.Run')
    def test_terminal_run_sets_is_terminal(self, mock_run_cls, client):
        mock_run_cls.load.return_value = _make_run_mock(
            current_stage='crm_sync', status='completed',
        )
        resp = client.get('/partials/run-detail/run-001')
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GET /stream/run/<run_id>
# ---------------------------------------------------------------------------

class TestStreamRun:
    """GET /stream/run/<run_id> returns an SSE event stream."""

    @patch('app.routes.monitor.Run')
    def test_returns_event_stream_mimetype(self, mock_run_cls, client):
        # Return a terminal run so the stream ends immediately
        mock_run_cls.load.return_value = _make_run_mock(
            status='completed', current_stage='crm_sync',
        )
        resp = client.get('/stream/run/run-001')
        assert resp.status_code == 200
        assert 'text/event-stream' in resp.content_type

    @patch('app.routes.monitor.Run')
    def test_stream_ends_on_terminal_run(self, mock_run_cls, client):
        mock_run_cls.load.return_value = _make_run_mock(
            status='completed', current_stage='crm_sync',
        )
        resp = client.get('/stream/run/run-001')
        data = resp.get_data(as_text=True)
        # SSE data lines should be present
        assert 'data:' in data

    @patch('app.routes.monitor.Run')
    def test_stream_ends_when_run_not_found(self, mock_run_cls, client):
        mock_run_cls.load.return_value = None
        resp = client.get('/stream/run/nonexistent')
        assert resp.status_code == 200
        data = resp.get_data(as_text=True)
        # No data lines since run was not found
        assert data == '' or 'data:' not in data

    @patch('app.routes.monitor.Run')
    def test_no_cache_headers(self, mock_run_cls, client):
        mock_run_cls.load.return_value = _make_run_mock(status='completed', current_stage='crm_sync')
        resp = client.get('/stream/run/run-001')
        assert resp.headers.get('Cache-Control') == 'no-cache'


# ---------------------------------------------------------------------------
# GET /partials/runs-table
# ---------------------------------------------------------------------------

class TestRunsTablePartial:
    """GET /partials/runs-table renders a filtered runs table."""

    @patch('app.routes.monitor.Run')
    def test_returns_200_no_filters(self, mock_run_cls, client):
        mock_run_cls.list_recent.return_value = []
        resp = client.get('/partials/runs-table')
        assert resp.status_code == 200

    @patch('app.routes.monitor.Run')
    def test_filters_by_platform(self, mock_run_cls, client):
        ig_run = _make_run_mock(id='ig', platform='instagram')
        pat_run = _make_run_mock(id='pat', platform='patreon')
        mock_run_cls.list_recent.return_value = [ig_run, pat_run]

        resp = client.get('/partials/runs-table?platform=instagram')
        assert resp.status_code == 200

    @patch('app.routes.monitor.Run')
    def test_filters_active_status(self, mock_run_cls, client):
        active = _make_run_mock(id='a', status='running')
        completed = _make_run_mock(id='c', status='completed')
        mock_run_cls.list_recent.return_value = [active, completed]

        resp = client.get('/partials/runs-table?status=active')
        assert resp.status_code == 200

    @patch('app.routes.monitor.Run')
    def test_filters_specific_status(self, mock_run_cls, client):
        failed = _make_run_mock(id='f', status='failed')
        queued = _make_run_mock(id='q', status='queued')
        mock_run_cls.list_recent.return_value = [failed, queued]

        resp = client.get('/partials/runs-table?status=failed')
        assert resp.status_code == 200

    @patch('app.routes.monitor.Run')
    def test_all_filter_returns_everything(self, mock_run_cls, client):
        mock_run_cls.list_recent.return_value = [
            _make_run_mock(id='1'), _make_run_mock(id='2'),
        ]
        resp = client.get('/partials/runs-table?platform=all&status=all')
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# POST /api/cost-estimate
# ---------------------------------------------------------------------------

class TestCostEstimate:
    """POST /api/cost-estimate returns estimated cost + budget info."""

    @patch('app.routes.monitor._estimate_total_cost', return_value=5.50)
    @patch('app.routes.monitor.get_default_budget', return_value=50.00)
    @patch('app.routes.monitor.get_confirmation_threshold', return_value=10.00)
    def test_returns_estimate(self, mock_thresh, mock_budget, mock_cost, client):
        resp = client.post('/api/cost-estimate', json={
            'platform': 'instagram',
            'filters': {'max_results': 50},
        })
        assert resp.status_code == 200
        assert resp.json['estimated_cost'] == 5.50
        assert resp.json['default_budget'] == 50.00
        assert resp.json['needs_confirmation'] is False

    @patch('app.routes.monitor._estimate_total_cost', return_value=15.00)
    @patch('app.routes.monitor.get_default_budget', return_value=50.00)
    @patch('app.routes.monitor.get_confirmation_threshold', return_value=10.00)
    def test_needs_confirmation_when_above_threshold(self, mock_thresh, mock_budget, mock_cost, client):
        resp = client.post('/api/cost-estimate', json={
            'platform': 'instagram',
            'filters': {'max_results': 200},
        })
        assert resp.status_code == 200
        assert resp.json['needs_confirmation'] is True

    def test_rejects_unsupported_platform(self, client):
        resp = client.post('/api/cost-estimate', json={'platform': 'tiktok'})
        assert resp.status_code == 400

    @patch('app.routes.monitor._estimate_total_cost', side_effect=Exception("oops"))
    def test_returns_500_on_error(self, mock_cost, client):
        resp = client.post('/api/cost-estimate', json={'platform': 'instagram'})
        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# GET /api/pipeline-info
# ---------------------------------------------------------------------------

class TestPipelineInfo:
    """GET /api/pipeline-info returns pipeline stage metadata."""

    @patch('app.routes.monitor.get_pipeline_info')
    def test_returns_json(self, mock_info, client):
        mock_info.return_value = {
            'instagram': {
                'discovery': {'description': 'Find profiles', 'apis': ['Apify'], 'est': 2},
            }
        }
        resp = client.get('/api/pipeline-info')
        assert resp.status_code == 200
        assert 'instagram' in resp.json

    @patch('app.routes.monitor.get_pipeline_info')
    def test_empty_registry(self, mock_info, client):
        mock_info.return_value = {}
        resp = client.get('/api/pipeline-info')
        assert resp.status_code == 200
        assert resp.json == {}


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

class TestMonitorIntegration:
    """Cross-endpoint consistency checks."""

    @patch('app.routes.monitor.get_run_status')
    @patch('app.routes.monitor.Run')
    def test_list_then_get_individual(self, mock_run_cls, mock_status, client):
        """List runs, then fetch one by ID -- both should succeed."""
        run = _make_run_mock(id='run-int-1', status='running')
        mock_run_cls.list_recent.return_value = [run]
        mock_status.return_value = run.to_dict()

        list_resp = client.get('/api/runs')
        assert list_resp.status_code == 200
        run_id = list_resp.json[0]['id']

        detail_resp = client.get(f'/api/runs/{run_id}')
        assert detail_resp.status_code == 200
        assert detail_resp.json['id'] == run_id

    @patch('app.routes.monitor.launch_run')
    @patch('app.routes.monitor.get_run_status')
    def test_create_then_get(self, mock_status, mock_launch, client):
        """Create a run, then fetch its status."""
        created = _make_run_mock(id='created-1', status='queued')
        mock_launch.return_value = created
        mock_status.return_value = created.to_dict()

        create_resp = client.post('/api/runs', json={'platform': 'instagram'})
        assert create_resp.status_code == 202
        run_id = create_resp.json['id']

        status_resp = client.get(f'/api/runs/{run_id}')
        assert status_resp.status_code == 200
        assert status_resp.json['status'] == 'queued'
