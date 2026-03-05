"""Tests for app.routes.enrollment — route tests via Flask test client."""
import json
import pytest
from unittest.mock import patch, MagicMock


class TestDispatchEndpoint:
    @patch('rq.Queue')
    @patch('app.extensions.redis_client')
    def test_dispatch_returns_202(self, mock_redis, mock_queue_cls, client):
        mock_job = MagicMock()
        mock_job.id = 'job-123'
        mock_queue_cls.return_value.enqueue.return_value = mock_job

        resp = client.post('/api/enrollment/dispatch',
                           json={'dry_run': False},
                           content_type='application/json')
        assert resp.status_code == 202
        data = resp.get_json()
        assert data['job_id'] == 'job-123'
        assert data['status'] == 'queued'

    @patch('rq.Queue')
    @patch('app.extensions.redis_client')
    def test_dispatch_passes_dry_run(self, mock_redis, mock_queue_cls, client):
        mock_job = MagicMock()
        mock_job.id = 'job-456'
        mock_queue_cls.return_value.enqueue.return_value = mock_job

        resp = client.post('/api/enrollment/dispatch',
                           json={'dry_run': True},
                           content_type='application/json')
        assert resp.status_code == 202

        enqueue_call = mock_queue_cls.return_value.enqueue
        enqueue_call.assert_called_once()
        assert enqueue_call.call_args.kwargs['dry_run'] is True

    @patch('rq.Queue', side_effect=Exception('Redis down'))
    @patch('app.extensions.redis_client')
    def test_dispatch_handles_error(self, mock_redis, mock_queue_cls, client):
        resp = client.post('/api/enrollment/dispatch',
                           json={},
                           content_type='application/json')
        assert resp.status_code == 500
        assert 'error' in resp.get_json()


class TestLastRunEndpoint:
    @patch('app.services.enrollment_dispatcher.get_last_run', return_value=None)
    def test_returns_no_runs(self, mock_last, client):
        resp = client.get('/api/enrollment/last-run')
        assert resp.status_code == 200
        assert resp.get_json()['status'] == 'no_runs'

    @patch('app.services.enrollment_dispatcher.get_last_run')
    def test_returns_last_run(self, mock_last, client):
        mock_last.return_value = {'status': 'completed', 'enrolled_count': 5}
        resp = client.get('/api/enrollment/last-run')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] == 'completed'
        assert data['enrolled_count'] == 5


class TestHistoryEndpoint:
    @patch('app.services.enrollment_dispatcher.get_run_history')
    def test_returns_history(self, mock_history, client):
        mock_history.return_value = [
            {'status': 'completed', 'enrolled_count': 3},
            {'status': 'skipped', 'reason': 'not_business_day'},
        ]
        resp = client.get('/api/enrollment/history')
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data) == 2

    @patch('app.services.enrollment_dispatcher.get_run_history')
    def test_passes_limit_param(self, mock_history, client):
        mock_history.return_value = []
        resp = client.get('/api/enrollment/history?limit=5')
        mock_history.assert_called_once_with(5)


class TestEnrollmentPage:
    @patch('app.services.enrollment_dispatcher.get_run_history', return_value=[])
    @patch('app.services.enrollment_dispatcher.get_last_run', return_value=None)
    def test_page_renders(self, mock_last, mock_history, client):
        resp = client.get('/enrollment')
        assert resp.status_code == 200
        assert b'Enrollment' in resp.data

    @patch('app.services.enrollment_dispatcher.get_run_history', return_value=[])
    @patch('app.services.enrollment_dispatcher.get_last_run')
    def test_page_shows_last_run(self, mock_last, mock_history, client):
        mock_last.return_value = {'status': 'completed', 'enrolled_count': 5,
                                   'queued_count': 10, 'total_slots': 20,
                                   'error_count': 0, 'run_date': '2026-03-04',
                                   'started_at': '2026-03-04T10:00:00'}
        resp = client.get('/enrollment')
        assert resp.status_code == 200
        assert b'Completed' in resp.data or b'completed' in resp.data


class TestEnrollmentStatusPartial:
    @patch('app.services.enrollment_dispatcher.get_last_run', return_value=None)
    def test_partial_renders_empty(self, mock_last, client):
        resp = client.get('/partials/enrollment-status')
        assert resp.status_code == 200
        assert b'No enrollment runs yet' in resp.data

    @patch('app.services.enrollment_dispatcher.get_last_run')
    def test_partial_renders_status(self, mock_last, client):
        mock_last.return_value = {
            'status': 'completed',
            'enrolled_count': 3,
            'queued_count': 5,
            'total_slots': 10,
            'error_count': 0,
            'dry_run': False,
            'run_date': '2026-03-04',
            'started_at': '2026-03-04T10:00:00',
        }
        resp = client.get('/partials/enrollment-status')
        assert resp.status_code == 200
        assert b'Completed' in resp.data or b'completed' in resp.data
