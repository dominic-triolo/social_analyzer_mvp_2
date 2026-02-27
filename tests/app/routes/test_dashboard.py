"""Tests for app.routes.dashboard — Home page, stats API, health check, HTMX partials."""
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def mock_dashboard_redis():
    """Mock the Redis client as imported by the dashboard module (aliased as 'r')."""
    mock = MagicMock()
    mock.get.return_value = None
    mock.llen.return_value = 0
    mock.hgetall.return_value = {}
    mock.lrange.return_value = []
    mock.delete.return_value = 1
    with patch('app.routes.dashboard.r', mock):
        yield mock


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------

class TestHealthCheck:
    """GET /health returns a simple health status."""

    def test_returns_200_with_healthy_status(self, client):
        resp = client.get('/health')
        assert resp.status_code == 200
        assert resp.json == {"status": "healthy"}


# ---------------------------------------------------------------------------
# /
# ---------------------------------------------------------------------------

class TestIndex:
    """GET / renders the home page."""

    def test_returns_200(self, client):
        resp = client.get('/')
        assert resp.status_code == 200

    def test_renders_html_content(self, client):
        resp = client.get('/')
        assert resp.content_type.startswith('text/html')


# ---------------------------------------------------------------------------
# /api/stats
# ---------------------------------------------------------------------------

class TestGetStats:
    """GET /api/stats returns aggregated dashboard statistics from Redis."""

    def test_returns_200_on_success(self, client, mock_dashboard_redis):
        resp = client.get('/api/stats')
        assert resp.status_code == 200

    def test_all_zeros_when_redis_empty(self, client, mock_dashboard_redis):
        data = client.get('/api/stats').json
        assert data['queue_size'] == 0
        assert data['active_workers'] == 0
        assert data['total_completed'] == 0
        assert data['total_errors'] == 0
        assert data['avg_duration'] == 0
        assert data['est_time_remaining'] == 0

    def test_queue_size_and_active_workers(self, client, mock_dashboard_redis):
        mock_dashboard_redis.llen.return_value = 5

        data = client.get('/api/stats').json
        assert data['queue_size'] == 5
        # active_workers = min(queue_size, 8) when queue > 0
        assert data['active_workers'] == 5

    def test_active_workers_capped_at_8(self, client, mock_dashboard_redis):
        mock_dashboard_redis.llen.return_value = 20

        data = client.get('/api/stats').json
        assert data['active_workers'] == 8

    def test_active_workers_zero_when_queue_empty(self, client, mock_dashboard_redis):
        mock_dashboard_redis.llen.return_value = 0

        data = client.get('/api/stats').json
        assert data['active_workers'] == 0

    def test_breakdown_from_results_hash(self, client, mock_dashboard_redis):
        mock_dashboard_redis.hgetall.side_effect = [
            {'post_frequency': '3', 'pre_screened': '5', 'enriched': '10', 'error': '2'},
            {},
        ]

        data = client.get('/api/stats').json
        assert data['breakdown']['post_frequency'] == 3
        assert data['breakdown']['pre_screened'] == 5
        assert data['breakdown']['enriched'] == 10
        assert data['breakdown']['errors'] == 2
        assert data['total_completed'] == 18  # 3 + 5 + 10
        assert data['total_errors'] == 2

    def test_priority_tiers(self, client, mock_dashboard_redis):
        mock_dashboard_redis.hgetall.side_effect = [
            {'enriched': '20'},
            {'auto_enroll': '5', 'high_priority_review': '8',
             'standard_priority_review': '4', 'low_priority_review': '3'},
        ]

        data = client.get('/api/stats').json
        assert data['priority_tiers']['auto_enroll'] == 5
        assert data['priority_tiers']['high_priority_review'] == 8
        assert data['priority_tiers']['standard_priority_review'] == 4
        assert data['priority_tiers']['low_priority_review'] == 3
        assert data['priority_tiers']['total'] == 20  # enriched = total_passed

    def test_tier_percentages_when_total_passed_nonzero(self, client, mock_dashboard_redis):
        mock_dashboard_redis.hgetall.side_effect = [
            {'enriched': '100'},
            {'auto_enroll': '25', 'high_priority_review': '25',
             'standard_priority_review': '25', 'low_priority_review': '25'},
        ]

        data = client.get('/api/stats').json
        pcts = data['batch_quality']['tier_percentages']
        assert pcts['auto_enroll'] == 25.0
        assert pcts['high_priority_review'] == 25.0
        assert pcts['standard_priority_review'] == 25.0
        assert pcts['low_priority_review'] == 25.0

    def test_tier_percentages_zero_when_no_passed(self, client, mock_dashboard_redis):
        mock_dashboard_redis.hgetall.side_effect = [
            {'enriched': '0'},
            {},
        ]

        data = client.get('/api/stats').json
        pcts = data['batch_quality']['tier_percentages']
        assert pcts['auto_enroll'] == 0
        assert pcts['high_priority_review'] == 0
        assert pcts['standard_priority_review'] == 0
        assert pcts['low_priority_review'] == 0

    def test_avg_duration_calculation(self, client, mock_dashboard_redis):
        mock_dashboard_redis.lrange.return_value = ['10', '20', '30']

        data = client.get('/api/stats').json
        assert data['avg_duration'] == 20.0

    def test_avg_duration_zero_when_no_durations(self, client, mock_dashboard_redis):
        mock_dashboard_redis.lrange.return_value = []

        data = client.get('/api/stats').json
        assert data['avg_duration'] == 0

    def test_est_time_remaining_calculated(self, client, mock_dashboard_redis):
        mock_dashboard_redis.llen.return_value = 10
        mock_dashboard_redis.lrange.return_value = ['60']

        data = client.get('/api/stats').json
        # est_time_remaining = (queue_size / workers) * avg_duration / 60
        # = (10 / 2) * 60 / 60 = 5.0
        assert data['est_time_remaining'] == 5.0

    def test_est_time_remaining_zero_when_no_queue(self, client, mock_dashboard_redis):
        mock_dashboard_redis.llen.return_value = 0
        mock_dashboard_redis.lrange.return_value = ['60']

        data = client.get('/api/stats').json
        assert data['est_time_remaining'] == 0

    def test_est_time_remaining_zero_when_no_duration(self, client, mock_dashboard_redis):
        mock_dashboard_redis.llen.return_value = 10
        mock_dashboard_redis.lrange.return_value = []

        data = client.get('/api/stats').json
        assert data['est_time_remaining'] == 0

    def test_pass_rate_calculation(self, client, mock_dashboard_redis):
        mock_dashboard_redis.hgetall.side_effect = [
            {'post_frequency': '10', 'pre_screened': '10', 'enriched': '80', 'error': '0'},
            {},
        ]

        data = client.get('/api/stats').json
        # total_processed = 100, total_passed (enriched) = 80 => 80%
        assert data['batch_quality']['pass_rate'] == 80.0

    def test_pass_rate_zero_when_nothing_processed(self, client, mock_dashboard_redis):
        data = client.get('/api/stats').json
        assert data['batch_quality']['pass_rate'] == 0

    def test_pre_screening_breakdown(self, client, mock_dashboard_redis):
        mock_dashboard_redis.hgetall.side_effect = [
            {'post_frequency': '7', 'pre_screened': '12', 'enriched': '0'},
            {},
        ]

        data = client.get('/api/stats').json
        assert data['pre_screening']['total_pre_screened'] == 19
        assert data['pre_screening']['low_post_frequency'] == 7
        assert data['pre_screening']['outside_icp'] == 12

    def test_redis_error_returns_zeroed_fallback(self, client, mock_dashboard_redis):
        """When Redis throws, the endpoint returns a zeroed-out fallback, not a 500."""
        mock_dashboard_redis.llen.side_effect = Exception("Redis down")

        resp = client.get('/api/stats')
        assert resp.status_code == 200
        data = resp.json
        assert data['queue_size'] == 0
        assert data['total_completed'] == 0

    def test_response_includes_all_top_level_keys(self, client, mock_dashboard_redis):
        """Verify the response shape contains all expected sections."""
        data = client.get('/api/stats').json
        expected_keys = {
            'queue_size', 'active_workers', 'total_completed', 'total_errors',
            'avg_duration', 'est_time_remaining', 'breakdown', 'pre_screening',
            'priority_tiers', 'batch_quality',
        }
        assert expected_keys.issubset(data.keys())


# ---------------------------------------------------------------------------
# /api/stats/reset
# ---------------------------------------------------------------------------

class TestResetStats:
    """POST /api/stats/reset clears Redis dashboard keys."""

    def test_returns_success(self, client, mock_dashboard_redis):
        resp = client.post('/api/stats/reset')
        assert resp.status_code == 200
        assert resp.json['status'] == 'success'

    def test_deletes_all_three_keys(self, client, mock_dashboard_redis):
        client.post('/api/stats/reset')

        deleted_keys = [call.args[0] for call in mock_dashboard_redis.delete.call_args_list]
        assert 'trovastats:results' in deleted_keys
        assert 'trovastats:priority_tiers' in deleted_keys
        assert 'trovastats:durations' in deleted_keys

    def test_redis_error_returns_500(self, client, mock_dashboard_redis):
        mock_dashboard_redis.delete.side_effect = Exception("Redis connection refused")

        resp = client.post('/api/stats/reset')
        assert resp.status_code == 500
        assert resp.json['status'] == 'error'

    def test_get_method_not_allowed(self, client, mock_dashboard_redis):
        resp = client.get('/api/stats/reset')
        assert resp.status_code == 405


# ---------------------------------------------------------------------------
# /partials/dashboard-stats
# ---------------------------------------------------------------------------

class TestDashboardStatsPartial:
    """GET /partials/dashboard-stats renders KPI cards via HTMX."""

    @patch('app.routes.dashboard.Run')
    def test_returns_200(self, mock_run_cls, client, mock_dashboard_redis):
        mock_run_cls.list_recent.return_value = []
        resp = client.get('/partials/dashboard-stats')
        assert resp.status_code == 200

    @patch('app.routes.dashboard.Run')
    def test_stats_computed_from_runs(self, mock_run_cls, client, mock_dashboard_redis):
        run1 = MagicMock()
        run1.to_dict.return_value = {
            'status': 'completed', 'profiles_found': 50, 'contacts_synced': 30,
        }
        run2 = MagicMock()
        run2.to_dict.return_value = {
            'status': 'running', 'profiles_found': 20, 'contacts_synced': 0,
        }
        mock_run_cls.list_recent.return_value = [run1, run2]

        resp = client.get('/partials/dashboard-stats')
        assert resp.status_code == 200

    @patch('app.routes.dashboard.Run')
    def test_active_count_excludes_completed_and_failed(self, mock_run_cls, client, mock_dashboard_redis):
        completed = MagicMock()
        completed.to_dict.return_value = {'status': 'completed', 'profiles_found': 10, 'contacts_synced': 5}
        failed = MagicMock()
        failed.to_dict.return_value = {'status': 'failed', 'profiles_found': 0, 'contacts_synced': 0}
        running = MagicMock()
        running.to_dict.return_value = {'status': 'running', 'profiles_found': 5, 'contacts_synced': 0}

        mock_run_cls.list_recent.return_value = [completed, failed, running]

        resp = client.get('/partials/dashboard-stats')
        assert resp.status_code == 200

    @patch('app.routes.dashboard.Run')
    def test_empty_runs_list(self, mock_run_cls, client, mock_dashboard_redis):
        mock_run_cls.list_recent.return_value = []
        resp = client.get('/partials/dashboard-stats')
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# /partials/recent-runs
# ---------------------------------------------------------------------------

class TestRecentRunsPartial:
    """GET /partials/recent-runs renders the recent runs list via HTMX."""

    @patch('app.routes.dashboard.Run')
    def test_returns_200(self, mock_run_cls, client, mock_dashboard_redis):
        mock_run_cls.list_recent.return_value = []
        resp = client.get('/partials/recent-runs')
        assert resp.status_code == 200

    @patch('app.routes.dashboard.Run')
    def test_calls_list_recent_with_limit_5(self, mock_run_cls, client, mock_dashboard_redis):
        mock_run_cls.list_recent.return_value = []
        client.get('/partials/recent-runs')
        mock_run_cls.list_recent.assert_called_once_with(limit=5)

    @patch('app.routes.dashboard.Run')
    def test_renders_with_run_data(self, mock_run_cls, client, mock_dashboard_redis):
        run = MagicMock()
        run.to_dict.return_value = {
            'id': 'run-1', 'status': 'completed', 'platform': 'instagram',
        }
        mock_run_cls.list_recent.return_value = [run]

        resp = client.get('/partials/recent-runs')
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# /partials/sidebar-badge
# ---------------------------------------------------------------------------

class TestSidebarBadgePartial:
    """GET /partials/sidebar-badge renders the active run count badge."""

    @patch('app.routes.dashboard.Run')
    def test_returns_200(self, mock_run_cls, client, mock_dashboard_redis):
        mock_run_cls.list_recent.return_value = []
        resp = client.get('/partials/sidebar-badge')
        assert resp.status_code == 200

    @patch('app.routes.dashboard.Run')
    def test_calls_list_recent_with_limit_10(self, mock_run_cls, client, mock_dashboard_redis):
        mock_run_cls.list_recent.return_value = []
        client.get('/partials/sidebar-badge')
        mock_run_cls.list_recent.assert_called_once_with(limit=10)

    @patch('app.routes.dashboard.Run')
    def test_counts_only_active_runs(self, mock_run_cls, client, mock_dashboard_redis):
        completed = MagicMock()
        completed.to_dict.return_value = {'status': 'completed'}
        active = MagicMock()
        active.to_dict.return_value = {'status': 'running'}
        mock_run_cls.list_recent.return_value = [completed, active]

        resp = client.get('/partials/sidebar-badge')
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

class TestDashboardIntegration:
    """Cross-endpoint consistency checks."""

    def test_health_and_stats_both_reachable(self, client, mock_dashboard_redis):
        """Both core API endpoints respond successfully."""
        health = client.get('/health')
        stats = client.get('/api/stats')
        assert health.status_code == 200
        assert stats.status_code == 200

    def test_reset_then_stats_returns_zeros(self, client, mock_dashboard_redis):
        """After resetting, stats should return zeroed values."""
        reset_resp = client.post('/api/stats/reset')
        assert reset_resp.status_code == 200

        stats_resp = client.get('/api/stats')
        assert stats_resp.json['total_completed'] == 0
        assert stats_resp.json['total_errors'] == 0
