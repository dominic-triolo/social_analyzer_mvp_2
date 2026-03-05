"""Tests for app.services.enrollment_dispatcher — mocks HubSpot + Redis + DB."""
import json
import pytest
from datetime import date
from unittest.mock import patch, MagicMock, call

from app.services.enrollment_dispatcher import (
    run_enrollment_dispatcher,
    get_last_run,
    get_run_history,
)


@pytest.fixture
def mock_redis_enrollment():
    """Mock Redis for enrollment dispatcher."""
    mock = MagicMock()
    mock.set.return_value = True  # lock acquired
    mock.get.return_value = None
    mock.lrange.return_value = []
    mock.delete.return_value = True
    mock.lpush.return_value = 1
    mock.ltrim.return_value = True
    with patch('app.services.enrollment_dispatcher.r', mock):
        yield mock


_TEST_CONFIG = {
    'inboxes': {'inbox_a': 'owner1', 'inbox_b': 'owner2'},
    'max_per_day': 25,
    'sequence_cadence': 3,
    'sequence_steps': 5,
    'outreach_weights': {
        'schedule_call': 4,
        'interest_check': 2,
        'self_service': 1,
    },
    'api_delay': 0,
    'timezone': 'America/Los_Angeles',
    'hubspot_properties': {
        'status_field': 'reply_sequence_queue_status',
        'inbox_field': 'reply_io_sequence',
        'date_field': 'recent_reply_sequence_enrolled_date',
        'segment_field': 'outreach_segment',
        'trigger_field': 'enroll_in_reply_sequence',
        'score_field': 'combined_lead_score',
        'createdate_field': 'hs_createdate',
    },
}


@pytest.fixture
def enrollment_config():
    """Patch enrollment config loader with test values."""
    with patch('app.services.enrollment_dispatcher.load_enrollment_config', return_value=_TEST_CONFIG):
        yield


class TestRunEnrollmentDispatcher:

    @patch('app.services.db.persist_enrollment_run')
    @patch('app.services.enrollment_dispatcher.is_business_day', return_value=False)
    def test_skips_non_business_day(self, mock_biz, mock_persist,
                                     mock_redis_enrollment, enrollment_config):
        result = run_enrollment_dispatcher()
        assert result['status'] == 'skipped'
        assert result['reason'] == 'not_business_day'

    @patch('app.services.db.persist_enrollment_run')
    @patch('app.services.enrollment_dispatcher.is_business_day', return_value=True)
    def test_skips_when_no_inboxes(self, mock_biz, mock_persist, mock_redis_enrollment):
        empty_cfg = {**_TEST_CONFIG, 'inboxes': {}}
        with patch('app.services.enrollment_dispatcher.load_enrollment_config', return_value=empty_cfg):
            result = run_enrollment_dispatcher()
        assert result['status'] == 'skipped'
        assert result['reason'] == 'no_inboxes_configured'

    @patch('app.services.db.persist_enrollment_run')
    @patch('app.services.enrollment_dispatcher.is_business_day', return_value=True)
    def test_force_overrides_business_day(self, mock_biz, mock_persist,
                                           mock_redis_enrollment, enrollment_config):
        """force=True should proceed even if is_business_day is False."""
        mock_biz.return_value = False
        with patch('app.services.hubspot.hubspot_search_contacts_all', return_value=[]):
            result = run_enrollment_dispatcher(force=True)
        # Should not skip for not_business_day
        assert result.get('reason') != 'not_business_day'

    @patch('app.services.db.persist_enrollment_run')
    @patch('app.services.enrollment_dispatcher.is_business_day', return_value=True)
    @patch('app.services.hubspot.hubspot_search_contacts_all')
    def test_skips_when_no_queued(self, mock_search, mock_biz, mock_persist,
                                   mock_redis_enrollment, enrollment_config):
        mock_search.side_effect = [
            [{'id': '1', 'properties': {'reply_sequence_queue_status': 'active'}}],  # active
            [],  # queued (empty)
        ]
        result = run_enrollment_dispatcher()
        assert result['status'] == 'skipped'
        assert result['reason'] == 'no_queued_contacts'
        assert result['active_count'] == 1

    @patch('app.services.db.persist_enrollment_run')
    @patch('app.services.enrollment_dispatcher.is_business_day', return_value=True)
    @patch('app.services.hubspot.hubspot_update_contact', return_value=True)
    @patch('app.services.hubspot.hubspot_search_contacts_all')
    def test_enrolls_contacts_successfully(self, mock_search, mock_update,
                                            mock_biz, mock_persist,
                                            mock_redis_enrollment, enrollment_config):
        mock_search.side_effect = [
            [],  # no active contacts
            [
                {'id': '100', 'properties': {'reply_sequence_queue_status': 'queued', 'outreach_segment': 'schedule_call', 'combined_lead_score': '50', 'hs_createdate': '2026-01-10'}},
                {'id': '101', 'properties': {'reply_sequence_queue_status': 'queued', 'outreach_segment': 'interest_check', 'combined_lead_score': '40', 'hs_createdate': '2026-01-11'}},
            ],
        ]

        result = run_enrollment_dispatcher()
        assert result['status'] == 'completed'
        assert result['enrolled_count'] == 2
        assert result['error_count'] == 0
        assert len(result['enrolled_details']) == 2
        assert mock_update.call_count == 2

    @patch('app.services.db.persist_enrollment_run')
    @patch('app.services.enrollment_dispatcher.is_business_day', return_value=True)
    @patch('app.services.hubspot.hubspot_update_contact', return_value=True)
    @patch('app.services.hubspot.hubspot_search_contacts_all')
    def test_dry_run_does_not_call_hubspot_update(self, mock_search, mock_update,
                                                    mock_biz, mock_persist,
                                                    mock_redis_enrollment, enrollment_config):
        mock_search.side_effect = [
            [],  # active
            [{'id': '100', 'properties': {'reply_sequence_queue_status': 'queued', 'outreach_segment': 'schedule_call', 'combined_lead_score': '50', 'hs_createdate': '2026-01-10'}}],
        ]

        result = run_enrollment_dispatcher(dry_run=True)
        assert result['status'] == 'completed'
        assert result['enrolled_count'] == 1
        assert result['dry_run'] is True
        assert result['enrolled_details'][0]['dry_run'] is True
        mock_update.assert_not_called()

    @patch('app.services.db.persist_enrollment_run')
    @patch('app.services.enrollment_dispatcher.is_business_day', return_value=True)
    @patch('app.services.hubspot.hubspot_update_contact', return_value=False)
    @patch('app.services.hubspot.hubspot_search_contacts_all')
    def test_tracks_hubspot_update_failures(self, mock_search, mock_update,
                                             mock_biz, mock_persist,
                                             mock_redis_enrollment, enrollment_config):
        mock_search.side_effect = [
            [],
            [{'id': '100', 'properties': {'reply_sequence_queue_status': 'queued', 'outreach_segment': 'schedule_call', 'combined_lead_score': '50', 'hs_createdate': '2026-01-10'}}],
        ]

        result = run_enrollment_dispatcher()
        assert result['status'] == 'completed'
        assert result['enrolled_count'] == 0
        assert result['error_count'] == 1

    def test_skips_concurrent_run(self, mock_redis_enrollment, enrollment_config):
        """If Redis lock cannot be acquired, skip."""
        mock_redis_enrollment.set.return_value = False  # lock NOT acquired

        result = run_enrollment_dispatcher()
        assert result['status'] == 'skipped'
        assert result['reason'] == 'concurrent_run'

    @patch('app.services.db.persist_enrollment_run')
    @patch('app.services.enrollment_dispatcher.is_business_day', return_value=True)
    @patch('app.services.hubspot.hubspot_search_contacts_all', side_effect=Exception('API down'))
    def test_handles_hubspot_exception(self, mock_search, mock_biz, mock_persist,
                                        mock_redis_enrollment, enrollment_config):
        result = run_enrollment_dispatcher()
        assert result['status'] == 'error'
        assert 'API down' in result['reason']


class TestGetLastRun:
    def test_returns_none_when_no_data(self, mock_redis_enrollment):
        result = get_last_run()
        assert result is None

    def test_returns_parsed_json(self, mock_redis_enrollment):
        mock_redis_enrollment.get.return_value = json.dumps({
            'status': 'completed',
            'enrolled_count': 5,
        })
        result = get_last_run()
        assert result['status'] == 'completed'
        assert result['enrolled_count'] == 5


class TestGetRunHistory:
    def test_returns_from_redis_when_available(self, mock_redis_enrollment):
        mock_redis_enrollment.lrange.return_value = [
            json.dumps({'status': 'completed', 'enrolled_count': 3}),
            json.dumps({'status': 'skipped', 'reason': 'not_business_day'}),
        ]
        result = get_run_history(limit=10)
        assert len(result) == 2
        assert result[0]['status'] == 'completed'

    @patch('app.services.db.get_enrollment_history')
    def test_falls_back_to_postgres(self, mock_db, mock_redis_enrollment):
        mock_redis_enrollment.lrange.return_value = []
        mock_db.return_value = [{'status': 'completed', 'enrolled_count': 1}]

        result = get_run_history(limit=10)
        assert len(result) == 1
        mock_db.assert_called_once_with(10)
