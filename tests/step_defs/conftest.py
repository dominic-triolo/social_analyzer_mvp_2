"""Shared BDD fixtures for enrollment step definitions."""
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def context():
    """Mutable dict shared across given/when/then steps in a scenario."""
    return {}


@pytest.fixture
def mock_redis_enrollment():
    """Mock Redis for enrollment dispatcher — lock acquired by default."""
    mock = MagicMock()
    mock.set.return_value = True  # lock acquired
    mock.get.return_value = None
    mock.lrange.return_value = []
    mock.delete.return_value = True
    mock.lpush.return_value = 1
    mock.ltrim.return_value = True
    with patch('app.services.enrollment_dispatcher.r', mock):
        yield mock


TEST_CONFIG = {
    'inboxes': {'inbox_a': 'owner1', 'inbox_b': 'owner2'},
    'max_per_day': 25,
    'sequence_cadence': 3,
    'sequence_steps': 5,
    'outreach_weights': {
        'schedule_call': 4,
        'interest_check': 2,
        'self_service': 1,
    },
    'api_delay': 0,  # no sleep in tests
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
    """Patch enrollment config loader with test values — yields the mock for overrides."""
    with patch(
        'app.services.enrollment_dispatcher.load_enrollment_config',
        return_value={**TEST_CONFIG},
    ) as m:
        yield m


@pytest.fixture
def mock_business_day():
    """Patch is_business_day — defaults to True (weekday)."""
    with patch(
        'app.services.enrollment_dispatcher.is_business_day', return_value=True,
    ) as m:
        yield m


@pytest.fixture
def mock_hubspot_search():
    """Patch HubSpot search — caller sets return_value / side_effect."""
    with patch('app.services.hubspot.hubspot_search_contacts_all') as m:
        yield m


@pytest.fixture
def mock_hubspot_update():
    """Patch HubSpot update — defaults to success."""
    with patch('app.services.hubspot.hubspot_update_contact', return_value=True) as m:
        yield m


@pytest.fixture
def mock_persist():
    """Patch DB persistence so tests don't need a real database."""
    with patch('app.services.db.persist_enrollment_run'):
        yield
