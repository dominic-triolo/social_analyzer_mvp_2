"""Shared test fixtures."""
import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base


@pytest.fixture
def db_engine():
    """In-memory SQLite engine with schema created."""
    engine = create_engine('sqlite:///:memory:')
    import app.models.db_run
    import app.models.lead
    import app.models.lead_run
    import app.models.filter_history
    import app.models.preset
    import app.models.metric_snapshot
    import app.models.enrollment_run
    import app.models.app_config
    import app.models.hubspot_list
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(db_engine):
    """SQLAlchemy session bound to in-memory SQLite. Rolls back after each test."""
    Session = sessionmaker(bind=db_engine)
    session = Session()
    yield session
    session.rollback()
    session.close()


@pytest.fixture(autouse=True)
def patch_get_session(db_session):
    """Route all get_session() calls to the test session.

    We disable close() so that route handlers calling session.close()
    in their finally blocks don't invalidate the shared test session.
    """
    _real_close = db_session.close
    db_session.close = lambda: None
    with patch('app.database.get_session', return_value=db_session):
        yield db_session
    db_session.close = _real_close


@pytest.fixture
def mock_redis():
    """Mock Redis client. Returns a MagicMock with common Redis methods."""
    mock = MagicMock()
    mock.get.return_value = None
    mock.setex.return_value = True
    mock.zadd.return_value = 1
    mock.zrevrange.return_value = []
    with patch('app.extensions.redis_client', mock):
        yield mock


@pytest.fixture
def app():
    """Flask test app."""
    from app import create_app
    app = create_app()
    app.config['TESTING'] = True
    yield app


@pytest.fixture
def client(app):
    """Flask test client."""
    with app.test_client() as c:
        yield c


@pytest.fixture
def make_run():
    """Factory fixture — builds a Run-like MagicMock without touching Redis."""
    def _make(**overrides):
        defaults = dict(
            id='run-test-001',
            platform='instagram',
            run_type='discovery',
            status='queued',
            current_stage='',
            filters={'max_results': 10},
            bdr_assignment='Test BDR',
            estimated_cost=1.50,
            actual_cost=None,
            created_at='2026-01-15T10:00:00',
            profiles_discovered=0,
            profiles_found=0,
            profiles_pre_screened=0,
            profiles_enriched=0,
            profiles_scored=0,
            contacts_synced=0,
            duplicates_skipped=0,
            hubspot_duplicates=0,
            tier_distribution={
                'auto_enroll': 0,
                'standard_review': 0,
            },
            error_count=0,
            errors=[],
            summary='',
            stage_outputs={},
            stage_timings={},
        )
        defaults.update(overrides)
        run = MagicMock()
        for k, v in defaults.items():
            setattr(run, k, v)
        return run
    return _make


@pytest.fixture
def sample_profiles():
    """List of profile dicts matching InsightIQ _standardize_results() format."""
    return [
        {
            'first_and_last_name': 'Creator One',
            'flagship_social_platform_handle': 'creator_one',
            'instagram_handle': 'https://www.instagram.com/creator_one/',
            'instagram_bio': 'Travel photographer and adventure lover',
            'instagram_followers': 50000,
            'average_engagement': 0.035,
            'email': 'creator_one@email.com',
            'phone': None,
            'tiktok_handle': None,
            'youtube_profile_link': None,
            'facebook_profile_link': None,
            'patreon_link': None,
            'pinterest_profile_link': None,
            'city': 'Los Angeles',
            'state': None,
            'country': 'US',
            'flagship_social_platform': 'instagram',
            'channel': 'Outbound',
            'channel_host_prospected': 'Phyllo',
            'funnel': 'Creator',
            'enrichment_status': 'pending',
        },
        {
            'first_and_last_name': 'Creator Two',
            'flagship_social_platform_handle': 'creator_two',
            'instagram_handle': 'https://www.instagram.com/creator_two/',
            'instagram_bio': 'Food and culture explorer',
            'instagram_followers': 120000,
            'average_engagement': 0.042,
            'email': 'creator_two@email.com',
            'phone': None,
            'tiktok_handle': None,
            'youtube_profile_link': None,
            'facebook_profile_link': None,
            'patreon_link': None,
            'pinterest_profile_link': None,
            'city': 'New York',
            'state': None,
            'country': 'US',
            'flagship_social_platform': 'instagram',
            'channel': 'Outbound',
            'channel_host_prospected': 'Phyllo',
            'funnel': 'Creator',
            'enrichment_status': 'pending',
        },
    ]
