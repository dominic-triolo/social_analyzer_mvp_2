"""Tests for app.routes.evaluation — analytics dashboard + API endpoints."""
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch

from app.models.db_run import DbRun
from app.models.lead import Lead
from app.models.lead_run import LeadRun


@pytest.fixture
def patch_eval_session(db_session):
    """Patch get_session where the evaluation routes imported it."""
    with patch('app.routes.evaluation.get_session', return_value=db_session):
        yield db_session


@pytest.fixture
def seed_completed_runs(patch_eval_session):
    """Insert completed runs across platforms."""
    session = patch_eval_session
    now = datetime.now()
    runs = [
        DbRun(id='r1', platform='instagram', status='completed',
               profiles_found=100, profiles_pre_screened=80,
               profiles_enriched=70, profiles_scored=50, contacts_synced=30,
               created_at=now - timedelta(hours=2), finished_at=now - timedelta(hours=1)),
        DbRun(id='r2', platform='instagram', status='completed',
               profiles_found=200, profiles_pre_screened=150,
               profiles_enriched=120, profiles_scored=90, contacts_synced=60,
               created_at=now - timedelta(days=1), finished_at=now - timedelta(days=1, hours=-1)),
        DbRun(id='r3', platform='patreon', status='completed',
               profiles_found=50, profiles_pre_screened=40,
               profiles_enriched=35, profiles_scored=25, contacts_synced=15,
               created_at=now - timedelta(days=2), finished_at=now - timedelta(days=2, hours=-1)),
        DbRun(id='r4', platform='instagram', status='failed',
               profiles_found=10, profiles_pre_screened=0,
               profiles_enriched=0, profiles_scored=0, contacts_synced=0,
               created_at=now - timedelta(days=3)),
    ]
    session.add_all(runs)
    session.commit()
    return session


@pytest.fixture
def seed_scored_leads(patch_eval_session):
    """Insert leads with scoring data."""
    session = patch_eval_session
    lead = Lead(platform='instagram', platform_id='user1', name='Test Creator')
    session.add(lead)
    session.flush()

    lead_runs = [
        LeadRun(lead_id=lead.id, run_id='r1', lead_score=0.85,
                priority_tier='auto_enroll'),
        LeadRun(lead_id=lead.id, run_id='r2', lead_score=0.65,
                priority_tier='high_priority_review'),
        LeadRun(lead_id=lead.id, run_id='r3', lead_score=0.45,
                priority_tier='standard_priority_review'),
        LeadRun(lead_id=lead.id, run_id='r4', lead_score=0.25,
                priority_tier='low_priority_review'),
    ]
    session.add_all(lead_runs)
    session.commit()
    return session


# ---------------------------------------------------------------------------
# /evaluation
# ---------------------------------------------------------------------------

class TestEvaluationPage:

    def test_returns_200(self, client):
        resp = client.get('/evaluation')
        assert resp.status_code == 200

    def test_renders_html(self, client):
        resp = client.get('/evaluation')
        assert resp.content_type.startswith('text/html')


# ---------------------------------------------------------------------------
# /partials/eval-kpis
# ---------------------------------------------------------------------------

class TestEvalKpisPartial:

    def test_returns_200_with_no_data(self, client, patch_eval_session):
        resp = client.get('/partials/eval-kpis')
        assert resp.status_code == 200

    def test_shows_dash_when_no_scored_leads(self, client, patch_eval_session):
        resp = client.get('/partials/eval-kpis')
        assert b'no runs completed yet' in resp.data

    def test_shows_scores_when_data_exists(self, client, seed_scored_leads):
        resp = client.get('/partials/eval-kpis')
        assert resp.status_code == 200
        assert b'Total Scored' in resp.data
        # Should not show the empty state message
        assert b'no runs completed yet' not in resp.data

    def test_auto_rate_calculated(self, client, seed_scored_leads):
        resp = client.get('/partials/eval-kpis')
        # 1 out of 4 is auto_enroll = 25%
        assert b'25.0%' in resp.data


# ---------------------------------------------------------------------------
# /api/evaluation/channels
# ---------------------------------------------------------------------------

class TestApiChannels:

    def test_returns_empty_list_with_no_runs(self, client, patch_eval_session):
        resp = client.get('/api/evaluation/channels')
        assert resp.status_code == 200
        assert resp.json == []

    def test_returns_per_platform_metrics(self, client, seed_completed_runs):
        resp = client.get('/api/evaluation/channels')
        assert resp.status_code == 200
        data = resp.json
        platforms = {d['platform'] for d in data}
        assert 'instagram' in platforms
        assert 'patreon' in platforms

    def test_excludes_failed_runs(self, client, seed_completed_runs):
        resp = client.get('/api/evaluation/channels')
        for entry in resp.json:
            if entry['platform'] == 'instagram':
                # Only 2 completed IG runs, not 3
                assert entry['run_count'] == 2

    def test_includes_avg_fields(self, client, seed_completed_runs):
        resp = client.get('/api/evaluation/channels')
        entry = resp.json[0]
        assert 'avg_found' in entry
        assert 'avg_scored' in entry
        assert 'avg_synced' in entry
        assert 'avg_duration_min' in entry

    def test_duration_calculated(self, client, seed_completed_runs):
        resp = client.get('/api/evaluation/channels')
        for entry in resp.json:
            assert entry['avg_duration_min'] >= 0


# ---------------------------------------------------------------------------
# /api/evaluation/funnel
# ---------------------------------------------------------------------------

class TestApiFunnel:

    def test_returns_six_stages(self, client, seed_completed_runs):
        resp = client.get('/api/evaluation/funnel')
        assert resp.status_code == 200
        data = resp.json
        assert len(data) == 6
        stages = [d['stage'] for d in data]
        assert stages == ['discovery', 'pre_screen', 'enrichment',
                          'analysis', 'scoring', 'crm_sync']

    def test_counts_sum_across_completed_runs(self, client, seed_completed_runs):
        resp = client.get('/api/evaluation/funnel')
        data = resp.json
        # r1(100) + r2(200) + r3(50) = 350 found (failed r4 excluded)
        discovery = next(d for d in data if d['stage'] == 'discovery')
        assert discovery['count'] == 350

    def test_filters_by_platform(self, client, seed_completed_runs):
        resp = client.get('/api/evaluation/funnel?platform=patreon')
        data = resp.json
        discovery = next(d for d in data if d['stage'] == 'discovery')
        assert discovery['count'] == 50  # only r3

    def test_analysis_equals_enrichment(self, client, seed_completed_runs):
        """Analysis doesn't drop profiles — uses enrichment count as proxy."""
        resp = client.get('/api/evaluation/funnel')
        data = resp.json
        enrichment = next(d for d in data if d['stage'] == 'enrichment')
        analysis = next(d for d in data if d['stage'] == 'analysis')
        assert enrichment['count'] == analysis['count']

    def test_all_zeros_when_no_completed_runs(self, client, patch_eval_session):
        resp = client.get('/api/evaluation/funnel')
        assert resp.status_code == 200
        for entry in resp.json:
            assert entry['count'] == 0

    def test_unknown_platform_returns_zeros(self, client, seed_completed_runs):
        resp = client.get('/api/evaluation/funnel?platform=tiktok')
        for entry in resp.json:
            assert entry['count'] == 0


# ---------------------------------------------------------------------------
# /api/evaluation/scoring
# ---------------------------------------------------------------------------

class TestApiScoring:

    def test_returns_tier_distribution(self, client, seed_scored_leads):
        resp = client.get('/api/evaluation/scoring')
        assert resp.status_code == 200
        tiers = resp.json['tier_distribution']
        assert tiers['auto_enroll'] == 1
        assert tiers['high_priority_review'] == 1
        assert tiers['standard_priority_review'] == 1
        assert tiers['low_priority_review'] == 1

    def test_returns_avg_lead_score(self, client, seed_scored_leads):
        resp = client.get('/api/evaluation/scoring')
        avg = resp.json['avg_lead_score']
        # (0.85 + 0.65 + 0.45 + 0.25) / 4 = 0.55
        assert abs(avg - 0.55) < 0.01

    def test_returns_total_scored(self, client, seed_scored_leads):
        resp = client.get('/api/evaluation/scoring')
        assert resp.json['total_scored'] == 4

    def test_empty_when_no_scored_leads(self, client, patch_eval_session):
        resp = client.get('/api/evaluation/scoring')
        assert resp.status_code == 200
        assert resp.json['tier_distribution'] == {}
        assert resp.json['total_scored'] == 0
        assert resp.json['avg_lead_score'] == 0


# ---------------------------------------------------------------------------
# /api/evaluation/trends
# ---------------------------------------------------------------------------

class TestApiTrends:
    """Trends endpoint uses cast(Date) which is Postgres-specific.
    Tests mock the session to avoid SQLite incompatibility."""

    @patch('app.routes.evaluation.get_session')
    def test_returns_daily_and_rolling(self, mock_gs, client):
        from unittest.mock import MagicMock
        from datetime import date
        mock_session = MagicMock()
        mock_gs.return_value = mock_session
        mock_row = MagicMock(date=date(2026, 2, 25), platform='instagram',
                             runs=2, avg_found=100, avg_scored=50, avg_synced=20)
        mock_session.query.return_value.filter.return_value.group_by.return_value \
            .order_by.return_value.all.return_value = [mock_row]
        resp = client.get('/api/evaluation/trends')
        assert resp.status_code == 200
        assert 'daily' in resp.json
        assert 'rolling_avg' in resp.json

    @patch('app.routes.evaluation.get_session')
    def test_daily_entries_have_required_fields(self, mock_gs, client):
        from unittest.mock import MagicMock
        from datetime import date
        mock_session = MagicMock()
        mock_gs.return_value = mock_session
        mock_row = MagicMock(date=date(2026, 2, 25), platform='instagram',
                             runs=1, avg_found=80, avg_scored=40, avg_synced=10)
        mock_session.query.return_value.filter.return_value.group_by.return_value \
            .order_by.return_value.all.return_value = [mock_row]
        resp = client.get('/api/evaluation/trends')
        for entry in resp.json['daily']:
            assert 'date' in entry
            assert 'platform' in entry
            assert 'avg_found' in entry

    @patch('app.routes.evaluation.get_session')
    def test_empty_when_no_data(self, mock_gs, client):
        from unittest.mock import MagicMock
        mock_session = MagicMock()
        mock_gs.return_value = mock_session
        mock_session.query.return_value.filter.return_value.group_by.return_value \
            .order_by.return_value.all.return_value = []
        resp = client.get('/api/evaluation/trends')
        assert resp.json['daily'] == []
        assert resp.json['rolling_avg']['avg_found'] == 0

    @patch('app.routes.evaluation.get_session')
    def test_rolling_avg_computed(self, mock_gs, client):
        from unittest.mock import MagicMock
        from datetime import date
        mock_session = MagicMock()
        mock_gs.return_value = mock_session
        rows = [
            MagicMock(date=date(2026, 2, 24), platform='instagram',
                      runs=1, avg_found=100, avg_scored=50, avg_synced=20),
            MagicMock(date=date(2026, 2, 25), platform='instagram',
                      runs=1, avg_found=200, avg_scored=100, avg_synced=40),
        ]
        mock_session.query.return_value.filter.return_value.group_by.return_value \
            .order_by.return_value.all.return_value = rows
        resp = client.get('/api/evaluation/trends')
        rolling = resp.json['rolling_avg']
        assert rolling['avg_found'] == 150  # (100+200)/2
        assert isinstance(rolling['avg_scored'], int)


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# /api/evaluation/benchmarks
# ---------------------------------------------------------------------------

class TestApiBenchmarks:

    def test_returns_200_with_no_data(self, client, patch_eval_session):
        resp = client.get('/api/evaluation/benchmarks')
        assert resp.status_code == 200

    def test_returns_empty_dict_with_no_completed_runs(self, client, patch_eval_session):
        resp = client.get('/api/evaluation/benchmarks')
        assert resp.json == {}

    def test_returns_per_platform_structure(self, client, seed_completed_runs):
        resp = client.get('/api/evaluation/benchmarks')
        assert resp.status_code == 200
        data = resp.json
        for plat in data:
            assert 'baseline' in data[plat]
            assert 'deviations' in data[plat]

    def test_filters_by_platform(self, client, seed_completed_runs):
        resp = client.get('/api/evaluation/benchmarks?platform=instagram')
        assert resp.status_code == 200
        data = resp.json
        # Should only have instagram key
        assert list(data.keys()) == ['instagram']

    def test_deviations_are_list(self, client, seed_completed_runs):
        resp = client.get('/api/evaluation/benchmarks')
        data = resp.json
        for plat in data:
            assert isinstance(data[plat]['deviations'], list)


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

class TestEvaluationIntegration:

    def test_all_endpoints_reachable(self, client, patch_eval_session):
        """All evaluation endpoints return 200 even with empty DB."""
        for url in ['/evaluation', '/partials/eval-kpis',
                    '/api/evaluation/channels', '/api/evaluation/funnel',
                    '/api/evaluation/scoring', '/api/evaluation/trends',
                    '/api/evaluation/benchmarks']:
            resp = client.get(url)
            assert resp.status_code == 200, f'{url} returned {resp.status_code}'

    def test_funnel_counts_decrease_through_stages(self, client, seed_completed_runs):
        """Funnel should generally decrease from discovery to crm_sync."""
        resp = client.get('/api/evaluation/funnel')
        counts = [d['count'] for d in resp.json]
        assert counts[0] >= counts[-1]  # discovery >= crm_sync
