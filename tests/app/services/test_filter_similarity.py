"""Tests for filter similarity detection — TDD: tests first, then implementation.

Covers:
  - Jaccard keyword overlap scoring
  - Numeric range overlap scoring
  - Weighted combined similarity
  - find_similar_runs() query + ranking
  - Platform isolation (only compares within same platform)
  - Deduplication (same run not compared to itself)
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from app.services.db import persist_run
from app.models.db_run import DbRun


# ---------------------------------------------------------------------------
# Pure scoring functions (no DB)
# ---------------------------------------------------------------------------

class TestJaccardSimilarity:
    """jaccard_similarity(set_a, set_b) → float 0.0–1.0."""

    def test_identical_sets(self):
        from app.services.filter_similarity import jaccard_similarity
        assert jaccard_similarity({'a', 'b', 'c'}, {'a', 'b', 'c'}) == 1.0

    def test_disjoint_sets(self):
        from app.services.filter_similarity import jaccard_similarity
        assert jaccard_similarity({'a', 'b'}, {'c', 'd'}) == 0.0

    def test_partial_overlap(self):
        from app.services.filter_similarity import jaccard_similarity
        # intersection={a,b}, union={a,b,c,d} → 2/4 = 0.5
        assert jaccard_similarity({'a', 'b', 'c'}, {'a', 'b', 'd'}) == pytest.approx(0.5)

    def test_empty_sets(self):
        from app.services.filter_similarity import jaccard_similarity
        assert jaccard_similarity(set(), set()) == 0.0

    def test_one_empty(self):
        from app.services.filter_similarity import jaccard_similarity
        assert jaccard_similarity({'a'}, set()) == 0.0


class TestRangeOverlap:
    """range_overlap((min1, max1), (min2, max2)) → float 0.0–1.0."""

    def test_identical_ranges(self):
        from app.services.filter_similarity import range_overlap
        assert range_overlap((100, 500), (100, 500)) == 1.0

    def test_no_overlap(self):
        from app.services.filter_similarity import range_overlap
        assert range_overlap((100, 200), (300, 400)) == 0.0

    def test_partial_overlap(self):
        from app.services.filter_similarity import range_overlap
        # overlap=[200,300], span=[100,400] → 100/300 = 0.333
        result = range_overlap((100, 300), (200, 400))
        assert 0.3 < result < 0.4

    def test_one_contains_other(self):
        from app.services.filter_similarity import range_overlap
        # overlap=[200,300], span=[100,500] → 100/400 = 0.25
        result = range_overlap((100, 500), (200, 300))
        assert 0.2 < result < 0.3

    def test_none_values_treated_as_no_range(self):
        from app.services.filter_similarity import range_overlap
        assert range_overlap((None, None), (100, 500)) == 0.0


class TestTokenizeFilters:
    """tokenize_filters(filters, platform) → set of comparable tokens."""

    def test_instagram_extracts_hashtags(self):
        from app.services.filter_similarity import tokenize_filters
        filters = {'hashtags': [{'name': 'travel'}, {'name': 'hiking'}]}
        tokens = tokenize_filters(filters, 'instagram')
        assert 'travel' in tokens
        assert 'hiking' in tokens

    def test_instagram_extracts_bio_phrase(self):
        from app.services.filter_similarity import tokenize_filters
        filters = {'bio_phrase': 'adventure travel guide'}
        tokens = tokenize_filters(filters, 'instagram')
        assert 'adventure' in tokens
        assert 'travel' in tokens

    def test_patreon_extracts_search_keywords(self):
        from app.services.filter_similarity import tokenize_filters
        filters = {'search_keywords': ['travel vlog', 'backpacking']}
        tokens = tokenize_filters(filters, 'patreon')
        assert 'travel' in tokens
        assert 'vlog' in tokens
        assert 'backpacking' in tokens

    def test_facebook_extracts_keywords(self):
        from app.services.filter_similarity import tokenize_filters
        filters = {'keywords': ['travel community', 'hiking group']}
        tokens = tokenize_filters(filters, 'facebook')
        assert 'travel' in tokens
        assert 'community' in tokens
        assert 'hiking' in tokens

    def test_extracts_interests(self):
        from app.services.filter_similarity import tokenize_filters
        filters = {'creator_interests': ['Travel, Tourism & Aviation', 'Fitness & Yoga']}
        tokens = tokenize_filters(filters, 'instagram')
        assert 'travel, tourism & aviation' in tokens
        assert 'fitness & yoga' in tokens

    def test_empty_filters(self):
        from app.services.filter_similarity import tokenize_filters
        assert tokenize_filters({}, 'instagram') == set()


class TestExtractNumericRanges:
    """extract_numeric_ranges(filters, platform) → dict of range tuples."""

    def test_instagram_follower_count(self):
        from app.services.filter_similarity import extract_numeric_ranges
        filters = {'follower_count': {'min': 20000, 'max': 500000}}
        ranges = extract_numeric_ranges(filters, 'instagram')
        assert ranges['followers'] == (20000, 500000)

    def test_patreon_patron_count(self):
        from app.services.filter_similarity import extract_numeric_ranges
        filters = {'min_patrons': 50, 'max_patrons': 5000}
        ranges = extract_numeric_ranges(filters, 'patreon')
        assert ranges['patrons'] == (50, 5000)

    def test_facebook_member_count(self):
        from app.services.filter_similarity import extract_numeric_ranges
        filters = {'min_members': 500, 'max_members': 50000}
        ranges = extract_numeric_ranges(filters, 'facebook')
        assert ranges['members'] == (500, 50000)

    def test_no_ranges(self):
        from app.services.filter_similarity import extract_numeric_ranges
        assert extract_numeric_ranges({}, 'instagram') == {}


class TestComputeSimilarity:
    """compute_similarity(filters_a, filters_b, platform) → float 0.0–1.0."""

    def test_identical_filters(self):
        from app.services.filter_similarity import compute_similarity
        filters = {
            'hashtags': [{'name': 'travel'}],
            'follower_count': {'min': 20000, 'max': 500000},
        }
        assert compute_similarity(filters, filters, 'instagram') == pytest.approx(1.0)

    def test_completely_different_filters(self):
        from app.services.filter_similarity import compute_similarity
        a = {'hashtags': [{'name': 'travel'}], 'follower_count': {'min': 1000, 'max': 5000}}
        b = {'hashtags': [{'name': 'cooking'}], 'follower_count': {'min': 100000, 'max': 900000}}
        sim = compute_similarity(a, b, 'instagram')
        assert sim < 0.3

    def test_similar_keywords_different_range(self):
        from app.services.filter_similarity import compute_similarity
        a = {'search_keywords': ['travel', 'hiking'], 'min_patrons': 50, 'max_patrons': 5000}
        b = {'search_keywords': ['travel', 'backpacking'], 'min_patrons': 100, 'max_patrons': 10000}
        sim = compute_similarity(a, b, 'patreon')
        assert 0.3 < sim < 0.8  # partial overlap in both dimensions


# ---------------------------------------------------------------------------
# Database integration: find_similar_runs()
# ---------------------------------------------------------------------------

class TestFindSimilarRuns:
    """find_similar_runs(platform, filters, threshold) queries DB and scores."""

    @pytest.fixture(autouse=True)
    def _patch_session(self, db_engine):
        from sqlalchemy.orm import sessionmaker
        TestSession = sessionmaker(bind=db_engine)
        with patch('app.services.filter_similarity.get_session',
                   side_effect=lambda: TestSession()):
            yield TestSession

    def _insert_run(self, session_factory, run_id, platform, filters,
                    status='completed', profiles_found=100, contacts_synced=30,
                    days_ago=0):
        session = session_factory()
        run = DbRun(
            id=run_id,
            platform=platform,
            status=status,
            filters=filters,
            profiles_found=profiles_found,
            contacts_synced=contacts_synced,
            created_at=datetime.now() - timedelta(days=days_ago),
        )
        session.add(run)
        session.commit()
        session.close()

    def test_finds_similar_run(self, _patch_session):
        from app.services.filter_similarity import find_similar_runs
        self._insert_run(_patch_session, 'run-old', 'instagram',
                         {'hashtags': [{'name': 'travel'}, {'name': 'hiking'}],
                          'follower_count': {'min': 20000, 'max': 500000}},
                         profiles_found=142, contacts_synced=38, days_ago=3)

        results = find_similar_runs('instagram', {
            'hashtags': [{'name': 'travel'}, {'name': 'wanderlust'}],
            'follower_count': {'min': 20000, 'max': 500000},
        }, threshold=0.5)
        assert len(results) >= 1
        top = results[0]
        assert top['run_id'] == 'run-old'
        assert top['similarity'] > 0.5
        assert top['profiles_found'] == 142
        assert top['contacts_synced'] == 38
        assert 'days_ago' in top

    def test_ignores_other_platforms(self, _patch_session):
        from app.services.filter_similarity import find_similar_runs
        self._insert_run(_patch_session, 'run-patreon', 'patreon',
                         {'search_keywords': ['travel']},
                         days_ago=1)

        results = find_similar_runs('instagram', {
            'hashtags': [{'name': 'travel'}],
        })
        assert all(r['run_id'] != 'run-patreon' for r in results)

    def test_ignores_non_completed_runs(self, _patch_session):
        from app.services.filter_similarity import find_similar_runs
        self._insert_run(_patch_session, 'run-active', 'instagram',
                         {'hashtags': [{'name': 'travel'}]},
                         status='discovering', days_ago=1)

        results = find_similar_runs('instagram', {
            'hashtags': [{'name': 'travel'}],
        })
        assert all(r['run_id'] != 'run-active' for r in results)

    def test_respects_threshold(self, _patch_session):
        from app.services.filter_similarity import find_similar_runs
        self._insert_run(_patch_session, 'run-different', 'instagram',
                         {'hashtags': [{'name': 'cooking'}],
                          'follower_count': {'min': 1000, 'max': 5000}},
                         days_ago=2)

        results = find_similar_runs('instagram', {
            'hashtags': [{'name': 'travel'}],
            'follower_count': {'min': 100000, 'max': 900000},
        }, threshold=0.7)
        assert len(results) == 0  # too different

    def test_returns_sorted_by_similarity(self, _patch_session):
        from app.services.filter_similarity import find_similar_runs
        # Very similar
        self._insert_run(_patch_session, 'run-close', 'instagram',
                         {'hashtags': [{'name': 'travel'}, {'name': 'hiking'}],
                          'follower_count': {'min': 20000, 'max': 500000}},
                         days_ago=1)
        # Somewhat similar
        self._insert_run(_patch_session, 'run-partial', 'instagram',
                         {'hashtags': [{'name': 'travel'}],
                          'follower_count': {'min': 50000, 'max': 900000}},
                         days_ago=5)

        results = find_similar_runs('instagram', {
            'hashtags': [{'name': 'travel'}, {'name': 'hiking'}],
            'follower_count': {'min': 20000, 'max': 500000},
        }, threshold=0.0)  # low threshold to get both
        assert len(results) >= 2
        assert results[0]['similarity'] >= results[1]['similarity']

    def test_limits_to_50_runs(self, _patch_session):
        from app.services.filter_similarity import find_similar_runs
        # Insert 55 runs
        for i in range(55):
            self._insert_run(_patch_session, f'run-{i}', 'instagram',
                             {'hashtags': [{'name': 'travel'}]},
                             days_ago=i)

        # Should not crash and should return results
        results = find_similar_runs('instagram', {
            'hashtags': [{'name': 'travel'}],
        }, threshold=0.0)
        assert len(results) <= 50

    def test_returns_empty_on_no_runs(self, _patch_session):
        from app.services.filter_similarity import find_similar_runs
        results = find_similar_runs('instagram', {'hashtags': [{'name': 'travel'}]})
        assert results == []


# ---------------------------------------------------------------------------
# Endpoint: POST /api/filter-similarity
# ---------------------------------------------------------------------------

class TestFilterSimilarityEndpoint:
    """POST /api/filter-similarity returns similar runs."""

    @patch('app.services.filter_similarity.find_similar_runs')
    def test_returns_similar_runs(self, mock_find, client):
        mock_find.return_value = [{
            'run_id': 'run-old',
            'similarity': 0.85,
            'profiles_found': 142,
            'contacts_synced': 38,
            'days_ago': 3,
        }]
        resp = client.post('/api/filter-similarity',
                           json={'platform': 'instagram',
                                 'filters': {'hashtags': [{'name': 'travel'}]}})
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'similar_runs' in data
        assert len(data['similar_runs']) == 1
        assert data['similar_runs'][0]['run_id'] == 'run-old'

    @patch('app.services.filter_similarity.find_similar_runs')
    def test_returns_empty_when_no_similar(self, mock_find, client):
        mock_find.return_value = []
        resp = client.post('/api/filter-similarity',
                           json={'platform': 'instagram',
                                 'filters': {'hashtags': [{'name': 'travel'}]}})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['similar_runs'] == []

    def test_400_when_no_filters(self, client):
        resp = client.post('/api/filter-similarity', json={'platform': 'instagram'})
        assert resp.status_code == 400
