"""Tests for EnrollmentRun model + persistence."""
import pytest
from datetime import date, datetime
from unittest.mock import patch

from app.models.enrollment_run import EnrollmentRun
from app.services.db import persist_enrollment_run, get_enrollment_history


@pytest.fixture(autouse=True)
def _patch_db_service_session(db_engine):
    """Route get_session() in app.services.db to test sessions."""
    from sqlalchemy.orm import sessionmaker
    TestSession = sessionmaker(bind=db_engine)
    with patch('app.services.db.get_session', side_effect=lambda: TestSession()):
        yield TestSession


class TestEnrollmentRunModel:
    def test_create_enrollment_run(self, db_session):
        run = EnrollmentRun(
            status='completed',
            enrolled_count=5,
            error_count=0,
            active_count=10,
            queued_count=8,
            total_slots=20,
            allocation={'cold': 3, 'warm': 2},
            enrolled_details=[{'contact_id': '100', 'inbox': 'a'}],
            errors=[],
            dry_run=False,
            run_date=date(2026, 3, 4),
        )
        db_session.add(run)
        db_session.flush()

        assert run.id is not None
        assert run.status == 'completed'
        assert run.enrolled_count == 5
        assert run.allocation == {'cold': 3, 'warm': 2}

    def test_default_values(self, db_session):
        run = EnrollmentRun(
            status='skipped',
            run_date=date(2026, 3, 4),
        )
        db_session.add(run)
        db_session.flush()

        assert run.enrolled_count == 0
        assert run.error_count == 0
        assert run.dry_run is False

    def test_nullable_fields(self, db_session):
        run = EnrollmentRun(
            status='error',
            reason='test error',
            run_date=date(2026, 3, 4),
        )
        db_session.add(run)
        db_session.flush()

        assert run.reason == 'test error'
        assert run.allocation is None
        assert run.enrolled_details is None


class TestPersistEnrollmentRun:
    def test_persists_and_returns_id(self, db_session):
        summary = {
            'status': 'completed',
            'enrolled_count': 3,
            'error_count': 0,
            'active_count': 5,
            'queued_count': 4,
            'total_slots': 10,
            'allocation': {'cold': 2, 'warm': 1},
            'enrolled_details': [{'contact_id': '1'}],
            'errors': [],
            'dry_run': False,
            'run_date': date(2026, 3, 4),
            'finished_at': datetime(2026, 3, 4, 10, 30),
        }
        run_id = persist_enrollment_run(summary)
        assert run_id is not None

        row = db_session.query(EnrollmentRun).get(run_id)
        assert row.status == 'completed'
        assert row.enrolled_count == 3

    def test_persists_skipped_run(self, db_session):
        summary = {
            'status': 'skipped',
            'reason': 'not_business_day',
            'run_date': date(2026, 3, 7),
        }
        run_id = persist_enrollment_run(summary)
        assert run_id is not None

        row = db_session.query(EnrollmentRun).get(run_id)
        assert row.reason == 'not_business_day'


class TestGetEnrollmentHistory:
    def test_returns_recent_runs(self, db_session):
        for i in range(3):
            run = EnrollmentRun(
                status='completed',
                enrolled_count=i,
                run_date=date(2026, 3, i + 1),
            )
            db_session.add(run)
        db_session.flush()

        results = get_enrollment_history(limit=10)
        assert len(results) == 3
        # Should be newest first (by started_at, but all same here)
        assert all(r['status'] == 'completed' for r in results)

    def test_respects_limit(self, db_session):
        for i in range(5):
            run = EnrollmentRun(
                status='completed',
                enrolled_count=i,
                run_date=date(2026, 3, i + 1),
            )
            db_session.add(run)
        db_session.flush()

        results = get_enrollment_history(limit=2)
        assert len(results) == 2

    def test_empty_table_returns_empty(self, db_session):
        results = get_enrollment_history()
        assert results == []

    def test_serializes_dates_as_strings(self, db_session):
        run = EnrollmentRun(
            status='completed',
            run_date=date(2026, 3, 4),
        )
        db_session.add(run)
        db_session.flush()

        results = get_enrollment_history()
        assert results[0]['run_date'] == '2026-03-04'
