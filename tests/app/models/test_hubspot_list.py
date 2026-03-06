"""Tests for app.models.hubspot_list — HubSpotList model CRUD."""
import pytest

from app.models.hubspot_list import HubSpotList


class TestHubSpotList:
    """Basic CRUD for the hubspot_lists table."""

    def test_create_and_read(self, db_session):
        db_session.add(HubSpotList(list_id='42', name='Test List', size=100, processing_type='MANUAL'))
        db_session.flush()

        row = db_session.get(HubSpotList, '42')
        assert row is not None
        assert row.name == 'Test List'
        assert row.size == 100
        assert row.processing_type == 'MANUAL'

    def test_size_defaults_to_zero(self, db_session):
        db_session.add(HubSpotList(list_id='1', name='Empty'))
        db_session.flush()

        row = db_session.get(HubSpotList, '1')
        assert row.size == 0

    def test_processing_type_nullable(self, db_session):
        db_session.add(HubSpotList(list_id='1', name='No Type', size=5))
        db_session.flush()

        row = db_session.get(HubSpotList, '1')
        assert row.processing_type is None

    def test_list_id_is_primary_key(self, db_session):
        db_session.add(HubSpotList(list_id='99', name='First', size=1))
        db_session.flush()

        # Same PK should raise on flush
        db_session.add(HubSpotList(list_id='99', name='Duplicate', size=2))
        with pytest.raises(Exception):
            db_session.flush()
        db_session.rollback()

    def test_query_order_by_name(self, db_session):
        db_session.add(HubSpotList(list_id='2', name='Zebra', size=10))
        db_session.add(HubSpotList(list_id='1', name='Alpha', size=20))
        db_session.flush()

        rows = db_session.query(HubSpotList).order_by(HubSpotList.name).all()
        assert [r.name for r in rows] == ['Alpha', 'Zebra']

    def test_delete(self, db_session):
        db_session.add(HubSpotList(list_id='1', name='To Delete', size=5))
        db_session.flush()

        db_session.query(HubSpotList).filter_by(list_id='1').delete()
        db_session.flush()

        assert db_session.get(HubSpotList, '1') is None

    def test_truncate_and_reinsert(self, db_session):
        for i in range(5):
            db_session.add(HubSpotList(list_id=str(i), name=f'List {i}', size=i * 10))
        db_session.flush()

        assert db_session.query(HubSpotList).count() == 5

        db_session.query(HubSpotList).delete()
        db_session.add(HubSpotList(list_id='new', name='Fresh', size=99))
        db_session.flush()

        assert db_session.query(HubSpotList).count() == 1
        assert db_session.get(HubSpotList, 'new').name == 'Fresh'
