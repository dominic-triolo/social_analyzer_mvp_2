"""Tests for sync_hubspot_lists_to_db() — truncate+reinsert, timestamp, guards."""
import pytest
from unittest.mock import patch, MagicMock

from app.models.hubspot_list import HubSpotList
from app.models.app_config import AppConfig
from app.services.hubspot import sync_hubspot_lists_to_db


class TestSyncHubspotListsToDb:
    """sync_hubspot_lists_to_db — full lifecycle tests."""

    @patch('app.services.hubspot.hubspot_list_all')
    def test_inserts_lists_into_db(self, mock_list_all, db_session):
        mock_list_all.return_value = [
            {'id': '1', 'name': 'List A', 'size': 100, 'processing_type': 'MANUAL'},
            {'id': '2', 'name': 'List B', 'size': 200, 'processing_type': 'DYNAMIC'},
        ]

        result = sync_hubspot_lists_to_db()

        assert result['count'] == 2
        assert result['synced_at'] is not None
        assert db_session.query(HubSpotList).count() == 2

        row = db_session.get(HubSpotList, '1')
        assert row.name == 'List A'
        assert row.size == 100
        assert row.processing_type == 'MANUAL'

    @patch('app.services.hubspot.hubspot_list_all')
    def test_truncates_before_insert(self, mock_list_all, db_session):
        # Pre-populate with old data
        db_session.add(HubSpotList(list_id='old', name='Old List', size=50))
        db_session.flush()

        mock_list_all.return_value = [
            {'id': 'new', 'name': 'New List', 'size': 75, 'processing_type': 'MANUAL'},
        ]

        sync_hubspot_lists_to_db()

        assert db_session.query(HubSpotList).count() == 1
        assert db_session.get(HubSpotList, 'old') is None
        assert db_session.get(HubSpotList, 'new') is not None

    @patch('app.services.hubspot.hubspot_list_all')
    def test_updates_synced_at_timestamp(self, mock_list_all, db_session):
        mock_list_all.return_value = [
            {'id': '1', 'name': 'Test', 'size': 10},
        ]

        result = sync_hubspot_lists_to_db()

        cfg = db_session.get(AppConfig, 'hubspot_lists_synced_at')
        assert cfg is not None
        assert cfg.value == result['synced_at']

    @patch('app.services.hubspot.hubspot_list_all')
    def test_updates_existing_timestamp(self, mock_list_all, db_session):
        db_session.add(AppConfig(key='hubspot_lists_synced_at', value='old-time'))
        db_session.flush()

        mock_list_all.return_value = [
            {'id': '1', 'name': 'Test', 'size': 10},
        ]

        result = sync_hubspot_lists_to_db()

        cfg = db_session.get(AppConfig, 'hubspot_lists_synced_at')
        assert cfg.value != 'old-time'
        assert cfg.value == result['synced_at']

    @patch('app.services.hubspot.hubspot_list_all')
    def test_refuses_truncate_on_empty_api_result(self, mock_list_all, db_session):
        db_session.add(HubSpotList(list_id='1', name='Existing', size=50))
        db_session.flush()

        mock_list_all.return_value = []

        result = sync_hubspot_lists_to_db()

        assert result['count'] == 0
        assert result['synced_at'] is None
        # Existing data should be preserved
        assert db_session.query(HubSpotList).count() == 1

    @patch('app.services.hubspot.hubspot_list_all')
    def test_handles_missing_processing_type(self, mock_list_all, db_session):
        mock_list_all.return_value = [
            {'id': '1', 'name': 'No Type', 'size': 10},
        ]

        sync_hubspot_lists_to_db()

        row = db_session.get(HubSpotList, '1')
        assert row.processing_type is None

    @patch('app.services.hubspot.hubspot_list_all')
    def test_returns_correct_count(self, mock_list_all, db_session):
        mock_list_all.return_value = [
            {'id': str(i), 'name': f'List {i}', 'size': i * 10}
            for i in range(50)
        ]

        result = sync_hubspot_lists_to_db()
        assert result['count'] == 50
