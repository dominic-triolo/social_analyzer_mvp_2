"""Tests for HubSpot list/segment helpers (rewarm pipeline).

Covers: hubspot_list_search, hubspot_get_list_members,
        hubspot_batch_get_contacts, hubspot_import_segment.
"""
import pytest
from unittest.mock import patch, MagicMock, call

from app.services.hubspot import (
    hubspot_list_search,
    hubspot_get_list_members,
    hubspot_batch_get_contacts,
    hubspot_import_segment,
    _DEFAULT_CONTACT_PROPERTIES,
)


# ── hubspot_list_search ───────────────────────────────────────────────


class TestHubspotListSearch:
    """POST /crm/v3/lists/search — find lists by name query."""

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_returns_matching_lists(self, mock_breaker):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {
            'lists': [
                {
                    'listId': 123,
                    'name': 'Potential Hosts',
                    'additionalProperties': {'hs_list_size': '42'},
                },
                {
                    'listId': 456,
                    'name': 'Active Hosts',
                    'additionalProperties': {'hs_list_size': '10'},
                },
            ],
        }
        mock_breaker.return_value.call.return_value = mock_resp

        result = hubspot_list_search('hosts')

        assert len(result) == 2
        assert result[0] == {'id': '123', 'name': 'Potential Hosts', 'size': 42}
        assert result[1] == {'id': '456', 'name': 'Active Hosts', 'size': 10}

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_returns_empty_when_no_matches(self, mock_breaker):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {'lists': []}
        mock_breaker.return_value.call.return_value = mock_resp

        result = hubspot_list_search('nonexistent')
        assert result == []

    @patch('app.services.hubspot.HUBSPOT_API_KEY', None)
    def test_returns_empty_when_no_api_key(self):
        result = hubspot_list_search('hosts')
        assert result == []

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_sends_correct_payload(self, mock_breaker):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {'lists': []}
        mock_breaker.return_value.call.return_value = mock_resp

        hubspot_list_search('travel hosts')

        call_args = mock_breaker.return_value.call.call_args
        payload = call_args.kwargs['json']
        assert payload['query'] == 'travel hosts'
        assert payload['processingTypes'] == ['MANUAL', 'SNAPSHOT', 'DYNAMIC']
        assert payload['objectTypeId'] == '0-1'

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_handles_api_error(self, mock_breaker):
        mock_resp = MagicMock(status_code=500)
        mock_resp.text = 'Internal Server Error'
        mock_breaker.return_value.call.return_value = mock_resp

        result = hubspot_list_search('hosts')
        assert result == []

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_handles_exception(self, mock_breaker):
        mock_breaker.return_value.call.side_effect = Exception('Connection refused')

        result = hubspot_list_search('hosts')
        assert result == []

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_handles_missing_hs_list_size(self, mock_breaker):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {
            'lists': [
                {
                    'listId': 789,
                    'name': 'Empty List',
                    'additionalProperties': {},
                },
            ],
        }
        mock_breaker.return_value.call.return_value = mock_resp

        result = hubspot_list_search('empty')
        assert result == [{'id': '789', 'name': 'Empty List', 'size': 0}]

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_uses_correct_url(self, mock_breaker):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {'lists': []}
        mock_breaker.return_value.call.return_value = mock_resp

        hubspot_list_search('hosts')

        call_args = mock_breaker.return_value.call.call_args
        url = call_args.args[1]
        assert url == 'https://api.hubapi.com/crm/v3/lists/search'


# ── hubspot_get_list_members ──────────────────────────────────────────


class TestHubspotGetListMembers:
    """GET /crm/v3/lists/{listId}/memberships — paginated member IDs."""

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_returns_contact_ids(self, mock_breaker):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {
            'results': [101, 102, 103],
            'paging': {},
        }
        mock_breaker.return_value.call.return_value = mock_resp

        result = hubspot_get_list_members('42')
        assert result == ['101', '102', '103']

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.hubspot.time')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_paginates_with_after_cursor(self, mock_breaker, mock_time):
        resp_page1 = MagicMock(status_code=200)
        resp_page1.json.return_value = {
            'results': [101, 102],
            'paging': {'next': {'after': 'cursor1'}},
        }
        resp_page2 = MagicMock(status_code=200)
        resp_page2.json.return_value = {
            'results': [103],
            'paging': {},
        }
        mock_breaker.return_value.call.side_effect = [resp_page1, resp_page2]

        result = hubspot_get_list_members('42')
        assert result == ['101', '102', '103']
        assert mock_breaker.return_value.call.call_count == 2

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.hubspot.time')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_respects_limit(self, mock_breaker, mock_time):
        resp_page1 = MagicMock(status_code=200)
        resp_page1.json.return_value = {
            'results': [101, 102, 103],
            'paging': {'next': {'after': 'cursor1'}},
        }
        mock_breaker.return_value.call.return_value = resp_page1

        result = hubspot_get_list_members('42', limit=2)
        assert result == ['101', '102']

    @patch('app.services.hubspot.HUBSPOT_API_KEY', None)
    def test_returns_empty_when_no_api_key(self):
        result = hubspot_get_list_members('42')
        assert result == []

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_handles_api_error(self, mock_breaker):
        mock_resp = MagicMock(status_code=404)
        mock_resp.text = 'List not found'
        mock_breaker.return_value.call.return_value = mock_resp

        result = hubspot_get_list_members('nonexistent')
        assert result == []

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_handles_exception(self, mock_breaker):
        mock_breaker.return_value.call.side_effect = Exception('Timeout')

        result = hubspot_get_list_members('42')
        assert result == []

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_returns_empty_when_no_members(self, mock_breaker):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {'results': [], 'paging': {}}
        mock_breaker.return_value.call.return_value = mock_resp

        result = hubspot_get_list_members('42')
        assert result == []

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_uses_correct_url(self, mock_breaker):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {'results': [], 'paging': {}}
        mock_breaker.return_value.call.return_value = mock_resp

        hubspot_get_list_members('999')

        call_args = mock_breaker.return_value.call.call_args
        url = call_args.args[1]
        assert url == 'https://api.hubapi.com/crm/v3/lists/999/memberships'

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.hubspot.time')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_sends_after_param_on_second_page(self, mock_breaker, mock_time):
        resp_page1 = MagicMock(status_code=200)
        resp_page1.json.return_value = {
            'results': [101],
            'paging': {'next': {'after': 'abc123'}},
        }
        resp_page2 = MagicMock(status_code=200)
        resp_page2.json.return_value = {
            'results': [102],
            'paging': {},
        }
        mock_breaker.return_value.call.side_effect = [resp_page1, resp_page2]

        hubspot_get_list_members('42')

        # First call — no after param
        first_call = mock_breaker.return_value.call.call_args_list[0]
        assert first_call.kwargs.get('params', {}) == {}

        # Second call — has after param
        second_call = mock_breaker.return_value.call.call_args_list[1]
        assert second_call.kwargs['params'] == {'after': 'abc123'}


# ── hubspot_batch_get_contacts ────────────────────────────────────────


class TestHubspotBatchGetContacts:
    """POST /crm/v3/objects/contacts/batch/read — batch by 100."""

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_returns_flattened_contacts(self, mock_breaker):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {
            'results': [
                {
                    'id': '101',
                    'properties': {
                        'email': 'alice@example.com',
                        'firstname': 'Alice',
                        'lastname': 'Smith',
                        'instagram_handle': '@alice',
                        'instagram_followers': '5000',
                        'city': 'Portland',
                        'state': 'OR',
                        'country': 'US',
                    },
                },
            ],
        }
        mock_breaker.return_value.call.return_value = mock_resp

        result = hubspot_batch_get_contacts(['101'])

        assert len(result) == 1
        assert result[0]['id'] == '101'
        assert result[0]['email'] == 'alice@example.com'
        assert result[0]['firstname'] == 'Alice'
        assert result[0]['instagram_handle'] == '@alice'

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_uses_default_properties(self, mock_breaker):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {'results': []}
        mock_breaker.return_value.call.return_value = mock_resp

        hubspot_batch_get_contacts(['101'])

        call_args = mock_breaker.return_value.call.call_args
        payload = call_args.kwargs['json']
        assert payload['properties'] == list(_DEFAULT_CONTACT_PROPERTIES)

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_uses_custom_properties(self, mock_breaker):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {'results': []}
        mock_breaker.return_value.call.return_value = mock_resp

        hubspot_batch_get_contacts(['101'], properties=['email', 'city'])

        call_args = mock_breaker.return_value.call.call_args
        payload = call_args.kwargs['json']
        assert payload['properties'] == ['email', 'city']

    @patch('app.services.hubspot.HUBSPOT_API_KEY', None)
    def test_returns_empty_when_no_api_key(self):
        result = hubspot_batch_get_contacts(['101'])
        assert result == []

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.hubspot.time')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_batches_over_100_ids(self, mock_breaker, mock_time):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {'results': []}
        mock_breaker.return_value.call.return_value = mock_resp

        ids = [str(i) for i in range(150)]
        hubspot_batch_get_contacts(ids)

        assert mock_breaker.return_value.call.call_count == 2
        first_payload = mock_breaker.return_value.call.call_args_list[0].kwargs['json']
        second_payload = mock_breaker.return_value.call.call_args_list[1].kwargs['json']
        assert len(first_payload['inputs']) == 100
        assert len(second_payload['inputs']) == 50
        mock_time.sleep.assert_called_once_with(0.1)

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_handles_207_partial_response(self, mock_breaker):
        mock_resp = MagicMock(status_code=207)
        mock_resp.json.return_value = {
            'results': [
                {'id': '101', 'properties': {'email': 'alice@example.com'}},
            ],
        }
        mock_breaker.return_value.call.return_value = mock_resp

        result = hubspot_batch_get_contacts(['101', '999'], properties=['email'])
        assert len(result) == 1
        assert result[0]['email'] == 'alice@example.com'

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_handles_api_error(self, mock_breaker):
        mock_resp = MagicMock(status_code=500)
        mock_resp.text = 'Internal Server Error'
        mock_breaker.return_value.call.return_value = mock_resp

        result = hubspot_batch_get_contacts(['101'])
        assert result == []

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_handles_exception(self, mock_breaker):
        mock_breaker.return_value.call.side_effect = Exception('Connection refused')

        result = hubspot_batch_get_contacts(['101'])
        assert result == []

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_fills_missing_properties_with_empty_string(self, mock_breaker):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {
            'results': [
                {
                    'id': '101',
                    'properties': {'email': 'alice@example.com'},
                },
            ],
        }
        mock_breaker.return_value.call.return_value = mock_resp

        result = hubspot_batch_get_contacts(['101'])
        assert result[0]['firstname'] == ''
        assert result[0]['instagram_handle'] == ''
        assert result[0]['city'] == ''

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.circuit_breaker.get_breaker')
    def test_sends_correct_payload_structure(self, mock_breaker):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {'results': []}
        mock_breaker.return_value.call.return_value = mock_resp

        hubspot_batch_get_contacts(['101', '102'])

        call_args = mock_breaker.return_value.call.call_args
        url = call_args.args[1]
        payload = call_args.kwargs['json']
        assert url == 'https://api.hubapi.com/crm/v3/objects/contacts/batch/read'
        assert payload['inputs'] == [{'id': '101'}, {'id': '102'}]


# ── hubspot_import_segment ────────────────────────────────────────────


class TestHubspotImportSegment:
    """Combines get members + batch get contacts."""

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.hubspot.hubspot_batch_get_contacts')
    @patch('app.services.hubspot.hubspot_get_list_members')
    def test_combines_members_and_contacts(self, mock_members, mock_contacts):
        mock_members.return_value = ['101', '102']
        mock_contacts.return_value = [
            {'id': '101', 'email': 'alice@example.com', 'firstname': 'Alice'},
            {'id': '102', 'email': 'bob@example.com', 'firstname': 'Bob'},
        ]

        result = hubspot_import_segment('42')

        mock_members.assert_called_once_with('42')
        mock_contacts.assert_called_once_with(['101', '102'])
        assert len(result) == 2
        assert result[0]['email'] == 'alice@example.com'
        assert result[1]['email'] == 'bob@example.com'

    @patch('app.services.hubspot.HUBSPOT_API_KEY', None)
    def test_returns_empty_when_no_api_key(self):
        result = hubspot_import_segment('42')
        assert result == []

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.hubspot.hubspot_get_list_members')
    def test_returns_empty_when_no_members(self, mock_members):
        mock_members.return_value = []

        result = hubspot_import_segment('42')
        assert result == []

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.hubspot.hubspot_batch_get_contacts')
    @patch('app.services.hubspot.hubspot_get_list_members')
    def test_passes_platform_param_for_logging(self, mock_members, mock_contacts):
        """platform param is used for logging, not for API calls."""
        mock_members.return_value = ['101']
        mock_contacts.return_value = [{'id': '101', 'email': 'a@b.com'}]

        result = hubspot_import_segment('42', platform='patreon')
        assert len(result) == 1

    @patch('app.services.hubspot.HUBSPOT_API_KEY', 'test-key')
    @patch('app.services.hubspot.hubspot_batch_get_contacts')
    @patch('app.services.hubspot.hubspot_get_list_members')
    def test_returns_empty_when_contacts_fetch_fails(self, mock_members, mock_contacts):
        mock_members.return_value = ['101']
        mock_contacts.return_value = []

        result = hubspot_import_segment('42')
        assert result == []
