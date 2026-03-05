#!/usr/bin/env python3
"""
Render the enrollment page with mock data and open in browser.

Usage: .venv/bin/python scripts/preview_enrollment.py
"""
import json
import webbrowser
import tempfile
import os
from unittest.mock import patch, MagicMock

# Mock Redis before any app imports
mock_redis = MagicMock()
mock_redis.get.return_value = None
mock_redis.lrange.return_value = []

with patch.dict(os.environ, {'REDIS_URL': 'redis://localhost:6379/0'}):
    with patch('redis.from_url', return_value=mock_redis):
        from app import create_app

app = create_app()

# Mock data
mock_last_run = {
    'status': 'completed',
    'enrolled_count': 18,
    'error_count': 1,
    'active_count': 42,
    'queued_count': 35,
    'total_slots': 50,
    'allocation': {'cold': 11, 'warm': 7},
    'enrolled_details': [
        {'contact_id': '101', 'inbox': 'Miriam Plascencia', 'segment': 'cold'},
        {'contact_id': '102', 'inbox': 'Miriam Plascencia', 'segment': 'cold'},
        {'contact_id': '103', 'inbox': 'Miriam Plascencia', 'segment': 'warm'},
        {'contact_id': '104', 'inbox': 'Majo Juarez', 'segment': 'cold'},
        {'contact_id': '105', 'inbox': 'Majo Juarez', 'segment': 'cold'},
        {'contact_id': '106', 'inbox': 'Majo Juarez', 'segment': 'warm'},
        {'contact_id': '107', 'inbox': 'Nicole Roma', 'segment': 'cold'},
        {'contact_id': '108', 'inbox': 'Nicole Roma', 'segment': 'cold'},
        {'contact_id': '109', 'inbox': 'Nicole Roma', 'segment': 'warm'},
        {'contact_id': '110', 'inbox': 'Salvatore Renteria', 'segment': 'cold'},
        {'contact_id': '111', 'inbox': 'Salvatore Renteria', 'segment': 'cold'},
        {'contact_id': '112', 'inbox': 'Sofia Gonzalez', 'segment': 'cold'},
        {'contact_id': '113', 'inbox': 'Sofia Gonzalez', 'segment': 'warm'},
        {'contact_id': '114', 'inbox': 'Sofia Gonzalez', 'segment': 'cold'},
        {'contact_id': '115', 'inbox': 'Tanya Pina', 'segment': 'cold'},
        {'contact_id': '116', 'inbox': 'Tanya Pina', 'segment': 'warm'},
        {'contact_id': '117', 'inbox': 'Tanya Pina', 'segment': 'cold'},
        {'contact_id': '118', 'inbox': 'Tanya Pina', 'segment': 'warm'},
    ],
    'errors': ['Failed to update contact 119'],
    'dry_run': False,
    'run_date': '2026-03-04',
    'started_at': '2026-03-04T09:00:05-08:00',
    'finished_at': '2026-03-04T09:00:42-08:00',
}

mock_runs = [
    mock_last_run,
    {
        'status': 'completed', 'enrolled_count': 22, 'error_count': 0,
        'queued_count': 40, 'total_slots': 50, 'allocation': {'cold': 13, 'warm': 9},
        'dry_run': False, 'run_date': '2026-03-03', 'reason': None,
        'started_at': '2026-03-03T09:00:03-08:00',
    },
    {
        'status': 'skipped', 'enrolled_count': 0, 'error_count': 0,
        'queued_count': 0, 'total_slots': 0, 'allocation': None,
        'dry_run': False, 'run_date': '2026-03-02', 'reason': 'not_business_day',
        'started_at': '2026-03-02T09:00:01-08:00',
    },
    {
        'status': 'skipped', 'enrolled_count': 0, 'error_count': 0,
        'queued_count': 0, 'total_slots': 0, 'allocation': None,
        'dry_run': False, 'run_date': '2026-03-01', 'reason': 'not_business_day',
        'started_at': '2026-03-01T09:00:01-08:00',
    },
    {
        'status': 'completed', 'enrolled_count': 15, 'error_count': 0,
        'queued_count': 28, 'total_slots': 50, 'allocation': {'cold': 9, 'warm': 6},
        'dry_run': False, 'run_date': '2026-02-28', 'reason': None,
        'started_at': '2026-02-28T09:00:04-08:00',
    },
    {
        'status': 'completed', 'enrolled_count': 8, 'error_count': 0,
        'queued_count': 12, 'total_slots': 50, 'allocation': {'cold': 5, 'warm': 3},
        'dry_run': True, 'run_date': '2026-02-27', 'reason': None,
        'started_at': '2026-02-27T15:30:00-08:00',
    },
    {
        'status': 'skipped', 'enrolled_count': 0, 'error_count': 0,
        'queued_count': 0, 'total_slots': 50, 'allocation': None,
        'dry_run': False, 'run_date': '2026-02-27', 'reason': 'no_queued_contacts',
        'started_at': '2026-02-27T09:00:02-08:00',
    },
    {
        'status': 'error', 'enrolled_count': 0, 'error_count': 1,
        'queued_count': 20, 'total_slots': 50, 'allocation': None,
        'dry_run': False, 'run_date': '2026-02-26', 'reason': 'HubSpot API timeout',
        'started_at': '2026-02-26T09:00:01-08:00',
    },
]

with app.test_request_context('/enrollment'):
    from flask import render_template
    from app.config import (
        ENROLLMENT_INBOXES, ENROLLMENT_MAX_PER_DAY,
        ENROLLMENT_SEQUENCE_CADENCE, ENROLLMENT_SEQUENCE_STEPS,
        ENROLLMENT_OUTREACH_WEIGHTS, ENROLLMENT_TIMEZONE,
        ENROLLMENT_HUBSPOT_PROPERTIES,
    )
    html = render_template(
        'enrollment.html',
        active_page='enrollment',
        last_run=mock_last_run,
        runs=mock_runs,
        config_inboxes=ENROLLMENT_INBOXES,
        config_max=ENROLLMENT_MAX_PER_DAY,
        config_cadence=ENROLLMENT_SEQUENCE_CADENCE,
        config_steps=ENROLLMENT_SEQUENCE_STEPS,
        config_weights=ENROLLMENT_OUTREACH_WEIGHTS,
        config_tz=ENROLLMENT_TIMEZONE,
        config_hs_props=ENROLLMENT_HUBSPOT_PROPERTIES,
    )

# Write to temp file and open
fd, path = tempfile.mkstemp(suffix='.html', prefix='enrollment_preview_')
with os.fdopen(fd, 'w') as f:
    f.write(html)

print(f"Preview written to: {path}")
webbrowser.open(f'file://{path}')
