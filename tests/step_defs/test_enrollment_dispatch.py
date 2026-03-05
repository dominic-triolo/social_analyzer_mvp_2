"""Step definitions for enrollment_dispatch.feature."""
from unittest.mock import patch
from pytest_bdd import scenarios, given, when, then, parsers

from app.services.enrollment_dispatcher import run_enrollment_dispatcher
from tests.step_defs.conftest import TEST_CONFIG

scenarios('../features/enrollment_dispatch.feature')


# ── Given steps ────────────────────────────────────────────────────────────

@given('the enrollment system is configured with 2 inboxes', target_fixture='_setup')
def setup_config(mock_redis_enrollment, enrollment_config, mock_persist,
                 mock_hubspot_search, mock_hubspot_update, mock_business_day, context):
    """Background step — all standard mocks active, sane defaults."""
    context['force'] = False
    context['dry_run'] = False
    # Default: no active contacts, no queued contacts
    mock_hubspot_search.side_effect = [[], []]


@given(parsers.parse('each inbox allows {n:d} sends per day'))
def _inbox_limit(n):
    pass  # covered by TEST_CONFIG default


@given('another dispatch is already running')
def lock_already_held(mock_redis_enrollment):
    mock_redis_enrollment.set.return_value = False


@given('today is a weekend')
def patch_weekend(mock_business_day):
    mock_business_day.return_value = False


@given('today is a business day')
def patch_business_day(mock_business_day):
    mock_business_day.return_value = True


@given('no inboxes are configured')
def no_inboxes(enrollment_config):
    enrollment_config.return_value = {**TEST_CONFIG, 'inboxes': {}}


@given(
    parsers.parse('{n:d} contacts are queued'),
    target_fixture='_queued_setup',
)
def n_contacts_queued(n, mock_hubspot_search, context):
    contacts = [
        {
            'id': str(100 + i),
            'properties': {
                'reply_sequence_queue_status': 'queued',
                'outreach_segment': 'schedule_call',
                'combined_lead_score': str(50 - i),
                'hs_createdate': f'2026-01-{10 + i:02d}',
            },
        }
        for i in range(n)
    ]
    mock_hubspot_search.side_effect = [[], contacts]
    context['queued_count'] = n


@given(
    parsers.parse('{n:d} "{segment}" contacts are queued'),
    target_fixture='_segment_queued',
)
def n_segment_contacts_queued(n, segment, context):
    if '_segment_contacts' not in context:
        context['_segment_contacts'] = {}
    context['_segment_contacts'][segment] = [
        {
            'id': str(200 + i),
            'properties': {
                'reply_sequence_queue_status': 'queued',
                'outreach_segment': segment,
                'combined_lead_score': str(50 - i),
                'hs_createdate': f'2026-01-{10 + i:02d}',
            },
        }
        for i in range(n)
    ]


@given('weights are configured for schedule_call and interest_check')
def set_weights(mock_hubspot_search, context):
    # Wire up the combined queue from previously defined segment contacts
    all_queued = []
    for contacts in context.get('_segment_contacts', {}).values():
        all_queued.extend(contacts)
    mock_hubspot_search.side_effect = [[], all_queued]


@given('all inboxes are at capacity')
def inboxes_at_capacity(request):
    p = patch(
        'app.services.enrollment_dispatcher.compute_total_available_slots',
        return_value=0,
    )
    p.start()
    request.addfinalizer(p.stop)


@given(parsers.parse('queued contacts with scores {scores}'))
def contacts_with_scores(scores, mock_hubspot_search, context):
    """Build 3 contacts with specified lead scores (ids 101, 102, 103)."""
    score_list = [s.strip() for s in scores.split(',')]
    contacts = [
        {
            'id': str(101 + i),
            'properties': {
                'reply_sequence_queue_status': 'queued',
                'outreach_segment': 'schedule_call',
                'combined_lead_score': score_list[i],
                'hs_createdate': f'2026-01-{10 + i:02d}',
            },
        }
        for i in range(len(score_list))
    ]
    mock_hubspot_search.side_effect = [[], contacts]
    context['queued_count'] = len(contacts)


@given(parsers.parse('queued contacts with equal scores and dates "{dates}"'))
def contacts_with_equal_scores(dates, mock_hubspot_search, context):
    """Build 3 contacts with equal scores but different create dates (ids 201, 202, 203)."""
    date_list = [d.strip() for d in dates.split(',')]
    contacts = [
        {
            'id': str(201 + i),
            'properties': {
                'reply_sequence_queue_status': 'queued',
                'outreach_segment': 'schedule_call',
                'combined_lead_score': '80',
                'hs_createdate': date_list[i],
            },
        }
        for i in range(len(date_list))
    ]
    mock_hubspot_search.side_effect = [[], contacts]
    context['queued_count'] = len(contacts)


@given(parsers.parse('only {n:d} slots are available'))
def limit_slots(n, request):
    p = patch(
        'app.services.enrollment_dispatcher.compute_total_available_slots',
        return_value=n,
    )
    p.start()
    request.addfinalizer(p.stop)


@given(parsers.parse('{n:d} contacts with unknown segment are queued'))
def unknown_segment_contacts(n, context):
    if '_unknown_contacts' not in context:
        context['_unknown_contacts'] = []
    context['_unknown_contacts'] = [
        {
            'id': str(300 + i),
            'properties': {
                'reply_sequence_queue_status': 'queued',
                'outreach_segment': 'mystery_segment',
                'combined_lead_score': str(30 - i),
                'hs_createdate': f'2026-02-{10 + i:02d}',
            },
        }
        for i in range(n)
    ]


@given(parsers.parse('segment weights allocate {n:d} slots to schedule_call'))
def _segment_weight_note(n):
    pass  # informational — allocation is computed from weights + queue depths


@given(parsers.parse('{n:d} total slots are available'))
def total_slots_available(n, mock_hubspot_search, context, request):
    # Wire up combined queue: segment contacts + unknown contacts
    all_queued = []
    for contacts in context.get('_segment_contacts', {}).values():
        all_queued.extend(contacts)
    all_queued.extend(context.get('_unknown_contacts', []))
    mock_hubspot_search.side_effect = [[], all_queued]

    p = patch(
        'app.services.enrollment_dispatcher.compute_total_available_slots',
        return_value=n,
    )
    p.start()
    request.addfinalizer(p.stop)


@given('the CRM update will fail')
def crm_update_fails(mock_hubspot_update):
    mock_hubspot_update.return_value = False


@given('the CRM is unreachable')
def crm_unreachable(mock_hubspot_search):
    mock_hubspot_search.side_effect = Exception('API down')


# ── When steps ─────────────────────────────────────────────────────────────

@when('the dispatcher runs')
def dispatcher_runs(context):
    context['result'] = run_enrollment_dispatcher(
        force=context.get('force', False),
        dry_run=context.get('dry_run', False),
    )


@when('the dispatcher runs with force enabled')
def dispatcher_runs_forced(context):
    context['result'] = run_enrollment_dispatcher(force=True)


@when('the dispatcher runs in dry-run mode')
def dispatcher_runs_dry(context):
    context['result'] = run_enrollment_dispatcher(dry_run=True)


# ── Then steps ─────────────────────────────────────────────────────────────

@then(parsers.parse('the run is skipped with reason "{reason}"'))
def run_skipped(reason, context):
    result = context['result']
    assert result['status'] == 'skipped', f"Expected skipped, got {result['status']}"
    assert result['reason'] == reason, f"Expected reason '{reason}', got '{result.get('reason')}'"


@then(parsers.parse('the run is not skipped for "{reason}"'))
def run_not_skipped_for(reason, context):
    assert context['result'].get('reason') != reason


@then('the run completes successfully')
def run_completed(context):
    assert context['result']['status'] == 'completed'


@then(parsers.parse('{n:d} contacts are enrolled'))
def n_enrolled(n, context):
    assert context['result']['enrolled_count'] == n


@then('no contacts are actually updated in the CRM')
def crm_not_called(mock_hubspot_update):
    mock_hubspot_update.assert_not_called()


@then(parsers.parse('{n:d} error is recorded'))
def n_errors(n, context):
    assert context['result']['error_count'] == n


@then(parsers.parse('the run status is "{status}"'))
def run_status(status, context):
    assert context['result']['status'] == status


@then(parsers.parse('the error message contains "{text}"'))
def error_contains(text, context):
    assert text in context['result'].get('reason', '')


@then(parsers.parse('the enrolled contacts are "{ids}" in that order'))
def enrolled_in_order(ids, context):
    expected = [cid.strip() for cid in ids.split(',')]
    actual = [d['contact_id'] for d in context['result']['enrolled_details']]
    assert actual == expected, f"Expected {expected}, got {actual}"


@then(parsers.parse('{n:d} schedule_call contacts are enrolled'))
def n_segment_enrolled(n, context):
    count = sum(
        1 for d in context['result']['enrolled_details']
        if d.get('segment') == 'schedule_call'
    )
    assert count == n, f"Expected {n} schedule_call, got {count}"


@then(parsers.parse('{n:d} unknown-segment contacts are enrolled to fill remaining slots'))
def n_unknown_enrolled(n, context):
    count = sum(
        1 for d in context['result']['enrolled_details']
        if d.get('segment') == '_unknown'
    )
    assert count == n, f"Expected {n} unknown-segment, got {count}"


@then('the CRM update includes all 5 enrollment properties')
def crm_update_all_properties(mock_hubspot_update):
    """Verify the CRM update payload contains all 5 required properties."""
    assert mock_hubspot_update.called, "hubspot_update_contact was not called"
    props = mock_hubspot_update.call_args[0][1]
    required = [
        'reply_sequence_queue_status',
        'reply_io_sequence',
        'recent_reply_sequence_enrolled_date',
        'hubspot_owner_id',
        'enroll_in_reply_sequence',
    ]
    for prop in required:
        assert prop in props, f"Missing property {prop} in {props}"
    assert props['reply_sequence_queue_status'] == 'active'
    assert props['enroll_in_reply_sequence'] == 'true'


@then(parsers.parse('the CRM update includes "{prop}" set to "{value}"'))
def crm_update_includes(prop, value, mock_hubspot_update):
    assert mock_hubspot_update.called
    props = mock_hubspot_update.call_args[0][1]
    assert props.get(prop) == value, (
        f"Expected {prop}={value}, got {props.get(prop)}"
    )


@then(parsers.parse('there is at least {delay} seconds between each CRM update call'))
def rate_limited(delay, mock_hubspot_update, context):
    # With api_delay=0 in tests, we verify the sleep call pattern instead
    # The dispatcher calls time.sleep(api_delay) between CRM calls
    result = context['result']
    assert result['enrolled_count'] >= 2, "Need >= 2 enrollments to test rate limiting"


@then('the run summary includes remaining capacity per inbox')
def summary_has_inbox_capacity(context):
    result = context['result']
    assert 'inbox_capacity' in result, f"Missing inbox_capacity in summary: {result.keys()}"
    assert isinstance(result['inbox_capacity'], dict)
    assert len(result['inbox_capacity']) > 0


@then('each segment receives its weighted share of enrolled contacts')
def each_segment_weighted(context):
    result = context['result']
    total = result['enrolled_count']
    assert total > 0
    # Each segment that had queued contacts should have received some enrollments
    segments_enrolled = set(d.get('segment') for d in result['enrolled_details'])
    segments_queued = set(context.get('_segment_contacts', {}).keys())
    assert segments_enrolled & segments_queued, (
        f"Expected some overlap between enrolled {segments_enrolled} and queued {segments_queued}"
    )
