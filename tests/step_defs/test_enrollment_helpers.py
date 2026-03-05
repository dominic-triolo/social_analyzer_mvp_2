"""Step definitions for enrollment_helpers.feature — pure functions, no mocks."""
from datetime import date, timedelta

import holidays
from pytest_bdd import scenarios, given, when, then, parsers

from app.services.enrollment_helpers import (
    is_business_day,
    allocate_slots_by_weight,
    best_inbox_for_enrollment,
)

scenarios('../features/enrollment_helpers.feature')


# ── Day-name to date mapping ──────────────────────────────────────────────
# 2026-03-02 is a Monday
_DAY_OFFSETS = {
    'Monday': 0,
    'Tuesday': 1,
    'Wednesday': 2,
    'Thursday': 3,
    'Friday': 4,
    'Saturday': 5,
    'Sunday': 6,
}
_BASE_MONDAY = date(2026, 3, 2)


# ── Business Day Detection ────────────────────────────────────────────────

@given(parsers.parse('the date is a {day_type}'), target_fixture='test_date')
def the_date_is(day_type):
    return _BASE_MONDAY + timedelta(days=_DAY_OFFSETS[day_type])


# ── Holiday name → date resolution ───────────────────────────────────────────

def _resolve_holiday(name: str, year: int) -> date:
    """Convert a human-readable holiday name to a concrete date."""
    # Custom (non-federal) holidays
    if name == 'Black Friday':
        us = holidays.US(years=year)
        for d, label in sorted(us.items()):
            if 'Thanksgiving' in label:
                return d + timedelta(days=1)
    if name == 'Christmas Eve':
        return date(year, 12, 24)
    if name == "New Year's Eve":
        return date(year, 12, 31)
    if name == 'Day after Christmas':
        return date(year, 12, 26)

    # Federal holidays — match by name substring
    us = holidays.US(years=year)
    for d, label in sorted(us.items()):
        if name in label:
            return d

    raise ValueError(f"Unknown holiday: {name}")


@given(parsers.parse('the date is {holiday} of {year:d}'), target_fixture='test_date')
def the_date_is_holiday(holiday, year):
    return _resolve_holiday(holiday, year)


@then(parsers.parse('it should be reported as {expected} business day'))
def check_business_day(expected, test_date):
    if expected == 'a':
        assert is_business_day(test_date) is True
    else:
        assert is_business_day(test_date) is False


# ── Slot Allocation ───────────────────────────────────────────────────────

@given(
    parsers.parse('{n:d} total slots are available'),
    target_fixture='context',
)
def total_slots(n):
    return {'total_slots': n, 'weights': {}, 'queue_depths': {}}


@given('segment weights are 60% cold and 40% warm')
def set_weights(context):
    context['weights'] = {'cold': 0.6, 'warm': 0.4}


@given('both segments have plenty of queued contacts')
def plenty_queued(context):
    context['queue_depths'] = {'cold': 100, 'warm': 100}


@given(parsers.parse('cold has only {n:d} queued contacts'))
def cold_queued(n, context):
    context['queue_depths']['cold'] = n


@given(parsers.parse('warm has {n:d} queued contacts'))
def warm_queued(n, context):
    context['queue_depths']['warm'] = n


@when('slots are allocated', target_fixture='allocation')
def do_allocate(context):
    return allocate_slots_by_weight(
        context['total_slots'],
        context['weights'],
        context.get('queue_depths', {}),
    )


@then(parsers.parse('cold gets {n:d} slots'))
def cold_slots(n, allocation):
    assert allocation.get('cold', 0) == n


@then(parsers.parse('warm gets {n:d} slots'))
def warm_slots(n, allocation):
    assert allocation.get('warm', 0) == n


# ── Inbox Selection ───────────────────────────────────────────────────────

@given(
    parsers.parse('inbox A has {n:d} committed sends today'),
    target_fixture='context',
)
def inbox_a_committed(n):
    today = date.today()
    return {
        'committed': {'inbox_a': {today: n}},
        'inboxes': {'inbox_a': 'o1'},
        'max_per_day': 25,
    }


@given(parsers.parse('inbox B has {n:d} committed sends today'))
def inbox_b_committed(n, context):
    today = date.today()
    context['committed']['inbox_b'] = {today: n}
    context['inboxes']['inbox_b'] = 'o2'


@given(parsers.parse('each inbox allows {n:d} sends per day'))
def set_max_per_day(n, context):
    context['max_per_day'] = n


@when('the best inbox is selected', target_fixture='selected_inbox')
def select_inbox(context):
    return best_inbox_for_enrollment(
        date.today(),
        context['committed'],
        context['inboxes'],
        cadence=3,
        max_per_day=context['max_per_day'],
    )


@then('inbox B is chosen')
def assert_inbox_b(selected_inbox):
    assert selected_inbox == 'inbox_b'


@then('no inbox is available')
def assert_no_inbox(selected_inbox):
    assert selected_inbox is None
