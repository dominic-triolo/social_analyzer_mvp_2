"""Tests for app.services.enrollment_helpers — pure functions, no mocks needed."""
import pytest
from datetime import date, timedelta

from app.services.enrollment_helpers import (
    is_business_day,
    build_committed_schedule,
    available_slots_for_inbox,
    compute_total_available_slots,
    allocate_slots_by_weight,
    best_inbox_for_enrollment,
    update_committed_for_enrollment,
)


class TestIsBusinessDay:
    def test_monday_is_business_day(self):
        # 2026-03-02 is a Monday
        assert is_business_day(date(2026, 3, 2)) is True

    def test_friday_is_business_day(self):
        # 2026-03-06 is a Friday
        assert is_business_day(date(2026, 3, 6)) is True

    def test_saturday_is_not_business_day(self):
        # 2026-03-07 is a Saturday
        assert is_business_day(date(2026, 3, 7)) is False

    def test_sunday_is_not_business_day(self):
        # 2026-03-08 is a Sunday
        assert is_business_day(date(2026, 3, 8)) is False

    def test_wednesday_is_business_day(self):
        # 2026-03-04 is a Wednesday
        assert is_business_day(date(2026, 3, 4)) is True

    def test_christmas_on_weekday_is_not_business_day(self):
        # 2025-12-25 is a Thursday
        assert is_business_day(date(2025, 12, 25)) is False

    def test_new_years_day_is_not_business_day(self):
        # 2026-01-01 is a Thursday
        assert is_business_day(date(2026, 1, 1)) is False

    def test_independence_day_is_not_business_day(self):
        # 2025-07-04 is a Friday
        assert is_business_day(date(2025, 7, 4)) is False

    def test_black_friday_is_not_business_day(self):
        # 2026: Thanksgiving = Nov 26 (Thu), Black Friday = Nov 27 (Fri)
        assert is_business_day(date(2026, 11, 27)) is False

    def test_christmas_eve_is_not_business_day(self):
        # 2026-12-24 is a Thursday
        assert is_business_day(date(2026, 12, 24)) is False

    def test_regular_weekday_near_holiday_is_business_day(self):
        # 2026-01-02 is a Friday, not a holiday
        assert is_business_day(date(2026, 1, 2)) is True


class TestBuildCommittedSchedule:
    def test_empty_contacts_returns_empty(self):
        result = build_committed_schedule([], cadence=3)
        assert result == {}

    def test_single_contact_projects_sends(self):
        today = date.today()
        contacts = [{
            'inbox': 'inbox_a',
            'enrollment_date': today,
            'total_steps': 3,
        }]
        result = build_committed_schedule(contacts, cadence=3)
        assert 'inbox_a' in result
        assert result['inbox_a'].get(today, 0) >= 1
        assert result['inbox_a'].get(today + timedelta(days=3), 0) >= 1
        assert result['inbox_a'].get(today + timedelta(days=6), 0) >= 1

    def test_multiple_contacts_same_inbox_accumulate(self):
        today = date.today()
        contacts = [
            {'inbox': 'inbox_a', 'enrollment_date': today, 'total_steps': 2},
            {'inbox': 'inbox_a', 'enrollment_date': today, 'total_steps': 2},
        ]
        result = build_committed_schedule(contacts, cadence=3)
        assert result['inbox_a'][today] == 2

    def test_past_dates_excluded(self):
        """Send dates before today should not appear in committed schedule."""
        past = date.today() - timedelta(days=30)
        contacts = [{
            'inbox': 'inbox_a',
            'enrollment_date': past,
            'total_steps': 2,
        }]
        result = build_committed_schedule(contacts, cadence=3)
        # Past dates should be excluded
        for d in result.get('inbox_a', {}):
            assert d >= date.today()

    def test_skips_contacts_without_inbox(self):
        contacts = [{'inbox': '', 'enrollment_date': date.today()}]
        result = build_committed_schedule(contacts, cadence=3)
        assert result == {}

    def test_skips_contacts_without_enrollment_date(self):
        contacts = [{'inbox': 'inbox_a', 'enrollment_date': None}]
        result = build_committed_schedule(contacts, cadence=3)
        assert result == {}


class TestAvailableSlots:
    def test_empty_committed_returns_max(self):
        result = available_slots_for_inbox('inbox_a', date.today(), {}, cadence=3, max_per_day=25)
        assert result == 25

    def test_full_inbox_returns_zero(self):
        today = date.today()
        committed = {
            'inbox_a': {today: 25},
        }
        result = available_slots_for_inbox('inbox_a', today, committed, cadence=3, max_per_day=25)
        assert result == 0

    def test_partial_fill_returns_remaining(self):
        today = date.today()
        committed = {
            'inbox_a': {today: 10},
        }
        result = available_slots_for_inbox('inbox_a', today, committed, cadence=3, max_per_day=25)
        assert result == 15

    def test_future_bottleneck_limits_slots(self):
        """If a future send date is near capacity, today's slots are limited."""
        today = date.today()
        future = today + timedelta(days=3)
        committed = {
            'inbox_a': {today: 0, future: 24},
        }
        # Adding 1 enrollment today adds 1 to future too, which would make 25 (at limit)
        result = available_slots_for_inbox('inbox_a', today, committed, cadence=3, max_per_day=25)
        assert result == 1


class TestComputeTotalAvailableSlots:
    def test_sums_across_inboxes(self):
        inboxes = {'inbox_a': 'owner1', 'inbox_b': 'owner2'}
        today = date.today()
        result = compute_total_available_slots(inboxes, today, {}, cadence=3, max_per_day=25)
        assert result == 50  # 25 per inbox × 2

    def test_respects_committed_capacity(self):
        inboxes = {'inbox_a': 'owner1'}
        today = date.today()
        committed = {'inbox_a': {today: 20}}
        result = compute_total_available_slots(inboxes, today, committed, cadence=3, max_per_day=25)
        assert result == 5


class TestAllocateSlotsByWeight:
    def test_basic_allocation(self):
        result = allocate_slots_by_weight(
            total_slots=10,
            weights={'cold': 0.6, 'warm': 0.4},
            queue_depths={'cold': 100, 'warm': 100},
        )
        assert result['cold'] == 6
        assert result['warm'] == 4

    def test_capped_by_queue_depth(self):
        result = allocate_slots_by_weight(
            total_slots=10,
            weights={'cold': 0.6, 'warm': 0.4},
            queue_depths={'cold': 3, 'warm': 100},
        )
        assert result['cold'] == 3
        # Remaining 7 slots should overflow to warm
        assert result['warm'] == 7

    def test_zero_slots_returns_zeros(self):
        result = allocate_slots_by_weight(
            total_slots=0,
            weights={'cold': 0.6, 'warm': 0.4},
            queue_depths={'cold': 10, 'warm': 10},
        )
        assert result == {'cold': 0, 'warm': 0}

    def test_empty_queue_returns_zeros(self):
        result = allocate_slots_by_weight(
            total_slots=10,
            weights={'cold': 0.6, 'warm': 0.4},
            queue_depths={'cold': 0, 'warm': 0},
        )
        assert result == {'cold': 0, 'warm': 0}

    def test_single_segment(self):
        result = allocate_slots_by_weight(
            total_slots=10,
            weights={'cold': 1.0},
            queue_depths={'cold': 15},
        )
        assert result == {'cold': 10}

    def test_overflow_redistribution(self):
        """If one segment has fewer queued than allocated, overflow goes to another."""
        result = allocate_slots_by_weight(
            total_slots=10,
            weights={'cold': 0.5, 'warm': 0.5},
            queue_depths={'cold': 2, 'warm': 20},
        )
        assert result['cold'] == 2
        assert result['warm'] == 8


class TestBestInboxForEnrollment:
    def test_picks_inbox_with_most_capacity(self):
        today = date.today()
        committed = {
            'inbox_a': {today: 20},
            'inbox_b': {today: 5},
        }
        inboxes = {'inbox_a': 'o1', 'inbox_b': 'o2'}
        result = best_inbox_for_enrollment(today, committed, inboxes, cadence=3, max_per_day=25)
        assert result == 'inbox_b'

    def test_returns_none_when_no_capacity(self):
        today = date.today()
        committed = {
            'inbox_a': {today: 25},
        }
        inboxes = {'inbox_a': 'o1'}
        result = best_inbox_for_enrollment(today, committed, inboxes, cadence=3, max_per_day=25)
        assert result is None

    def test_empty_committed_picks_any(self):
        inboxes = {'inbox_a': 'o1', 'inbox_b': 'o2'}
        result = best_inbox_for_enrollment(date.today(), {}, inboxes, cadence=3, max_per_day=25)
        assert result in inboxes

    def test_filters_by_allowed_types(self):
        """Inboxes restricted to certain types are excluded for other types."""
        today = date.today()
        inboxes = {'Dom': 'o1', 'Kendall': 'o2', 'Matt': 'o3'}
        allowed = {'Dom': ['schedule_call', 'rewarm_schedule_call']}
        # interest_check should skip Dom, pick Kendall or Matt
        result = best_inbox_for_enrollment(
            today, {}, inboxes, cadence=3, max_per_day=25,
            outreach_type='interest_check', inbox_allowed_types=allowed,
        )
        assert result in ('Kendall', 'Matt')

    def test_allowed_type_matches(self):
        """Inbox with allowed type is still eligible."""
        today = date.today()
        inboxes = {'Dom': 'o1'}
        allowed = {'Dom': ['schedule_call', 'rewarm_schedule_call']}
        result = best_inbox_for_enrollment(
            today, {}, inboxes, cadence=3, max_per_day=25,
            outreach_type='schedule_call', inbox_allowed_types=allowed,
        )
        assert result == 'Dom'

    def test_unlisted_inbox_handles_all_types(self):
        """Inboxes not in inbox_allowed_types can handle any type."""
        today = date.today()
        inboxes = {'Dom': 'o1', 'Kendall': 'o2'}
        allowed = {'Dom': ['schedule_call']}
        result = best_inbox_for_enrollment(
            today, {}, inboxes, cadence=3, max_per_day=25,
            outreach_type='self_service', inbox_allowed_types=allowed,
        )
        assert result == 'Kendall'

    def test_no_allowed_types_means_no_filtering(self):
        """When inbox_allowed_types is empty, all inboxes are eligible."""
        today = date.today()
        inboxes = {'Dom': 'o1', 'Kendall': 'o2'}
        result = best_inbox_for_enrollment(
            today, {}, inboxes, cadence=3, max_per_day=25,
            outreach_type='schedule_call', inbox_allowed_types={},
        )
        assert result in inboxes

    def test_returns_none_when_all_filtered_out(self):
        """Returns None when all inboxes are filtered out by type."""
        today = date.today()
        inboxes = {'Dom': 'o1', 'Jenn': 'o2'}
        allowed = {
            'Dom': ['schedule_call'],
            'Jenn': ['schedule_call'],
        }
        result = best_inbox_for_enrollment(
            today, {}, inboxes, cadence=3, max_per_day=25,
            outreach_type='interest_check', inbox_allowed_types=allowed,
        )
        assert result is None


class TestUpdateCommittedForEnrollment:
    def test_adds_to_empty_committed(self):
        today = date.today()
        committed = {}
        result = update_committed_for_enrollment('inbox_a', today, committed, cadence=3)
        assert result['inbox_a'][today] == 1
        assert result['inbox_a'][today + timedelta(days=3)] == 1

    def test_increments_existing(self):
        today = date.today()
        committed = {'inbox_a': {today: 5}}
        result = update_committed_for_enrollment('inbox_a', today, committed, cadence=3)
        assert result['inbox_a'][today] == 6

    def test_mutates_in_place(self):
        today = date.today()
        committed = {}
        returned = update_committed_for_enrollment('inbox_a', today, committed, cadence=3)
        assert returned is committed
