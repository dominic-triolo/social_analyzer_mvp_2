"""
Pure helper functions for enrollment dispatch scheduling.

The only external dependency is `holidays` (pure date math, no I/O).
"""
from datetime import date, timedelta
from typing import Dict, Optional

import holidays

# US federal holidays + custom "dead email" days
_US_HOLIDAYS = holidays.US(years=range(2024, 2035))

_CUSTOM_SKIP_DAYS = {
    # Black Friday: day after Thanksgiving (4th Thu of Nov)
    # Christmas Eve, Day after Christmas, New Year's Eve
}


def _build_custom_days(year: int) -> set:
    """Return custom non-federal skip dates for a given year."""
    us = holidays.US(years=year)
    days = set()
    # Black Friday — day after Thanksgiving
    for d, name in sorted(us.items()):
        if 'Thanksgiving' in name:
            days.add(d + timedelta(days=1))
            break
    # Fixed-date custom days
    days.add(date(year, 12, 24))  # Christmas Eve
    days.add(date(year, 12, 26))  # Day after Christmas
    days.add(date(year, 12, 31))  # New Year's Eve
    return days


def is_business_day(d: date) -> bool:
    """Return True if d is a weekday that is not a US holiday or dead-email day."""
    if d.weekday() >= 5:
        return False
    if d in _US_HOLIDAYS:
        return False
    if d in _build_custom_days(d.year):
        return False
    return True


def build_committed_schedule(active_contacts: list, cadence: int) -> Dict[str, Dict[date, int]]:
    """Build a committed-send schedule from currently active contacts.

    Each active contact has an inbox and enrollment_date. Based on the cadence
    (days between sends), we project future send dates for each inbox.

    active_contacts: list of dicts with keys 'inbox', 'enrollment_date' (date obj),
                     and optionally 'total_steps' (default 5).
    cadence: days between sequence emails.

    Returns: {inbox_name: {date: send_count, ...}, ...}
    """
    committed: Dict[str, Dict[date, int]] = {}
    today = date.today()

    for contact in active_contacts:
        inbox = contact.get('inbox', '')
        enrollment_date = contact.get('enrollment_date')
        total_steps = contact.get('total_steps', 5)

        if not inbox or not enrollment_date:
            continue

        if inbox not in committed:
            committed[inbox] = {}

        # Project send dates: enrollment_date, +cadence, +2*cadence, ...
        for step in range(total_steps):
            send_date = enrollment_date + timedelta(days=step * cadence)
            if send_date >= today:
                committed[inbox][send_date] = committed[inbox].get(send_date, 0) + 1

    return committed


def available_slots_for_inbox(inbox: str, d: date, committed: Dict[str, Dict[date, int]],
                               cadence: int, max_per_day: int) -> int:
    """How many new enrollments can this inbox accept on date d?

    A new enrollment on date d will generate sends on d, d+cadence, d+2*cadence, ...
    We check that adding 1 to each of those future dates stays within max_per_day.
    """
    inbox_schedule = committed.get(inbox, {})
    min_remaining = max_per_day

    # Check each future send date for this potential enrollment
    for step in range(5):  # assume 5-step sequence
        send_date = d + timedelta(days=step * cadence)
        current = inbox_schedule.get(send_date, 0)
        remaining = max_per_day - current
        min_remaining = min(min_remaining, remaining)

    return max(0, min_remaining)


def compute_total_available_slots(inboxes: dict, d: date,
                                   committed: Dict[str, Dict[date, int]],
                                   cadence: int, max_per_day: int) -> int:
    """Total slots across all inboxes for date d."""
    total = 0
    for inbox in inboxes:
        total += available_slots_for_inbox(inbox, d, committed, cadence, max_per_day)
    return total


def allocate_slots_by_weight(total_slots: int, weights: Dict[str, float],
                              queue_depths: Dict[str, int]) -> Dict[str, int]:
    """Distribute total_slots across outreach segments by weight, capped by queue depth.

    weights: {segment: weight_fraction} e.g. {"cold": 0.6, "warm": 0.4}
    queue_depths: {segment: count_of_queued_contacts}

    Returns: {segment: slots_allocated}
    """
    if total_slots <= 0:
        return {seg: 0 for seg in weights}

    allocation: Dict[str, int] = {}
    remaining = total_slots

    # First pass: allocate by weight, capped by queue depth
    for segment, weight in sorted(weights.items(), key=lambda x: -x[1]):
        ideal = int(total_slots * weight)
        available = queue_depths.get(segment, 0)
        allocated = min(ideal, available, remaining)
        allocation[segment] = allocated
        remaining -= allocated

    # Second pass: distribute leftover to segments that still have queued contacts
    if remaining > 0:
        for segment in sorted(weights.keys(), key=lambda s: -weights[s]):
            available = queue_depths.get(segment, 0) - allocation[segment]
            give = min(available, remaining)
            if give > 0:
                allocation[segment] += give
                remaining -= give

    return allocation


def best_inbox_for_enrollment(d: date, committed: Dict[str, Dict[date, int]],
                               inboxes: dict, cadence: int,
                               max_per_day: int) -> Optional[str]:
    """Pick the inbox with the most available capacity on date d.

    Returns inbox name or None if no capacity.
    """
    best = None
    best_slots = 0

    for inbox in inboxes:
        slots = available_slots_for_inbox(inbox, d, committed, cadence, max_per_day)
        if slots > best_slots:
            best_slots = slots
            best = inbox

    return best


def update_committed_for_enrollment(inbox: str, d: date,
                                     committed: Dict[str, Dict[date, int]],
                                     cadence: int) -> Dict[str, Dict[date, int]]:
    """Add a new enrollment to the committed schedule (mutates and returns committed).

    Projects sends on d, d+cadence, d+2*cadence, ... for 5 steps.
    """
    if inbox not in committed:
        committed[inbox] = {}

    for step in range(5):
        send_date = d + timedelta(days=step * cadence)
        committed[inbox][send_date] = committed[inbox].get(send_date, 0) + 1

    return committed
