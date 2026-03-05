Feature: Enrollment Helpers
  Pure scheduling math behind the enrollment dispatcher.

  An "inbox" is a BDR's email account that sends sequence emails.
  Each inbox has a daily send cap (e.g. 25/day). Because one enrollment
  creates a chain of future sends (5 emails spaced by a cadence of
  N days), the system must check capacity across all future send dates
  before accepting a new enrollment today.

  "Slots" are the number of new enrollments an inbox (or all inboxes
  combined) can accept on a given day without any future date exceeding
  the daily cap.

  "Segments" represent outreach priority tiers. Segment names come from
  the HubSpot property `outreach_segment` — current values are
  "schedule_call", "interest_check", and "self_service". Helpers are
  segment-agnostic: they accept arbitrary weight dicts, so adding or
  renaming segments requires no helper code changes.

  Weights control what fraction of available slots each segment receives,
  but allocation is capped by actual queue depth — if only 3 leads
  are queued for a segment, it can't use more than 3 slots regardless
  of weight.

  # ── Business Day Detection ───────────────────────────────────────────

  Scenario Outline: Business day detection
    Given the date is a <day_type>
    Then it should be reported as <expected> business day

    Examples:
      | day_type  | expected |
      | Monday    | a        |
      | Friday    | a        |
      | Saturday  | not a    |
      | Sunday    | not a    |

  Scenario Outline: US holidays are not business days
    Given the date is <holiday> of <year>
    Then it should be reported as not a business day

    Examples:
      | holiday          | year |
      | New Year's Day   | 2026 |
      | Memorial Day     | 2026 |
      | Independence Day | 2026 |
      | Labor Day        | 2026 |
      | Thanksgiving     | 2026 |
      | Christmas Day    | 2026 |

  Scenario Outline: Non-federal dead-email days are not business days
    Given the date is <holiday> of <year>
    Then it should be reported as not a business day

    Examples:
      | holiday       | year |
      | Black Friday  | 2026 |
      | Christmas Eve | 2026 |

  # ── Slot Allocation ─────────────────────────────────────────────────

  Scenario: Slots are allocated proportionally to segment weights
    Given 10 total slots are available
    And segment weights are 60% cold and 40% warm
    And both segments have plenty of queued contacts
    When slots are allocated
    Then cold gets 6 slots
    And warm gets 4 slots

  Scenario: Allocation is capped by queue depth
    Given 10 total slots are available
    And segment weights are 60% cold and 40% warm
    And cold has only 3 queued contacts
    And warm has 100 queued contacts
    When slots are allocated
    Then cold gets 3 slots
    And warm gets 7 slots

  Scenario: Zero available slots yields zero allocation
    Given 0 total slots are available
    And segment weights are 60% cold and 40% warm
    When slots are allocated
    Then cold gets 0 slots
    And warm gets 0 slots

  # ── Inbox Selection ──────────────────────────────────────────────────

  Scenario: Least-loaded inbox is selected
    Given inbox A has 20 committed sends today
    And inbox B has 5 committed sends today
    And each inbox allows 25 sends per day
    When the best inbox is selected
    Then inbox B is chosen

  Scenario: No inbox returned when all are full
    Given inbox A has 25 committed sends today
    And each inbox allows 25 sends per day
    When the best inbox is selected
    Then no inbox is available
