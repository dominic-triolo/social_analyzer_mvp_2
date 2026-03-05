Feature: Enrollment Dispatcher
  TrovaTrip discovers potential travel-host leads and loads them into
  HubSpot as "queued" contacts. The enrollment dispatcher is a daily
  cron job that activates a batch of those queued contacts into Reply.io
  email sequences — one contact per BDR inbox (e.g. "Jenn", "Dom").

  Each inbox has a daily send limit (e.g. 25 emails/day). Because a
  single enrollment triggers a multi-step sequence (5 emails spaced
  every 3 days), enrolling one contact today commits future sends on
  that inbox. The dispatcher balances new enrollments across inboxes
  so no single inbox exceeds its daily cap on any future date.

  Contacts are split into outreach segments stored in the HubSpot
  property `outreach_segment` — values are "schedule_call",
  "interest_check", and "self_service". Configurable weights control
  what share of daily slots each segment gets, so the team can
  prioritize high-intent leads without starving the pipeline.

  HubSpot property mapping (source of truth):

    | Purpose        | Property                            | R/W  |
    | Queue status   | reply_sequence_queue_status          | R+W  |
    | Inbox / BDR    | reply_io_sequence                    | W    |
    | Segment        | outreach_segment                     | R    |
    | Trigger flag   | enroll_in_reply_sequence             | W    |
    | Enrolled date  | recent_reply_sequence_enrolled_date  | W    |
    | Owner          | hubspot_owner_id                     | W    |
    | Lead score     | combined_lead_score                  | R    |
    | Create date    | hs_createdate                        | R    |

  Inboxes are configurable via YAML + dashboard UI. Each maps a display
  name (e.g. "Jenn") to a HubSpot owner ID. Current inboxes: Jenn, Dom,
  Matt, Kendall, Ryan. Adding/removing an inbox should be a config
  change, not a code change.

  Configuration is managed separately (see enrollment_config.feature)
  with priority chain: DB (dashboard) > YAML file > hardcoded defaults.

  Background:
    Given the enrollment system is configured with 2 inboxes
    And each inbox allows 25 sends per day

  # ── Guard Rails ──────────────────────────────────────────────────────

  Scenario: Skip when another dispatch is already running
    Given another dispatch is already running
    When the dispatcher runs
    Then the run is skipped with reason "concurrent_run"

  Scenario: Skip on weekends unless forced
    Given today is a weekend
    When the dispatcher runs
    Then the run is skipped with reason "not_business_day"

  Scenario: Force override bypasses the business day check
    Given today is a weekend
    When the dispatcher runs with force enabled
    Then the run is not skipped for "not_business_day"

  Scenario: Skip when no inboxes are configured
    Given no inboxes are configured
    When the dispatcher runs
    Then the run is skipped with reason "no_inboxes_configured"

  Scenario: Skip when no contacts are queued
    Given today is a business day
    And 0 contacts are queued
    When the dispatcher runs
    Then the run is skipped with reason "no_queued_contacts"

  Scenario: Skip when all inboxes are at capacity
    Given today is a business day
    And 5 contacts are queued
    And all inboxes are at capacity
    When the dispatcher runs
    Then the run is skipped with reason "no_capacity"

  # ── Contact Selection ────────────────────────────────────────────────

  Scenario: Queued contacts are sorted by lead score descending
    Given today is a business day
    And queued contacts with scores 40, 90, 70
    And only 2 slots are available
    When the dispatcher runs
    Then the enrolled contacts are "102, 103" in that order

  Scenario: Contacts with equal lead score are sorted by create date ascending
    Given today is a business day
    And queued contacts with equal scores and dates "2026-02-01, 2026-01-15, 2026-01-20"
    And only 2 slots are available
    When the dispatcher runs
    Then the enrolled contacts are "202, 203" in that order

  Scenario: Unclassified contacts fill remaining capacity
    Given today is a business day
    And 2 "schedule_call" contacts are queued
    And 3 contacts with unknown segment are queued
    And segment weights allocate 2 slots to schedule_call
    And 4 total slots are available
    When the dispatcher runs
    Then 2 schedule_call contacts are enrolled
    And 2 unknown-segment contacts are enrolled to fill remaining slots

  # ── Enrollment ───────────────────────────────────────────────────────

  Scenario: Enroll queued contacts on a business day
    Given today is a business day
    And 3 contacts are queued
    When the dispatcher runs
    Then the run completes successfully
    And 3 contacts are enrolled

  Scenario: HubSpot update writes all required properties
    Given today is a business day
    And 1 contacts are queued
    When the dispatcher runs
    Then the CRM update includes all 5 enrollment properties

  Scenario: Trigger flag enroll_in_reply_sequence is set to true
    Given today is a business day
    And 1 contacts are queued
    When the dispatcher runs
    Then the CRM update includes "enroll_in_reply_sequence" set to "true"

  Scenario: Rate limit between CRM calls
    Given today is a business day
    And 3 contacts are queued
    When the dispatcher runs
    Then there is at least 0.1 seconds between each CRM update call

  Scenario: Segment weights control allocation
    Given today is a business day
    And 100 "schedule_call" contacts are queued
    And 100 "interest_check" contacts are queued
    And weights are configured for schedule_call and interest_check
    When the dispatcher runs
    Then each segment receives its weighted share of enrolled contacts

  Scenario: Run summary includes per-inbox remaining capacity
    Given today is a business day
    And 5 contacts are queued
    When the dispatcher runs
    Then the run summary includes remaining capacity per inbox

  Scenario: Dry run does not update contacts
    Given today is a business day
    And 2 contacts are queued
    When the dispatcher runs in dry-run mode
    Then the run completes successfully
    And 2 contacts are enrolled
    And no contacts are actually updated in the CRM

  # ── Error Handling ───────────────────────────────────────────────────

  Scenario: CRM update failure is tracked as an error
    Given today is a business day
    And 1 contacts are queued
    And the CRM update will fail
    When the dispatcher runs
    Then the run completes successfully
    And 0 contacts are enrolled
    And 1 error is recorded

  Scenario: CRM outage is caught and reported
    Given today is a business day
    And the CRM is unreachable
    When the dispatcher runs
    Then the run status is "error"
    And the error message contains "API down"
