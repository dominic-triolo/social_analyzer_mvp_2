Feature: Enrollment Configuration
  The enrollment dispatcher reads its config (inboxes, send limits,
  segment weights, rate limiting, HubSpot property mapping) from a
  layered priority chain:

    1. Database (app_config table) — written by the dashboard UI
    2. YAML file (config/enrollment.yml) — repo defaults
    3. Hardcoded defaults — built into the code

  The dashboard settings page lets the team edit config without
  touching code or YAML. Saving writes to the database; resetting
  deletes the DB row, reverting to the YAML file. If neither DB
  nor YAML exist, the dispatcher uses built-in defaults (no inboxes
  configured, 25 max sends/day).

  Adding or removing an inbox, changing segment weights, or adjusting
  rate limits should be a config change — not a code change.

  # ── Priority Chain ─────────────────────────────────────────────────

  Scenario: Config falls back to YAML when no DB config exists
    Given no database config exists
    And the YAML file defines 3 inboxes
    When the config is loaded
    Then the config has 3 inboxes

  Scenario: DB config overrides YAML file
    Given the YAML file defines 3 inboxes
    And the database config defines 5 inboxes
    When the config is loaded
    Then the config has 5 inboxes

  Scenario: Config falls back to defaults when neither DB nor YAML exist
    Given no database config exists
    And no YAML file exists
    When the config is loaded
    Then the config has 0 inboxes
    And the config has max_per_day 25

  # ── Dashboard ──────────────────────────────────────────────────────

  Scenario: Dashboard save persists config to the database
    Given the YAML file defines 3 inboxes
    When the user saves config with 4 inboxes via the dashboard
    And the config is loaded
    Then the config has 4 inboxes

  Scenario: Resetting config reverts to YAML defaults
    Given the YAML file defines 3 inboxes
    And the database config defines 5 inboxes
    When the user resets config to file defaults
    And the config is loaded
    Then the config has 3 inboxes

  Scenario: Adding an inbox is a config change not a code change
    Given the YAML file defines 3 inboxes
    When the user saves config with an added inbox "NewBDR" via the dashboard
    And the config is loaded
    Then the config includes inbox "NewBDR"

  Scenario: Removing an inbox is a config change not a code change
    Given the YAML file defines 3 inboxes
    When the user saves config without inbox "BDR1" via the dashboard
    And the config is loaded
    Then the config does not include inbox "BDR1"
    And the config has 2 inboxes

  Scenario: Changing segment weights takes effect on next run
    Given the YAML file defines weights schedule_call=0.5 interest_check=0.3 self_service=0.2
    When the user saves config with weights schedule_call=0.7 interest_check=0.2 self_service=0.1
    And the config is loaded
    Then the config weight for "schedule_call" is 0.7

  Scenario: Changing max sends per day takes effect on next run
    Given the YAML file defines max_per_day 25
    When the user saves config with max_per_day 40
    And the config is loaded
    Then the config has max_per_day 40
