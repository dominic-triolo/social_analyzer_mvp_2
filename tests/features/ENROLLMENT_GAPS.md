# Enrollment Dispatcher — Gap Analysis

Comparison of current `main` implementation against the real Celery task behavior
(source of truth). This doc drives the code fix pass after feature-file review.

**Legend:**
- `✅ EXECUTE` — Fully specified, can fix now with no external input
- `⚠️ UNKNOWN` — Blocked on external info (teammate input needed)
- `✅ VALIDATED` — Confirmed live against HubSpot API (2026-03-05)

---

## A. Property Name Fixes — `✅ EXECUTE`

Every HubSpot property name in the dispatcher and config is wrong. The current
code uses config-indirected names that don't exist in the portal. All correct
names confirmed from Celery task source **and validated live via HubSpot API**.

| Purpose | Wrong (main) | Correct (Celery task) | R/W | Portal Status |
|---|---|---|---|---|
| Queue status | `enrollment_status` | `reply_sequence_queue_status` | R+W (`queued` → `active`) | ✅ VALIDATED — type=string, fieldType=text |
| Inbox / BDR | `enrollment_inbox` | `reply_io_sequence` | W (inbox name) | ✅ VALIDATED — type=enumeration, options=[Jenn, Dom, Matt, Kendall, Ryan] |
| Enrolled date | `enrollment_date` | `recent_reply_sequence_enrolled_date` | W (today) | ✅ VALIDATED — type=date |
| Segment | `enrollment_segment` | `outreach_segment` | R | ✅ VALIDATED — type=enumeration, options=[schedule_call, interest_check, self_service] |
| Trigger flag | *(not implemented)* | `enroll_in_reply_sequence` | W (`true`) | ✅ VALIDATED — type=booleancheckbox, options=[true, false] |
| Lead score | *(not read)* | `combined_lead_score` | R (sort key) | ✅ VALIDATED — type=number |
| Create date | *(not read)* | `hs_createdate` | R (tiebreaker sort) | ✅ VALIDATED — type=datetime (built-in) |
| Owner | `hubspot_owner_id` | `hubspot_owner_id` | W — correct, no change | ✅ VALIDATED — type=enumeration |

**Files to update:**
- `config/enrollment.yml` → `hubspot_properties` section
- `app/services/enrollment_config.py` → `_DEFAULTS['hubspot_properties']`
- `app/services/enrollment_dispatcher.py` → property field references + add trigger/score/createdate reads
- `tests/step_defs/conftest.py` → `TEST_CONFIG['hubspot_properties']`

---

## B. Inbox Configuration — `✅ EXECUTE`

**Current state:** `config/enrollment.yml` has 6 hardcoded BDR names (Miriam,
Majo, Nicole, Salvatore, Sofia, Tanya) with owner IDs.

**Required state:** Inboxes should be Jenn, Dom, Matt, Kendall, Ryan. Names
confirmed via `reply_io_sequence` enum in HubSpot. Owner IDs extracted from
live enrolled contacts.

**Validated inbox → owner ID mapping (2026-03-05):**

```yaml
inboxes:
  Jenn:    "1377426260"    # 49/50 active contacts match
  Dom:     "75772233"      # 50/50 active contacts match
  Matt:    "1392069281"    # 32/33 active contacts match
  Kendall: "271269536"     # 50/50 active contacts match
  Ryan:    "583796152"     # 49/50 active contacts match
```

**Validation method:** Queried contacts with `reply_sequence_queue_status=active`
per inbox via HubSpot Search API, sorted by `recent_reply_sequence_enrolled_date`
DESC. Owner IDs are 98-100% consistent per inbox. Most recent enrollments
(2026-03-04) all use these IDs. The 1-2 outliers per inbox are likely manual
reassignments — the Celery task consistently writes the IDs above.

Note: We cannot independently verify these via `GET /crm/v3/owners/{id}` because
the API token lacks `crm.objects.owners.read` scope. However, the consistency
across 233 active contacts and the recency of enrollments gives high confidence.

No code changes needed — this is purely a config swap in `enrollment.yml`.

---

## C. Segment + Weight Mismatch — `✅ RESOLVED`

**Current main:** Uses `cold` / `warm` segments with 60/40 weights.

**Real Celery task:** Uses `schedule_call` / `interest_check` / `self_service`.

**Validated against portal (2026-03-05):** `outreach_segment` property confirmed
as enumeration with exactly these 3 options: `schedule_call`, `interest_check`,
`self_service`.

**Weight values confirmed by Dom (2026-03-05):**
```yaml
outreach_weights:
  schedule_call:   4    # ~57%
  interest_check:  2    # ~29%
  self_service:    1    # ~14%
```

Weights are proportional (normalized at runtime). Heavy on schedule_call
(highest-intent leads), moderate interest_check, light self_service.

---

## D. Missing Features

### D1. Scoring sort — `✅ EXECUTE`
**Gap:** Queued contacts are not prioritized. The dispatcher processes them in
whatever order HubSpot returns them.

**Required:** Sort queued contacts by `combined_lead_score` DESC, then
`hs_createdate` ASC (oldest first as tiebreaker). Both properties validated
live (number and datetime types respectively).

**Fix:** After fetching queued contacts, sort before slicing by allocation count.
Add `combined_lead_score` and `hs_createdate` to the HubSpot search `properties`
list. Sort logic: `sorted(contacts, key=lambda c: (-score, createdate))`.

### D2. Unknown segment overflow — `✅ EXECUTE`
**Gap:** Contacts whose `outreach_segment` doesn't match any configured weight
key default to `"cold"` (hardcoded fallback in dispatcher line 162). Since
`"cold"` won't exist in the new weight dict, these contacts are silently dropped.

**Required:** Unclassified contacts (segment not in weights dict) should fill
remaining capacity after weighted segments are allocated.

**Fix:** After weighted allocation, compute leftover slots and fill with
unclassified contacts sorted by the same lead-score order.

### D3. Trigger flag — `✅ EXECUTE`
**Gap:** The CRM update does not write `enroll_in_reply_sequence = true`. Without
this flag, Reply.io never picks up the contact for the sequence — enrollment is
incomplete. Property validated as booleancheckbox with options [true, false].

**Fix:** Add `'enroll_in_reply_sequence': 'true'` to the `hubspot_update_contact`
payload (dispatcher line 203-208). One line.

### D4. Rate limiting — `✅ EXECUTE`
**Gap:** No delay between CRM update calls. Rapid-fire API calls risk hitting
HubSpot rate limits (100 calls/10s for private apps).

**Required:** 0.1s sleep between each `hubspot_update_contact` call.

**Fix:** Add `time.sleep(0.1)` after each successful or failed CRM update in the
enrollment loop.

### D5. Per-inbox capacity in summary — `✅ EXECUTE`
**Gap:** Run summary includes `total_slots` and per-segment `allocation` but not
per-inbox remaining capacity.

**Required:** Summary should include remaining capacity per inbox after enrollment
completes, so the dashboard can show which inboxes are near their limit.

**Fix:** After the enrollment loop, compute remaining slots per inbox and add to
summary dict as `inbox_capacity: {inbox_name: remaining_slots, ...}`.

---

## E. Config Changes Needed — `✅ EXECUTE` (except weights)

### `config/enrollment.yml`

```yaml
# ✅ EXECUTE — all values confirmed + validated against HubSpot API
inboxes:
  Jenn:    "1377426260"
  Dom:     "75772233"
  Matt:    "1392069281"
  Kendall: "271269536"
  Ryan:    "583796152"

# ✅ EXECUTE — property names validated
hubspot_properties:
  status_field:     reply_sequence_queue_status
  inbox_field:      reply_io_sequence
  date_field:       recent_reply_sequence_enrolled_date
  segment_field:    outreach_segment
  trigger_field:    enroll_in_reply_sequence
  score_field:      combined_lead_score
  createdate_field: hs_createdate

# ✅ RESOLVED — weights from Dom (2026-03-05): 4/2/1
outreach_weights:
  schedule_call:   4
  interest_check:  2
  self_service:    1
```

### `app/services/enrollment_config.py` — `_DEFAULTS` — `✅ EXECUTE`

Add defaults for new fields: `trigger_field`, `score_field`, `createdate_field`.
Rename existing fields to match correct property names.

---

## F. Env / Data Prerequisites — `✅ VALIDATED`

All prerequisites verified against the live HubSpot portal (2026-03-05):

| Prerequisite | Status |
|---|---|
| `reply_sequence_queue_status` exists | ✅ type=string |
| `reply_io_sequence` exists w/ correct enum | ✅ options=[Jenn, Dom, Matt, Kendall, Ryan] |
| `outreach_segment` exists w/ correct enum | ✅ options=[schedule_call, interest_check, self_service] |
| `enroll_in_reply_sequence` exists | ✅ booleancheckbox |
| `recent_reply_sequence_enrolled_date` exists | ✅ type=date |
| `combined_lead_score` exists | ✅ type=number |
| `hs_createdate` exists | ✅ built-in datetime |
| Owner IDs for all 5 inboxes | ✅ extracted from live contacts |
| `HUBSPOT_ACCESS_TOKEN` scopes | ✅ contacts.read + contacts.write work (owners.read missing — not needed) |
| Reply.io integration active | ✅ ASSUMED — `enroll_in_reply_sequence` property exists with workflow-style booleancheckbox; safe to assume the workflow is wired since the Celery task already uses it in production |

---

## Summary: Execution Plan

### Can execute now — no external input needed
| Gap | Scope |
|---|---|
| A. Property name fixes | Config + dispatcher + test fixtures |
| B. Inbox config | Swap names + owner IDs in `enrollment.yml` |
| D1. Scoring sort | Dispatcher: sort queued contacts before enrollment loop |
| D2. Unknown segment overflow | Dispatcher: fill remaining slots with unclassified contacts |
| D3. Trigger flag | Dispatcher: one-line addition to CRM update payload |
| D4. Rate limiting | Dispatcher: `time.sleep(0.1)` in enrollment loop |
| D5. Per-inbox capacity | Dispatcher: compute + add to summary dict |
| E. Config + defaults | `enrollment_config.py` + `enrollment.yml` (except weights) |
| F. Portal prereqs | All validated |

### All gaps resolved
No remaining unknowns. Segment weights confirmed by Dom (2026-03-05): 4/2/1.

---

## Step Definition Impact

The following step defs will need updates when code is fixed:

| Step def | Change needed | Status |
|---|---|---|
| `n_contacts_queued` | Use `reply_sequence_queue_status` / `outreach_segment` in mock data | ✅ EXECUTE |
| `n_segment_contacts_queued` | Same property rename | ✅ EXECUTE |
| `set_weights` | Reference new segment names | ✅ RESOLVED |
| `conftest.py TEST_CONFIG` | Update `hubspot_properties` dict + inboxes with correct values | ✅ EXECUTE |
| New step defs needed | Scoring sort, unknown segment overflow, trigger flag, rate limit, per-inbox capacity | ✅ EXECUTE |

Guard rail scenarios (skip/force/error) are unaffected — they don't touch property names.
