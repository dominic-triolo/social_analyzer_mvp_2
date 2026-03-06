# Rewarm Webhook — Deferred Feature Requirements

## Context
The cc_env branch has a rewarm webhook (`/api/rewarm`) that re-scores existing contacts
when their social data is updated (e.g., new posts, follower count change). This allows
contacts who were previously below threshold to be re-evaluated and potentially auto-enrolled.

## Core Behavior
1. **Trigger**: External system (HubSpot workflow or cron) POSTs to `/api/rewarm` with a list of contact IDs
2. **Fetch fresh data**: For each contact, pull latest social data from InsightIQ or R2 cache
3. **Re-score**: Run through the scoring pipeline with updated evidence
4. **Compare**: If new score crosses an enrollment threshold that the old score didn't, mark as "rewarmed"
5. **Update CRM**: Push updated score + tier to HubSpot, optionally re-queue for enrollment

## Data Flow
```
POST /api/rewarm
  { "contact_ids": ["abc123", "def456"], "reason": "30_day_rescore" }

→ For each contact:
  1. Load cached analysis from R2 (save_analysis_cache / load_analysis_cache)
  2. Optionally re-fetch social data if cache is stale (>30 days)
  3. Re-run generate_evidence_based_score()
  4. Compare old tier vs new tier
  5. If upgraded: update HubSpot properties, optionally queue for enrollment
  6. Log result to enrollment run history
```

## Outreach Segment
- Rewarmed contacts get `outreach_segment = "rewarm_schedule_call"` in HubSpot
- This segment is already configured in `config/enrollment.yml` with weight 2
- Enrollment dispatcher handles it like any other segment

## API Shape
```
POST /api/rewarm
Authorization: Bearer {API_KEY}
Content-Type: application/json

{
  "contact_ids": ["abc123"],
  "reason": "30_day_rescore",        // optional, for logging
  "force_refetch": false              // optional, skip cache and re-fetch from InsightIQ
}

Response:
{
  "processed": 1,
  "upgraded": 1,
  "unchanged": 0,
  "errors": 0,
  "details": [
    {
      "contact_id": "abc123",
      "old_score": 0.42,
      "new_score": 0.55,
      "old_tier": "standard_priority_review",
      "new_tier": "auto_enroll",
      "action": "queued_for_enrollment"
    }
  ]
}
```

## Implementation Notes
- Should run as an RQ job (like enrollment dispatcher) to avoid blocking the web worker
- Needs `load_analysis_cache()` counterpart to existing `save_analysis_cache()` in `app/services/r2.py`
- Auth: same `API_KEY` check as other `/api/*` endpoints
- Rate limit: respect InsightIQ API limits if re-fetching (circuit breaker already in place)
- Consider batch size limit (e.g., max 50 contacts per request)

## Dependencies
- `app/services/r2.py` — needs `load_analysis_cache(contact_id)` function
- `app/pipeline/scoring.py` — `generate_evidence_based_score()` already exists
- `app/services/hubspot.py` — `hubspot_update_contact()` already exists
- `config/enrollment.yml` — `rewarm_schedule_call` segment already added
