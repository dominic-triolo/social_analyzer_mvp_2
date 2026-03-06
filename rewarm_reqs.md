# Rewarm Pipeline — Feature Requirements

## Context
Contacts already in HubSpot may have been scored previously but didn't meet thresholds,
or were never run through the social analysis pipeline at all. The rewarm flow lets ops
select a HubSpot segment (list) and a platform (Instagram for v1), import those contacts,
and run them through a simplified version of the pipeline to re-evaluate and potentially
auto-enroll them.

**Key assumption**: HubSpot contacts in rewarmable segments have an Instagram handle stored.
The rewarm is platform-based — just like discovery runs target a specific platform, rewarm
runs target a platform too. V1 is Instagram-only, matching our primary discovery platform.

---

## Job Stories

**JS-1: Re-evaluate stale contacts**
When I have a HubSpot segment of contacts who were scored months ago but didn't qualify,
I want to re-run them through the analysis pipeline with fresh social data,
so I can catch people whose online presence has grown enough to now meet our thresholds.

**JS-2: Evaluate never-scored CRM contacts**
When I have a list of contacts in HubSpot who were never run through social analysis
(e.g., imported from events or referrals),
I want to select that segment and kick off a rewarm run,
so I can score them without having to manually re-discover them.

**JS-3: Separate tracking**
When I'm reviewing pipeline results,
I want to clearly see which runs were discovery vs rewarm,
so I can evaluate the effectiveness of each funnel independently.

**JS-4: Controlled rollout**
When I'm starting a rewarm on a large segment,
I want to control batch size and see progress stage-by-stage,
so I can monitor quality before committing to the full list.

---

## Two Run Types

The system now has two distinct run types:

| | Discovery Run | Rewarm Run |
|---|---|---|
| **Entry point** | Platform search (Instagram, etc.) | HubSpot segment + platform select |
| **Pipeline** | discovery → prescreen → enrichment → analysis → scoring → crm_sync | segment_import → enrichment → analysis → scoring → crm_sync |
| **Platform** | Selected at run creation | Selected at run creation (IG for v1) |
| **Trigger** | "New Run" in sidebar | "New Rewarm" in sidebar |
| **Tab** | Discovery tab | Rewarm tab |

Both run types produce results that are tagged and filterable in reporting.

## Pipeline Shape

```
Rewarm pipeline:  segment_import → enrichment → analysis → scoring → crm_sync
```

### Stage 1: Segment Import (replaces discovery + prescreen)
- Pulls contacts from a selected HubSpot list/segment
- No discovery needed — these are known contacts already in the CRM
- No prescreen needed — they've already been vetted enough to be in the segment
- Extracts the Instagram handle from the HubSpot contact properties
- Platform-based: the selected platform determines which social handle to pull and which enrichment adapter to use
- Contacts without a handle for the selected platform are skipped (surfaced in results as "no handle")
- Produces the same profile structure that enrichment expects as input

### Stages 2–4: Enrichment → Analysis → Scoring
- Same pipeline stages as the main flow
- Potentially simplified — e.g., skip steps if data already exists and is fresh
- Scoring compares new score/tier against whatever was previously stored on the contact

### Stage 5: CRM Sync
- Pushes updated score + tier back to HubSpot
- If a contact crossed an enrollment threshold, marks them for enrollment
- Rewarmed contacts get `outreach_segment = "rewarm_schedule_call"`

## UI Structure

### Sidebar
```
Sidebar:
  ├── Discovery
  │   ├── New Run
  │   └── [existing run history]
  ├── Rewarm
  │   ├── New Rewarm
  │   └── [rewarm run history]
  ├── Reporting          ← tabs for Discovery / Rewarm
  └── Settings
```

### New Rewarm Page
- **Platform selector**: dropdown (Instagram for v1, extensible to others later)
- **Segment selector**: dropdown populated from HubSpot Lists API (select one or more segments)
- **Dry run toggle**: checkbox — runs full pipeline but skips CRM sync, so ops can review scores first
- "Start Rewarm" button → kicks off RQ job
- Progress tracking same as discovery runs (stage-by-stage)

### Reporting
- New tab structure: **Discovery** | **Rewarm**
- Each run shows a tag/badge indicating its type
- Same metrics (contacts processed, scores, tier distribution) but filtered by run type

### Configuration
- **Staleness threshold**: how old cached data can be before re-fetching (e.g., 30 days)
- **Batch size**: max contacts per rewarm run (e.g., 50–100)
- **Auto-enroll on upgrade**: toggle whether threshold-crossing contacts are auto-queued for enrollment

## Data Flow
```
1. User clicks "New Rewarm" → selects HubSpot segment → clicks "Start Rewarm"
2. Segment Import stage:
   - Fetch contact list from HubSpot Lists API
   - For each contact, extract social handles (Instagram, etc.) from HubSpot properties
   - Build profile objects compatible with enrichment stage input
3. Enrichment:
   - Check R2 cache for existing analysis data
   - If stale or missing, fetch fresh data from InsightIQ
   - If fresh enough, skip re-fetch
4. Analysis:
   - Run GPT analysis on social data (or reuse if data hasn't changed)
5. Scoring:
   - Run generate_evidence_based_score()
   - Compare old tier (from HubSpot) vs new tier
6. CRM Sync:
   - Update HubSpot contact with new score/tier
   - If upgraded past threshold → queue for enrollment
   - Log results to run history
```

## API Shape (internal, triggered by UI)
```
POST /api/rewarm
Authorization: Bearer {API_KEY}
Content-Type: application/json

{
  "platform": "instagram",            // required, determines which handle to pull
  "hubspot_list_ids": ["list-id-1"],
  "dry_run": false,                  // optional, skip CRM sync if true
  "staleness_days": 30,              // optional, default from config
  "force_refetch": false              // optional, ignore cache entirely
}

Response:
{
  "run_id": "rewarm-abc123",
  "status": "queued",
  "contact_count": 42
}
```

Results tracked via normal run history with `run_type = "rewarm"`.

## HubSpot Lists API — Implementation Reference

See `docs/hubspot-lists-api.md` for full cheat sheet. Key points for Agent B:

**Two-step fetch** (memberships return IDs only, not properties):
1. **Search lists** → `POST /crm/v3/lists/search` with `objectTypeId: "0-1"` — populates the segment dropdown
2. **Get member IDs** → `GET /crm/v3/lists/{listId}/memberships` — returns contact IDs in the list
3. **Batch-fetch contacts** → `POST /crm/v3/objects/contacts/batch/read` with `properties: ["instagram_handle", "email", "firstname", "lastname"]` — reuse existing pattern from `check_existing_contacts()` in `hubspot.py`

**Auth**: same `HUBSPOT_API_KEY` (private app token). Needs `crm.lists.read` scope (verify it's on our token).

**List types**: `MANUAL`, `DYNAMIC`, `SNAPSHOT` — all readable. Show all contact lists in the dropdown, let ops pick.

**Rate limits**: standard HubSpot API limits. Batch read is 100 per call. Sleep 0.1s between batches (existing pattern).

**v1 sunset**: April 30, 2026 — we're on v3, no migration needed.

---

## Risky Assumptions

### Value
| Assumption | Risk | Confidence | How to test |
|---|---|---|---|
| ~~Contacts in HubSpot segments have social handles stored~~ | **Resolved** — rewarmable segments are assumed to have IG handles. Contacts missing a handle for the selected platform are skipped and surfaced in results. | **High** | — |
| Re-scoring with fresh data will meaningfully change tiers | If scores barely move, the feature adds cost with no conversion lift | **Medium** | Dry-run 50 old contacts, compare old vs new scores |
| Ops team will actually use this vs. just re-running discovery | If the UX friction is similar, they'll default to what they know | **Medium** | Validate with ops that segment-pick-and-go is faster than re-discovery |

### Usability
| Assumption | Risk | Confidence | How to test |
|---|---|---|---|
| HubSpot list names are meaningful enough to select from a dropdown | If there are hundreds of lists with cryptic names, the dropdown is unusable | **Medium** | Check how many lists exist and whether they have descriptive names |
| Stage-by-stage progress is sufficient feedback | Rewarm runs may feel like a black box if individual contact status isn't visible | **Medium** | Prototype the progress view, get ops feedback |

### Feasibility
| Assumption | Risk | Confidence | How to test |
|---|---|---|---|
| ~~HubSpot Lists API returns contacts with social properties in a single call~~ | **Resolved** — it doesn't. Memberships endpoint returns IDs only. Need a two-step: get member IDs → batch-fetch contacts with properties. Pattern already exists in `check_existing_contacts()`. | **High** | — |
| Enrichment/analysis/scoring stages can run unchanged on rewarm profiles | Profile structure from segment import may differ subtly from discovery output | **Medium** | Build segment import, feed output into enrichment, see what breaks |
| R2 cache keying works for contacts that came from HubSpot (not discovery) | Cache may be keyed on discovery-specific IDs that don't exist for rewarm contacts | **Medium** | Check `save_analysis_cache` key structure |

### Viability
| Assumption | Risk | Confidence | How to test |
|---|---|---|---|
| InsightIQ API costs for re-enriching are acceptable | Re-fetching hundreds of contacts could blow through API budget | **Medium** | Estimate cost per contact, multiply by segment size |
| GPT analysis costs scale linearly and are budgetable | Large segments could spike OpenAI spend unexpectedly | **Medium** | Calculate per-contact analysis cost, add batch size guard |

---

## Pre-Mortem

*It's 30 days after launch. The rewarm feature failed. What went wrong?*

### Tigers (Real Risks)

**T1: ~~HubSpot contacts have no social handles~~** — **Resolved**
Rewarmable segments are assumed to have IG handles. Contacts without a handle for the selected platform are skipped and reported. This is a known/accepted skip, not a failure.

**T2: Profile shape mismatch breaks downstream stages** — Launch-blocking
Segment import produces profiles that are subtly different from discovery output (missing fields, different ID scheme). Enrichment or scoring silently drops them or crashes.
- *Mitigation*: Define an explicit profile interface/contract. Write integration tests that feed segment-imported profiles through the full pipeline.
- *Owner*: Engineering
- *When*: During segment import implementation

**T3: API cost surprise on large segments** — Fast-follow
Ops selects a 500-contact segment. InsightIQ + OpenAI costs hit $200+ for a single run. No one noticed because there was no cost estimate before clicking "Start".
- *Mitigation*: Show estimated cost (or at least contact count) on the "Start Rewarm" confirmation. Hard cap batch size at 100 for v1.
- *Owner*: Engineering
- *When*: Before production rollout

### Paper Tigers (Overblown Concerns)

**PT1: "We need scheduling from day one"**
Manual is fine for v1. Ops runs this maybe weekly. The overhead of clicking "New Rewarm" is trivial compared to the value of seeing results before committing to automation.

**PT2: "Rewarm scoring needs a different model than discovery"**
Same `generate_evidence_based_score()` works for both. The input data is the same shape. No reason to fork the scoring logic.

### Elephants (Unspoken Worries)

**E1: Do we actually know which HubSpot lists are "rewarmable"?**
Not all lists make sense to rewarm. Some are event attendees, some are dead leads, some are active customers. There's no tagging convention. Ops might pick the wrong list and waste a run.
- *Investigate*: Interview ops about their list hygiene. Consider adding a "rewarm-eligible" tag convention.

**E2: ~~What happens when a contact is rewarmed but their score goes down?~~** — **Resolved**
Always update. Trust the segment builder. A lower score is valid new information.

**E3: ~~Overlap between discovery and rewarm runs~~** — **Resolved**
Discovery already drops contacts that exist in HubSpot. Rewarm only targets existing HubSpot contacts. No overlap possible.

---

## Decisions Made
- **Manual only** — no scheduled/cron rewarms for now
- **Separate UI flow** — rewarm is its own tab + "New Rewarm" entry point, not mixed into discovery
- **Run tagging** — all runs tagged as `discovery` or `rewarm` for filtering in reporting
- **Dry run mode** — toggle on "New Rewarm" page; runs full pipeline but skips CRM sync
- **Always update on re-score** — even downgrades. Trust the segment builder to pick the right contacts.
- **No dedup needed** — discovery drops existing HubSpot contacts, rewarm only targets existing ones. No overlap.

## Open Questions
- [x] ~~Which HubSpot contact properties hold social handles?~~ → Assume IG handle exists; rewarm is platform-based
- [x] ~~How do we handle contacts with no social handles?~~ → Skip and surface in results as "no handle"
- [x] ~~Which specific HubSpot property name stores the IG handle?~~ → `instagram_handle` (full URL, e.g. `https://www.instagram.com/mollyyeh/`). Set by InsightIQ standardization, used throughout pipeline via fallback chain `profile_url → instagram_handle → url`.
- [x] ~~Should there be a "dry run" mode?~~ → **Yes.** Runs the full pipeline but skips CRM sync. Lets ops validate scores before committing.
- [x] ~~What's the downgrade policy?~~ → **Always update.** Trust the segment builder to pick the right contacts. If a score goes down, that's valid new information — write it to HubSpot.
- [x] ~~Dedup strategy?~~ → **Not needed.** Discovery already drops contacts that exist in HubSpot (`check_existing_contacts`). Rewarm only targets contacts already in HubSpot. The two pools don't overlap.
