"""
Filter similarity detection — compares discovery filters against recent runs.

Uses Jaccard similarity on tokenized keywords + numeric range overlap
to find prior runs with similar search parameters.
"""
import logging
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional

from app.database import get_session
from app.models.db_run import DbRun

logger = logging.getLogger('services.filter_similarity')


# ── Pure scoring functions ────────────────────────────────────────────────────

def jaccard_similarity(set_a: Set[str], set_b: Set[str]) -> float:
    """Jaccard index: |intersection| / |union|. Returns 0.0 for empty sets."""
    if not set_a and not set_b:
        return 0.0
    union = set_a | set_b
    if not union:
        return 0.0
    return len(set_a & set_b) / len(union)


def range_overlap(range_a: Tuple, range_b: Tuple) -> float:
    """
    Overlap ratio of two numeric ranges.

    Returns |overlap| / |span| where span is min-to-max across both ranges.
    Returns 0.0 if either range has None values or no overlap.
    """
    min_a, max_a = range_a
    min_b, max_b = range_b

    if min_a is None or max_a is None or min_b is None or max_b is None:
        return 0.0

    overlap_start = max(min_a, min_b)
    overlap_end = min(max_a, max_b)
    overlap = max(0, overlap_end - overlap_start)

    span = max(max_a, max_b) - min(min_a, min_b)
    if span <= 0:
        return 1.0 if overlap >= 0 and min_a == min_b else 0.0

    return overlap / span


def tokenize_filters(filters: Dict, platform: str) -> Set[str]:
    """
    Extract comparable keyword tokens from a filters dict.

    Platform-aware: pulls hashtags/bio for Instagram, search_keywords for
    Patreon, keywords for Facebook. Also extracts interest categories.
    """
    tokens = set()

    # Instagram hashtags
    for h in filters.get('hashtags', []):
        name = h.get('name', '') if isinstance(h, dict) else str(h)
        for word in name.lower().split():
            tokens.add(word.strip('#').strip())

    # Bio phrase
    bio = filters.get('bio_phrase', '')
    if bio:
        for word in bio.lower().split():
            tokens.add(word.strip())

    # Patreon search_keywords
    for kw in filters.get('search_keywords', []):
        for word in kw.lower().split():
            tokens.add(word.strip())

    # Facebook keywords
    for kw in filters.get('keywords', []):
        for word in kw.lower().split():
            tokens.add(word.strip())

    # Interest categories (both creator and audience)
    for key in ('creator_interests', 'audience_interests'):
        for interest in filters.get(key, []):
            tokens.add(interest.lower())

    # Discard empty strings
    tokens.discard('')

    return tokens


def extract_numeric_ranges(filters: Dict, platform: str) -> Dict[str, Tuple]:
    """
    Extract numeric range tuples from filters.

    Returns dict like {'followers': (20000, 500000), 'patrons': (50, 5000)}.
    """
    ranges = {}

    # Instagram follower count
    fc = filters.get('follower_count', {})
    if isinstance(fc, dict) and (fc.get('min') is not None or fc.get('max') is not None):
        ranges['followers'] = (fc.get('min'), fc.get('max'))

    # Patreon patron count
    if filters.get('min_patrons') is not None or filters.get('max_patrons') is not None:
        min_p = filters.get('min_patrons')
        max_p = filters.get('max_patrons')
        if min_p is not None or max_p is not None:
            ranges['patrons'] = (min_p, max_p)

    # Facebook member count
    if filters.get('min_members') is not None or filters.get('max_members') is not None:
        min_m = filters.get('min_members')
        max_m = filters.get('max_members')
        if min_m is not None or max_m is not None:
            ranges['members'] = (min_m, max_m)

    return ranges


def compute_similarity(filters_a: Dict, filters_b: Dict, platform: str) -> float:
    """
    Compute weighted similarity score between two filter sets.

    Weights: 60% keyword overlap (Jaccard), 40% numeric range overlap.
    If no keywords exist, range overlap gets 100%. If no ranges, keywords get 100%.
    """
    tokens_a = tokenize_filters(filters_a, platform)
    tokens_b = tokenize_filters(filters_b, platform)
    keyword_sim = jaccard_similarity(tokens_a, tokens_b)

    ranges_a = extract_numeric_ranges(filters_a, platform)
    ranges_b = extract_numeric_ranges(filters_b, platform)

    # Compute average range overlap across shared range keys
    all_range_keys = set(ranges_a.keys()) | set(ranges_b.keys())
    if all_range_keys:
        range_scores = []
        for key in all_range_keys:
            ra = ranges_a.get(key, (None, None))
            rb = ranges_b.get(key, (None, None))
            range_scores.append(range_overlap(ra, rb))
        range_sim = sum(range_scores) / len(range_scores)
    else:
        range_sim = 0.0

    # Weighted combination
    has_keywords = bool(tokens_a or tokens_b)
    has_ranges = bool(all_range_keys)

    if has_keywords and has_ranges:
        return 0.6 * keyword_sim + 0.4 * range_sim
    elif has_keywords:
        return keyword_sim
    elif has_ranges:
        return range_sim
    else:
        return 0.0


# ── Database query ────────────────────────────────────────────────────────────

def find_similar_runs(
    platform: str,
    filters: Dict,
    threshold: float = 0.7,
    limit: int = 50,
) -> List[Dict]:
    """
    Find completed runs with similar filters, ranked by similarity.

    Returns list of dicts: [{run_id, similarity, profiles_found,
    contacts_synced, days_ago}, ...] sorted by similarity descending.
    Only includes runs above the threshold.
    """
    try:
        session = get_session()
        try:
            recent_runs = (
                session.query(DbRun)
                .filter(
                    DbRun.platform == platform,
                    DbRun.status == 'completed',
                )
                .order_by(DbRun.created_at.desc())
                .limit(limit)
                .all()
            )

            results = []
            for run in recent_runs:
                run_filters = run.filters or {}
                sim = compute_similarity(filters, run_filters, platform)
                if sim >= threshold:
                    created = run.created_at
                    if created and created.tzinfo:
                        created = created.replace(tzinfo=None)
                    days_ago = (datetime.now() - created).days if created else 0
                    results.append({
                        'run_id': run.id,
                        'similarity': round(sim, 3),
                        'profiles_found': run.profiles_found or 0,
                        'contacts_synced': run.contacts_synced or 0,
                        'days_ago': days_ago,
                    })

            results.sort(key=lambda r: r['similarity'], reverse=True)
            return results
        finally:
            session.close()
    except Exception:
        logger.error("find_similar_runs failed", exc_info=True)
        return []
