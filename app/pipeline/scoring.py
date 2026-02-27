"""
Pipeline Stage 5: SCORING — Evidence-based scoring + tier assignment.

Instagram: 5-dimension evidence scoring (visual quality, travel relevance, etc.)
Patreon:   Niche alignment, monetization, audience identity scoring.
Facebook:  Group health, admin fit, community engagement scoring.

All adapters output: lead_score (float), priority_tier (str), section_scores (dict).
"""
import os
import json
import logging
from typing import Dict, List, Any

import yaml

from app.extensions import openai_client as client
from app.pipeline.base import StageAdapter, StageResult

logger = logging.getLogger('pipeline.scoring')


# ── Scoring config (YAML with hardcoded fallback) ────────────────────────────

_scoring_config = None


def _default_config():
    """Hardcoded fallback if YAML is missing."""
    return {
        'version': 'default',
        'weights': {
            'niche_and_audience_identity': 0.30,
            'creator_authenticity_and_presence': 0.30,
            'monetization_and_business_mindset': 0.20,
            'community_infrastructure': 0.15,
            'engagement_and_connection': 0.05,
        },
        'category_penalties': {
            'Entertainment': -0.10,
        },
        'follower_boosts': [
            {'min_followers': 100000, 'boost': 0.15},
            {'min_followers': 75000, 'boost': 0.10},
            {'min_followers': 50000, 'boost': 0.05},
        ],
        'engagement': {
            'above_threshold_per_post': 0.03,
            'below_threshold_per_post': 0.03,
            'hidden_penalty_per_post': 0.05,
            'max_adjustment': 0.20,
        },
        'tiers': {
            'auto_enroll': {
                'manual_score_threshold': 0.65,
                'manual_score_precision': 0.833,
                'full_score_threshold': 0.80,
                'full_score_precision': 0.705,
            },
            'standard_priority_review': {
                'full_score_threshold': 0.25,
                'precision': 0.681,
            },
        },
        'travel_experience_floor': 0.50,
        'engagement_penalties': {
            'hidden_engagement_cap': 0.15,
            'low_engagement_cap': 0.15,
            'total_cap': 0.20,
            'hidden_penalty_per_post': 0.05,
            'low_penalty_per_post': 0.03,
            'like_threshold': 150,
            'comment_threshold': 10,
        },
    }


def load_scoring_config():
    """Load scoring config from YAML, with in-memory cache and hardcoded fallback."""
    global _scoring_config
    if _scoring_config is not None:
        return _scoring_config

    config_path = os.path.join(os.path.dirname(__file__), 'scoring_config.yaml')
    try:
        with open(config_path, 'r') as f:
            _scoring_config = yaml.safe_load(f)
        logger.info("Config loaded from YAML (version=%s)", _scoring_config.get('version', '?'))
    except Exception as e:
        logger.warning("YAML config not found (%s), using defaults", e)
        _scoring_config = _default_config()

    return _scoring_config


# ── Category examples for scoring ────────────────────────────────────────────

CATEGORY_EXAMPLES = None


def load_category_examples():
    """Load category-specific good/bad fit examples."""
    global CATEGORY_EXAMPLES
    if CATEGORY_EXAMPLES is None:
        examples_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'category_examples.json',
        )
        try:
            with open(examples_path, 'r') as f:
                CATEGORY_EXAMPLES = json.load(f)
            logger.info("Category examples loaded")
        except Exception as e:
            logger.warning("Could not load category examples: %s", e)
            CATEGORY_EXAMPLES = {}
    return CATEGORY_EXAMPLES


def format_category_examples(category: str) -> str:
    """Format category-specific examples for prompt inclusion."""
    examples = load_category_examples()
    if category not in examples:
        return ""

    cat_examples = examples[category]
    good_fits = cat_examples.get('good_fits', [])
    bad_fits = cat_examples.get('bad_fits', [])

    good_text = f"\n{'='*70}\nGOOD FIT EXAMPLES for {category}:\n{'='*70}\n"
    for idx, ex in enumerate(good_fits, 1):
        good_text += f"\n{idx}. @{ex['handle']}"
        if ex.get('niche'):
            good_text += f" - {ex['niche']}"
        if ex.get('why'):
            good_text += f"\n   Why good fit: {ex['why'].replace('- ', '').strip()}"
        if ex.get('trip_concept'):
            good_text += f"\n   Trip concept: {ex['trip_concept']}"
        good_text += "\n"

    bad_text = f"\n{'='*70}\nBAD FIT EXAMPLES for {category}:\n{'='*70}\n"
    for idx, ex in enumerate(bad_fits, 1):
        bad_text += f"\n{idx}. @{ex['handle']}"
        if ex.get('niche'):
            bad_text += f" - {ex['niche']}"
        if ex.get('why'):
            bad_text += f"\n   Why bad fit: {ex['why'].replace('- ', '').strip()}"
        bad_text += "\n"

    pattern_text = f"\n{'='*70}\nCRITICAL PATTERNS for {category}:\n{'='*70}\n"
    pattern_text += "Based on these examples:\n"
    pattern_text += "- Good fits show WHO the creator is (not just what they do)\n"
    pattern_text += "- Good fits have audience wanting to connect with EACH OTHER\n"
    pattern_text += "- Good fits mix expertise with personal/lifestyle content\n"
    pattern_text += "- Good fits have community infrastructure (email/podcast/groups)\n"
    pattern_text += "- Bad fits are transactional/promotional only\n"
    pattern_text += "- Bad fits have fans (one-way admiration) not community (two-way connection)\n"
    pattern_text += "- Bad fits don't show personality or vulnerability\n\n"

    return good_text + bad_text + pattern_text


def calculate_engagement_penalties(content_analyses: List[Dict]) -> Dict[str, float]:
    """Calculate engagement-based penalties from content analyses."""
    cfg = load_scoring_config().get('engagement_penalties', {})
    like_threshold = cfg.get('like_threshold', 150)
    comment_threshold = cfg.get('comment_threshold', 10)
    hidden_per = cfg.get('hidden_penalty_per_post', 0.05)
    low_per = cfg.get('low_penalty_per_post', 0.03)
    hidden_cap = cfg.get('hidden_engagement_cap', 0.15)
    low_cap = cfg.get('low_engagement_cap', 0.15)
    total_cap = cfg.get('total_cap', 0.20)

    hidden_engagement_posts = []
    low_engagement_posts = []

    for post in content_analyses:
        if post.get('is_pinned', False):
            continue
        if post.get('likes_and_views_disabled', False):
            hidden_engagement_posts.append(post)
            continue
        engagement = post.get('engagement', {})
        like_count = engagement.get('like_count', 0) or 0
        comment_count = engagement.get('comment_count', 0) or 0
        if like_count < like_threshold and comment_count < comment_threshold:
            low_engagement_posts.append(post)

    hidden_penalty = min(len(hidden_engagement_posts) * hidden_per, hidden_cap)
    low_engagement_penalty = min(len(low_engagement_posts) * low_per, low_cap)
    total_penalty = min(hidden_penalty + low_engagement_penalty, total_cap)

    return {
        'hidden_engagement_penalty': hidden_penalty,
        'low_engagement_penalty': low_engagement_penalty,
        'hidden_count': len(hidden_engagement_posts),
        'low_engagement_count': len(low_engagement_posts),
        'total_penalty': total_penalty,
    }


def generate_evidence_based_score(
    bio_evidence: Dict[str, Any],
    caption_evidence: Dict[str, Any],
    thumbnail_evidence: Dict[str, Any],
    content_analyses: List[Dict[str, Any]],
    creator_profile: Dict[str, Any],
    follower_count: int,
) -> Dict[str, Any]:
    """Generate TrovaTrip lead score using evidence-based approach — v3.0."""

    cfg = load_scoring_config()
    weights = cfg.get('weights', _default_config()['weights'])
    category_penalties = cfg.get('category_penalties', {})
    follower_boosts = cfg.get('follower_boosts', [])
    eng_cfg = cfg.get('engagement', {})
    tiers_cfg = cfg.get('tiers', {})

    primary_category = creator_profile.get('primary_category', 'unknown')
    category_examples_text = format_category_examples(primary_category)

    content_summaries = []
    for idx, item in enumerate(content_analyses, 1):
        summary_parts = [f"Content {idx} ({item['type']}): {item.get('summary', '')}"]
        if item.get('shows_pov'):
            summary_parts.append("Shows POV/perspective")
        if item.get('shows_authenticity'):
            summary_parts.append("Shows authenticity")
        if item.get('shows_vulnerability'):
            summary_parts.append("Shows vulnerability")
        content_summaries.append(" | ".join(summary_parts))

    combined_summaries = "\n\n".join(content_summaries)

    evidence_summary = f"""
=== EVIDENCE GATHERED ===

BIO EVIDENCE:
- Niche identified: {bio_evidence.get('niche_signals', {}).get('niche_identified', False)}
  Description: {bio_evidence.get('niche_signals', {}).get('niche_description', 'N/A')}
- In-person events: {bio_evidence.get('in_person_events', {}).get('evidence_found', False)}
  Types: {', '.join(bio_evidence.get('in_person_events', {}).get('event_types', []))}
- Community platforms: {bio_evidence.get('community_platforms', {}).get('evidence_found', False)}
  Platforms: {', '.join(bio_evidence.get('community_platforms', {}).get('platforms', []))}
- Monetization: {bio_evidence.get('monetization', {}).get('evidence_found', False)}
  Types: {', '.join(bio_evidence.get('monetization', {}).get('types', []))}

THUMBNAIL GRID EVIDENCE (12 posts):
- Creator visibility: {thumbnail_evidence.get('creator_visibility', {}).get('frequency', 'none')}
- Niche consistency: {thumbnail_evidence.get('niche_consistency', {}).get('consistent_theme', False)}
  Theme: {thumbnail_evidence.get('niche_consistency', {}).get('niche_description', 'N/A')}
- Event promotion posts: {thumbnail_evidence.get('event_promotion', {}).get('post_count', 0)}/12
- Engagement cues: {thumbnail_evidence.get('audience_engagement_cues', {}).get('post_count', 0)}/12

CAPTION EVIDENCE (12 posts):
- In-person event mentions: {caption_evidence.get('in_person_events', {}).get('mention_count', 0)}/12 posts
- Community platform mentions: {caption_evidence.get('community_platforms', {}).get('mention_count', 0)}/12 posts
- Questions to audience: {caption_evidence.get('audience_engagement', {}).get('question_count', 0)}/12 posts
- Authenticity/vulnerability degree: {caption_evidence.get('authenticity_vulnerability', {}).get('degree', 0.0):.2f}/1.0
  Posts showing this: {caption_evidence.get('authenticity_vulnerability', {}).get('post_count', 0)}/12

DEEP CONTENT ANALYSIS (3 posts):
{combined_summaries}

CREATOR PROFILE:
- Primary Category: {primary_category}
- Content Types: {creator_profile.get('content_types', 'N/A')}
- Presence: {creator_profile.get('creator_presence', 'N/A')}
"""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{
            "role": "system",
            "content": f"""You score creators for TrovaTrip, a group travel platform where creators host trips with their communities.

{category_examples_text}

CRITICAL SCORING PRINCIPLES:
1. A good fit is someone whose AUDIENCE wants to meet EACH OTHER and the host
2. In-person events (classes, retreats, trips) are HIGH VALUE signals
3. Weight evidence by frequency and confidence across multiple sources
4. Entertainment profiles should be scored more harshly

SECTION DEFINITIONS:

1. **niche_and_audience_identity** (0.0-1.0)
2. **creator_authenticity_and_presence** (0.0-1.0)
3. **monetization_and_business_mindset** (0.0-1.0)
4. **community_infrastructure** (0.0-1.0)
5. **engagement_and_connection** (0.0-1.0)

RESPOND ONLY with JSON:
{{
  "niche_and_audience_identity": 0.0-1.0,
  "creator_authenticity_and_presence": 0.0-1.0,
  "monetization_and_business_mindset": 0.0-1.0,
  "community_infrastructure": 0.0-1.0,
  "engagement_and_connection": 0.0-1.0,
  "score_reasoning": "2-3 sentences explaining fit for group travel."
}}"""
        }, {
            "role": "user",
            "content": evidence_summary,
        }],
        response_format={"type": "json_object"},
    )

    result = json.loads(response.choices[0].message.content)
    logger.debug("Evidence-based score response: %s", json.dumps(result, indent=2))

    niche = result.get('niche_and_audience_identity', 0.0)
    authenticity = result.get('creator_authenticity_and_presence', 0.0)
    monetization = result.get('monetization_and_business_mindset', 0.0)
    community = result.get('community_infrastructure', 0.0)
    engagement = result.get('engagement_and_connection', 0.0)

    # Weighted sum from config
    manual_score = (
        (niche * weights.get('niche_and_audience_identity', 0.30)) +
        (authenticity * weights.get('creator_authenticity_and_presence', 0.30)) +
        (monetization * weights.get('monetization_and_business_mindset', 0.20)) +
        (community * weights.get('community_infrastructure', 0.15)) +
        (engagement * weights.get('engagement_and_connection', 0.05))
    )

    # Category penalty from config
    category_penalty = category_penalties.get(primary_category, 0.0)
    manual_score_with_penalty = max(0.0, min(1.0, manual_score + category_penalty))

    # Follower boost from config (first match wins)
    follower_boost = 0.0
    for tier in follower_boosts:
        if follower_count >= tier['min_followers']:
            follower_boost = tier['boost']
            break

    # Engagement adjustment from config
    above_per = eng_cfg.get('above_threshold_per_post', 0.03)
    below_per = eng_cfg.get('below_threshold_per_post', 0.03)
    hidden_per = eng_cfg.get('hidden_penalty_per_post', 0.05)
    max_adj = eng_cfg.get('max_adjustment', 0.20)

    eng_metrics = thumbnail_evidence.get('engagement_metrics', {})
    posts_above = eng_metrics.get('posts_above_threshold', 0)
    posts_below = eng_metrics.get('posts_below_threshold', 0)
    posts_hidden = eng_metrics.get('posts_hidden', 0)

    engagement_adjustment = (posts_above * above_per) - (posts_below * below_per) - (posts_hidden * hidden_per)
    engagement_adjustment = max(-max_adj, min(max_adj, engagement_adjustment))

    full_score = max(0.0, min(1.0, manual_score_with_penalty + follower_boost + engagement_adjustment))

    # Tier assignment from config
    ae_cfg = tiers_cfg.get('auto_enroll', {})
    spr_cfg = tiers_cfg.get('standard_priority_review', {})

    if manual_score_with_penalty >= ae_cfg.get('manual_score_threshold', 0.65):
        priority_tier = "auto_enroll"
        expected_precision = ae_cfg.get('manual_score_precision', 0.833)
        tier_reasoning = f"Manual score >={ae_cfg.get('manual_score_threshold', 0.65)} ({int(expected_precision*100)}% precision)"
    elif full_score >= ae_cfg.get('full_score_threshold', 0.80):
        priority_tier = "auto_enroll"
        expected_precision = ae_cfg.get('full_score_precision', 0.705)
        tier_reasoning = f"Full score >={ae_cfg.get('full_score_threshold', 0.80)} ({int(expected_precision*100)}% precision)"
    elif full_score >= spr_cfg.get('full_score_threshold', 0.25):
        priority_tier = "standard_priority_review"
        expected_precision = spr_cfg.get('precision', 0.681)
        tier_reasoning = f"Full score >={spr_cfg.get('full_score_threshold', 0.25)} ({int(expected_precision*100)}% precision)"
    else:
        priority_tier = "low_priority_review"
        expected_precision = 0.0
        tier_reasoning = "Below review thresholds"

    score_reasoning = result.get('score_reasoning', '')
    adjustments = []
    if follower_boost > 0:
        adjustments.append(f"Follower boost: +{follower_boost:.2f} ({follower_count:,} followers)")
    if engagement_adjustment != 0:
        adjustments.append(f"Engagement: {engagement_adjustment:+.2f} ({posts_above} above / {posts_below} below / {posts_hidden} hidden)")
    if category_penalty != 0:
        adjustments.append(f"Entertainment penalty: {category_penalty:.2f}")
    if adjustments:
        score_reasoning += " | ADJUSTMENTS: " + "; ".join(adjustments)
    score_reasoning += f" | TIER: {priority_tier} ({tier_reasoning})"

    section_scores = {
        "niche_and_audience_identity": niche,
        "creator_authenticity_and_presence": authenticity,
        "monetization_and_business_mindset": monetization,
        "community_infrastructure": community,
        "engagement_and_connection": engagement,
    }

    return {
        "section_scores": section_scores,
        "manual_score": manual_score_with_penalty,
        "lead_score": full_score,
        "follower_boost": follower_boost,
        "engagement_adjustment": engagement_adjustment,
        "category_penalty": category_penalty,
        "priority_tier": priority_tier,
        "expected_precision": expected_precision,
        "score_reasoning": score_reasoning,
    }


# ── Adapters ──────────────────────────────────────────────────────────────────

class InstagramScoring(StageAdapter):
    """
    IG scoring: 5-dimension evidence-based scoring.
    Dimensions: niche, authenticity, monetization, community, engagement.
    """
    platform = 'instagram'
    stage = 'scoring'
    description = '5-dimension evidence scoring + tier assignment'
    apis = ['OpenAI']

    DIMENSIONS = ['niche_and_audience_identity', 'creator_authenticity_and_presence',
                  'monetization_and_business_mindset', 'community_infrastructure',
                  'engagement_and_connection']

    def estimate_cost(self, count: int) -> float:
        return count * 0.02

    def run(self, profiles, run) -> StageResult:
        from app.services.r2 import save_analysis_cache
        from app.services.openai_client import extract_first_names_from_instagram_profile

        cfg = load_scoring_config()
        travel_floor = cfg.get('travel_experience_floor', 0.50)

        scored = []
        errors = []

        for profile in profiles:
            profile_url = profile.get('profile_url') or profile.get('url', '')
            contact_id = profile.get('contact_id') or profile.get('id', '')

            try:
                lead_analysis = generate_evidence_based_score(
                    bio_evidence=profile['_bio_evidence'],
                    caption_evidence=profile['_caption_evidence'],
                    thumbnail_evidence=profile['_thumbnail_evidence'],
                    content_analyses=profile.get('_content_analyses', []),
                    creator_profile=profile.get('_creator_profile', {}),
                    follower_count=profile.get('follower_count', 0),
                )

                # Travel experience boost
                if profile.get('_has_travel_experience') and lead_analysis['lead_score'] < travel_floor:
                    lead_analysis['lead_score'] = travel_floor
                    lead_analysis['score_reasoning'] += " | TRAVEL EXPERIENCE BOOST"

                # Extract first name
                social_data = profile.get('_social_data', {})
                _info = social_data.get('data', [{}])[0].get('profile', {}) if social_data else {}
                bio = profile.get('bio', '')
                first_name = extract_first_names_from_instagram_profile(
                    _info.get('platform_username', ''),
                    _info.get('full_name', ''),
                    bio or _info.get('introduction', ''),
                    profile.get('_content_analyses', []),
                )

                # Cache for rescoring
                if contact_id:
                    save_analysis_cache(contact_id, {
                        'contact_id': contact_id, 'profile_url': profile_url,
                        'bio': bio, 'follower_count': profile.get('follower_count', 0),
                        'content_analyses': profile.get('_content_analyses', []),
                        'creator_profile': profile.get('_creator_profile', {}),
                        'bio_evidence': profile['_bio_evidence'],
                        'caption_evidence': profile['_caption_evidence'],
                        'thumbnail_evidence': profile['_thumbnail_evidence'],
                        'has_travel_experience': profile.get('_has_travel_experience', False),
                        'first_name': first_name,
                    })

                profile['_lead_analysis'] = lead_analysis
                profile['_first_name'] = first_name
                scored.append(profile)

                # Update tier distribution
                tier = lead_analysis.get('priority_tier', 'low_priority_review')
                if tier in run.tier_distribution:
                    run.tier_distribution[tier] += 1

                run.increment_stage_progress('scoring', 'completed')
                logger.info("%s: score=%.3f (%s)", profile_url, lead_analysis['lead_score'], tier)

            except Exception as e:
                logger.error("Error on %s: %s", profile_url, e)
                errors.append(str(e))
                run.increment_stage_progress('scoring', 'failed')

        return StageResult(
            profiles=scored,
            processed=len(profiles),
            failed=len(errors),
            errors=errors,
            cost=len(profiles) * 0.02,
        )


class PatreonScoring(StageAdapter):
    """
    Patreon scoring: same 5 dimensions but weighted for text-based creators.
    No visual content, so engagement_metrics penalties are zeroed out.
    """
    platform = 'patreon'
    stage = 'scoring'
    description = '5-dimension scoring (patron count as proxy)'
    apis = ['OpenAI']

    def estimate_cost(self, count: int) -> float:
        return count * 0.02

    def run(self, profiles, run) -> StageResult:
        scored = []
        errors = []

        for profile in profiles:
            creator_name = profile.get('creator_name') or profile.get('name', 'Unknown')

            try:
                lead_analysis = generate_evidence_based_score(
                    bio_evidence=profile.get('_bio_evidence', {}),
                    caption_evidence=profile.get('_caption_evidence', {}),
                    thumbnail_evidence=profile.get('_thumbnail_evidence', {}),
                    content_analyses=profile.get('_content_analyses', []),
                    creator_profile=profile.get('_creator_profile', {}),
                    follower_count=int(profile.get('patron_count') or profile.get('total_members') or 0),
                )

                profile['_lead_analysis'] = lead_analysis
                scored.append(profile)

                tier = lead_analysis.get('priority_tier', 'low_priority_review')
                if tier in run.tier_distribution:
                    run.tier_distribution[tier] += 1

                run.increment_stage_progress('scoring', 'completed')
                logger.info("%s: score=%.3f (%s)", creator_name, lead_analysis['lead_score'], tier)

            except Exception as e:
                logger.error("Error on %s: %s", creator_name, e)
                errors.append(str(e))
                run.increment_stage_progress('scoring', 'failed')

        return StageResult(
            profiles=scored,
            processed=len(profiles),
            failed=len(errors),
            errors=errors,
            cost=len(profiles) * 0.02,
        )


class FacebookScoring(StageAdapter):
    """
    Facebook scoring: same 5 dimensions, tuned for group admin evaluation.
    """
    platform = 'facebook'
    stage = 'scoring'
    description = '5-dimension scoring (member count as proxy)'
    apis = ['OpenAI']

    def estimate_cost(self, count: int) -> float:
        return count * 0.02

    def run(self, profiles, run) -> StageResult:
        scored = []
        errors = []

        for profile in profiles:
            group_name = profile.get('group_name', 'Unknown Group')

            try:
                lead_analysis = generate_evidence_based_score(
                    bio_evidence=profile.get('_bio_evidence', {}),
                    caption_evidence=profile.get('_caption_evidence', {}),
                    thumbnail_evidence=profile.get('_thumbnail_evidence', {}),
                    content_analyses=profile.get('_content_analyses', []),
                    creator_profile=profile.get('_creator_profile', {}),
                    follower_count=profile.get('member_count', 0),
                )

                profile['_lead_analysis'] = lead_analysis
                scored.append(profile)

                tier = lead_analysis.get('priority_tier', 'low_priority_review')
                if tier in run.tier_distribution:
                    run.tier_distribution[tier] += 1

                run.increment_stage_progress('scoring', 'completed')
                logger.info("%s: score=%.3f (%s)", group_name, lead_analysis['lead_score'], tier)

            except Exception as e:
                logger.error("Error on %s: %s", group_name, e)
                errors.append(str(e))
                run.increment_stage_progress('scoring', 'failed')

        return StageResult(
            profiles=scored,
            processed=len(profiles),
            failed=len(errors),
            errors=errors,
            cost=len(profiles) * 0.02,
        )


# ── Adapter registry ─────────────────────────────────────────────────────────

ADAPTERS: Dict[str, type] = {
    'instagram': InstagramScoring,
    'patreon': PatreonScoring,
    'facebook': FacebookScoring,
}
