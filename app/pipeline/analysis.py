"""
Pipeline Stage 4: ANALYSIS — Deep content analysis per platform.

Instagram: GPT-4o vision on posts + Whisper on reels + evidence gathering.
Patreon:   Text content analysis + tier structure + patron engagement signals.
Facebook:  Group health metrics + admin profile signals.

All adapters attach analysis results to profiles for the scoring stage.
"""
import json
import logging
from typing import Dict, List, Any

import requests

from app.services.openai_client import (
    analyze_content_item,
    analyze_bio_evidence,
    analyze_caption_evidence,
    generate_creator_profile,
)
from app.services.r2 import rehost_media_on_r2, create_thumbnail_grid
from app.extensions import openai_client as client
from app.pipeline.base import StageAdapter, StageResult

logger = logging.getLogger('pipeline.analysis')


# ── Shared analysis functions ─────────────────────────────────────────────────

def analyze_thumbnail_evidence(
    thumbnail_urls: List[str],
    engagement_data: List[Dict],
    contact_id: str,
) -> Dict[str, Any]:
    """Extract structured evidence from thumbnail grid (up to 12 posts)."""
    if not thumbnail_urls:
        return {
            "creator_visibility": {"visible_in_content": False, "frequency": "none", "confidence": 0.0},
            "niche_consistency": {"consistent_theme": False, "niche_description": "", "confidence": 0.0},
            "event_promotion": {"evidence_found": False, "post_count": 0, "confidence": 0.0},
            "engagement_metrics": {"posts_above_threshold": 0, "posts_below_threshold": 0, "posts_hidden": 0},
        }

    grid_url = create_thumbnail_grid(thumbnail_urls, contact_id)

    posts_above, posts_below, posts_hidden = 0, 0, 0
    for data in engagement_data:
        if data.get('is_pinned', False):
            continue
        if data.get('likes_and_views_disabled', False):
            posts_hidden += 1
        else:
            eng = data.get('engagement', {})
            likes = eng.get('like_count', 0) or 0
            comments = eng.get('comment_count', 0) or 0
            if likes >= 150 and comments >= 15:
                posts_above += 1
            else:
                posts_below += 1

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{
            "role": "user",
            "content": [{
                "type": "text",
                "text": """Analyze this Instagram thumbnail grid (3x4 layout, 12 posts) for group travel host potential.

Extract evidence for:

1. CREATOR VISIBILITY: Is the creator visible as a person in the thumbnails?
   - Frequency: "most" (8+), "some" (4-7), "rare" (1-3), "none"

2. NICHE CONSISTENCY: Do thumbnails show a consistent content theme?

3. EVENT PROMOTION: Visual signs of in-person events?

4. AUDIENCE ENGAGEMENT CUES: Text overlays suggesting engagement?

Respond ONLY with JSON:
{
  "creator_visibility": {"visible_in_content": true/false, "frequency": "most/some/rare/none", "confidence": 0.0-1.0},
  "niche_consistency": {"consistent_theme": true/false, "niche_description": "...", "confidence": 0.0-1.0},
  "event_promotion": {"evidence_found": true/false, "post_count": 0-12, "confidence": 0.0-1.0},
  "audience_engagement_cues": {"invitational_language": true/false, "post_count": 0-12, "confidence": 0.0-1.0}
}"""
            }, {
                "type": "image_url",
                "image_url": {"url": grid_url}
            }]
        }],
        response_format={"type": "json_object"},
    )

    result = json.loads(response.choices[0].message.content)
    result['engagement_metrics'] = {
        "posts_above_threshold": posts_above,
        "posts_below_threshold": posts_below,
        "posts_hidden": posts_hidden,
        "posts_analyzed": len(engagement_data),
    }

    logger.debug("Thumbnail evidence: %s", json.dumps(result, indent=2))
    return result


def analyze_selected_content(
    filtered_items: List[Dict],
    selected_indices: List[int],
    contact_id: str,
) -> List[Dict[str, Any]]:
    """Analyze 3 selected content items (rehost + GPT-4o/Whisper)."""
    content_analyses = []

    for idx in selected_indices[:3]:
        if idx >= len(filtered_items):
            logger.warning("Index %d out of range, skipping", idx)
            continue

        item = filtered_items[idx]
        content_format = item.get('format')
        media_url = None
        media_format = None

        if content_format == 'VIDEO':
            media_url = item.get('media_url')
            media_format = 'VIDEO'
        elif content_format == 'COLLECTION':
            content_group_media = item.get('content_group_media', [])
            if content_group_media:
                media_url = content_group_media[0].get('media_url')
            else:
                media_url = item.get('thumbnail_url')
            media_format = 'IMAGE'
        else:
            media_url = item.get('media_url') or item.get('thumbnail_url')
            media_format = 'IMAGE'

        if not media_url:
            logger.warning("Item at index %d: No media URL, skipping", idx)
            continue

        media_url = media_url.rstrip('.')

        if media_format == 'VIDEO':
            try:
                head_response = requests.head(media_url, timeout=10)
                content_length = int(head_response.headers.get('content-length', 0))
                if content_length > 25 * 1024 * 1024:
                    logger.warning("Item %d: Video too large (%.1fMB), skipping", idx, content_length / 1024 / 1024)
                    continue
            except Exception as e:
                logger.warning("Item %d: Could not check video size: %s, attempting anyway", idx, e)

        try:
            rehosted_url = rehost_media_on_r2(media_url, contact_id, media_format)
            analysis = analyze_content_item(rehosted_url, media_format)
            analysis['description'] = item.get('description', '')
            analysis['is_pinned'] = item.get('is_pinned', False)
            analysis['likes_and_views_disabled'] = item.get('likes_and_views_disabled', False)
            analysis['engagement'] = item.get('engagement', {})
            content_analyses.append(analysis)
            logger.debug("Item %d: Successfully analyzed", idx)
        except Exception as e:
            logger.error("Item %d: Error analyzing: %s", idx, e)

    return content_analyses


def gather_evidence(filtered_items: List[Dict], bio: str, contact_id: str):
    """Gather evidence from bio, captions, and thumbnails (all 12 posts)."""
    thumbnail_urls = []
    captions = []
    engagement_data = []

    for item in filtered_items[:12]:
        thumb_url = item.get('thumbnail_url')
        if thumb_url:
            thumbnail_urls.append(thumb_url)
        caption = item.get('description', '') or item.get('title', '')
        if caption:
            captions.append(caption[:500])
        engagement_data.append({
            'is_pinned': item.get('is_pinned', False),
            'likes_and_views_disabled': item.get('likes_and_views_disabled', False),
            'engagement': item.get('engagement', {}),
        })

    logger.info("Gathering evidence from: %d thumbnails, %d captions", len(thumbnail_urls), len(captions))

    bio_evidence = analyze_bio_evidence(bio)
    caption_evidence = analyze_caption_evidence(captions)
    thumbnail_evidence = analyze_thumbnail_evidence(thumbnail_urls, engagement_data, contact_id)

    logger.info("Evidence gathering complete")
    return bio_evidence, caption_evidence, thumbnail_evidence


# ── Adapters ──────────────────────────────────────────────────────────────────

class InstagramAnalysis(StageAdapter):
    """
    IG analysis: GPT-4o vision on 3 posts + Whisper on reels + evidence gathering.
    Uses _content_items and _selected_indices from prescreen stage.
    """
    platform = 'instagram'

    def estimate_cost(self, count: int) -> float:
        return count * 0.15
    stage = 'analysis'
    description = 'GPT-4o vision on 3 posts + Whisper audio transcription'
    apis = ['OpenAI']

    def run(self, profiles, run) -> StageResult:
        analyzed = []
        errors = []

        for profile in profiles:
            profile_url = profile.get('profile_url') or profile.get('instagram_handle') or profile.get('url', '')
            contact_id = profile.get('contact_id') or profile.get('id', run.id)
            bio = profile.get('bio', '')
            filtered_items = profile.get('_content_items', [])
            selected_indices = profile.get('_selected_indices', [0, 1, 2])

            if not filtered_items:
                errors.append(f"No content for {profile_url}")
                continue

            try:
                # Deep analysis of 3 selected posts
                content_analyses = analyze_selected_content(filtered_items, selected_indices, contact_id)
                if not content_analyses:
                    errors.append(f"Could not analyze content for {profile_url}")
                    continue

                # Evidence gathering (bio + captions + thumbnail grid)
                bio_evidence, caption_evidence, thumbnail_evidence = gather_evidence(
                    filtered_items, bio, contact_id
                )

                # Creator profile synthesis
                creator_profile = generate_creator_profile(content_analyses)

                # Attach results
                profile['_content_analyses'] = content_analyses
                profile['_bio_evidence'] = bio_evidence
                profile['_caption_evidence'] = caption_evidence
                profile['_thumbnail_evidence'] = thumbnail_evidence
                profile['_creator_profile'] = creator_profile

                analyzed.append(profile)
                run.increment_stage_progress('analysis', 'completed')
                logger.info("Analyzed %s: %d items", profile_url, len(content_analyses))

            except Exception as e:
                logger.error("Error on %s: %s", profile_url, e)
                errors.append(f"{profile_url}: {str(e)}")
                run.increment_stage_progress('analysis', 'failed')

        return StageResult(
            profiles=analyzed,
            processed=len(profiles),
            failed=len(errors),
            errors=errors,
            cost=len(profiles) * 0.15,
        )


class PatreonAnalysis(StageAdapter):
    """
    Patreon analysis: text content analysis + tier structure + patron signals.
    Uses GPT-4o to evaluate creator's written content and community indicators.
    """
    platform = 'patreon'
    stage = 'analysis'
    description = 'GPT-4o text analysis — bio, tiers, patron signals'
    apis = ['OpenAI']

    def estimate_cost(self, count: int) -> float:
        return count * 0.10

    def run(self, profiles, run) -> StageResult:
        analyzed = []
        errors = []

        for profile in profiles:
            creator_name = profile.get('creator_name') or profile.get('name', 'Unknown')

            try:
                # Build content summary from available Patreon data
                bio = profile.get('about', '') or profile.get('summary', '') or profile.get('description', '')
                patron_count = int(profile.get('patron_count') or profile.get('total_members') or 0)
                post_count = int(profile.get('post_count') or profile.get('total_posts') or 0)
                tiers = profile.get('tiers', [])
                social_links = {
                    k: profile.get(k) for k in
                    ['instagram_url', 'youtube_url', 'twitter_url', 'facebook_url', 'personal_website']
                    if profile.get(k)
                }

                # GPT-4o text analysis
                analysis_prompt = f"""Analyze this Patreon creator for group travel host potential (TrovaTrip).

Creator: {creator_name}
Patrons: {patron_count}
Posts: {post_count}
Bio/About: {bio[:1500]}
Tier count: {len(tiers)}
Social links: {', '.join(social_links.keys())}
Enriched email: {profile.get('email', 'none')}
Website: {profile.get('personal_website', 'none')}

Evaluate for:
1. NICHE: What is their content niche? Is their audience identity-based (people who ARE something) vs interest-based?
2. COMMUNITY: Do they have community infrastructure? (Discord, Facebook group, email list, meetups)
3. MONETIZATION: Tier structure suggests business mindset?
4. ENGAGEMENT: Any signs of in-person events, retreats, meetups?
5. AUTHENTICITY: Does their content show personal presence?

Respond ONLY with JSON:
{{
  "niche_description": "...",
  "audience_type": "identity" or "interest" or "mixed",
  "community_signals": ["list", "of", "signals"],
  "monetization_sophistication": "high" / "medium" / "low",
  "event_evidence": true/false,
  "authenticity_score": 0.0-1.0,
  "overall_assessment": "1-2 sentences on group travel fit"
}}"""

                response = client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": analysis_prompt}],
                    response_format={"type": "json_object"},
                )
                analysis_result = json.loads(response.choices[0].message.content)

                # Attach results in same shape as IG for scoring compatibility
                profile['_creator_profile'] = {
                    'primary_category': analysis_result.get('niche_description', 'Unknown'),
                    'content_types': 'Patreon posts',
                    'creator_presence': 'text-based',
                    'audience_type': analysis_result.get('audience_type', 'unknown'),
                }
                profile['_analysis_result'] = analysis_result
                profile['_content_analyses'] = []  # No visual content analysis for Patreon
                profile['_bio_evidence'] = {
                    'niche_signals': {'niche_identified': True, 'niche_description': analysis_result.get('niche_description', '')},
                    'in_person_events': {'evidence_found': analysis_result.get('event_evidence', False), 'event_types': []},
                    'community_platforms': {'evidence_found': bool(analysis_result.get('community_signals')),
                                            'platforms': analysis_result.get('community_signals', [])},
                    'monetization': {'evidence_found': analysis_result.get('monetization_sophistication') != 'low',
                                     'types': ['patreon_tiers']},
                }
                profile['_caption_evidence'] = {
                    'in_person_events': {'mention_count': 1 if analysis_result.get('event_evidence') else 0},
                    'community_platforms': {'mention_count': len(analysis_result.get('community_signals', []))},
                    'audience_engagement': {'question_count': 0},
                    'authenticity_vulnerability': {'degree': analysis_result.get('authenticity_score', 0.5), 'post_count': 0},
                }
                profile['_thumbnail_evidence'] = {
                    'creator_visibility': {'visible_in_content': False, 'frequency': 'none', 'confidence': 0.0},
                    'niche_consistency': {'consistent_theme': True, 'niche_description': analysis_result.get('niche_description', ''), 'confidence': 0.7},
                    'event_promotion': {'evidence_found': analysis_result.get('event_evidence', False), 'post_count': 0, 'confidence': 0.5},
                    'engagement_metrics': {'posts_above_threshold': 0, 'posts_below_threshold': 0, 'posts_hidden': 0, 'posts_analyzed': 0},
                }

                analyzed.append(profile)
                run.increment_stage_progress('analysis', 'completed')
                logger.info("%s: niche_description=%s", creator_name, analysis_result.get('niche_description', '?'))

            except Exception as e:
                logger.error("Error on %s: %s", creator_name, e)
                errors.append(f"{creator_name}: {str(e)}")
                run.increment_stage_progress('analysis', 'failed')

        return StageResult(
            profiles=analyzed,
            processed=len(profiles),
            failed=len(errors),
            errors=errors,
            cost=len(profiles) * 0.10,
        )


class FacebookAnalysis(StageAdapter):
    """
    Facebook group analysis: group health + admin profile signals.
    Evaluates whether the group and its admin are a good fit for hosting trips.
    """
    platform = 'facebook'
    stage = 'analysis'
    description = 'GPT-4o text analysis — group health, admin profile'
    apis = ['OpenAI']

    def estimate_cost(self, count: int) -> float:
        return count * 0.10

    def run(self, profiles, run) -> StageResult:
        analyzed = []
        errors = []

        for profile in profiles:
            group_name = profile.get('group_name', 'Unknown Group')

            try:
                description = profile.get('description', '')
                member_count = profile.get('member_count', 0)
                posts_per_month = profile.get('posts_per_month')
                admin_name = profile.get('creator_name', '')
                admin_email = profile.get('email', '')
                admin_website = profile.get('personal_website', '')

                analysis_prompt = f"""Analyze this Facebook Group for group travel host potential (TrovaTrip).

Group: {group_name}
Members: {member_count}
Posts/month: {posts_per_month or 'unknown'}
Description: {description[:1500]}
Admin name: {admin_name or 'unknown'}
Admin email: {admin_email or 'none found'}
Admin website: {admin_website or 'none found'}

Evaluate for:
1. NICHE: What is this group about? Is the community identity-based?
2. ENGAGEMENT: Does the group appear active? (posts/month, member count)
3. TRAVEL FIT: Could this community be interested in group travel?
4. ADMIN PROFILE: Is the admin identifiable and likely to be a host?
5. COMMUNITY HEALTH: Signs of healthy community vs spam/inactive?

Respond ONLY with JSON:
{{
  "niche_description": "...",
  "community_health": "healthy" / "moderate" / "low",
  "travel_relevance": "high" / "medium" / "low",
  "admin_identifiable": true/false,
  "engagement_level": "high" / "medium" / "low",
  "overall_assessment": "1-2 sentences on group travel fit"
}}"""

                response = client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": analysis_prompt}],
                    response_format={"type": "json_object"},
                )
                analysis_result = json.loads(response.choices[0].message.content)

                profile['_creator_profile'] = {
                    'primary_category': analysis_result.get('niche_description', 'Facebook Group'),
                    'content_types': 'Group posts',
                    'creator_presence': 'group admin',
                    'community_health': analysis_result.get('community_health', 'unknown'),
                }
                profile['_analysis_result'] = analysis_result
                profile['_content_analyses'] = []
                profile['_bio_evidence'] = {
                    'niche_signals': {'niche_identified': True, 'niche_description': analysis_result.get('niche_description', '')},
                    'in_person_events': {'evidence_found': False, 'event_types': []},
                    'community_platforms': {'evidence_found': True, 'platforms': ['facebook_group']},
                    'monetization': {'evidence_found': False, 'types': []},
                }
                profile['_caption_evidence'] = {
                    'in_person_events': {'mention_count': 0},
                    'community_platforms': {'mention_count': 1},
                    'audience_engagement': {'question_count': 0},
                    'authenticity_vulnerability': {'degree': 0.3, 'post_count': 0},
                }
                profile['_thumbnail_evidence'] = {
                    'creator_visibility': {'visible_in_content': False, 'frequency': 'none', 'confidence': 0.0},
                    'niche_consistency': {'consistent_theme': True, 'niche_description': analysis_result.get('niche_description', ''), 'confidence': 0.6},
                    'event_promotion': {'evidence_found': False, 'post_count': 0, 'confidence': 0.0},
                    'engagement_metrics': {'posts_above_threshold': 0, 'posts_below_threshold': 0, 'posts_hidden': 0, 'posts_analyzed': 0},
                }

                analyzed.append(profile)
                run.increment_stage_progress('analysis', 'completed')
                logger.info("%s: niche_description=%s", group_name, analysis_result.get('niche_description', '?'))

            except Exception as e:
                logger.error("Error on %s: %s", group_name, e)
                errors.append(f"{group_name}: {str(e)}")
                run.increment_stage_progress('analysis', 'failed')

        return StageResult(
            profiles=analyzed,
            processed=len(profiles),
            failed=len(errors),
            errors=errors,
            cost=len(profiles) * 0.10,
        )


# ── Adapter registry ─────────────────────────────────────────────────────────

ADAPTERS: Dict[str, type] = {
    'instagram': InstagramAnalysis,
    'patreon': PatreonAnalysis,
    'facebook': FacebookAnalysis,
}
