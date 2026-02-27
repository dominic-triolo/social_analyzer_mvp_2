"""
Pipeline Stage 2: PRE-SCREEN — Quick disqualification.

Instagram: post frequency check + GPT-4o vision snapshot.
Patreon:   NSFW filter + patron count + post count.
Facebook:  Member count + visibility + posts/month.
"""
import json
import logging
import base64
from io import BytesIO
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Tuple

import requests
from PIL import Image, ImageDraw, ImageFont

from app.extensions import openai_client as client
from app.pipeline.base import StageAdapter, StageResult

logger = logging.getLogger('pipeline.prescreen')


# ── Shared helpers (used by IG adapter + enrichment task) ─────────────────────

def check_post_frequency(content_items: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """
    Check if profile should be disqualified based on post frequency.
    Returns: (should_disqualify, reason)
    """
    non_pinned = [item for item in content_items if not item.get('is_pinned', False)]
    if not non_pinned:
        return True, "No non-pinned posts found"

    try:
        dates = []
        for item in non_pinned:
            pub_date_str = item.get('published_at')
            if pub_date_str:
                pub_date = datetime.fromisoformat(pub_date_str.replace('Z', '+00:00'))
                dates.append(pub_date)

        if not dates:
            return True, "No valid publish dates found"

        dates.sort(reverse=True)
        current_date = datetime.now(timezone.utc)
        six_weeks = timedelta(weeks=6)

        most_recent = dates[0]
        if current_date - most_recent > six_weeks:
            days_ago = (current_date - most_recent).days
            return True, f"Most recent post is {days_ago} days old (>6 weeks)"

        for i in range(len(dates) - 1):
            gap = dates[i] - dates[i + 1]
            if gap > six_weeks:
                return True, f"Gap of {gap.days} days between posts (>6 weeks)"

        logger.info("Post frequency check passed: %d posts, most recent %d days ago", len(dates), (current_date - most_recent).days)
        return False, ""

    except Exception as e:
        logger.error("Error checking post frequency: %s", e)
        return True, f"Error parsing dates: {str(e)}"


def create_profile_snapshot(profile_data: Dict[str, Any], content_items: List[Dict[str, Any]]) -> Image.Image:
    """Create a visual snapshot of the profile with bio and content thumbnails."""
    width, height = 1200, 1600
    img = Image.new('RGB', (width, height), 'white')
    draw = ImageDraw.Draw(img)

    try:
        font_header = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 32)
        font_bio = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 18)
        font_stats = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 16)
    except Exception:
        font_header = ImageFont.load_default()
        font_bio = ImageFont.load_default()
        font_stats = ImageFont.load_default()

    y_offset = 40
    username = profile_data.get('username', 'Unknown')
    draw.text((40, y_offset), f"@{username}", font=font_header, fill='black')
    y_offset += 50

    follower_count = profile_data.get('follower_count', 'N/A')
    if isinstance(follower_count, (int, float)) and follower_count > 0:
        stats = f"Followers: {int(follower_count):,}"
    elif follower_count != 'N/A':
        stats = f"Followers: {follower_count}"
    else:
        stats = "Follower count not available"
    draw.text((40, y_offset), stats, font=font_stats, fill='gray')
    y_offset += 40

    bio = profile_data.get('bio', 'No bio available')
    max_width = width - 80
    words = bio.split()
    lines, current_line = [], []
    for word in words:
        test_line = ' '.join(current_line + [word])
        if len(test_line) * 10 < max_width:
            current_line.append(word)
        else:
            if current_line:
                lines.append(' '.join(current_line))
            current_line = [word]
    if current_line:
        lines.append(' '.join(current_line))

    for line in lines[:4]:
        draw.text((40, y_offset), line, font=font_bio, fill='black')
        y_offset += 25
    if len(lines) > 4:
        draw.text((40, y_offset), "...", font=font_bio, fill='gray')
        y_offset += 25

    y_offset += 40
    thumb_size, spacing = 200, 20
    for idx, item in enumerate(content_items[:10]):
        row = idx // 5
        col = idx % 5
        x = 40 + col * (thumb_size + spacing)
        y = y_offset + row * (thumb_size + spacing)
        try:
            thumb_url = item.get('thumbnail_url')
            if thumb_url:
                response = requests.get(thumb_url, timeout=5)
                thumb = Image.open(BytesIO(response.content))
                thumb = thumb.resize((thumb_size, thumb_size), Image.Resampling.LANCZOS)
                img.paste(thumb, (x, y))
            else:
                draw.rectangle([x, y, x+thumb_size, y+thumb_size], outline='lightgray', width=2, fill='#f0f0f0')
                draw.text((x + thumb_size//2 - 20, y + thumb_size//2), "No Image", font=font_stats, fill='gray')
        except Exception as e:
            logger.error("Error loading thumbnail %d: %s", idx, e)
            draw.rectangle([x, y, x+thumb_size, y+thumb_size], outline='red', width=2, fill='#ffe0e0')
            draw.text((x + thumb_size//2 - 15, y + thumb_size//2), "Error", font=font_stats, fill='red')

    return img


def check_for_travel_experience(bio: str, content_items: List[Dict[str, Any]]) -> bool:
    """Check if creator has hosted or is marketing group travel experiences."""
    travel_keywords = [
        'retreat', 'workshop', 'trip', 'tour', 'travel', 'getaway',
        'join me', 'join us', 'book now', 'spaces available', 'registration open',
        'destination', 'experience', 'journey', 'expedition',
        'hosted', 'hosting', 'trips',
    ]

    bio_lower = bio.lower()
    if any(keyword in bio_lower for keyword in travel_keywords):
        logger.info("Travel indicators found in bio: %s...", bio[:100])
        return True

    for item in content_items[:10]:
        description = item.get('description', '').lower()
        title = item.get('title', '').lower()
        combined_text = f"{description} {title}"
        if any(keyword in combined_text for keyword in travel_keywords):
            booking_indicators = ['sign up', 'register', 'book', 'join', 'spots', 'spaces', 'limited', 'reserve']
            if any(indicator in combined_text for indicator in booking_indicators):
                return True

    return False


def pre_screen_profile(snapshot_image: Image.Image, profile_data: Dict[str, Any]) -> Dict[str, Any]:
    """Pre-screen profile using snapshot to identify obvious bad fits."""
    buffered = BytesIO()
    snapshot_image.save(buffered, format="PNG")
    img_base64 = base64.b64encode(buffered.getvalue()).decode()

    username = profile_data.get('username', 'Unknown')

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{
            "role": "system",
            "content": """You are a pre-screener for TrovaTrip, a group travel platform. Based on profile snapshots, quickly identify obvious BAD FITS to save processing time.

ONLY DISQUALIFY when you have HIGH CONFIDENCE the profile is an unsupported type or focuses on unsupported activities.

UNSUPPORTED ACTIVITIES (disqualify if PRIMARY focus):
- Golf, biking, motorcycles, driving/cars/racing
- Competitive sports: football, soccer, basketball, hockey, etc.
- Snowsports: skiing, snowboarding, figure skating
- Watersports: surfing, kitesurfing, scuba diving (as primary/athlete focus)
- Hunting
- Family travel content

SUPPORTED ACTIVITIES (do NOT disqualify):
- Dance (including pole dance), yoga, barre, pilates
- General fitness, running
- Camping, hiking, backpacking
- Van/bus/RV life
- Food/beverage, nutrition, vegetarianism, veganism, pescatarianism
- Mental health, spirituality, positivity / empowerment
- Art and design (visual art, interior design, etc)
- Literature/books (book clubs)
- Learning (history, art, etc)
- Professional coaches, personal coaches
- Watersports as casual activity (not athlete/competitive focus)
- Podcasts (ALWAYS pass to next stage)

CRITICAL: If a niche or activity is NOT listed in UNSUPPORTED ACTIVITIES or UNSUPPORTED PROFILE TYPES, DO NOT REJECT IT.

UNSUPPORTED PROFILE TYPES (disqualify if HIGH CONFIDENCE):
- Brand accounts (no personal creator)
- Meme accounts / content aggregators
- Accounts that only repost content
- Explicit or offensive content
- Content focused on firearms
- News/media brand accounts
- Creator appears under age 18
- Non-English speaking creator

CONTENT SELECTION (if passing to next stage):
Select the 3 pieces of content (by index 0-9) that are MOST REPRESENTATIVE.

Respond ONLY with JSON:
{
  "decision": "reject" or "continue",
  "reasoning": "1-2 sentences explaining why",
  "selected_content_indices": [0, 3, 7]
}"""
        }, {
            "role": "user",
            "content": [
                {"type": "text", "text": f"Profile: @{username}\n\nShould we continue analyzing this profile? If yes, which 3 pieces of content (by grid position 0-9, top-left to bottom-right) should we analyze?"},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_base64}", "detail": "high"}}
            ]
        }],
        response_format={"type": "json_object"},
        max_tokens=500,
    )

    result = json.loads(response.choices[0].message.content)
    logger.debug("Pre-screen result: %s", result)
    return result


# ── Adapters ──────────────────────────────────────────────────────────────────

class InstagramPrescreen(StageAdapter):
    """
    IG pre-screen: fetch content → post frequency check → GPT-4o snapshot.

    NOTE: This adapter fetches content from InsightIQ because pre-screening
    needs the content items. The fetched content is attached to each profile
    dict as '_content_items' for downstream stages to reuse.
    """
    platform = 'instagram'
    stage = 'pre_screen'
    description = 'Post frequency check + GPT-4o content scan'
    apis = ['InsightIQ', 'OpenAI']

    def estimate_cost(self, count: int) -> float:
        return count * 0.05

    def run(self, profiles, run) -> StageResult:
        from app.services.insightiq import fetch_social_content, filter_content_items

        passed = []
        errors = []
        skipped = 0

        for i, profile in enumerate(profiles):
            profile_url = profile.get('profile_url') or profile.get('instagram_handle') or profile.get('url', '')
            bio = profile.get('bio', '')
            follower_count = profile.get('follower_count', 0)

            try:
                # Fetch content
                social_data = fetch_social_content(profile_url)
                content_items = social_data.get('data', [])
                if not content_items:
                    skipped += 1
                    continue

                filtered_items = filter_content_items(content_items)
                if not filtered_items:
                    skipped += 1
                    continue

                # Post frequency check
                should_disqualify, reason = check_post_frequency(filtered_items)
                if should_disqualify:
                    logger.info("%s: DISQUALIFIED - %s", profile_url, reason)
                    profile['_prescreen_result'] = 'disqualified'
                    profile['_prescreen_reason'] = reason
                    profile['_prescreen_score'] = 0.15
                    skipped += 1
                    continue

                # GPT-4o snapshot screen
                profile_info = social_data.get('data', [{}])[0].get('profile', {})
                profile_data = {
                    'username': profile_info.get('platform_username', 'Unknown'),
                    'bio': bio or 'Bio not provided',
                    'follower_count': follower_count or profile_info.get('follower_count', 'N/A'),
                    'image_url': profile_info.get('image_url', ''),
                }
                snapshot = create_profile_snapshot(profile_data, filtered_items)
                screen_result = pre_screen_profile(snapshot, profile_data)

                if screen_result.get('decision') == 'reject':
                    logger.info("%s: REJECTED - %s", profile_url, screen_result.get('reasoning'))
                    profile['_prescreen_result'] = 'rejected'
                    profile['_prescreen_reason'] = screen_result.get('reasoning', '')
                    profile['_prescreen_score'] = 0.20
                    skipped += 1
                    continue

                # Passed — attach content for downstream stages
                profile['_content_items'] = filtered_items
                profile['_social_data'] = social_data
                profile['_selected_indices'] = screen_result.get('selected_content_indices', [0, 1, 2])
                profile['_profile_data'] = profile_data
                profile['_has_travel_experience'] = check_for_travel_experience(bio, filtered_items)
                passed.append(profile)

                run.increment_stage_progress('pre_screen', 'completed')

            except Exception as e:
                logger.error("Error on %s: %s", profile_url, e)
                errors.append(f"{profile_url}: {str(e)}")
                run.increment_stage_progress('pre_screen', 'failed')

        return StageResult(
            profiles=passed,
            processed=len(profiles),
            failed=len(errors),
            skipped=skipped,
            errors=errors,
            cost=len(profiles) * 0.05,
        )


class PatreonPrescreen(StageAdapter):
    """Patreon pre-screen: NSFW filter + patron count + post count."""
    platform = 'patreon'
    stage = 'pre_screen'
    description = 'NSFW filter + patron count + post count'
    apis = []

    def estimate_cost(self, count: int) -> float:
        return 0.0  # No API calls

    def run(self, profiles, run) -> StageResult:
        filters = run.filters or {}
        min_patrons = int(filters.get('min_patrons') or 0)
        max_patrons = int(filters.get('max_patrons') or 0)
        min_posts = int(filters.get('min_posts') or 0)

        passed = []
        skipped = 0

        for p in profiles:
            # NSFW filter
            if p.get('is_nsfw', 0) == 1:
                skipped += 1
                continue

            # Patron count
            patron_count = int(p.get('patron_count') or p.get('total_members') or 0)
            if min_patrons > 0 and patron_count < min_patrons:
                skipped += 1
                continue
            if max_patrons > 0 and patron_count > max_patrons:
                skipped += 1
                continue

            # Post count
            post_count = int(p.get('post_count') or p.get('total_posts') or p.get('posts_count') or 0)
            if min_posts > 0 and post_count < min_posts:
                skipped += 1
                continue

            passed.append(p)

        logger.info("%d/%d passed (skipped %d)", len(passed), len(profiles), skipped)

        return StageResult(
            profiles=passed,
            processed=len(profiles),
            skipped=skipped,
        )


class FacebookPrescreen(StageAdapter):
    """Facebook pre-screen: member count + visibility + posts/month."""
    platform = 'facebook'
    stage = 'pre_screen'
    description = 'Member count + visibility + posts/month'
    apis = []

    def estimate_cost(self, count: int) -> float:
        return 0.0  # No API calls

    def run(self, profiles, run) -> StageResult:
        filters = run.filters or {}
        min_members = int(filters.get('min_members') or 0)
        max_members = int(filters.get('max_members') or 0)
        visibility = filters.get('visibility', 'all')
        min_posts_per_month = int(filters.get('min_posts_per_month') or 0)

        passed = []
        skipped = 0

        for p in profiles:
            mc = p.get('member_count', 0)
            if min_members > 0 and 0 < mc < min_members:
                skipped += 1
                continue
            if max_members > 0 and mc > max_members:
                skipped += 1
                continue

            combined = (p.get('_search_title', '') + ' ' + p.get('_search_snippet', '')).lower()
            if visibility == 'public' and 'private group' in combined:
                skipped += 1
                continue
            if visibility == 'private' and 'public group' in combined:
                skipped += 1
                continue

            ppm = p.get('posts_per_month')
            if min_posts_per_month > 0 and ppm is not None and ppm < min_posts_per_month:
                skipped += 1
                continue

            passed.append(p)

        logger.info("%d/%d passed (skipped %d)", len(passed), len(profiles), skipped)

        return StageResult(
            profiles=passed,
            processed=len(profiles),
            skipped=skipped,
        )


# ── Adapter registry ─────────────────────────────────────────────────────────

ADAPTERS: Dict[str, type] = {
    'instagram': InstagramPrescreen,
    'patreon': PatreonPrescreen,
    'facebook': FacebookPrescreen,
}
