"""
OpenAI API helpers — GPT-4.1 vision, Whisper transcription, with retry logic.
"""
import json
import logging
import os
import tempfile
import time
import requests
from typing import Dict, List, Any

from app.extensions import openai_client as client

logger = logging.getLogger('services.openai')


def _chat_completion(**kwargs):
    """Route chat completion through the OpenAI circuit breaker."""
    from app.services.circuit_breaker import get_breaker
    cb = get_breaker('openai')
    return cb.call(client.chat.completions.create, **kwargs)


def analyze_content_item(media_url: str, media_format: str) -> Dict[str, Any]:
    """Analyze a single content item — focus on POV, authenticity, vulnerability, engagement."""
    if media_format == 'IMAGE':
        response = _chat_completion(
            model="gpt-4.1",
            messages=[{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": """Analyze this social media image for group travel host potential.

Focus on:
1. NICHE/THEME: What specific content category/theme?
2. CREATOR POV/PERSPECTIVE: Does the creator show their unique perspective, personality, or opinion (not just expertise)?
3. AUTHENTICITY: Does creator share personal details about themselves?
4. VULNERABILITY: Does creator show challenges, failures, or vulnerability?
5. ENGAGEMENT FACILITATION: Does the content invite discourse or community connection?
6. IN-PERSON EVENTS: Any visual signs of events, classes, retreats, trips?

Respond in JSON:
{
  "summary": "3-4 sentence summary covering theme, creator's POV/personality, authenticity, and engagement",
  "niche_theme": "specific category",
  "shows_pov": true/false,
  "shows_authenticity": true/false,
  "shows_vulnerability": true/false,
  "facilitates_engagement": true/false,
  "event_promotion": true/false
}"""
                }, {
                    "type": "image_url",
                    "image_url": {"url": media_url}
                }]
            }],
            response_format={"type": "json_object"},
        )
        result = json.loads(response.choices[0].message.content)
        return {"type": "IMAGE", "url": media_url, **result}

    else:  # VIDEO
        transcript = transcribe_video_with_whisper(media_url)
        response = _chat_completion(
            model="gpt-4.1",
            messages=[{
                "role": "user",
                "content": f"""Analyze this video transcript for group travel host potential.

TRANSCRIPTION: {transcript}

Focus on:
1. NICHE/THEME: What specific content category/theme?
2. CREATOR POV/PERSPECTIVE: Does the creator show their unique perspective, personality, or opinion (not just expertise)?
3. AUTHENTICITY: Does creator share personal details about themselves?
4. VULNERABILITY: Does creator show challenges, failures, or vulnerability?
5. ENGAGEMENT FACILITATION: Does the content invite discourse or community connection?
6. IN-PERSON EVENTS: Any mentions of events, classes, retreats, trips?

Respond in JSON:
{{
  "summary": "3-4 sentence summary covering theme, creator's POV/personality, authenticity, and engagement",
  "niche_theme": "specific category",
  "shows_pov": true/false,
  "shows_authenticity": true/false,
  "shows_vulnerability": true/false,
  "facilitates_engagement": true/false,
  "event_promotion": true/false
}}"""
            }],
            response_format={"type": "json_object"},
        )
        result = json.loads(response.choices[0].message.content)
        return {"type": "VIDEO", "url": media_url, **result}


def transcribe_video_with_whisper(video_url: str, max_retries: int = 3) -> str:
    """Transcribe video using Whisper with retry logic for rate limits."""
    for attempt in range(max_retries):
        try:
            video_response = requests.get(video_url, timeout=30)
            video_response.raise_for_status()

            with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as temp_video:
                temp_video.write(video_response.content)
                temp_video_path = temp_video.name

            try:
                with open(temp_video_path, 'rb') as audio_file:
                    transcript = client.audio.transcriptions.create(
                        model="whisper-1", file=audio_file,
                    )
                return transcript.text
            finally:
                os.unlink(temp_video_path)

        except Exception as e:
            error_str = str(e).lower()
            is_rate_limit = 'rate_limit' in error_str or '429' in error_str or 'rate limit' in error_str
            if is_rate_limit and attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10
                logger.warning("Whisper rate limit hit, waiting %ds (attempt %d/%d)", wait_time, attempt + 1, max_retries)
                time.sleep(wait_time)
            else:
                if attempt == max_retries - 1:
                    logger.error("Whisper failed after %d attempts: %s", max_retries, e)
                raise


def analyze_bio_evidence(bio: str) -> Dict[str, Any]:
    """Extract structured evidence from Instagram bio."""
    if not bio or len(bio.strip()) < 10:
        return {
            "niche_signals": {"niche_identified": False, "niche_description": "", "confidence": 0.0},
            "in_person_events": {"evidence_found": False, "event_types": [], "confidence": 0.0},
            "community_platforms": {"evidence_found": False, "platforms": [], "confidence": 0.0},
            "monetization": {"evidence_found": False, "types": [], "confidence": 0.0},
        }

    response = _chat_completion(
        model="gpt-4.1",
        messages=[{
            "role": "user",
            "content": f"""Analyze this Instagram bio for group travel host potential.

BIO: {bio}

Extract evidence for:

1. NICHE IDENTITY: Does the creator clearly identify their niche/content focus?
2. IN-PERSON EVENTS: Signs of hosting in-person gatherings?
3. COMMUNITY PLATFORMS: Owned communication channels?
4. MONETIZATION: Signs of selling products/services?

Respond ONLY with JSON:
{{
  "niche_signals": {{
    "niche_identified": true/false,
    "niche_description": "Brief description of niche/focus",
    "confidence": 0.0-1.0
  }},
  "in_person_events": {{
    "evidence_found": true/false,
    "event_types": ["list", "of", "event", "types"],
    "confidence": 0.0-1.0
  }},
  "community_platforms": {{
    "evidence_found": true/false,
    "platforms": ["list", "of", "platforms"],
    "confidence": 0.0-1.0
  }},
  "monetization": {{
    "evidence_found": true/false,
    "types": ["list", "of", "monetization", "types"],
    "confidence": 0.0-1.0
  }}
}}"""
        }],
        response_format={"type": "json_object"},
    )
    result = json.loads(response.choices[0].message.content)
    logger.debug("Bio Evidence: %s", json.dumps(result, indent=2))
    return result


def analyze_caption_evidence(captions: List[str]) -> Dict[str, Any]:
    """Extract structured evidence from Instagram captions (up to 12 posts)."""
    if not captions:
        return {
            "in_person_events": {"evidence_found": False, "mention_count": 0, "confidence": 0.0},
            "community_platforms": {"evidence_found": False, "mention_count": 0, "confidence": 0.0},
            "audience_engagement": {"asks_questions": False, "question_count": 0, "confidence": 0.0},
            "authenticity_vulnerability": {"shares_personal_details": False, "shows_vulnerability": False, "degree": 0.0, "post_count": 0},
        }

    truncated_captions = [cap[:500] if cap else "" for cap in captions]
    captions_text = "\n\n---\n\n".join([f"CAPTION {i+1}: {cap}" for i, cap in enumerate(truncated_captions) if cap])

    response = _chat_completion(
        model="gpt-4.1",
        messages=[{
            "role": "user",
            "content": f"""Analyze these Instagram captions for group travel host potential.

{captions_text}

Extract evidence for:

1. IN-PERSON EVENTS: Mentions of classes, workshops, coaching, retreats, trips, tours?
2. COMMUNITY PLATFORMS: Mentions of private groups, Discord, podcast, email list?
3. AUDIENCE ENGAGEMENT: Does creator ask questions to their audience?
4. AUTHENTICITY & VULNERABILITY: Does creator share personal details or show vulnerability?

Respond ONLY with JSON:
{{
  "in_person_events": {{
    "evidence_found": true/false,
    "mention_count": 0-12,
    "confidence": 0.0-1.0
  }},
  "community_platforms": {{
    "evidence_found": true/false,
    "mention_count": 0-12,
    "confidence": 0.0-1.0
  }},
  "audience_engagement": {{
    "asks_questions": true/false,
    "question_count": 0-12,
    "confidence": 0.0-1.0
  }},
  "authenticity_vulnerability": {{
    "shares_personal_details": true/false,
    "shows_vulnerability": true/false,
    "degree": 0.0-1.0,
    "post_count": 0-12
  }}
}}"""
        }],
        response_format={"type": "json_object"},
    )
    result = json.loads(response.choices[0].message.content)
    logger.debug("Caption Evidence: %s", json.dumps(result, indent=2))
    return result


def generate_creator_profile(content_analyses: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate creator profile from content summaries."""
    summaries = []
    for idx, item in enumerate(content_analyses, 1):
        summary_text = f"Content {idx} ({item['type']}): {item['summary']}"
        if item.get('description'):
            summary_text += f"\nOriginal: {item['description']}"
        summaries.append(summary_text)

    combined = "\n\n".join(summaries)

    response = _chat_completion(
        model="gpt-4.1",
        messages=[{
            "role": "system",
            "content": """You analyze creators to profile their content strategy, audience engagement, and monetization.

Additionally, classify the creator into ONE primary category:
- Empowerment: Personal development, coaching, motivation, empowerment
- Entertainment: Performing arts, comedy, music, entertainment
- Fitness & sport: Fitness, yoga, pilates, dance, sports (non-competitive focus)
- Health & wellness: Mental health, wellness, spirituality, holistic health
- Learning: Education, history, book clubs, teaching, academic content
- Lifestyle: General lifestyle, fashion, beauty, home, parenting (non-family-travel)
- Art & Design: Visual art, design, photography, creative arts
- Exploration: Travel, adventure travel, cultural exploration
- Food & Drink: Food, cooking, culinary, wine, restaurants, nutrition
- Outdoor & Adventure: Hiking, camping, van life, outdoor activities

Choose the SINGLE category that best represents the creator's primary content focus."""
        }, {
            "role": "user",
            "content": f"""Create structured creator profile covering: content category, content types, audience engagement, creator presence, monetization, community building.

CONTENT: {combined}

Return JSON with these fields:
- content_category: Brief description of content themes
- primary_category: ONE category from the list above
- content_types: Types of content they create
- audience_engagement: How they engage with audience
- creator_presence: On-screen presence and personality
- monetization: Evidence of monetization or business mindset
- community_building: Community infrastructure and engagement"""
        }],
        response_format={"type": "json_object"},
    )

    result = json.loads(response.choices[0].message.content)
    if 'primary_category' not in result:
        result['primary_category'] = 'unknown'
    logger.debug("Creator Profile: %s", json.dumps(result, indent=2))
    return result


import re

# Prefixes that are not first names — used by deterministic Layer 1
_NON_NAME_PREFIXES = {
    'the', 'dr', 'dr.', 'coach', 'dj', 'mc', 'pastor', 'rev', 'rev.',
    'chef', 'prof', 'prof.', 'sir', 'lady', 'king', 'queen', 'prince',
    'princess', 'captain', 'capt', 'capt.', 'mr', 'mr.', 'mrs', 'mrs.',
    'ms', 'ms.', 'miss',
}

# Couple signal patterns
_COUPLE_PATTERN = re.compile(r'\s+and\s+|\s*&\s*', re.IGNORECASE)


def _deterministic_first_name(full_name: str) -> str | None:
    """Layer 1: Extract first name deterministically from full_name.

    Returns the first name if it's obviously extractable, or None to
    fall through to GPT.
    """
    if not full_name or not full_name.strip():
        return None

    # Strip emoji and non-ASCII decorators
    cleaned = re.sub(r'[^\w\s&.\'-]', '', full_name, flags=re.UNICODE).strip()
    if not cleaned:
        return None

    # Detect couple signals — defer to GPT
    if _COUPLE_PATTERN.search(cleaned):
        return None

    parts = cleaned.split()
    if not parts:
        return None

    first_word = parts[0]

    # Skip non-name prefixes — defer to GPT for these
    if first_word.lower() in _NON_NAME_PREFIXES:
        return None

    # Must be at least 2 alpha chars and purely alphabetic
    if len(first_word) < 2 or not first_word.isalpha():
        return None

    return first_word.capitalize()


def extract_first_names_from_instagram_profile(
    username: str, full_name: str, bio: str,
    content_analyses: List[Dict] = None,
) -> str:
    """Extract first name(s) from Instagram profile.

    3-layer approach:
      Layer 1 (deterministic): Clean first word from full_name if unambiguous.
      Layer 2 (GPT-4o-mini): Existing logic for complex cases.
      Layer 3 (fallback): Short alpha username or "there".
    """
    def _full_name_fallback() -> str:
        if full_name and full_name.strip():
            first = full_name.strip().split(' ')[0]
            return first if first else "there"
        return "there"

    def _username_fallback() -> str:
        """Layer 3: Short alpha username → capitalize. Otherwise → 'there'."""
        if username and username.isalpha() and 2 <= len(username) <= 12:
            return username.capitalize()
        return "there"

    if not username and not full_name:
        return "there"

    # Layer 1: deterministic extraction
    deterministic = _deterministic_first_name(full_name)
    if deterministic:
        logger.debug("@%s -> '%s' (deterministic)", username, deterministic)
        return deterministic

    # Layer 2: GPT extraction
    if not client:
        return _username_fallback() if not full_name else _full_name_fallback()

    content_context = ""
    if content_analyses:
        captions = []
        for item in content_analyses[:5]:
            summary = item.get('summary', '')
            caption = item.get('caption', '')
            if summary:
                captions.append(summary[:200])
            elif caption:
                captions.append(caption[:200])
        if captions:
            content_context = "\n".join(captions)

    prompt = f"""Extract the first name(s) from this Instagram profile.

Profile Information:
- Username: @{username}
- Full Name Field: {full_name if full_name else 'Not provided'}
- Bio: {bio[:300] if bio else 'Not provided'}

{('Recent Content Context:' + chr(10) + content_context[:800]) if content_context else ''}

Rules:
1. Single person → "John"; Couple → "John and Jane"; Group → "John, Jane, and Bill"
2. Capitalize properly. For couples: no comma before "and". For 3+: comma before final "and"
3. Check username, full name, bio, content for name clues
4. If brand/company, return brand name. If unknown, return: there

Return ONLY the name(s). No quotes, no explanation."""

    try:
        response = _chat_completion(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "You are a precise data extraction assistant. Return only the requested format with no additional text."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            max_tokens=50,
        )
        first_names = response.choices[0].message.content.strip().strip('"').strip("'")
        if not first_names or first_names.lower() in ('', 'none', 'unknown', 'n/a', 'not provided', 'there'):
            first_names = _username_fallback()
        logger.debug("@%s -> '%s' (gpt)", username, first_names)
        return first_names
    except Exception as e:
        logger.error("Error extracting first name for @%s: %s", username, e)
        return _username_fallback() if not full_name else _full_name_fallback()
