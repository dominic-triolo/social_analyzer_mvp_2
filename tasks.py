import os
import json
import requests
import tempfile
import base64
import hashlib
import time
from typing import Dict, List, Any, Tuple
from datetime import datetime, timedelta
import boto3
from botocore.client import Config
from celery_app import celery_app
from openai import OpenAI
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# Configuration from environment variables
INSIGHTIQ_USERNAME = os.getenv('INSIGHTIQ_USERNAME')
INSIGHTIQ_PASSWORD = os.getenv('INSIGHTIQ_PASSWORD')
INSIGHTIQ_WORK_PLATFORM_ID = os.getenv('INSIGHTIQ_WORK_PLATFORM_ID')
INSIGHTIQ_API_URL = os.getenv('INSIGHTIQ_API_URL', 'https://api.staging.insightiq.ai')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
HUBSPOT_WEBHOOK_URL = os.getenv('HUBSPOT_WEBHOOK_URL')
INSIGHTIQ_CLIENT_ID = os.getenv('INSIGHTIQ_CLIENT_ID')
INSIGHTIQ_SECRET = os.getenv('INSIGHTIQ_SECRET')
HUBSPOT_API_KEY = os.getenv('HUBSPOT_API_KEY')
APIFY_API_TOKEN = os.getenv('APIFY_API_TOKEN')
APOLLO_API_KEY = os.getenv('APOLLO_API_KEY')

# R2 Configuration
R2_ACCESS_KEY_ID = os.getenv('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.getenv('R2_SECRET_ACCESS_KEY')
R2_BUCKET_NAME = os.getenv('R2_BUCKET_NAME')
R2_ENDPOINT_URL = os.getenv('R2_ENDPOINT_URL')
R2_PUBLIC_URL = os.getenv('R2_PUBLIC_URL')

# Initialize R2 client
r2_client = None
if R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY and R2_ENDPOINT_URL:
    try:
        r2_client = boto3.client(
            's3',
            endpoint_url=R2_ENDPOINT_URL,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            config=Config(signature_version='s3v4'),
            region_name='auto'
        )
        print("R2 client initialized successfully")
    except Exception as e:
        print(f"ERROR initializing R2 client: {e}")

# Initialize OpenAI client
client = None
if OPENAI_API_KEY:
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        print("OpenAI client initialized")
    except Exception as e:
        print(f"ERROR initializing OpenAI client: {e}")


# Load category-specific examples for scoring
CATEGORY_EXAMPLES = None

def load_category_examples():
    """Load category-specific good/bad fit examples"""
    global CATEGORY_EXAMPLES
    if CATEGORY_EXAMPLES is None:
        examples_path = os.path.join(os.path.dirname(__file__), 'category_examples.json')
        try:
            with open(examples_path, 'r') as f:
                CATEGORY_EXAMPLES = json.load(f)
            print("✓ Category examples loaded")
        except Exception as e:
            print(f"⚠ Could not load category examples: {e}")
            CATEGORY_EXAMPLES = {}
    return CATEGORY_EXAMPLES


def format_category_examples(category: str) -> str:
    """Format category-specific examples for prompt inclusion"""
    examples = load_category_examples()
    
    if category not in examples:
        return ""
    
    cat_examples = examples[category]
    good_fits = cat_examples.get('good_fits', [])
    bad_fits = cat_examples.get('bad_fits', [])
    
    # Format good fit examples
    good_text = f"\n{'='*70}\nGOOD FIT EXAMPLES for {category}:\n{'='*70}\n"
    for idx, ex in enumerate(good_fits, 1):
        good_text += f"\n{idx}. @{ex['handle']}"
        if ex.get('niche'):
            good_text += f" - {ex['niche']}"
        if ex.get('why'):
            # Clean up the "why" text
            why_clean = ex['why'].replace('- ', '').strip()
            good_text += f"\n   Why good fit: {why_clean}"
        if ex.get('trip_concept'):
            good_text += f"\n   Trip concept: {ex['trip_concept']}"
        good_text += "\n"
    
    # Format bad fit examples
    bad_text = f"\n{'='*70}\nBAD FIT EXAMPLES for {category}:\n{'='*70}\n"
    for idx, ex in enumerate(bad_fits, 1):
        bad_text += f"\n{idx}. @{ex['handle']}"
        if ex.get('niche'):
            bad_text += f" - {ex['niche']}"
        if ex.get('why'):
            # Clean up the "why" text
            why_clean = ex['why'].replace('- ', '').strip()
            bad_text += f"\n   Why bad fit: {why_clean}"
        bad_text += "\n"
    
    # Add category-specific patterns
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


def save_analysis_cache(contact_id: str, cache_data: dict) -> bool:
    """Save analysis results to R2 for later re-scoring"""
    if not r2_client:
        print("R2 client not available, skipping cache")
        return False
    
    try:
        key = f"analysis-cache/{contact_id}.json"
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=json.dumps(cache_data, indent=2),
            ContentType='application/json'
        )
        print(f"Analysis cached to R2: {key}")
        return True
    except Exception as e:
        print(f"Error caching analysis: {e}")
        return False


def load_analysis_cache(contact_id: str) -> dict:
    """Load cached analysis results from R2"""
    if not r2_client:
        raise Exception("R2 client not available")
    
    try:
        key = f"analysis-cache/{contact_id}.json"
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        cache_data = json.loads(obj['Body'].read())
        print(f"Analysis loaded from cache: {key}")
        return cache_data
    except Exception as e:
        print(f"Error loading cache: {e}")
        raise


def fetch_social_content(profile_url: str) -> Dict[str, Any]:
    """Fetch content from InsightIQ API"""
    url = f"{INSIGHTIQ_API_URL}/v1/social/creators/contents/fetch"
    
    credentials = f"{INSIGHTIQ_USERNAME}:{INSIGHTIQ_PASSWORD}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    payload = {
        "profile_url": profile_url,
        "work_platform_id": INSIGHTIQ_WORK_PLATFORM_ID
    }
    
    # Enhanced logging
    print(f"InsightIQ Request URL: {url}")
    print(f"Profile URL: {profile_url}")
    print(f"Work Platform ID: {INSIGHTIQ_WORK_PLATFORM_ID}")
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        print(f"InsightIQ Response Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"ERROR Response Body: {response.text}")
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"InsightIQ API Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response Status: {e.response.status_code}")
            print(f"Response Body: {e.response.text}")
        raise


def filter_content_items(content_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter out Stories from content items"""
    filtered = [item for item in content_items if item.get('type') != 'STORY']
    print(f"Filtered content: {len(content_items)} total → {len(filtered)} after removing Stories")
    return filtered


def check_post_frequency(content_items: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """
    Check if profile should be disqualified based on post frequency
    Returns: (should_disqualify, reason)
    """
    # Filter out pinned posts for frequency check
    non_pinned = [item for item in content_items if not item.get('is_pinned', False)]
    
    if not non_pinned:
        return True, "No non-pinned posts found"
    
    # Parse published dates
    try:
        dates = []
        for item in non_pinned:
            pub_date_str = item.get('published_at')
            if pub_date_str:
                # Parse ISO format: "2026-01-27T17:51:42"
                pub_date = datetime.fromisoformat(pub_date_str.replace('Z', '+00:00'))
                dates.append(pub_date)
        
        if not dates:
            return True, "No valid publish dates found"
        
        # Sort dates (most recent first)
        dates.sort(reverse=True)
        
        current_date = datetime.now()
        six_weeks = timedelta(weeks=6)
        
        # Check 1: Most recent post is >6 weeks old
        most_recent = dates[0]
        if current_date - most_recent > six_weeks:
            days_ago = (current_date - most_recent).days
            return True, f"Most recent post is {days_ago} days old (>6 weeks)"
        
        # Check 2: Any gap between consecutive posts >6 weeks
        for i in range(len(dates) - 1):
            gap = dates[i] - dates[i + 1]
            if gap > six_weeks:
                gap_days = gap.days
                return True, f"Gap of {gap_days} days between posts (>6 weeks)"
        
        print(f"Post frequency check passed: {len(dates)} posts, most recent {(current_date - most_recent).days} days ago")
        return False, ""
        
    except Exception as e:
        print(f"Error checking post frequency: {e}")
        return True, f"Error parsing dates: {str(e)}"


def create_profile_snapshot(profile_data: Dict[str, Any], content_items: List[Dict[str, Any]]) -> Image.Image:
    """
    Create a visual snapshot of the profile with bio and content thumbnails
    
    Args:
        profile_data: {username, bio, follower_count, following_count, image_url}
        content_items: List of content items with thumbnail_url
    """
    # Canvas dimensions
    width = 1200
    height = 1600
    
    # Create white canvas
    img = Image.new('RGB', (width, height), 'white')
    draw = ImageDraw.Draw(img)
    
    # Try to use default font (will work in most Linux environments)
    try:
        font_header = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 32)
        font_bio = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 18)
        font_stats = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 16)
    except:
        # Fallback to default font
        font_header = ImageFont.load_default()
        font_bio = ImageFont.load_default()
        font_stats = ImageFont.load_default()
    
    y_offset = 40
    
    # Draw username
    username = profile_data.get('username', 'Unknown')
    draw.text((40, y_offset), f"@{username}", font=font_header, fill='black')
    y_offset += 50
    
    # Draw follower count (if available)
    follower_count = profile_data.get('follower_count', 'N/A')
    
    # Format follower count
    if isinstance(follower_count, (int, float)) and follower_count > 0:
        stats = f"Followers: {int(follower_count):,}"
    elif follower_count != 'N/A':
        stats = f"Followers: {follower_count}"
    else:
        stats = "Follower count not available"
    
    draw.text((40, y_offset), stats, font=font_stats, fill='gray')
    y_offset += 40
    
    # Draw bio
    bio = profile_data.get('bio', 'No bio available')
    # Simple word wrap
    max_width = width - 80
    words = bio.split()
    lines = []
    current_line = []
    
    for word in words:
        test_line = ' '.join(current_line + [word])
        # Rough estimation of text width
        if len(test_line) * 10 < max_width:  # ~10 pixels per char
            current_line.append(word)
        else:
            if current_line:
                lines.append(' '.join(current_line))
            current_line = [word]
    
    if current_line:
        lines.append(' '.join(current_line))
    
    # Limit bio to 4 lines
    for line in lines[:4]:
        draw.text((40, y_offset), line, font=font_bio, fill='black')
        y_offset += 25
    
    if len(lines) > 4:
        draw.text((40, y_offset), "...", font=font_bio, fill='gray')
        y_offset += 25
    
    # Draw content thumbnails (2 rows of 5)
    y_offset += 40
    thumb_size = 200
    spacing = 20
    
    for idx, item in enumerate(content_items[:10]):
        row = idx // 5
        col = idx % 5
        
        x = 40 + col * (thumb_size + spacing)
        y = y_offset + row * (thumb_size + spacing)
        
        # Download and paste thumbnail
        try:
            thumb_url = item.get('thumbnail_url')
            if thumb_url:
                response = requests.get(thumb_url, timeout=5)
                thumb = Image.open(BytesIO(response.content))
                thumb = thumb.resize((thumb_size, thumb_size), Image.Resampling.LANCZOS)
                img.paste(thumb, (x, y))
            else:
                # Draw placeholder
                draw.rectangle([x, y, x+thumb_size, y+thumb_size], outline='lightgray', width=2, fill='#f0f0f0')
                draw.text((x + thumb_size//2 - 20, y + thumb_size//2), "No Image", font=font_stats, fill='gray')
        except Exception as e:
            print(f"Error loading thumbnail {idx}: {e}")
            # Draw placeholder on error
            draw.rectangle([x, y, x+thumb_size, y+thumb_size], outline='red', width=2, fill='#ffe0e0')
            draw.text((x + thumb_size//2 - 15, y + thumb_size//2), "Error", font=font_stats, fill='red')
    
    return img


def check_for_travel_experience(bio: str, content_items: List[Dict[str, Any]]) -> bool:
    """
    Check if creator has hosted or is marketing group travel experiences
    Returns True if travel experience indicators are found
    """
    # Keywords that indicate group travel hosting
    travel_keywords = [
        'retreat', 'workshop', 'trip', 'tour', 'travel', 'getaway',
        'join me', 'join us', 'book now', 'spaces available', 'registration open',
        'destination', 'experience', 'journey', 'expedition',
        'hosted', 'hosting', 'trips'
    ]
    
    # Check bio for travel indicators
    bio_lower = bio.lower()
    bio_has_travel = any(keyword in bio_lower for keyword in travel_keywords)
    
    if bio_has_travel:
        print(f"Travel indicators found in bio: {bio[:100]}...")
        return True
    
    # Check content descriptions for travel indicators
    for item in content_items[:10]:  # Check up to 10 items
        description = item.get('description', '').lower()
        title = item.get('title', '').lower()
        
        combined_text = f"{description} {title}"
        
        # Look for strong indicators of hosted travel
        if any(keyword in combined_text for keyword in travel_keywords):
            # Extra validation: look for group/booking language
            booking_indicators = ['sign up', 'register', 'book', 'join', 'spots', 'spaces', 'limited', 'reserve']
            if any(indicator in combined_text for indicator in booking_indicators):
                print(f"Travel experience found in content: {title[:50] if title else description[:50]}...")
                return True
    
    return False


def pre_screen_profile(snapshot_image: Image.Image, profile_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pre-screen profile using snapshot to identify obvious bad fits
    Returns: {"decision": "reject"/"continue", "reasoning": "...", "selected_content_indices": [0,2,5]}
    """
    # Convert image to base64
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
- Family travel content (see detailed definition below)

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
- Podcasts (ALWAYS pass to next stage - NEVER reject podcasts)

CRITICAL: If a niche or activity is NOT listed in UNSUPPORTED ACTIVITIES or UNSUPPORTED PROFILE TYPES, DO NOT REJECT IT.
Examples of niches to PASS TO NEXT STAGE (not in unsupported list):
- Board games, card games, tabletop gaming
- Crafts, knitting, sewing, DIY
- Technology, coding, software
- Business, entrepreneurship, marketing
- Photography, videography
- Any hobby or interest not explicitly listed as unsupported

FAMILY TRAVEL CONTENT (disqualify):
Only disqualify if the PRIMARY content focus is family/kids. Look for:
- Account name includes "family", "kids", or children's names as main identity
- Bio centers around being a parent as primary identity (e.g., "Mom of 3", "Raising tiny humans", "Our family adventures")
- Content grid shows majority of posts feature children as the main subject
- Content appears to be family vlogs, parenting tips, or kid-focused activities

DO NOT DISQUALIFY if:
- Creator mentions being a parent but leads with their own interests (e.g., "Chef | Baker | Mom")
- Children appear occasionally but content focuses on creator's expertise/niche
- Couples/travel partners (without kids) even if bio mentions "husband", "wife", "partner"
- Adult content about food, travel, wellness, etc. where creator happens to be a parent

UNSUPPORTED PROFILE TYPES (disqualify if HIGH CONFIDENCE):
- Brand accounts (no personal creator) - company/restaurant/product accounts
- Meme accounts / content aggregators
- Accounts that only repost content (not original)
- Explicit or offensive content
- Content focused on firearms
- News/media brand accounts (even if hosted by a person)
- Creator appears under age 18
- Non-English speaking creator (primary language is not English)

PASS TO NEXT STAGE if:
- Is not an UNSUPPORTED PROFILE TYPE and does not show any UNSUPPORTED ACTIVITIES
- Personal creator sharing their expertise, lifestyle, or interests
- Podcast creator (ALWAYS pass podcasts regardless of topic)
- ANY niche or activity not explicitly listed in unsupported categories
- ANY uncertainty about whether to disqualify (be permissive, not restrictive)

CONTENT SELECTION (if passing to next stage):
Select the 3 pieces of content (by index 0-9) that are MOST REPRESENTATIVE of the profile and best for deeper analysis. Choose content that:
- Shows the creator's personality and style
- Demonstrates their niche/expertise
- Shows face-forward engagement (if available)
- Avoid purely aesthetic/sponsored content if possible

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
        max_tokens=500
    )
    
    result = json.loads(response.choices[0].message.content)
    print(f"Pre-screen result: {result}")
    return result


def rehost_media_on_r2(media_url: str, contact_id: str, media_format: str) -> str:
    """Download media from Instagram CDN and upload to R2"""
    if not r2_client:
        return media_url
    
    try:
        # Download with shorter timeout and retry
        max_retries = 2
        for attempt in range(max_retries):
            try:
                media_response = requests.get(media_url, timeout=15)
                media_response.raise_for_status()
                break
            except requests.exceptions.Timeout:
                if attempt == max_retries - 1:
                    print(f"Media download timed out after {max_retries} attempts, using original URL")
                    return media_url
                print(f"Download timeout, retrying... (attempt {attempt + 1}/{max_retries})")
        
        url_hash = hashlib.md5(media_url.encode()).hexdigest()
        extension = 'mp4' if media_format == 'VIDEO' else 'jpg'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        object_key = f"social_content/{contact_id}/{timestamp}_{url_hash}.{extension}"
        
        content_type = 'video/mp4' if media_format == 'VIDEO' else 'image/jpeg'
        
        # Upload to R2 with timeout
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=object_key,
            Body=media_response.content,
            ContentType=content_type
        )
        
        rehosted_url = f"{R2_PUBLIC_URL}/{object_key}"
        print(f"Successfully re-hosted to R2: {object_key}")
        return rehosted_url
        
    except Exception as e:
        print(f"ERROR re-hosting media: {e}")
        print("Falling back to original URL")
        return media_url


def transcribe_video_with_whisper(video_url: str, max_retries: int = 3) -> str:
    """
    Transcribe video using Whisper with retry logic for rate limits
    Handles Whisper's 50 RPM limit gracefully with exponential backoff
    """
    import time
    
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
                        model="whisper-1",
                        file=audio_file
                    )
                return transcript.text
            finally:
                os.unlink(temp_video_path)
                
        except Exception as e:
            error_str = str(e).lower()
            is_rate_limit = 'rate_limit' in error_str or '429' in error_str or 'rate limit' in error_str
            
            if is_rate_limit and attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10  # 10s, 20s, 30s
                print(f"⚠️  Whisper rate limit hit, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                if attempt == max_retries - 1:
                    print(f"❌ Whisper failed after {max_retries} attempts: {e}")
                raise




def analyze_content_item(media_url: str, media_format: str) -> Dict[str, Any]:
    """Analyze a single content item - focus on POV, authenticity, vulnerability, engagement"""
    
    if media_format == 'IMAGE':
        response = client.chat.completions.create(
            model="gpt-4o",
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
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        return {"type": "IMAGE", "url": media_url, **result}
    
    else:  # VIDEO
        transcript = transcribe_video_with_whisper(media_url)
        
        response = client.chat.completions.create(
            model="gpt-4o",
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
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        return {"type": "VIDEO", "url": media_url, **result}


def analyze_bio_evidence(bio: str) -> Dict[str, Any]:
    """
    Extract structured evidence from Instagram bio
    Focus: niche identity, in-person events, community platforms, monetization
    """
    if not bio or len(bio.strip()) < 10:
        return {
            "niche_signals": {"niche_identified": False, "niche_description": "", "confidence": 0.0},
            "in_person_events": {"evidence_found": False, "event_types": [], "confidence": 0.0},
            "community_platforms": {"evidence_found": False, "platforms": [], "confidence": 0.0},
            "monetization": {"evidence_found": False, "types": [], "confidence": 0.0}
        }
    
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{
            "role": "user",
            "content": f"""Analyze this Instagram bio for group travel host potential.

BIO: {bio}

Extract evidence for:

1. NICHE IDENTITY: Does the creator clearly identify their niche/content focus?
   - Look for: travel, food, recipes, wellness, yoga, fitness, art, design, etc.
   - Examples: "Food blogger", "Wellness coach", "Asian cuisine enthusiast"
   
2. IN-PERSON EVENTS: Signs of hosting in-person gatherings?
   - Look for: classes, workshops, coaching, retreats, trips, tours, meetups
   - These are HIGH VALUE signals for group travel potential
   
3. COMMUNITY PLATFORMS: Owned communication channels?
   - Look for: podcast, Discord, email list, newsletter, Patreon, private group, membership
   - Must be platforms where audience actively joins/subscribes
   
4. MONETIZATION: Signs of selling products/services?
   - Look for: courses, coaching, products, merch, services, brand deals
   - Does NOT include donations or "support me"

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
        response_format={"type": "json_object"}
    )
    
    result = json.loads(response.choices[0].message.content)
    print(f"Bio Evidence: {json.dumps(result, indent=2)}")
    return result


def analyze_caption_evidence(captions: List[str]) -> Dict[str, Any]:
    """
    Extract structured evidence from Instagram captions (up to 12 posts)
    Focus: in-person events, community channels, audience questions, authenticity, vulnerability
    """
    if not captions:
        return {
            "in_person_events": {"evidence_found": False, "mention_count": 0, "confidence": 0.0},
            "community_platforms": {"evidence_found": False, "mention_count": 0, "confidence": 0.0},
            "audience_engagement": {"asks_questions": False, "question_count": 0, "confidence": 0.0},
            "authenticity_vulnerability": {"shares_personal_details": False, "shows_vulnerability": False, "degree": 0.0, "post_count": 0}
        }
    
    # Truncate captions to first 500 chars
    truncated_captions = [cap[:500] if cap else "" for cap in captions]
    captions_text = "\n\n---\n\n".join([f"CAPTION {i+1}: {cap}" for i, cap in enumerate(truncated_captions) if cap])
    
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{
            "role": "user",
            "content": f"""Analyze these Instagram captions for group travel host potential.

{captions_text}

Extract evidence for:

1. IN-PERSON EVENTS: Mentions of classes, workshops, coaching, retreats, trips, tours?
   - Count how many captions mention these (HIGH VALUE signals)
   
2. COMMUNITY PLATFORMS: Mentions of private groups, Discord, podcast, email list?
   - Count how many captions advertise these
   
3. AUDIENCE ENGAGEMENT: Does creator ask questions to their audience?
   - Look for: "What do you think?", "Have you tried?", "Tell me about..."
   - Count how many captions include questions
   
4. AUTHENTICITY & VULNERABILITY: Does creator share personal details or show vulnerability?
   - Personal details: family, background, personal experiences, opinions
   - Vulnerability: challenges, failures, fears, growth
   - Rate DEGREE: How much do they open up across ALL captions? (0.0-1.0 scale)
   - Count how many captions show this

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
        response_format={"type": "json_object"}
    )
    
    result = json.loads(response.choices[0].message.content)
    print(f"Caption Evidence: {json.dumps(result, indent=2)}")
    return result


def create_thumbnail_grid(thumbnail_urls: List[str], contact_id: str) -> str:
    """
    Create a 3x4 grid image from up to 12 thumbnails and upload to R2
    Uses parallel downloads for 3-4x speed improvement
    Returns: R2 URL of the grid image
    """
    from PIL import Image
    import io
    import concurrent.futures
    
    def download_single_image(url):
        """Download and resize a single thumbnail"""
        try:
            response = requests.get(url, timeout=10)
            img = Image.open(io.BytesIO(response.content))
            # Resize to standard size (400x400)
            return img.resize((400, 400), Image.Resampling.LANCZOS)
        except Exception as e:
            print(f"Error loading thumbnail {url}: {e}")
            # Create blank placeholder
            return Image.new('RGB', (400, 400), color='gray')
    
    # Download images in parallel (4 at a time for optimal performance)
    images = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        images = list(executor.map(download_single_image, thumbnail_urls[:12]))
    
    # Pad to 12 if needed
    while len(images) < 12:
        images.append(Image.new('RGB', (400, 400), color='lightgray'))
    
    # Create 3x4 grid (3 columns, 4 rows)
    grid_width = 400 * 3
    grid_height = 400 * 4
    grid = Image.new('RGB', (grid_width, grid_height))
    
    for idx, img in enumerate(images[:12]):
        col = idx % 3
        row = idx // 3
        x = col * 400
        y = row * 400
        grid.paste(img, (x, y))
    
    # Upload to R2
    buffer = io.BytesIO()
    grid.save(buffer, format='JPEG', quality=85)
    buffer.seek(0)
    
    key = f"thumbnail-grids/{contact_id}.jpg"
    r2_client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=key,
        Body=buffer.getvalue(),
        ContentType='image/jpeg'
    )
    
    # Use R2_PUBLIC_URL environment variable
    grid_url = f"{R2_PUBLIC_URL}/{key}"
    print(f"Thumbnail grid created: {grid_url}")
    return grid_url


def analyze_thumbnail_evidence(thumbnail_urls: List[str], engagement_data: List[Dict], contact_id: str) -> Dict[str, Any]:
    """
    Extract structured evidence from thumbnail grid (up to 12 posts)
    Focus: creator visibility, niche consistency, engagement metrics, event promotion
    """
    if not thumbnail_urls:
        return {
            "creator_visibility": {"visible_in_content": False, "frequency": "none", "confidence": 0.0},
            "niche_consistency": {"consistent_theme": False, "niche_description": "", "confidence": 0.0},
            "event_promotion": {"evidence_found": False, "post_count": 0, "confidence": 0.0},
            "engagement_metrics": {"posts_above_threshold": 0, "posts_below_threshold": 0, "posts_hidden": 0}
        }
    
    # Create grid image
    grid_url = create_thumbnail_grid(thumbnail_urls, contact_id)
    
    # Calculate engagement metrics
    posts_above = 0
    posts_below = 0
    posts_hidden = 0
    
    for data in engagement_data:
        if data.get('is_pinned', False):
            continue  # Skip pinned posts
        
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
   - Look for: face, full body, recognizable person
   - Frequency: "most" (8+ posts), "some" (4-7 posts), "rare" (1-3 posts), "none"
   
2. NICHE CONSISTENCY: Do thumbnails show a consistent content theme/category?
   - Look for: repeated visual patterns, consistent subject matter
   - Describe the niche if identifiable
   
3. EVENT PROMOTION: Visual signs of in-person events?
   - Look for: text overlays mentioning events, retreat/trip photos, class/workshop imagery
   - Count how many posts show this
   
4. AUDIENCE ENGAGEMENT CUES: Do visible text overlays suggest engagement?
   - Look for text like: "do X with me", "come try X", "going to X", question marks
   - Any calls to action or invitations visible?

Respond ONLY with JSON:
{
  "creator_visibility": {
    "visible_in_content": true/false,
    "frequency": "most/some/rare/none",
    "confidence": 0.0-1.0
  },
  "niche_consistency": {
    "consistent_theme": true/false,
    "niche_description": "Brief description of visual theme",
    "confidence": 0.0-1.0
  },
  "event_promotion": {
    "evidence_found": true/false,
    "post_count": 0-12,
    "confidence": 0.0-1.0
  },
  "audience_engagement_cues": {
    "invitational_language": true/false,
    "post_count": 0-12,
    "confidence": 0.0-1.0
  }
}"""
            }, {
                "type": "image_url",
                "image_url": {"url": grid_url}
            }]
        }],
        response_format={"type": "json_object"}
    )
    
    result = json.loads(response.choices[0].message.content)
    
    # Add engagement metrics
    result['engagement_metrics'] = {
        "posts_above_threshold": posts_above,
        "posts_below_threshold": posts_below,
        "posts_hidden": posts_hidden,
        "posts_analyzed": len(engagement_data)
    }
    
    print(f"Thumbnail Evidence: {json.dumps(result, indent=2)}")
    return result


def generate_creator_profile(content_analyses: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate creator profile from content summaries"""
    summaries = []
    for idx, item in enumerate(content_analyses, 1):
        summary_text = f"Content {idx} ({item['type']}): {item['summary']}"
        if item.get('description'):
            summary_text += f"\nOriginal: {item['description']}"
        summaries.append(summary_text)
    
    combined = "\n\n".join(summaries)
    
    response = client.chat.completions.create(
        model="gpt-4o",
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
- primary_category: ONE category from the list above (e.g., "Food & Drink", "Exploration", "Empowerment")
- content_types: Types of content they create
- audience_engagement: How they engage with audience
- creator_presence: On-screen presence and personality
- monetization: Evidence of monetization or business mindset
- community_building: Community infrastructure and engagement

Example format:
{{
  "content_category": "Wellness and mindfulness content",
  "primary_category": "Health & wellness",
  "content_types": "Educational videos, meditation guides",
  "audience_engagement": "High engagement through comments",
  "creator_presence": "Calm and authentic on-camera presence",
  "monetization": "Offers paid courses and memberships",
  "community_building": "Active Discord community and email list"
}}"""
        }],
        response_format={"type": "json_object"}
    )
    
    result = json.loads(response.choices[0].message.content)
    
    # Ensure primary_category is present (fallback to unknown if not provided)
    if 'primary_category' not in result:
        result['primary_category'] = 'unknown'
        print("Warning: primary_category not provided by AI, defaulting to 'unknown'")
    
    print(f"Creator Profile: {json.dumps(result, indent=2)}")
    return result


def calculate_engagement_penalties(content_analyses: List[Dict]) -> Dict[str, float]:
    """
    Calculate engagement-based penalties from content analyses
    
    Penalizes profiles for:
    1. Posts with hidden engagement (likes_and_views_disabled: true)
    2. Posts with low engagement (<150 likes AND <10 comments)
    
    Excludes pinned posts from analysis.
    
    Returns:
        {
            'hidden_engagement_penalty': float,
            'low_engagement_penalty': float,
            'hidden_count': int,
            'low_engagement_count': int,
            'total_penalty': float
        }
    """
    hidden_engagement_posts = []
    low_engagement_posts = []
    
    for post in content_analyses:
        # Skip pinned posts (evergreen content)
        if post.get('is_pinned', False):
            continue
        
        # Check for hidden engagement
        if post.get('likes_and_views_disabled', False):
            hidden_engagement_posts.append(post)
            continue  # Don't double-count
        
        # Check for low engagement (only if engagement is visible)
        engagement = post.get('engagement', {})
        like_count = engagement.get('like_count', 0) or 0  # Handle None
        comment_count = engagement.get('comment_count', 0) or 0  # Handle None
        
        # Both conditions must be true for penalty
        if like_count < 150 and comment_count < 10:
            low_engagement_posts.append(post)
    
    # Calculate penalties with caps
    hidden_penalty = len(hidden_engagement_posts) * 0.05
    hidden_penalty = min(hidden_penalty, 0.15)  # Cap at 3 posts
    
    low_engagement_penalty = len(low_engagement_posts) * 0.03
    low_engagement_penalty = min(low_engagement_penalty, 0.15)  # Cap at 5 posts
    
    total_penalty = min(hidden_penalty + low_engagement_penalty, 0.20)  # Overall cap
    
    return {
        'hidden_engagement_penalty': hidden_penalty,
        'low_engagement_penalty': low_engagement_penalty,
        'hidden_count': len(hidden_engagement_posts),
        'low_engagement_count': len(low_engagement_posts),
        'total_penalty': total_penalty
    }


# New evidence-based scoring function to replace generate_lead_score

def generate_evidence_based_score(
    bio_evidence: Dict[str, Any],
    caption_evidence: Dict[str, Any],
    thumbnail_evidence: Dict[str, Any],
    content_analyses: List[Dict[str, Any]],
    creator_profile: Dict[str, Any],
    follower_count: int
) -> Dict[str, Any]:
    """
    Generate TrovaTrip lead score using evidence-based approach - v3.0
    
    Uses structured evidence from multiple sources to calculate 5 section scores,
    then applies follower boost, engagement adjustments, and category penalty.
    """
    
    # Get primary category and examples
    primary_category = creator_profile.get('primary_category', 'unknown')
    category_examples_text = format_category_examples(primary_category)
    
    # Prepare content summaries
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
    
    # Build evidence summary for GPT
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
    
    # Call GPT for holistic scoring
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

BAD FITS:
- Pure performers with fan bases (not communities)
- Religious fundamentalists with bigoted content
- Comedians/musicians who ONLY post performance content
- No monetization or business mindset
- No clear niche or community identity

SECTION DEFINITIONS:

1. **niche_and_audience_identity** (0.0-1.0)
   Weight evidence:
   - Niche clearly identified across bio/thumbnails/content: 40%
   - Niche consistency (same theme repeated): 20%
   - Specific sub-category vs generic: 20%
   - Audience identity implied by niche: 20%

2. **creator_authenticity_and_presence** (0.0-1.0)
   Weight evidence:
   - Creator visible in content (frequency matters): 30%
   - Shares personal details (count mentions): 25%
   - Shows vulnerability (degree scale): 25%
   - POV/perspective evident: 20%

3. **monetization_and_business_mindset** (0.0-1.0)
   Weight evidence:
   - Monetization (products, services, courses): 60%
   - **IN-PERSON EVENTS (HEAVILY WEIGHTED)**: 40%

4. **community_infrastructure** (0.0-1.0)
   Weight evidence:
   - Owned channels (podcast, email, Patreon, Discord): 50%
   - **IN-PERSON EVENTS/COMMUNITY**: 30%
   - Community platform mentions: 20%

5. **engagement_and_connection** (0.0-1.0)
   Weight evidence:
   - Asks questions / prompts discourse (frequency): 30%
   - Degree of audience connection facilitated: 40%
   - Content facilitates engagement: 30%

For each section, assess TO WHAT DEGREE the creator demonstrates these qualities.
Use the FREQUENCY and CONFIDENCE of signals across ALL evidence sources.

RESPOND ONLY with JSON (no preamble):
{{
  "niche_and_audience_identity": 0.0-1.0,
  "creator_authenticity_and_presence": 0.0-1.0,
  "monetization_and_business_mindset": 0.0-1.0,
  "community_infrastructure": 0.0-1.0,
  "engagement_and_connection": 0.0-1.0,
  "score_reasoning": "2-3 sentences explaining fit for group travel. Reference evidence frequency and confidence."
}}"""
        }, {
            "role": "user",
            "content": evidence_summary
        }],
        response_format={"type": "json_object"}
    )
    
    result = json.loads(response.choices[0].message.content)
    print(f"Evidence-Based Score Response: {json.dumps(result, indent=2)}")
    
    # Extract section scores
    niche = result.get('niche_and_audience_identity', 0.0)
    authenticity = result.get('creator_authenticity_and_presence', 0.0)
    monetization = result.get('monetization_and_business_mindset', 0.0)
    community = result.get('community_infrastructure', 0.0)
    engagement = result.get('engagement_and_connection', 0.0)
    
    # Calculate MANUAL score (optimized weights, no adjustments)
    # Based on analysis: authenticity is strongest separator (+0.126)
    manual_score = (
        (niche * 0.30) +
        (authenticity * 0.30) +  # Increased from 0.25 (strongest separator)
        (monetization * 0.20) +
        (community * 0.15) +
        (engagement * 0.05)      # Decreased from 0.10
    )
    
    # Apply entertainment penalty to manual score
    category_penalty = -0.10 if primary_category == "Entertainment" else 0.0  # Doubled from -0.05
    manual_score_with_penalty = manual_score + category_penalty
    manual_score_with_penalty = max(0.0, min(1.0, manual_score_with_penalty))
    
    # Calculate adjustments for FULL score
    # Follower count boost (TIERED, not cumulative)
    if follower_count >= 100000:
        follower_boost = 0.15
    elif follower_count >= 75000:
        follower_boost = 0.10
    elif follower_count >= 50000:
        follower_boost = 0.05
    else:
        follower_boost = 0.0
    
    # Engagement metrics adjustment
    eng_metrics = thumbnail_evidence.get('engagement_metrics', {})
    posts_above = eng_metrics.get('posts_above_threshold', 0)
    posts_below = eng_metrics.get('posts_below_threshold', 0)
    posts_hidden = eng_metrics.get('posts_hidden', 0)
    
    engagement_adjustment = (
        (posts_above * 0.03) -   # Boost for high engagement
        (posts_below * 0.03) -   # Penalty for low engagement
        (posts_hidden * 0.05)    # Penalty for hidden engagement
    )
    engagement_adjustment = max(-0.20, min(0.20, engagement_adjustment))  # Cap at ±0.20
    
    # Calculate FULL score (manual + adjustments)
    full_score = manual_score_with_penalty + follower_boost + engagement_adjustment
    full_score = max(0.0, min(1.0, full_score))
    
    # Determine priority tier using two-tier logic
    if manual_score_with_penalty >= 0.65:
        priority_tier = "auto_enroll"
        expected_precision = 0.833
        tier_reasoning = "Manual score ≥0.65 (83% precision)"
    elif full_score >= 0.8:
        priority_tier = "auto_enroll"
        expected_precision = 0.705
        tier_reasoning = "Full score ≥0.80 (70% precision)"
    elif full_score >= 0.25:
        priority_tier = "standard_priority_review"
        expected_precision = 0.681
        tier_reasoning = "Full score ≥0.45 (68% precision)"
    else:
        priority_tier = "low_priority_review"
        expected_precision = 0.0
        tier_reasoning = "Below review thresholds"
    
    # Build detailed reasoning
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
    
    # Build section scores dict
    section_scores = {
        "niche_and_audience_identity": niche,
        "creator_authenticity_and_presence": authenticity,
        "monetization_and_business_mindset": monetization,
        "community_infrastructure": community,
        "engagement_and_connection": engagement
    }
    
    print(f"  Manual score: {manual_score:.3f}")
    print(f"  Category penalty: {category_penalty:+.3f}")
    print(f"  Manual + penalty: {manual_score_with_penalty:.3f}")
    print(f"  Follower boost: +{follower_boost:.3f}")
    print(f"  Engagement adjustment: {engagement_adjustment:+.3f}")
    print(f"  FULL SCORE: {full_score:.3f}")
    print(f"  PRIORITY TIER: {priority_tier} (expected precision: {expected_precision:.1%})")
    
    return {
        "section_scores": section_scores,
        "manual_score": manual_score_with_penalty,
        "lead_score": full_score,
        "follower_boost": follower_boost,
        "engagement_adjustment": engagement_adjustment,
        "category_penalty": category_penalty,
        "priority_tier": priority_tier,
        "expected_precision": expected_precision,
        "score_reasoning": score_reasoning
    }
def extract_first_names_from_instagram_profile(username: str, full_name: str, bio: str, 
                                               content_analyses: List[Dict] = None) -> str:
    """
    Use OpenAI to extract properly formatted first name(s) from Instagram profile
    
    Args:
        username: Instagram handle (e.g., "morgandrinkscoffee")
        full_name: Full name from profile (e.g., "Morgan Smith")
        bio: Biography text
        content_analyses: List of analyzed content (captions may mention names)
    
    Returns:
        str: Formatted first name(s)
            - Single person: "John"
            - Couple: "John and Jane"
            - 3+ people: "John, Jane, and Bill"
            - Fallback: "there"
    """
    if not client:
        print("OpenAI client not initialized, using fallback")
        return "there"
    
    # Quick validation
    if not username and not full_name:
        return "there"
    
    # Extract useful context from content analyses
    content_context = ""
    if content_analyses:
        # Get first few captions/summaries for context
        captions = []
        for item in content_analyses[:5]:  # Use first 5 pieces of content
            summary = item.get('summary', '')
            caption = item.get('caption', '')
            
            if summary:
                captions.append(summary[:200])  # First 200 chars
            elif caption:
                captions.append(caption[:200])
        
        if captions:
            content_context = "\n".join(captions)
    
    prompt = f"""Extract the first name(s) from this Instagram profile.

Profile Information:
- Username: @{username}
- Full Name Field: {full_name if full_name else 'Not provided'}
- Bio: {bio[:300] if bio else 'Not provided'}

{f'''Recent Content Context (may mention their name):
{content_context[:800]}
''' if content_context else ''}

Rules:
1. Determine if this is:
   - Single person → Return just their first name: "John"
   - Couple (2 people) → Return both first names: "John and Jane"
   - Group (3+ people) → Return all first names: "John, Jane, and Bill"

2. Formatting:
   - Use ONLY first names (not full names or last names)
   - Capitalize properly: "John" not "john" or "JOHN"
   - For couples: "Name and Name" (no comma before "and")
   - For 3+: "Name, Name, and Name" (comma before final "and")

3. How to find the name:
   - Check the username for clues (e.g., @morgandrinkscoffee → "Morgan")
   - Check the full name field
   - Check the bio for self-references
   - Check content summaries for how they refer to themselves
   - Look for patterns like "I'm [Name]" or "Hi, I'm [Name]"

4. Special cases:
   - If brand/company (no people), return the brand name
   - If you cannot determine, return exactly: there

5. Context clues:
   - Look for "we", "couple", "married", "partners" → likely 2 people
   - Look for "friends", "squad", "crew", "trio" → likely 3+ people
   - "&" or "and" in name field → likely 2+ people
   - Single name like "John Smith" → 1 person

Return ONLY the name(s). No quotes, no explanation, just the name(s) or "there".

Examples:
Input: @johnsmith, "John Smith", "Travel blogger", Content: "I'm John and I love..."
Output: John

Input: @morgandrinkscoffee, "", "", Content: "Hey it's Morgan here with another coffee review"
Output: Morgan

Input: @thejohnsons, "John & Sarah", "Married couple", Content: "We're exploring..."
Output: John and Sarah

Input: @travelsquad, "The Squad", "Mike, Lisa, Tom", Content: "The three of us went to..."
Output: Mike, Lisa, and Tom

Input: @nikefitness, "Nike Fitness", "Official account", Content: "New workout collection"
Output: Nike Fitness

Input: @randomuser123, "", "", Content: "Check out this cool thing"
Output: there
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a precise data extraction assistant. Return only the requested format with no additional text."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.3,
            max_tokens=50
        )
        
        first_names = response.choices[0].message.content.strip()
        
        # Remove any quotes that might have been added
        first_names = first_names.strip('"').strip("'")
        
        # If empty or just punctuation, use fallback
        if not first_names or first_names.lower() in ['', 'none', 'unknown', 'n/a', 'not provided']:
            first_names = "there"
        
        print(f"[FIRST_NAME] @{username} → '{first_names}'")
        
        return first_names
        
    except Exception as e:
        print(f"[FIRST_NAME] Error for @{username}: {e}")
        return "there"

def send_to_hubspot(contact_id: str, lead_score: float, section_scores: Dict, score_reasoning: str, 
                       creator_profile: Dict, content_analyses: List[Dict], lead_analysis: Dict = None,
                       first_name: str = "there"):
    """Send results to HubSpot with validation"""
    content_summaries = [f"Content {idx} ({item['type']}): {item['summary']}" 
                        for idx, item in enumerate(content_analyses, 1)]
    
    # Extract additional fields from lead_analysis if provided
    manual_score = lead_analysis.get('manual_score', 0.0) if lead_analysis else 0.0
    follower_boost = lead_analysis.get('follower_boost', 0.0) if lead_analysis else 0.0
    engagement_adjustment = lead_analysis.get('engagement_adjustment', 0.0) if lead_analysis else 0.0
    category_penalty = lead_analysis.get('category_penalty', 0.0) if lead_analysis else 0.0
    priority_tier = lead_analysis.get('priority_tier', '') if lead_analysis else ''
    expected_precision = lead_analysis.get('expected_precision', 0.0) if lead_analysis else 0.0
    
    # Helper function to safely convert values to strings
    def safe_str(value):
        if value is None:
            return ''
        if isinstance(value, list):
            str_items = [str(item) for item in value if item is not None]
            return ', '.join(str_items)
        if isinstance(value, dict):
            return json.dumps(value)
        return str(value)
    
    # Handle community_building as either string or list
    community_building = creator_profile.get('community_building', '')
    if isinstance(community_building, list):
        community_text = ' '.join(str(item) for item in community_building if item).lower()
    else:
        community_text = str(community_building).lower()
    
    platforms = []
    for keyword, name in [('email', 'Email List'), ('patreon', 'Patreon'), 
                         ('discord', 'Discord'), ('substack', 'Substack')]:
        if keyword in community_text and name not in platforms:
            platforms.append(name)
    
    # VALIDATION: Check for enrichment success
    enrichment_status = "success"
    error_details = []
    
    if not content_analyses or len(content_analyses) == 0:
        enrichment_status = "error"
        error_details.append("No content analyzed")
    
    if not score_reasoning or len(score_reasoning) < 10:
        enrichment_status = "error"
        error_details.append("Missing or invalid score reasoning")
    
    if lead_score == 0.0 and all(score == 0.0 for score in section_scores.values()):
        enrichment_status = "warning"
        error_details.append("All scores are 0.0 - possible disqualification or error")
    
    if not creator_profile.get('content_category'):
        enrichment_status = "warning" if enrichment_status == "success" else "error"
        error_details.append("Missing content category")
    
    # Check for placeholder/error text in reasoning
    error_keywords = ['error', 'failed', 'could not', 'unable to', 'missing data', 'no content', 'unavailable']
    if any(keyword in score_reasoning.lower() for keyword in error_keywords):
        enrichment_status = "warning" if enrichment_status == "success" else enrichment_status
        error_details.append("Error indicators found in reasoning")
    
    # Track result type in Redis for dashboard stats
    try:
        import redis
        import time
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        r = redis.from_url(redis_url, decode_responses=True)
        
        # Determine result type
        result_type = 'enriched'  # Default
        if 'post frequency check' in score_reasoning.lower():
            result_type = 'post_frequency'
        elif 'pre-screen rejected' in score_reasoning.lower() or 'pre-screened' in score_reasoning.lower():
            result_type = 'pre_screened'
        elif enrichment_status == 'error':
            result_type = 'error'
        
        # Increment counter
        r.hincrby('trovastats:results', result_type, 1)
        
        # Track priority tier if enriched (from lead_analysis if available)
        if result_type == 'enriched' and lead_analysis:
            priority_tier = lead_analysis.get('priority_tier', 'unknown')
            r.hincrby('trovastats:priority_tiers', priority_tier, 1)
        
    except Exception as e:
        print(f"Error tracking stats in Redis: {e}")
    
    payload = {
        "contact_id": contact_id,
        "first_name": first_name,
        "lead_score": lead_score,
        "manual_score": manual_score,  # NEW: Score without follower/engagement adjustments
        "follower_boost_applied": follower_boost,  # NEW: Amount of follower boost
        "engagement_adjustment_applied": engagement_adjustment,  # NEW: Engagement adjustment
        "category_penalty_applied": category_penalty,  # NEW: Entertainment penalty
        "priority_tier": priority_tier,  # NEW: HIGH_PRIORITY, STANDARD_REVIEW, etc.
        "expected_precision": expected_precision,  # NEW: Expected precision for this tier
        "score_reasoning": score_reasoning,
        # Support both old (v2.1) and new (v3.0) section score names
        "score_niche_and_audience": section_scores.get('niche_and_audience_identity', 0.0),
        "score_host_likeability": section_scores.get('creator_authenticity_and_presence', 
                                                      section_scores.get('host_likeability_and_content_style', 0.0)),
        "score_monetization": section_scores.get('monetization_and_business_mindset', 0.0),
        "score_community_infrastructure": section_scores.get('community_infrastructure', 0.0),
        "score_trip_fit": section_scores.get('engagement_and_connection',
                                             section_scores.get('trip_fit_and_travelability', 0.0)),
        "content_summary_structured": "\n\n".join(content_summaries),
        "profile_category": safe_str(creator_profile.get('content_category')),
        "primary_category": safe_str(creator_profile.get('primary_category', 'unknown')),
        "profile_content_types": safe_str(creator_profile.get('content_types')),
        "profile_engagement": safe_str(creator_profile.get('audience_engagement')),
        "profile_presence": safe_str(creator_profile.get('creator_presence')),
        "profile_monetization": safe_str(creator_profile.get('monetization')),
        "profile_community_building": safe_str(community_building),
        "has_community_platform": len(platforms) > 0,
        "community_platforms_detected": ", ".join(platforms) if platforms else "None",
        "analyzed_at": datetime.now().isoformat(),
        "enrichment_status": enrichment_status,
        "enrichment_error_details": "; ".join(error_details) if error_details else "",
        "items_analyzed": len(content_analyses)
    }
    
    print(f"Sending to HubSpot: {HUBSPOT_WEBHOOK_URL}")
    print(f"Enrichment Status: {enrichment_status}")
    if error_details:
        print(f"Error Details: {'; '.join(error_details)}")
    
    response = requests.post(HUBSPOT_WEBHOOK_URL, json=payload, timeout=10)
    print(f"HubSpot response: {response.status_code}")


@celery_app.task(bind=True, name='tasks.process_creator_profile')
def process_creator_profile(self, contact_id: str, profile_url: str, bio: str = '', follower_count: int = 0):
    """Background task to process a creator profile with pre-screening"""
    import time
    import random
    
    # Track start time for performance metrics
    start_time = time.time()
    
    # Stagger processing to avoid OpenAI TPM bursts (V3.0 uses ~12K tokens per profile)
    # Increased from 1-3 to 3-5 seconds for high-volume production safety
    time.sleep(random.uniform(3, 5))
    
    try:
        print(f"=== PROCESSING: {contact_id} ===")
        if bio:
            print(f"Bio provided: {bio[:100]}...")
        if follower_count:
            print(f"Follower count: {follower_count:,}")
        
        # Step 1: Fetch content from InsightIQ
        self.update_state(state='PROGRESS', meta={'stage': 'Fetching content from InsightIQ'})
        social_data = fetch_social_content(profile_url)
        content_items = social_data.get('data', [])
        
        if not content_items:
            return {"status": "error", "message": "No content found"}
        
        print(f"Fetched {len(content_items)} content items from InsightIQ")
        
        # Step 2: Filter out Stories
        self.update_state(state='PROGRESS', meta={'stage': 'Filtering content'})
        filtered_items = filter_content_items(content_items)
        
        if not filtered_items:
            return {"status": "error", "message": "No content after filtering Stories"}
        
        # Step 3: Check post frequency (disqualify inactive/reactivated profiles)
        self.update_state(state='PROGRESS', meta={'stage': 'Checking post frequency'})
        should_disqualify, frequency_reason = check_post_frequency(filtered_items)
        
        if should_disqualify:
            print(f"DISQUALIFIED: {frequency_reason}")
            # Send low score to HubSpot with reason
            send_to_hubspot(
                contact_id,
                lead_score=0.15,
                section_scores={
                    'niche_and_audience_identity': 0.15,
                    'host_likeability_and_content_style': 0.15,
                    'monetization_and_business_mindset': 0.15,
                    'community_infrastructure': 0.15,
                    'trip_fit_and_travelability': 0.15
                },
                score_reasoning=f"Profile disqualified - post frequency check: {frequency_reason}",
                creator_profile={'content_category': 'Inactive/Low frequency'},
                content_analyses=[],
                first_name="there"
            )
            return {
                "status": "success",
                "contact_id": contact_id,
                "disqualified": True,
                "reason": frequency_reason,
                "lead_score": 0.15
            }
        
        # Step 4: Create profile snapshot
        self.update_state(state='PROGRESS', meta={'stage': 'Creating profile snapshot'})

        
        # Extract profile data for snapshot - use provided data first, fallback to InsightIQ
        profile_info = social_data.get('data', [{}])[0].get('profile', {})
        profile_data = {
            'username': profile_info.get('platform_username', 'Unknown'),
            'bio': bio if bio else 'Bio not provided',
            'follower_count': follower_count if follower_count else profile_info.get('follower_count', 'N/A'),
            'image_url': profile_info.get('image_url', '')
        }
        
        snapshot_image = create_profile_snapshot(profile_data, filtered_items)
        print("Profile snapshot created")
        
        # Step 5: Pre-screen with snapshot
        self.update_state(state='PROGRESS', meta={'stage': 'Pre-screening profile'})
        pre_screen_result = pre_screen_profile(snapshot_image, profile_data)
        
        if pre_screen_result.get('decision') == 'reject':
            print(f"PRE-SCREEN REJECTED: {pre_screen_result.get('reasoning')}")
            # Send low score to HubSpot
            send_to_hubspot(
                contact_id,
                lead_score=0.20,
                section_scores={
                    'niche_and_audience_identity': 0.20,
                    'creator_authenticity_and_presence': 0.20,
                    'monetization_and_business_mindset': 0.20,
                    'community_infrastructure': 0.20,
                    'engagement_and_connection': 0.20
                },
                score_reasoning=f"Pre-screen rejected: {pre_screen_result.get('reasoning')}",
                creator_profile={'content_category': 'Pre-screened out'},
                content_analyses=[],
                first_name="there"
            )
            return {
                "status": "success",
                "contact_id": contact_id,
                "pre_screen_rejected": True,
                "reason": pre_screen_result.get('reasoning'),
                "lead_score": 0.20
            }
        
        # Step 5.5: Check if creator has hosted group travel experiences
        # This ensures they get into manual review range even if other signals are weak
        self.update_state(state='PROGRESS', meta={'stage': 'Checking for travel experience'})
        has_travel_experience = check_for_travel_experience(bio, filtered_items)
        
        if has_travel_experience:
            print(f"TRAVEL EXPERIENCE DETECTED: Creator has hosted or is marketing group travel")
        
        # Step 6: Deep analysis of selected content
        self.update_state(state='PROGRESS', meta={'stage': 'Analyzing selected content'})
        
        selected_indices = pre_screen_result.get('selected_content_indices', [])
        print(f"Pre-screen passed. Selected content indices: {selected_indices}")
        
        # If no indices selected, fall back to first 3 items
        if not selected_indices:
            selected_indices = [0, 1, 2]
            print("No indices selected by pre-screen, using first 3 items")
        
        content_analyses = []
        
        for idx in selected_indices[:3]:  # Limit to 3
            if idx >= len(filtered_items):
                print(f"Index {idx} out of range, skipping")
                continue
            
            item = filtered_items[idx]
            print(f"Processing selected item at index {idx}")
            
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
                print(f"Item at index {idx}: No media URL, skipping")
                continue
            
            media_url = media_url.rstrip('.')
            
            # Check video file size
            if media_format == 'VIDEO':
                try:
                    head_response = requests.head(media_url, timeout=10)
                    content_length = int(head_response.headers.get('content-length', 0))
                    max_size = 25 * 1024 * 1024
                    
                    if content_length > max_size:
                        print(f"Item {idx}: Video too large ({content_length / 1024 / 1024:.1f}MB), skipping")
                        continue
                except Exception as e:
                    print(f"Item {idx}: Could not check video size: {e}, attempting anyway")
            
            try:
                rehosted_url = rehost_media_on_r2(media_url, contact_id, media_format)
                analysis = analyze_content_item(rehosted_url, media_format)
                analysis['description'] = item.get('description', '')
                
                # Add engagement metadata for penalty calculation
                analysis['is_pinned'] = item.get('is_pinned', False)
                analysis['likes_and_views_disabled'] = item.get('likes_and_views_disabled', False)
                analysis['engagement'] = item.get('engagement', {})
                
                content_analyses.append(analysis)
                print(f"Item {idx}: Successfully analyzed")
                
            except Exception as e:
                print(f"Item {idx}: Error analyzing: {e}")
                continue
        
        if not content_analyses:
            return {"status": "error", "message": "Could not analyze any selected content items"}
        
        print(f"Successfully analyzed {len(content_analyses)} items")
        
        # Step 6.5: Gather evidence from bio, captions, and thumbnails (all 12 posts)
        self.update_state(state='PROGRESS', meta={'stage': 'Gathering profile evidence'})
        
        # Extract thumbnail URLs and captions from all 12 posts
        thumbnail_urls = []
        captions = []
        engagement_data = []
        
        for item in filtered_items[:12]:  # Analyze up to 12 posts
            # Get thumbnail
            thumb_url = item.get('thumbnail_url')
            if thumb_url:
                thumbnail_urls.append(thumb_url)
            
            # Get caption (truncate to 500 chars)
            caption = item.get('description', '') or item.get('title', '')
            if caption:
                captions.append(caption[:500])
            
            # Get engagement metadata
            engagement_data.append({
                'is_pinned': item.get('is_pinned', False),
                'likes_and_views_disabled': item.get('likes_and_views_disabled', False),
                'engagement': item.get('engagement', {})
            })
        
        print(f"Gathering evidence from: {len(thumbnail_urls)} thumbnails, {len(captions)} captions")
        
        # Analyze bio
        bio_evidence = analyze_bio_evidence(bio)
        
        # Analyze captions
        caption_evidence = analyze_caption_evidence(captions)
        
        # Analyze thumbnail grid
        thumbnail_evidence = analyze_thumbnail_evidence(thumbnail_urls, engagement_data, contact_id)
        
        print("Evidence gathering complete")
        
        # Step 7: Generate creator profile
        self.update_state(state='PROGRESS', meta={'stage': 'Generating creator profile'})
        creator_profile = generate_creator_profile(content_analyses)
        
        # Step 7.5: Cache analysis results for future re-scoring
        cache_data = {
            'contact_id': contact_id,
            'profile_url': profile_url,
            'bio': bio,
            'follower_count': follower_count,
            'content_analyses': content_analyses,
            'creator_profile': creator_profile,
            'bio_evidence': bio_evidence,
            'caption_evidence': caption_evidence,
            'thumbnail_evidence': thumbnail_evidence,
            'has_travel_experience': has_travel_experience,
            'timestamp': datetime.now().isoformat(),
            'items_analyzed': len(content_analyses)
        }
        save_analysis_cache(contact_id, cache_data)
        
        # Step 8: Calculate lead score using evidence-based approach
        self.update_state(state='PROGRESS', meta={'stage': 'Calculating lead score'})
        lead_analysis = generate_evidence_based_score(
            bio_evidence=bio_evidence,
            caption_evidence=caption_evidence,
            thumbnail_evidence=thumbnail_evidence,
            content_analyses=content_analyses,
            creator_profile=creator_profile,
            follower_count=follower_count
        )
         # Extract first name AFTER content analysis (so we have content context)
        self.update_state(state='PROGRESS', meta={'stage': 'Extracting first name'})
        
        ig_username = social_data.get('profile', {}).get('username', '')
        ig_full_name = social_data.get('profile', {}).get('full_name', '')
        ig_bio = bio if bio else social_data.get('profile', {}).get('biography', '')
        
        # NOW we pass content_analyses to give more context
        first_name = extract_first_names_from_instagram_profile(
            ig_username, 
            ig_full_name, 
            ig_bio,
            content_analyses  # <-- IMPORTANT: Pass the analyzed content
        )
        print(f"[FIRST_NAME] Extracted: '{first_name}'")
        
        # Step 8.5: Boost score if travel experience detected
        # Ensures creators with group travel experience make it to manual review
        if has_travel_experience and lead_analysis['lead_score'] < 0.50:
            original_score = lead_analysis['lead_score']
            lead_analysis['lead_score'] = 0.50
            lead_analysis['score_reasoning'] = f"{lead_analysis.get('score_reasoning', '')} | TRAVEL EXPERIENCE BOOST: Creator has hosted or marketed group travel experiences (original score: {original_score:.2f}, boosted to 0.50 for manual review)"
            print(f"SCORE BOOSTED: {original_score:.2f} → 0.50 (travel experience detected)")
        
        # Step 9: Send to HubSpot
        self.update_state(state='PROGRESS', meta={'stage': 'Sending to HubSpot'})
        send_to_hubspot(
            contact_id,
            lead_analysis['lead_score'],
            lead_analysis.get('section_scores', {}),
            lead_analysis.get('score_reasoning', ''),
            creator_profile,
            content_analyses,
            lead_analysis,  # NEW: Pass full analysis for two-tier fields
            first_name=first_name
        )
        
        # Track processing duration
        duration = time.time() - start_time
        try:
            import redis
            redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
            r = redis.from_url(redis_url, decode_responses=True)
            # Add duration to list (keep last 100)
            r.lpush('trovastats:durations', int(duration))
            r.ltrim('trovastats:durations', 0, 99)
        except Exception as e:
            print(f"Error tracking duration: {e}")
        
        print(f"=== COMPLETE: {contact_id} - Score: {lead_analysis['lead_score']} - Duration: {duration:.1f}s ===")
        
        return {
            "status": "success",
            "contact_id": contact_id,
            "lead_score": lead_analysis['lead_score'],
            "section_scores": lead_analysis.get('section_scores', {}),
            "creator_profile": creator_profile,
            "items_analyzed": len(content_analyses),
            "pre_screen_passed": True,
            "travel_experience_detected": has_travel_experience,
            "processing_duration": duration
        }
        
    except Exception as e:
        print(f"=== ERROR: {contact_id} ===")
        print(f"Error: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        
        return {
            "status": "error",
            "contact_id": contact_id,
            "message": str(e)
        }


@celery_app.task(name='rescore_single_profile', bind=True, max_retries=3)
def rescore_single_profile(self, contact_id: str):
    """
    Re-score a single profile asynchronously (Celery background task)
    Rate-limited to avoid OpenAI API limits
    """
    import time
    
    try:
        print(f"[RESCORE] Starting re-score for contact {contact_id}")
        
        # Rate limiting: Sleep 3 seconds before each re-score
        # This prevents hitting OpenAI's 30K TPM limit
        # With 2 workers, this allows ~40 requests/min = safe under limit
        time.sleep(3)
        
        # Load cached analysis from R2
        cache_data = load_analysis_cache(contact_id)
        
        if not cache_data:
            print(f"[RESCORE] No cached data found for {contact_id}")
            return {
                'status': 'error',
                'contact_id': contact_id,
                'reason': 'No cached analysis found'
            }
        
        # Extract data
        content_analyses = cache_data.get('content_analyses', [])
        creator_profile = cache_data.get('creator_profile', {})
        has_travel_experience = cache_data.get('has_travel_experience', False)
        follower_count = cache_data.get('follower_count', 0)
        
        # Get cached evidence (v3.0 profiles will have this)
        bio_evidence = cache_data.get('bio_evidence')
        caption_evidence = cache_data.get('caption_evidence')
        thumbnail_evidence = cache_data.get('thumbnail_evidence')
        
        # Re-score with new evidence-based approach
        if bio_evidence and caption_evidence and thumbnail_evidence:
            print(f"[RESCORE] Using evidence-based scoring (v3.0)")
            lead_analysis = generate_evidence_based_score(
                bio_evidence=bio_evidence,
                caption_evidence=caption_evidence,
                thumbnail_evidence=thumbnail_evidence,
                content_analyses=content_analyses,
                creator_profile=creator_profile,
                follower_count=follower_count
            )
        else:
            print(f"[RESCORE] Missing evidence, profile needs full re-analysis")
            return {
                'status': 'error',
                'contact_id': contact_id,
                'reason': 'Profile analyzed with old version, needs full re-analysis'
            }
        
        # Apply travel boost if applicable
        if has_travel_experience and lead_analysis['lead_score'] < 0.50:
            original_score = lead_analysis['lead_score']
            lead_analysis['lead_score'] = 0.50
            lead_analysis['score_reasoning'] = f"{lead_analysis.get('score_reasoning', '')} | TRAVEL EXPERIENCE BOOST (original: {original_score:.2f})"
        
        # Send updated score to HubSpot
        send_to_hubspot(
            contact_id=contact_id,
            lead_score=lead_analysis['lead_score'],
            section_scores=lead_analysis.get('section_scores', {}),
            score_reasoning=lead_analysis.get('score_reasoning', ''),
            creator_profile=creator_profile,
            content_analyses=content_analyses,
            lead_analysis=lead_analysis,  # NEW: Pass full analysis
            first_name=first_name
        )
        
        print(f"[RESCORE] ✓ Successfully re-scored {contact_id}: {lead_analysis['lead_score']:.3f}")
        
        return {
            'status': 'success',
            'contact_id': contact_id,
            'new_score': lead_analysis['lead_score']
        }
        
    except Exception as e:
        print(f"[RESCORE] Error re-scoring {contact_id}: {e}")
        
        # If rate limit error, retry with longer delay
        if "rate_limit" in str(e).lower() or "429" in str(e):
            print(f"[RESCORE] Rate limit hit, retrying {contact_id} after 120s")
            raise self.retry(exc=e, countdown=120, max_retries=3)
        
        # Other errors retry after 60s
        raise self.retry(exc=e, countdown=60, max_retries=3)


INSIGHTIQ_CLIENT_ID = os.getenv('INSIGHTIQ_CLIENT_ID')
INSIGHTIQ_SECRET = os.getenv('INSIGHTIQ_SECRET')
HUBSPOT_API_KEY = os.getenv('HUBSPOT_API_KEY')
HUBSPOT_API_URL = 'https://api.hubapi.com'


# ============================================================================
# InsightIQ Discovery Class
# ============================================================================

class InsightIQDiscovery:
    """
    InsightIQ discovery client with fixed parameters
    
    Fixed parameters (not user-configurable):
    - Email required (MUST_HAVE)
    - English creators only
    - USA creators only
    - USA audience 30%+
    - Sort by follower count descending
    - Audience credibility: EXCELLENT, HIGH, NORMAL
    """
    
    FIXED_PARAMS = {
        'specific_contact_details': [
            {'type': 'EMAIL', 'preference': 'MUST_HAVE'}
        ],
        'creator_language': {'code': 'en'},
        'creator_locations': ['cb8c4bd2-7661-4761-971a-c27322e2f209'],  # USA
        'audience_locations': [
            {
                'location_id': 'cb8c4bd2-7661-4761-971a-c27322e2f209',
                'percentage_value': 30,
                'operator': 'GT'
            }
        ],
        'sort_by': {
            'field': 'FOLLOWER_COUNT',
            'order': 'DESCENDING'
        },
        'audience_credibility_category': ['EXCELLENT', 'HIGH', 'NORMAL']
    }
    
    PLATFORM_CONFIGS = {
        'instagram': {
            'work_platform_id': '9bb8913b-ddd9-430b-a66a-d74d846e6c66',
            'network_name': 'instagram',
        },
        'youtube': {
            'work_platform_id': '14d9ddf5-51c6-415e-bde6-f8ed36ad7054',
            'network_name': 'youtube',
        },
        'tiktok': {
            'work_platform_id': 'de55aeec-0dc8-4119-bf90-16b3d1f0c987',
            'network_name': 'tiktok',
        },
        'facebook': {
            'work_platform_id': 'ad2fec62-2987-40a0-89fb-23485972598c',
            'network_name': 'facebook',
        }
    }
    
    def __init__(self, client_id, secret):
        """Initialize with InsightIQ credentials"""
        self.client_id = client_id
        self.secret = secret
        
        encoded = base64.b64encode(f"{client_id}:{secret}".encode()).decode()
        self.headers = {'Authorization': f'Basic {encoded}'}
        
    def search_profiles(self, platform='instagram', user_filters=None):
        """Search for creator profiles with fixed base parameters"""
        if platform not in self.PLATFORM_CONFIGS:
            raise ValueError(f"Unsupported platform: {platform}")
        
        platform_config = self.PLATFORM_CONFIGS[platform]
        user_filters = user_filters or {}
        
        # Start with fixed parameters
        parameters = self.FIXED_PARAMS.copy()
        
        # Add platform
        parameters['work_platform_id'] = platform_config['work_platform_id']
        
        # Add max_results
        parameters['max_results'] = min(user_filters.get('max_results', 500), 4000)
        
        # Add follower count filter
        follower_filter = user_filters.get('follower_count', {})
        if platform == 'youtube':
            parameters['subscriber_count'] = {
                'min': follower_filter.get('min', 20000),
                'max': follower_filter.get('max', 900000)
            }
        else:
            parameters['follower_count'] = {
                'min': follower_filter.get('min', 20000),
                'max': follower_filter.get('max', 900000)
            }
        
        # Add lookalike (mutually exclusive)
        lookalike_type = user_filters.get('lookalike_type')
        lookalike_username = user_filters.get('lookalike_username', '').strip()
        
        if lookalike_type == 'creator' and lookalike_username:
            parameters['creator_lookalikes'] = lookalike_username
            print(f"Using creator lookalike: {lookalike_username}")
        elif lookalike_type == 'audience' and lookalike_username:
            parameters['audience_lookalikes'] = lookalike_username
            print(f"Using audience lookalike: {lookalike_username}")
        
        # Add optional filters
        if 'creator_interests' in user_filters and user_filters['creator_interests']:
            parameters['creator_interests'] = user_filters['creator_interests']
            print(f"Creator interests: {parameters['creator_interests']}")
        
        if 'audience_interests' in user_filters and user_filters['audience_interests']:
            parameters['audience_interests'] = user_filters['audience_interests']
            print(f"Audience interests: {parameters['audience_interests']}")
        
        if 'hashtags' in user_filters and user_filters['hashtags']:
            parameters['hashtags'] = user_filters['hashtags']
            print(f"Hashtags: {parameters['hashtags']}")
        
        print(f"Starting {platform} discovery with fixed parameters...")
        
        job_id = self._start_job(parameters)
        print(f"Waiting for results (job_id: {job_id})...")
        raw_results = self._fetch_results(job_id)
        print(f"Processing {len(raw_results)} profiles...")
        
        return self._standardize_results(raw_results, platform)
    def _start_job(self, parameters):
        """Start InsightIQ export job"""
        url = 'https://api.insightiq.ai/v1/social/creators/profiles/search-export'
        
        try:
            response = requests.post(url=url, headers=self.headers, json=parameters, timeout=30)
            
            if response.status_code not in (200, 202):
                print(f"API error: {response.status_code} - {response.text}")
                raise Exception(f"Failed to start job: {response.text}")
            
            job_id = response.json().get('id')
            if not job_id:
                raise Exception("No job ID returned from API")
            
            print(f"Job started successfully: {job_id}")
            return job_id
            
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            raise Exception(f"Failed to connect to InsightIQ API: {e}")
    
    def _fetch_results(self, job_id):
        """Poll for job results"""
        url = f'https://api.insightiq.ai/v1/social/creators/profiles/search-export/{job_id}'
        offset, limit = 0, 100
        all_results = []
        
        max_wait_time = 600  # 10 minutes
        start_time = time.time()
        poll_count = 0
        
        while True:
            elapsed = time.time() - start_time
            if elapsed > max_wait_time:
                raise Exception(f"Job timeout after {max_wait_time} seconds")
            
            poll_count += 1
            
            try:
                response = requests.get(
                    url=url,
                    headers=self.headers,
                    params={'offset': offset, 'limit': limit},
                    timeout=30
                )
                
                if response.status_code != 200:
                    raise Exception(f"Failed to fetch results: {response.text}")
                
                data = response.json()
                
                if data.get('status') == 'IN_PROGRESS':
                    print(f"Job still processing (poll #{poll_count}, elapsed: {int(elapsed)}s), waiting 60 seconds...")
                    time.sleep(60)
                    continue
                
                if data.get('status') == 'FAILED':
                    error_msg = data.get('error', 'Unknown error')
                    raise Exception(f"Job failed: {error_msg}")
                
                batch_results = data.get('data', [])
                all_results.extend(batch_results)
                
                total_results = data.get('metadata', {}).get('total_results', 0)
                print(f"Fetched {len(all_results)}/{total_results} profiles")
                
                if offset + limit >= total_results or len(batch_results) == 0:
                    break
                
                offset += limit
                
            except requests.exceptions.RequestException as e:
                raise Exception(f"Failed to fetch results: {e}")
        
        print(f"Fetch complete: {len(all_results)} total profiles")
        return all_results
    
    def _standardize_results(self, raw_results, platform):
        """Convert raw API results to standardized format for HubSpot"""
        standardized = []
        
        for i, profile in enumerate(raw_results):
            try:
                # Extract contact details
                contact_details = self._extract_contact_details(
                    profile.get('contact_details', [])
                )
                
                # Get location
                location = profile.get('location', {})
                
                # Standardized output mapped to HubSpot properties
                standardized_profile = {
                    # Core identity
                    'first_and_last_name': profile.get('full_name', ''),
                    'flagship_social_platform_handle': profile.get('platform_username', ''),
                    'instagram_handle': profile.get('url', ''),
                    'instagram_bio': profile.get('introduction', ''),
                    
                    # Metrics
                    'instagram_followers': profile.get('follower_count', 0),
                    'average_engagement': profile.get('engagement_rate', 0),
                    
                    # Contact info from contact_details array
                    'email': contact_details.get('email'),
                    'phone': contact_details.get('phone'),
                    'tiktok_handle': contact_details.get('tiktok'),
                    'youtube_profile_link': contact_details.get('youtube'),
                    'facebook_profile_link': contact_details.get('facebook'),
                    'patreon_link': contact_details.get('patreon'),
                    'pinterest_profile_link': contact_details.get('pinterest'),
                    
                    # Location
                    'city': location.get('city'),
                    'state': location.get('state'),
                    'country': location.get('country'),
                    
                    # Additional metadata
                    'platform': platform,
                    'is_verified': profile.get('is_verified', False),
                    'audience_credibility': profile.get('audience_credibility_category'),
                    
                    # Discovery tracking
                    'discovery_source': 'insightiq_discovery'
                }
                
                standardized.append(standardized_profile)
                
            except Exception as e:
                print(f"Failed to process profile #{i+1}: {e}")
                continue
        
        print(f"Successfully processed {len(standardized)} profiles")
        return standardized


    def _extract_contact_details(self, contact_details):
        """Extract and format contact details - handles duplicates by taking first occurrence"""
        contacts = {}
        
        for detail in contact_details:
            contact_type = detail.get('type', '').lower()
            contact_value = detail.get('value', '')
            
            if contact_type and contact_value:
                # Only set if not already set (takes first occurrence)
                if contact_type not in contacts:
                    contacts[contact_type] = contact_value
        
        return contacts

# ============================================================================
# Discovery Tasks
# ============================================================================

@celery_app.task(name='tasks.discover_instagram_profiles')
def discover_instagram_profiles(user_filters=None, job_id=None):
    """Run Instagram profile discovery with fixed base parameters"""
    if job_id is None:
        job_id = discover_instagram_profiles.request.id
    
    try:
        update_discovery_job_status(job_id, status='discovering')
        
        client_id = INSIGHTIQ_CLIENT_ID
        secret = INSIGHTIQ_SECRET
        
        if not client_id or not secret:
            raise ValueError("INSIGHTIQ_CLIENT_ID and INSIGHTIQ_SECRET must be set")
        
        user_filters = user_filters or {}
        lookalike_type = user_filters.get('lookalike_type')
        lookalike_username = user_filters.get('lookalike_username', '').strip()
        
        if lookalike_type and lookalike_type not in ('creator', 'audience'):
            raise ValueError("lookalike_type must be 'creator' or 'audience'")
        
        if lookalike_type and not lookalike_username:
            raise ValueError("lookalike_username required when lookalike_type is set")
        
        print(f"Starting discovery with filters: {user_filters}")
        
        discovery_client = InsightIQDiscovery(client_id, secret)
        profiles = discovery_client.search_profiles(platform='instagram', user_filters=user_filters)
        
        print(f"Discovery complete: {len(profiles)} profiles found")
        
        update_discovery_job_status(job_id, status='importing', profiles_found=len(profiles))
        
        import_results = import_profiles_to_hubspot(profiles, job_id)
        
        update_discovery_job_status(
            job_id,
            status='completed',
            profiles_found=len(profiles),
            new_contacts_created=import_results['created'],
            duplicates_skipped=import_results['skipped']
        )
        
        print(f"Job {job_id} completed: {import_results['created']} created, {import_results['skipped']} skipped")
        
        return {
            'status': 'completed',
            'profiles_found': len(profiles),
            'new_contacts': import_results['created'],
            'duplicates': import_results['skipped']
        }
        
    except Exception as e:
        print(f"Discovery failed: {e}")
        import traceback
        traceback.print_exc()
        update_discovery_job_status(job_id, status='failed', error=str(e))
        raise

@celery_app.task(name='tasks.discover_patreon_profiles')
def discover_patreon_profiles(user_filters=None, job_id=None):
    """Run Patreon profile discovery via Apify scraper with enrichment"""
    if job_id is None:
        job_id = discover_patreon_profiles.request.id
    
    try:
        update_discovery_job_status(job_id, status='discovering')
        
        if not APIFY_API_TOKEN:
            raise ValueError("APIFY_API_TOKEN must be set in environment")
        
        user_filters = user_filters or {}
        search_keywords = user_filters.get('search_keywords', [])
        max_results = user_filters.get('max_results', 100)
        
        if not search_keywords:
            raise ValueError("search_keywords required for Patreon discovery")
        
        print(f"Starting Patreon discovery with keywords: {search_keywords}, max: {max_results}")
        
        # Run Apify scraper
        from apify_client import ApifyClient
        
        client = ApifyClient(APIFY_API_TOKEN)
        
        # SIMPLE CONFIG - Let the scraper use its defaults
        run_input = {
            "searchQueries": search_keywords,
            "maxRequestsPerCrawl": max_results,
            "proxyConfiguration": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"],
            },
            "maxConcurrency": 1,
            "maxRequestRetries": 5,
            "requestHandlerTimeoutSecs": 180,
        }
        
        print("Starting Apify scraper (this may take a few minutes)...")
        run = client.actor("mJiXU9PT4eLHuY0pi").call(run_input=run_input)
        
        print(f"Apify run complete: {run['id']}")
        
        # Fetch results
        all_items = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            all_items.append(item)
        
        print(f"Apify returned {len(all_items)} total items from dataset")
        
        # Filter NSFW
        profiles = []
        nsfw_count = 0
        
        for item in all_items:
            if item.get('is_nsfw', 0) == 1:
                print(f"Skipping NSFW profile: {item.get('name', 'Unknown')}")
                nsfw_count += 1
                continue
            profiles.append(item)
        
        print(f"After NSFW filtering: {len(profiles)} profiles (excluded {nsfw_count} NSFW)")
        
        # Handle no profiles
        if len(profiles) == 0:
            if len(all_items) == 0:
                warning_msg = "Apify scraper returned 0 results. The scraper typically has ~5% yield. Try max_results of 100-500 to get 5-25 actual profiles."
            else:
                warning_msg = f"All {len(all_items)} profiles were NSFW. Try different keywords."
            
            print(f"Warning: {warning_msg}")
            update_discovery_job_status(job_id, status='completed', profiles_found=0, new_contacts_created=0, duplicates_skipped=0)
            
            return {
                'status': 'completed',
                'profiles_found': 0,
                'new_contacts': 0,
                'duplicates': 0,
                'warning': warning_msg
            }
        
        print(f"Patreon discovery complete: {len(profiles)} valid profiles found")
        
        # Enrichment
        update_discovery_job_status(job_id, status='enriching', profiles_found=len(profiles))
        enriched_profiles = enrich_patreon_profiles_enhanced(profiles, job_id)
        
        # Import to HubSpot
        update_discovery_job_status(job_id, status='importing')
        standardized_profiles = standardize_patreon_profiles(enriched_profiles)
        import_results = import_profiles_to_hubspot(standardized_profiles, job_id)
        
        # Final status
        update_discovery_job_status(
            job_id,
            status='completed',
            profiles_found=len(profiles),
            new_contacts_created=import_results['created'],
            duplicates_skipped=import_results['skipped']
        )
        
        print(f"Job {job_id} completed: {import_results['created']} created, {import_results['skipped']} skipped")
        
        return {
            'status': 'completed',
            'profiles_found': len(profiles),
            'new_contacts': import_results['created'],
            'duplicates': import_results['skipped']
        }
        
    except Exception as e:
        print(f"Patreon discovery failed: {e}")
        import traceback
        traceback.print_exc()
        update_discovery_job_status(job_id, status='failed', error=str(e))
        raise


def standardize_patreon_profiles_improved(raw_profiles: List[Dict]) -> List[Dict]:
    """
    Convert Apify scraper results to standardized format for HubSpot
    NOW INCLUDES social graph and Apollo data
    """
    standardized = []
    
    for i, profile in enumerate(raw_profiles):
        try:
            # Extract name parts
            full_name = profile.get('creator_name') or profile.get('name', '')
            
            # Use Apollo name if available
            if profile.get('apollo_first_name'):
                full_name = f"{profile['apollo_first_name']} {profile.get('apollo_last_name', '')}".strip()
            
            # Patreon handle from URL
            patreon_url = profile.get('url', '')
            patreon_handle = patreon_url.split('/')[-1] if patreon_url else ''
            
            standardized_profile = {
                # Core identity
                'first_and_last_name': full_name,
                'flagship_social_platform_handle': patreon_handle,
                'patreon_link': patreon_url,
                
                # Bio/Description
                'patreon_description': profile.get('about', ''),
                
                # Patreon-specific metrics
                'patreon_patron_count': profile.get('patron_count', 0),
                'patreon_paid_members': profile.get('paid_members', 0),
                'patreon_total_members': profile.get('total_members', 0),
                'patreon_post_count': profile.get('post_count', 0),
                'patreon_earnings_per_month': profile.get('earnings_per_month', 0),
                'patreon_creation_name': profile.get('creation_name', ''),
                
                # Email - prefer Apollo, fallback to social graph
                'email': profile.get('apollo_email') or profile.get('social_graph_email'),
                
                # Phone - prefer Apollo
                'phone': profile.get('apollo_phone'),
                
                # Social links (URLs from Patreon scraper + social graph)
                'instagram_handle': profile.get('instagram') or profile.get('social_links', {}).get('instagram'),
                'youtube_profile_link': profile.get('youtube') or profile.get('social_links', {}).get('youtube'),
                'tiktok_handle': profile.get('tiktok') or profile.get('social_links', {}).get('tiktok'),
                'facebook_profile_link': profile.get('facebook') or profile.get('social_links', {}).get('facebook'),
                'twitch_url': profile.get('twitch') or profile.get('social_links', {}).get('twitch'),
                'twitter_url': profile.get('social_links', {}).get('twitter'),
                'linkedin_profile_url': profile.get('social_links', {}).get('linkedin'),
                'discord_url': profile.get('social_links', {}).get('discord'),
                
                # Personal website from social graph
                'personal_website': profile.get('personal_website'),
                
                # Apollo enrichment
                'apollo_title': profile.get('apollo_title'),
                'apollo_linkedin': profile.get('apollo_linkedin'),
                'apollo_enriched': bool(profile.get('apollo_email')),
                
                # Metadata
                'platform': 'patreon',
                'discovery_source': 'apify_patreon_scraper'
            }
            
            # Remove None/empty values
            standardized_profile = {k: v for k, v in standardized_profile.items() 
                                   if v is not None and v != '' and v != 0}
            
            standardized.append(standardized_profile)
            
        except Exception as e:
            print(f"Failed to process Patreon profile #{i+1}: {e}")
            continue
    
    print(f"Successfully processed {len(standardized)} Patreon profiles")
    return standardized
# ============================================================================
# FACEBOOK GROUPS DISCOVERY + SOCIAL GRAPH + APOLLO ENRICHMENT
# Add to tasks.py after Patreon discovery task
# ============================================================================

import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from typing import Dict, List, Optional, Set

# Add these to your config section:
APOLLO_API_KEY = os.getenv('APOLLO_API_KEY')

# ============================================================================
# APOLLO.IO EMAIL ENRICHMENT
# ============================================================================

class ApolloEnrichment:
    """Apollo.io API client for finding emails"""
    
    BASE_URL = "https://api.apollo.io/api/v1"
    
    # Skip these domains (social media, platforms)
    SKIP_DOMAINS = [
        "meetup.com", "eventbrite.com", "youtube.com", "youtu.be",
        "reddit.com", "facebook.com", "instagram.com", "twitter.com",
        "x.com", "linkedin.com", "patreon.com", "tiktok.com",
        "google.com", "yelp.com", "substack.com", "discord.com",
        "discord.gg", "github.com", "medium.com"
    ]
    
    def __init__(self, api_key: str):
        self.api_key = api_key
    
    def person_match(self, name: str = None, domain: str = None, 
                    org_name: str = None, linkedin_url: str = None) -> Optional[Dict]:
        """
        Find email and profile info for a person
        
        Args:
            name: Person's full name
            domain: Website domain (e.g., "example.com")
            org_name: Organization name
            linkedin_url: LinkedIn profile URL
        
        Returns:
            dict with email, phone, linkedin, title, location, etc.
        """
        if not self.api_key:
            return None
        
        # Build request data
        data = {'reveal_personal_emails': True}
        if name:
            data['name'] = name
        if domain:
            data['domain'] = domain
        if org_name:
            data['organization_name'] = org_name
        if linkedin_url:
            data['linkedin_url'] = linkedin_url
        
        try:
            response = requests.post(
                f"{self.BASE_URL}/people/match",
                headers={
                    'Content-Type': 'application/json',
                    'x-api-key': self.api_key,
                    'Cache-Control': 'no-cache'
                },
                json=data,
                timeout=15
            )
            
            if response.status_code == 429:
                print("[APOLLO] Rate limited")
                time.sleep(2)
                return None
            
            if not response.ok:
                print(f"[APOLLO] Error {response.status_code}")
                return None
            
            result = response.json()
            person = result.get('person', {})
            
            if not person:
                return None
            
            # Extract phone number
            phone_numbers = person.get('phone_numbers', [])
            phone = phone_numbers[0].get('raw_number', '') if phone_numbers else ''
            
            # Build location
            location_parts = [person.get('city'), person.get('state'), person.get('country')]
            location = ', '.join(filter(None, location_parts))
            
            return {
                'email': person.get('email', ''),
                'first_name': person.get('first_name', ''),
                'last_name': person.get('last_name', ''),
                'full_name': person.get('name', ''),
                'title': person.get('title', ''),
                'linkedin': person.get('linkedin_url', ''),
                'twitter': person.get('twitter_url', ''),
                'facebook': person.get('facebook_url', ''),
                'phone': phone,
                'location': location,
                'headline': person.get('headline', ''),
                'organization': person.get('organization', {}).get('name', ''),
                'org_phone': person.get('organization', {}).get('phone', '')
            }
            
        except Exception as e:
            print(f"[APOLLO] Error: {e}")
            return None
    
    @staticmethod
    def extract_domain(url: str) -> str:
        """Extract domain from URL"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.replace('www.', '')
            return domain
        except:
            return ""
    
    @staticmethod
    def is_enrichable_domain(domain: str) -> bool:
        """Check if domain should be enriched (not social media)"""
        if not domain:
            return False
        return not any(skip in domain for skip in ApolloEnrichment.SKIP_DOMAINS)


# ============================================================================
# SOCIAL GRAPH BUILDER (Website Crawler + Link Aggregators)
# ============================================================================

class SocialGraphBuilder:
    """
    Crawls websites and link aggregators (Linktree, Beacons) to find:
    - Emails
    - Social media profiles (Instagram, YouTube, Twitter, LinkedIn, TikTok)
    - Additional owned channels
    """
    
    LINK_AGGREGATORS = [
        'linktr.ee', 'beacons.ai', 'linkin.bio', 'linkpop.com',
        'hoo.be', 'campsite.bio', 'lnk.bio', 'tap.bio', 'solo.to',
        'bio.link', 'carrd.co'
    ]
    
    SOCIAL_PATTERNS = {
        'instagram': r'instagram\.com/([a-zA-Z0-9._]+)',
        'youtube': r'youtube\.com/(c/|channel/|@)?([a-zA-Z0-9_-]+)',
        'twitter': r'(twitter\.com|x\.com)/([a-zA-Z0-9_]+)',
        'linkedin': r'linkedin\.com/in/([a-zA-Z0-9-]+)',
        'tiktok': r'tiktok\.com/@([a-zA-Z0-9._]+)',
        'facebook': r'facebook\.com/([a-zA-Z0-9.]+)',
        'twitch': r'twitch\.tv/([a-zA-Z0-9_]+)',
        'discord': r'discord\.(gg|com)/([a-zA-Z0-9]+)'
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def build_graph(self, website_url: str, leader_name: str = None) -> Dict:
        """
        Build social graph from website
        
        Args:
            website_url: URL to crawl
            leader_name: Optional name to search for emails
        
        Returns:
            dict with emails, social_links, and additional_urls
        """
        result = {
            'emails': [],
            'social_links': {},
            'personal_website': None,
            'linktree_url': None
        }
        
        if not website_url:
            return result
        
        try:
            # Check if it's a link aggregator
            is_aggregator = any(agg in website_url.lower() for agg in self.LINK_AGGREGATORS)
            
            if is_aggregator:
                result['linktree_url'] = website_url
                agg_data = self._scrape_link_aggregator(website_url)
                result['social_links'].update(agg_data['social_links'])
                result['emails'].extend(agg_data['emails'])
                
                # If aggregator has a personal website, crawl it too
                if agg_data.get('personal_website'):
                    result['personal_website'] = agg_data['personal_website']
                    website_data = self._scrape_website(agg_data['personal_website'], leader_name)
                    result['emails'].extend(website_data['emails'])
                    result['social_links'].update(website_data['social_links'])
            else:
                # Regular website - scrape it
                website_data = self._scrape_website(website_url, leader_name)
                result['emails'].extend(website_data['emails'])
                result['social_links'].update(website_data['social_links'])
                
                # Check if they link to a Linktree/Beacons
                if website_data.get('linktree_url'):
                    result['linktree_url'] = website_data['linktree_url']
                    agg_data = self._scrape_link_aggregator(website_data['linktree_url'])
                    result['social_links'].update(agg_data['social_links'])
            
            # Deduplicate emails
            result['emails'] = list(set(result['emails']))
            
            return result
            
        except Exception as e:
            print(f"[SOCIAL_GRAPH] Error building graph: {e}")
            return result
    
    def _scrape_website(self, url: str, leader_name: str = None) -> Dict:
        """Scrape a regular website for emails and social links"""
        result = {
            'emails': [],
            'social_links': {},
            'linktree_url': None
        }
        
        try:
            response = self.session.get(url, timeout=10, allow_redirects=True)
            if not response.ok:
                return result
            
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.get_text()
            
            # Extract emails
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = re.findall(email_pattern, text)
            
            # Filter out image/asset emails
            blocked_patterns = ['.png', '.jpg', '.gif', '.jpeg', '.webp', '.svg',
                              'sentry.io', 'example.com', 'cloudfront', 'amazonaws']
            result['emails'] = [
                email.lower() for email in emails
                if not any(pattern in email.lower() for pattern in blocked_patterns)
            ]
            
            # Extract obfuscated emails (name [at] domain [dot] com)
            obfuscated_pattern = r'([a-zA-Z0-9._%+-]+)\s*(?:\[at\]|\(at\)|{at}|\bat\b)\s*([a-zA-Z0-9.-]+)\s*(?:\[dot\]|\(dot\)|{dot}|\.)\s*([a-zA-Z]{2,})'
            obfuscated = re.findall(obfuscated_pattern, text, re.IGNORECASE)
            for parts in obfuscated:
                email = f"{parts[0]}@{parts[1]}.{parts[2]}".lower().replace(' ', '')
                if '@' in email:
                    result['emails'].append(email)
            
            # Extract mailto links
            mailto_links = soup.find_all('a', href=re.compile(r'^mailto:'))
            for link in mailto_links:
                email = link['href'].replace('mailto:', '').split('?')[0].strip().lower()
                if email and '@' in email:
                    result['emails'].append(email)
            
            # Extract social links
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link['href']
                
                # Check for link aggregators
                if any(agg in href.lower() for agg in self.LINK_AGGREGATORS):
                    result['linktree_url'] = href
                
                # Extract social media profiles
                for platform, pattern in self.SOCIAL_PATTERNS.items():
                    if platform in href.lower():
                        match = re.search(pattern, href, re.IGNORECASE)
                        if match:
                            result['social_links'][platform] = href.split('?')[0]
            
            return result
            
        except Exception as e:
            print(f"[SOCIAL_GRAPH] Error scraping website {url}: {e}")
            return result
    
    def _scrape_link_aggregator(self, url: str) -> Dict:
        """Scrape Linktree/Beacons for social links"""
        result = {
            'social_links': {},
            'emails': [],
            'personal_website': None
        }
        
        try:
            response = self.session.get(url, timeout=10, allow_redirects=True)
            if not response.ok:
                return result
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract all links
            all_links = soup.find_all('a', href=True)
            
            skip_hosts = ['instagram.com', 'youtube.com', 'twitter.com', 'x.com',
                         'facebook.com', 'tiktok.com', 'linkedin.com', 'twitch.tv',
                         'patreon.com', 'spotify.com', 'apple.com', 'google.com']
            
            for link in all_links:
                href = link['href']
                
                # Extract social media
                for platform, pattern in self.SOCIAL_PATTERNS.items():
                    if platform in href.lower():
                        match = re.search(pattern, href, re.IGNORECASE)
                        if match:
                            result['social_links'][platform] = href.split('?')[0]
                
                # Find personal website (not social media)
                if href.startswith('http') and not any(skip in href.lower() for skip in skip_hosts):
                    if not result['personal_website']:
                        result['personal_website'] = href
            
            return result
            
        except Exception as e:
            print(f"[SOCIAL_GRAPH] Error scraping aggregator {url}: {e}")
            return result

# ============================================================================
# FACEBOOK GROUPS DISCOVERY TASK
# ============================================================================

@celery_app.task(name='tasks.discover_facebook_groups')
def discover_facebook_groups(user_filters=None, job_id=None):
    """
    Discover Facebook Groups via Google Search + Apify
    
    Args:
        user_filters: dict with:
            - keywords: list of str (required)
            - max_results: int (default 100)
            - min_members: int (optional)
            - max_members: int (optional)
    
    Returns:
        dict with results summary
    """
    if job_id is None:
        job_id = discover_facebook_groups.request.id
    
    try:
        update_discovery_job_status(job_id, status='discovering')
        
        if not APIFY_API_TOKEN:
            raise ValueError("APIFY_API_TOKEN must be set")
        
        user_filters = user_filters or {}
        keywords = user_filters.get('keywords', [])
        max_results = user_filters.get('max_results', 100)
        min_members = user_filters.get('min_members', 0)
        max_members = user_filters.get('max_members', 0)
        
        if not keywords:
            raise ValueError("keywords required for Facebook Groups discovery")
        
        print(f"Starting Facebook Groups discovery with keywords: {keywords}, max: {max_results}")
        
        # Expand keywords into Google search queries
        google_queries = []
        for kw in keywords:
            google_queries.append(f'site:facebook.com/groups "{kw}"')
            google_queries.append(f'site:facebook.com/groups {kw} community')
            google_queries.append(f'site:facebook.com/groups {kw} group')
        
        print(f"Expanded to {len(google_queries)} Google queries")
        
        # Run Google search via Apify
        from apify_client import ApifyClient
        client = ApifyClient(APIFY_API_TOKEN)
        
        run_input = {
            'queries': '\n'.join(google_queries[:10]),  # Limit to 10 queries
            'maxPagesPerQuery': 5,
            'resultsPerPage': 20,
            'countryCode': 'us',
            'languageCode': 'en',
            'mobileResults': False
        }
        
        print("Starting Google search...")
        run = client.actor("apify~google-search-scraper").call(run_input=run_input)
        
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        cost = run.get("usageTotalUsd", 0)
        
        print(f"Google search complete: {len(items)} result pages, cost: ${cost:.4f}")
        
        # Process results
        profiles = []
        seen_urls = set()
        
        for item in items:
            if len(profiles) >= max_results:
                break
            
            organic_results = item.get('organicResults', [])
            
            for result in organic_results:
                if len(profiles) >= max_results:
                    break
                
                url = result.get('url', '')
                if 'facebook.com/groups/' not in url:
                    continue
                
                # Extract group URL
                group_url = extract_facebook_group_url(url)
                
                if group_url in seen_urls:
                    continue
                seen_urls.add(group_url)
                
                title = result.get('title', '')
                snippet = result.get('description', '')
                
                # Clean group name from title
                group_name = title.replace(' | Facebook', '').replace(' - Facebook', '').strip()
                
                # Extract member count from snippet
                member_count = extract_member_count_from_text(f"{title} {snippet}")
                
                # Filter by member count
                if min_members > 0 and member_count > 0 and member_count < min_members:
                    continue
                if max_members > 0 and member_count > 0 and member_count > max_members:
                    continue
                
                profiles.append({
                    'name': group_name,
                    'url': group_url,
                    'description': snippet[:2000],
                    'member_count': member_count,
                    'source': 'facebook_groups'
                })
        
        print(f"Facebook Groups discovery complete: {len(profiles)} groups found")
        
        # Update status
        update_discovery_job_status(job_id, status='enriching', profiles_found=len(profiles))
        
        # Enrich profiles with social graph + Apollo
        enriched_profiles = enrich_community_profiles(profiles, job_id)
        
        # Import to HubSpot
        update_discovery_job_status(job_id, status='importing')
        standardized = standardize_community_profiles(enriched_profiles)
        import_results = import_profiles_to_hubspot(standardized, job_id)
        
        # Final status
        update_discovery_job_status(
            job_id,
            status='completed',
            profiles_found=len(profiles),
            new_contacts_created=import_results['created'],
            duplicates_skipped=import_results['skipped']
        )
        
        return {
            'status': 'completed',
            'profiles_found': len(profiles),
            'new_contacts': import_results['created'],
            'duplicates': import_results['skipped']
        }
        
    except Exception as e:
        print(f"Facebook Groups discovery failed: {e}")
        import traceback
        traceback.print_exc()
        update_discovery_job_status(job_id, status='failed', error=str(e))
        raise


# ============================================================================
# COMMUNITY PROFILE ENRICHMENT (Social Graph + Apollo)
# ============================================================================

def enrich_community_profiles(profiles: List[Dict], job_id: str) -> List[Dict]:
    """
    Enrich community profiles with:
    1. Social graph (crawl website for emails + social links)
    2. Apollo.io email finding (if we have name + domain)
    
    Args:
        profiles: List of community profile dicts
        job_id: Job tracking ID
    
    Returns:
        Enriched profiles
    """
    print(f"Enriching {len(profiles)} community profiles...")
    
    social_graph_builder = SocialGraphBuilder()
    apollo_client = ApolloEnrichment(APOLLO_API_KEY) if APOLLO_API_KEY else None
    
    enriched_count = 0
    apollo_count = 0
    
    for profile in profiles:
        try:
            # Step 1: Build social graph from website/Facebook page
            website_url = profile.get('url', '')
            leader_name = profile.get('name', '')
            
            if website_url:
                graph_data = social_graph_builder.build_graph(website_url, leader_name)
                
                # Add emails
                if graph_data['emails']:
                    profile['email'] = graph_data['emails'][0]  # Take first email
                    enriched_count += 1
                
                # Add social links
                profile['instagram_url'] = graph_data['social_links'].get('instagram')
                profile['youtube_url'] = graph_data['social_links'].get('youtube')
                profile['twitter_url'] = graph_data['social_links'].get('twitter')
                profile['linkedin_url'] = graph_data['social_links'].get('linkedin')
                profile['tiktok_url'] = graph_data['social_links'].get('tiktok')
                profile['twitch_url'] = graph_data['social_links'].get('twitch')
                profile['discord_url'] = graph_data['social_links'].get('discord')
                
                # Store personal website if different from Facebook
                if graph_data['personal_website']:
                    profile['personal_website'] = graph_data['personal_website']
            
            # Step 2: Try Apollo.io if we don't have email yet
            if not profile.get('email') and apollo_client:
                # Extract domain from personal website
                domain = None
                if profile.get('personal_website'):
                    domain = apollo_client.extract_domain(profile['personal_website'])
                    
                    # Only enrich if it's not a social media domain
                    if domain and apollo_client.is_enrichable_domain(domain):
                        apollo_data = apollo_client.person_match(
                            name=leader_name,
                            domain=domain
                        )
                        
                        if apollo_data and apollo_data.get('email'):
                            profile['email'] = apollo_data['email']
                            profile['apollo_first_name'] = apollo_data.get('first_name')
                            profile['apollo_last_name'] = apollo_data.get('last_name')
                            profile['apollo_title'] = apollo_data.get('title')
                            profile['apollo_phone'] = apollo_data.get('phone')
                            profile['apollo_linkedin'] = apollo_data.get('linkedin')
                            apollo_count += 1
                            print(f"[APOLLO] Found email for {leader_name}")
        
        except Exception as e:
            print(f"Error enriching profile {profile.get('name')}: {e}")
            continue
    
    print(f"Enrichment complete: {enriched_count} emails from social graph, {apollo_count} from Apollo")
    return profiles


def standardize_community_profiles(profiles: List[Dict]) -> List[Dict]:
    """Convert community profiles to HubSpot format"""
    standardized = []
    
    for profile in profiles:
        standardized_profile = {
            # Core identity
            'first_and_last_name': profile.get('apollo_first_name', '') + ' ' + profile.get('apollo_last_name', ''),
            'community_name': profile.get('name', ''),
            'facebook_profile_link': profile.get('url', ''),
            'community_description': profile.get('description', ''),
            
            # Metrics
            'facebook_group_members': profile.get('member_count', 0),
            
            # Contact info
            'email': profile.get('email'),
            'phone': profile.get('apollo_phone'),
            
            # Social links
            'instagram_handle': profile.get('instagram_url'),
            'youtube_profile_link': profile.get('youtube_url'),
            'twitter_url': profile.get('twitter_url'),
            'linkedin_profile_url': profile.get('linkedin_url'),
            'tiktok_handle': profile.get('tiktok_url'),
            'twitch_url': profile.get('twitch_url'),
            'discord_url': profile.get('discord_url'),
            'personal_website': profile.get('personal_website'),
            
            # Metadata
            'platform': 'facebook_groups',
            'discovery_source': 'google_facebook_search',
            'apollo_enriched': bool(profile.get('apollo_first_name'))
        }
        
        # Remove None values
        standardized_profile = {k: v for k, v in standardized_profile.items() 
                               if v is not None and v != '' and v != 0}
        
        standardized.append(standardized_profile)
    
    return standardized


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def extract_facebook_group_url(raw_url: str) -> str:
    """Extract clean Facebook group URL"""
    try:
        # Remove query params
        url = raw_url.split('?')[0]
        
        # Extract group ID or slug
        if '/groups/' in url:
            parts = url.split('/groups/')
            if len(parts) > 1:
                group_id = parts[1].split('/')[0]
                return f"https://www.facebook.com/groups/{group_id}"
        
        return url
    except:
        return raw_url


def extract_member_count_from_text(text: str) -> int:
    """Extract member count from text like '5.2K members' or '1,234 members'"""
    try:
        # Look for patterns like "5.2K members", "1,234 members", etc.
        patterns = [
            r'([\d,]+\.?\d*[KkMm]?)\s+members?',
            r'([\d,]+\.?\d*[KkMm]?)\s+people',
            r'([\d,]+)\s+in group'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                count_str = match.group(1).replace(',', '').upper()
                
                # Handle K and M suffixes
                if 'K' in count_str:
                    return int(float(count_str.replace('K', '')) * 1000)
                elif 'M' in count_str:
                    return int(float(count_str.replace('M', '')) * 1000000)
                else:
                    return int(count_str)
        
        return 0
    except:
        return 0


# ============================================================================
# ENHANCED PATREON ENRICHMENT
# This extends the basic Patreon enrichment to actually crawl Patreon pages
# ============================================================================

def enrich_patreon_profiles_enhanced(raw_profiles: List[Dict], job_id: str) -> List[Dict]:
    """
    Enhanced Patreon enrichment:
    1. Crawls the Patreon page for personal websites
    2. For each social media link (Instagram, YouTube, Twitter):
       - Scrape the profile page for email addresses
       - Look for bio links (linktree/beacons)
       - Follow those to find more emails
    3. Use Apollo.io with any discovered website domains
    """
    print(f"Enhanced enrichment for {len(raw_profiles)} Patreon profiles...")
    
    social_graph_builder = SocialGraphBuilder()
    apollo_client = ApolloEnrichment(APOLLO_API_KEY) if APOLLO_API_KEY else None
    
    patreon_crawl_count = 0
    social_profile_crawl_count = 0
    social_graph_count = 0
    apollo_count = 0
    
    for profile in raw_profiles:
        try:
            creator_name = profile.get('creator_name') or profile.get('name', '')
            patreon_url = profile.get('url', '')
            
            # Store social links from Patreon scraper
            profile['social_links'] = {
                'instagram': profile.get('instagram'),
                'youtube': profile.get('youtube'),
                'twitter': profile.get('twitter'),
                'facebook': profile.get('facebook'),
                'twitch': profile.get('twitch'),
                'tiktok': profile.get('tiktok')
            }
            
            # Step 1: Crawl the Patreon page for personal websites
            if patreon_url:
                print(f"[PATREON_CRAWL] Scraping {patreon_url}...")
                
                try:
                    response = requests.get(patreon_url, timeout=10, headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    })
                    
                    if response.ok:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        links = soup.find_all('a', href=True)
                        
                        skip_hosts = ['patreon.com', 'youtube.com', 'instagram.com', 
                                     'twitter.com', 'x.com', 'facebook.com', 'tiktok.com',
                                     'spotify.com', 'apple.com', 'google.com']
                        
                        personal_websites = []
                        
                        for link in links:
                            href = link['href']
                            if href.startswith('http') and not any(skip in href.lower() for skip in skip_hosts):
                                personal_websites.append(href)
                        
                        if personal_websites:
                            website_url = personal_websites[0]
                            print(f"[PATREON_CRAWL] Found website: {website_url}")
                            profile['personal_website'] = website_url
                            patreon_crawl_count += 1
                            
                            # Build social graph from personal website
                            graph_data = social_graph_builder.build_graph(website_url, creator_name)
                            
                            if graph_data['emails']:
                                profile['social_graph_email'] = graph_data['emails'][0]
                                social_graph_count += 1
                                print(f"[SOCIAL_GRAPH] Found email for {creator_name}")
                            
                            # Merge social links
                            for platform, url in graph_data['social_links'].items():
                                if url and not profile['social_links'].get(platform):
                                    profile['social_links'][platform] = url
                    
                    time.sleep(0.5)
                    
                except Exception as e:
                    print(f"[PATREON_CRAWL] Error crawling {patreon_url}: {e}")
            
            # Step 2: NEW - Scrape social media profiles for emails
            # If we don't have an email yet, try scraping their social profiles
            if not profile.get('social_graph_email') and not profile.get('apollo_email'):
                
                # Try Instagram first (often has email in bio or bio link)
                instagram_url = profile['social_links'].get('instagram')
                if instagram_url:
                    print(f"[INSTAGRAM_CRAWL] Scraping {instagram_url} for email...")
                    try:
                        response = requests.get(instagram_url, timeout=10, headers={
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                        })
                        
                        if response.ok:
                            soup = BeautifulSoup(response.text, 'html.parser')
                            
                            # Look for emails in meta tags
                            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                            text = soup.get_text()
                            emails = re.findall(email_pattern, text)
                            
                            # Filter out image emails
                            valid_emails = [e for e in emails if not any(x in e.lower() for x in 
                                          ['.png', '.jpg', '.gif', 'cdninstagram', 'fbcdn'])]
                            
                            if valid_emails:
                                profile['social_graph_email'] = valid_emails[0]
                                social_profile_crawl_count += 1
                                social_graph_count += 1
                                print(f"[INSTAGRAM_CRAWL] Found email: {valid_emails[0]}")
                            
                            # Look for bio links (linktree, beacons, etc.)
                            all_links = soup.find_all('a', href=True)
                            for link in all_links:
                                href = link['href']
                                
                                # Check if it's a link aggregator
                                if any(agg in href.lower() for agg in 
                                      ['linktr.ee', 'beacons.ai', 'bio.link', 'linkin.bio', 'carrd.co']):
                                    print(f"[INSTAGRAM_CRAWL] Found bio link: {href}")
                                    
                                    # Crawl the bio link
                                    bio_data = social_graph_builder.build_graph(href, creator_name)
                                    
                                    if bio_data['emails'] and not profile.get('social_graph_email'):
                                        profile['social_graph_email'] = bio_data['emails'][0]
                                        social_profile_crawl_count += 1
                                        social_graph_count += 1
                                        print(f"[BIO_LINK] Found email: {bio_data['emails'][0]}")
                                    
                                    # Get personal website from bio link
                                    if bio_data.get('personal_website'):
                                        profile['personal_website'] = bio_data['personal_website']
                                    
                                    break  # Found bio link, stop looking
                        
                        time.sleep(0.5)
                        
                    except Exception as e:
                        print(f"[INSTAGRAM_CRAWL] Error: {e}")
                
                # Try YouTube if still no email
                if not profile.get('social_graph_email'):
                    youtube_url = profile['social_links'].get('youtube')
                    if youtube_url:
                        print(f"[YOUTUBE_CRAWL] Scraping {youtube_url} for email...")
                        try:
                            response = requests.get(youtube_url, timeout=10, headers={
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                            })
                            
                            if response.ok:
                                soup = BeautifulSoup(response.text, 'html.parser')
                                text = soup.get_text()
                                
                                # Look for emails
                                email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                                emails = re.findall(email_pattern, text)
                                
                                valid_emails = [e for e in emails if not any(x in e.lower() for x in 
                                              ['.png', '.jpg', '.gif', 'youtube', 'google'])]
                                
                                if valid_emails:
                                    profile['social_graph_email'] = valid_emails[0]
                                    social_profile_crawl_count += 1
                                    social_graph_count += 1
                                    print(f"[YOUTUBE_CRAWL] Found email: {valid_emails[0]}")
                                
                                # Look for links in description
                                all_links = soup.find_all('a', href=True)
                                for link in all_links:
                                    href = link['href']
                                    
                                    # Check for personal website or bio link
                                    if href.startswith('http'):
                                        # Skip YouTube/Google links
                                        if any(skip in href.lower() for skip in ['youtube.com', 'google.com', 'youtu.be']):
                                            continue
                                        
                                        # Check if it's a bio link
                                        if any(agg in href.lower() for agg in 
                                              ['linktr.ee', 'beacons.ai', 'bio.link', 'carrd.co']):
                                            bio_data = social_graph_builder.build_graph(href, creator_name)
                                            
                                            if bio_data['emails'] and not profile.get('social_graph_email'):
                                                profile['social_graph_email'] = bio_data['emails'][0]
                                                social_profile_crawl_count += 1
                                                social_graph_count += 1
                                                print(f"[YOUTUBE_BIO_LINK] Found email: {bio_data['emails'][0]}")
                                            
                                            if bio_data.get('personal_website'):
                                                profile['personal_website'] = bio_data['personal_website']
                                            
                                            break
                                        
                                        # Or it's a personal website
                                        elif not profile.get('personal_website'):
                                            profile['personal_website'] = href
                                            print(f"[YOUTUBE_CRAWL] Found website: {href}")
                            
                            time.sleep(0.5)
                            
                        except Exception as e:
                            print(f"[YOUTUBE_CRAWL] Error: {e}")
                
                # Try Twitter if still no email
                if not profile.get('social_graph_email'):
                    twitter_url = profile['social_links'].get('twitter')
                    if twitter_url:
                        print(f"[TWITTER_CRAWL] Scraping {twitter_url} for email...")
                        try:
                            response = requests.get(twitter_url, timeout=10, headers={
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                            })
                            
                            if response.ok:
                                soup = BeautifulSoup(response.text, 'html.parser')
                                text = soup.get_text()
                                
                                # Look for emails
                                email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                                emails = re.findall(email_pattern, text)
                                
                                valid_emails = [e for e in emails if not any(x in e.lower() for x in 
                                              ['.png', '.jpg', 'twitter', 'x.com'])]
                                
                                if valid_emails:
                                    profile['social_graph_email'] = valid_emails[0]
                                    social_profile_crawl_count += 1
                                    social_graph_count += 1
                                    print(f"[TWITTER_CRAWL] Found email: {valid_emails[0]}")
                                
                                # Look for bio links
                                all_links = soup.find_all('a', href=True)
                                for link in all_links:
                                    href = link['href']
                                    
                                    if any(agg in href.lower() for agg in 
                                          ['linktr.ee', 'beacons.ai', 'bio.link', 'carrd.co']):
                                        bio_data = social_graph_builder.build_graph(href, creator_name)
                                        
                                        if bio_data['emails'] and not profile.get('social_graph_email'):
                                            profile['social_graph_email'] = bio_data['emails'][0]
                                            social_profile_crawl_count += 1
                                            social_graph_count += 1
                                            print(f"[TWITTER_BIO_LINK] Found email: {bio_data['emails'][0]}")
                                        
                                        if bio_data.get('personal_website'):
                                            profile['personal_website'] = bio_data['personal_website']
                                        
                                        break
                            
                            time.sleep(0.5)
                            
                        except Exception as e:
                            print(f"[TWITTER_CRAWL] Error: {e}")
            
            # Step 3: Try Apollo.io if we found a personal website but still no email
            if apollo_client and not profile.get('social_graph_email') and profile.get('personal_website'):
                domain = apollo_client.extract_domain(profile['personal_website'])
                
                if domain and apollo_client.is_enrichable_domain(domain):
                    apollo_data = apollo_client.person_match(
                        name=creator_name,
                        domain=domain
                    )
                    
                    if apollo_data and apollo_data.get('email'):
                        profile['apollo_email'] = apollo_data['email']
                        profile['apollo_first_name'] = apollo_data.get('first_name')
                        profile['apollo_last_name'] = apollo_data.get('last_name')
                        profile['apollo_title'] = apollo_data.get('title')
                        profile['apollo_phone'] = apollo_data.get('phone')
                        profile['apollo_linkedin'] = apollo_data.get('linkedin')
                        apollo_count += 1
                        print(f"[APOLLO] Found email for {creator_name}")
                
                time.sleep(0.5)
        
        except Exception as e:
            print(f"Error in enhanced enrichment for {profile.get('name')}: {e}")
            continue
    
    print(f"Enhanced enrichment complete:")
    print(f"  - {patreon_crawl_count} personal websites from Patreon pages")
    print(f"  - {social_profile_crawl_count} social profiles crawled (Instagram/YouTube/Twitter)")
    print(f"  - {social_graph_count} total emails from social graph")
    print(f"  - {apollo_count} emails from Apollo")
    
    return raw_profiles


def standardize_patreon_profiles(raw_profiles: List[Dict]) -> List[Dict]:
    """
    Convert Apify scraper results to standardized format for HubSpot
    
    NOW INCLUDES social graph and Apollo data
    """
    standardized = []
    
    for i, profile in enumerate(raw_profiles):
        try:
            # Extract name parts
            full_name = profile.get('creator_name') or profile.get('name', '')
            
            # Use Apollo name if available
            if profile.get('apollo_first_name'):
                full_name = f"{profile['apollo_first_name']} {profile.get('apollo_last_name', '')}".strip()
            
            # Patreon handle from URL
            patreon_url = profile.get('url', '')
            patreon_handle = patreon_url.split('/')[-1] if patreon_url else ''
            
            standardized_profile = {
                # Core identity
                'first_and_last_name': full_name,
                'flagship_social_platform_handle': patreon_handle,
                'patreon_link': patreon_url,
                
                # Bio/Description
                'patreon_description': profile.get('about', ''),
                
                # Patreon-specific metrics
                'patreon_patron_count': profile.get('patron_count', 0),
                'patreon_paid_members': profile.get('paid_members', 0),
                'patreon_total_members': profile.get('total_members', 0),
                'patreon_post_count': profile.get('post_count', 0),
                'patreon_earnings_per_month': profile.get('earnings_per_month', 0),
                'patreon_creation_name': profile.get('creation_name', ''),
                
                # Email - prefer Apollo, fallback to social graph, fallback to Patreon scraper
                'email': profile.get('apollo_email') or profile.get('social_graph_email') or profile.get('email'),
                
                # Phone - prefer Apollo
                'phone': profile.get('apollo_phone') or profile.get('phone'),
                
                # Social links (URLs, not usernames) - from Patreon scraper
                'instagram_handle': profile.get('instagram'),
                'youtube_profile_link': profile.get('youtube'),
                'tiktok_handle': profile.get('tiktok'),
                'facebook_profile_link': profile.get('facebook'),
                'twitch_url': profile.get('twitch'),
                
                # Apollo enrichment
                'personal_website': profile.get('personal_website'),
                'twitter_url': profile.get('social_links', {}).get('twitter'),
                'linkedin_profile_url': profile.get('social_links', {}).get('linkedin'),
                'discord_url': profile.get('social_links', {}).get('discord'),
                'apollo_title': profile.get('apollo_title'),
                'apollo_linkedin': profile.get('apollo_linkedin'),
                'apollo_title': profile.get('apollo_title'),
                'apollo_linkedin': profile.get('apollo_linkedin'),
                'apollo_enriched': bool(profile.get('apollo_email')),
                
                # Metadata
                'platform': 'patreon',
                'discovery_source': 'apify_patreon_scraper'
            }
            
            # Remove None/empty values
            standardized_profile = {k: v for k, v in standardized_profile.items() 
                                   if v is not None and v != '' and v != 0}
            
            standardized.append(standardized_profile)
            
        except Exception as e:
            print(f"Failed to process Patreon profile #{i+1}: {e}")
            continue
    
    print(f"Successfully processed {len(standardized)} Patreon profiles")
    return standardized

def standardize_patreon_profiles(raw_profiles):
    """
    Convert Apify scraper results to standardized format for HubSpot
    
    Automatically filters out NSFW profiles.
    """
    standardized = []
    
    for i, profile in enumerate(raw_profiles):
        try:
            # Skip NSFW profiles
            if profile.get('is_nsfw', 0) == 1:
                print(f"Skipping NSFW profile: {profile.get('name', 'Unknown')}")
                continue
            
            # Extract name parts
            full_name = profile.get('creator_name') or profile.get('name', '')
            name_parts = full_name.split() if full_name else []
            first_name = name_parts[0] if name_parts else ''
            last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''
            
            # Patreon handle from URL
            patreon_url = profile.get('url', '')
            patreon_handle = patreon_url.split('/')[-1] if patreon_url else ''
            
            standardized_profile = {
                # Core identity
                'first_and_last_name': full_name,
                'flagship_social_platform_handle': patreon_handle,
                'patreon_link': patreon_url,
                
                # Bio/Description
                'patreon_description': profile.get('about', ''),
                
                # Patreon-specific metrics
                'patreon_patron_count': profile.get('patron_count', 0),
                'patreon_paid_members': profile.get('paid_members', 0),
                'patreon_total_members': profile.get('total_members', 0),
                'patreon_post_count': profile.get('post_count', 0),
                'patreon_earnings_per_month': profile.get('earnings_per_month', 0),
                'patreon_creation_name': profile.get('creation_name', ''),
                
                # Social links (URLs, not usernames)
                'instagram_handle': profile.get('instagram'),  # Full URL
                'youtube_profile_link': profile.get('youtube'),
                'tiktok_handle': profile.get('tiktok'),  # Full URL
                'facebook_profile_link': profile.get('facebook'),
                'twitch_url': profile.get('twitch'),
                
                # Metadata
                'platform': 'patreon',
                'discovery_source': 'apify_patreon_scraper'
            }
            
            # Remove None/empty values
            standardized_profile = {k: v for k, v in standardized_profile.items() 
                                   if v is not None and v != '' and v != 0}
            
            standardized.append(standardized_profile)
            
        except Exception as e:
            print(f"Failed to process Patreon profile #{i+1}: {e}")
            continue
    
    print(f"Successfully processed {len(standardized)} Patreon profiles (NSFW profiles excluded)")
    return standardized

def update_discovery_job_status(job_id, status, **kwargs):
    """Update discovery job status in Redis"""
    try:
        import redis
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        r = redis.from_url(redis_url, decode_responses=True)
        
        job_key = f'discovery_job:{job_id}'
        
        job_data = r.get(job_key)
        if job_data:
            job_data = json.loads(job_data)
        else:
            job_data = {'job_id': job_id}
        
        job_data['status'] = status
        job_data['updated_at'] = datetime.now().isoformat()
        job_data.update(kwargs)
        
        r.setex(job_key, 86400, json.dumps(job_data))
        
        print(f"Job {job_id} status updated: {status}")
    except Exception as e:
        print(f"Failed to update job status: {e}")


def import_profiles_to_hubspot(profiles, job_id):
    """Import discovered profiles to HubSpot via batch API"""
    if not HUBSPOT_API_KEY:
        raise ValueError("HUBSPOT_API_KEY must be set in environment")
    
    contacts = []
    
    print(f"Preparing {len(profiles)} profiles for HubSpot import")
    
    for idx, profile in enumerate(profiles):
        # Map discovery fields to HubSpot properties
        properties = {
            # Core identity
            'first_and_last_name': profile.get('first_and_last_name', ''),
            'flagship_social_platform_handle': profile.get('flagship_social_platform_handle', ''),
            'instagram_handle': profile.get('instagram_handle', ''),
            'instagram_bio': profile.get('instagram_bio', ''),
            
            # Metrics
            'instagram_followers': profile.get('instagram_followers', 0),
            'average_engagement': profile.get('average_engagement', 0),
            
            # Contact info
            'email': profile.get('email'),
            'phone': profile.get('phone'),
            'tiktok_handle': profile.get('tiktok_handle'),
            'youtube_profile_link': profile.get('youtube_profile_link'),
            'facebook_profile_link': profile.get('facebook_profile_link'),
            'patreon_link': profile.get('patreon_link'),
            'pinterest_profile_link': profile.get('pinterest_profile_link'),
            
            # Location
            'city': profile.get('city'),
            'state': profile.get('state'),
            'country': profile.get('country'),
            
            # Discovery metadata
            'enrichment_status': 'pending',
        }
        
        # Remove None values (HubSpot API doesn't like them)
        properties = {k: v for k, v in properties.items() if v is not None and v != ''}
        
        contacts.append({
            'properties': properties,
            'objectWriteTraceId': f"{job_id}_{idx}"
        })
    
    # Batch import (max 100 per request)
    created_count = 0
    skipped_count = 0
    total_batches = (len(contacts) + 99) // 100
    
    print(f"Importing in {total_batches} batches...")
    
    for i in range(0, len(contacts), 100):
        batch = contacts[i:i+100]
        batch_num = (i // 100) + 1
        
        try:
            print(f"Importing batch {batch_num}/{total_batches} ({len(batch)} contacts)...")
            
            response = requests.post(
                f"{HUBSPOT_API_URL}/crm/v3/objects/contacts/batch/create",
                headers={
                    'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                    'Content-Type': 'application/json'
                },
                json={'inputs': batch},
                timeout=30
            )
            
            if response.status_code == 201:
                # All created successfully
                created_count += len(batch)
                print(f"Batch {batch_num}: {len(batch)} contacts created")
                
            elif response.status_code == 207:
                # Multi-status: some created, some duplicates
                result = response.json()
                batch_created = len(result.get('results', []))
                batch_errors = result.get('errors', [])
                batch_skipped = len(batch_errors)
                
                created_count += batch_created
                skipped_count += batch_skipped
                
                print(f"Batch {batch_num}: {batch_created} created, {batch_skipped} duplicates/errors")
                
                # Log first few errors for debugging
                for error in batch_errors[:3]:
                    print(f"Error: {error.get('message', 'Unknown error')}")
                
            else:
                # Error
                print(f"Batch import error: {response.status_code} - {response.text}")
                skipped_count += len(batch)
        
        except Exception as e:
            print(f"Exception importing batch {batch_num}: {e}")
            skipped_count += len(batch)
        
        # Small delay between batches to avoid rate limits
        if i + 100 < len(contacts):
            time.sleep(0.5)
    
    print(f"Import complete: {created_count} created, {skipped_count} skipped/errors")
    
    return {
        'created': created_count,
        'skipped': skipped_count
    }
class ApolloEnrichment:
    """Apollo.io API client for finding emails"""
    
    BASE_URL = "https://api.apollo.io/api/v1"
    
    SKIP_DOMAINS = [
        "meetup.com", "eventbrite.com", "youtube.com", "youtu.be",
        "reddit.com", "facebook.com", "instagram.com", "twitter.com",
        "x.com", "linkedin.com", "patreon.com", "tiktok.com",
        "google.com", "yelp.com", "substack.com", "discord.com",
        "discord.gg", "github.com", "medium.com"
    ]
    
    def __init__(self, api_key: str):
        self.api_key = api_key
    
    def person_match(self, name: str = None, domain: str = None, 
                    org_name: str = None, linkedin_url: str = None) -> Dict:
        """Find email and profile info for a person"""
        if not self.api_key:
            return None
        
        data = {'reveal_personal_emails': True}
        if name:
            data['name'] = name
        if domain:
            data['domain'] = domain
        if org_name:
            data['organization_name'] = org_name
        if linkedin_url:
            data['linkedin_url'] = linkedin_url
        
        try:
            response = requests.post(
                f"{self.BASE_URL}/people/match",
                headers={
                    'Content-Type': 'application/json',
                    'x-api-key': self.api_key,
                    'Cache-Control': 'no-cache'
                },
                json=data,
                timeout=15
            )
            
            if response.status_code == 429:
                print("[APOLLO] Rate limited")
                time.sleep(2)
                return None
            
            if not response.ok:
                print(f"[APOLLO] Error {response.status_code}")
                return None
            
            result = response.json()
            person = result.get('person', {})
            
            if not person:
                return None
            
            phone_numbers = person.get('phone_numbers', [])
            phone = phone_numbers[0].get('raw_number', '') if phone_numbers else ''
            
            location_parts = [person.get('city'), person.get('state'), person.get('country')]
            location = ', '.join(filter(None, location_parts))
            
            return {
                'email': person.get('email', ''),
                'first_name': person.get('first_name', ''),
                'last_name': person.get('last_name', ''),
                'full_name': person.get('name', ''),
                'title': person.get('title', ''),
                'linkedin': person.get('linkedin_url', ''),
                'twitter': person.get('twitter_url', ''),
                'facebook': person.get('facebook_url', ''),
                'phone': phone,
                'location': location,
                'headline': person.get('headline', ''),
                'organization': person.get('organization', {}).get('name', ''),
                'org_phone': person.get('organization', {}).get('phone', '')
            }
            
        except Exception as e:
            print(f"[APOLLO] Error: {e}")
            return None
    
    @staticmethod
    def extract_domain(url: str) -> str:
        """Extract domain from URL"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.replace('www.', '')
            return domain
        except:
            return ""
    
    @staticmethod
    def is_enrichable_domain(domain: str) -> bool:
        """Check if domain should be enriched (not social media)"""
        if not domain:
            return False
        return not any(skip in domain for skip in ApolloEnrichment.SKIP_DOMAINS)


# ============================================================================
# SOCIAL GRAPH BUILDER
# ============================================================================

class SocialGraphBuilder:
    """Crawls websites and link aggregators to find emails and social profiles"""
    
    LINK_AGGREGATORS = [
        'linktr.ee', 'beacons.ai', 'linkin.bio', 'linkpop.com',
        'hoo.be', 'campsite.bio', 'lnk.bio', 'tap.bio', 'solo.to',
        'bio.link', 'carrd.co'
    ]
    
    SOCIAL_PATTERNS = {
        'instagram': r'instagram\.com/([a-zA-Z0-9._]+)',
        'youtube': r'youtube\.com/(c/|channel/|@)?([a-zA-Z0-9_-]+)',
        'twitter': r'(twitter\.com|x\.com)/([a-zA-Z0-9_]+)',
        'linkedin': r'linkedin\.com/in/([a-zA-Z0-9-]+)',
        'tiktok': r'tiktok\.com/@([a-zA-Z0-9._]+)',
        'facebook': r'facebook\.com/([a-zA-Z0-9.]+)',
        'twitch': r'twitch\.tv/([a-zA-Z0-9_]+)',
        'discord': r'discord\.(gg|com)/([a-zA-Z0-9]+)'
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def build_graph(self, website_url: str, leader_name: str = None) -> Dict:
        """Build social graph from website"""
        result = {
            'emails': [],
            'social_links': {},
            'personal_website': None,
            'linktree_url': None
        }
        
        if not website_url:
            return result
        
        try:
            is_aggregator = any(agg in website_url.lower() for agg in self.LINK_AGGREGATORS)
            
            if is_aggregator:
                result['linktree_url'] = website_url
                agg_data = self._scrape_link_aggregator(website_url)
                result['social_links'].update(agg_data['social_links'])
                result['emails'].extend(agg_data['emails'])
                
                if agg_data.get('personal_website'):
                    result['personal_website'] = agg_data['personal_website']
                    website_data = self._scrape_website(agg_data['personal_website'], leader_name)
                    result['emails'].extend(website_data['emails'])
                    result['social_links'].update(website_data['social_links'])
            else:
                website_data = self._scrape_website(website_url, leader_name)
                result['emails'].extend(website_data['emails'])
                result['social_links'].update(website_data['social_links'])
                
                if website_data.get('linktree_url'):
                    result['linktree_url'] = website_data['linktree_url']
                    agg_data = self._scrape_link_aggregator(website_data['linktree_url'])
                    result['social_links'].update(agg_data['social_links'])
            
            result['emails'] = list(set(result['emails']))
            
            return result
            
        except Exception as e:
            print(f"[SOCIAL_GRAPH] Error building graph: {e}")
            return result
    
    def _scrape_website(self, url: str, leader_name: str = None) -> Dict:
        """Scrape a regular website for emails and social links"""
        result = {
            'emails': [],
            'social_links': {},
            'linktree_url': None
        }
        
        try:
            response = self.session.get(url, timeout=10, allow_redirects=True)
            if not response.ok:
                return result
            
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.get_text()
            
            # Extract emails
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = re.findall(email_pattern, text)
            
            blocked_patterns = ['.png', '.jpg', '.gif', '.jpeg', '.webp', '.svg',
                              'sentry.io', 'example.com', 'cloudfront', 'amazonaws']
            result['emails'] = [
                email.lower() for email in emails
                if not any(pattern in email.lower() for pattern in blocked_patterns)
            ]
            
            # Extract obfuscated emails
            obfuscated_pattern = r'([a-zA-Z0-9._%+-]+)\s*(?:\[at\]|\(at\)|{at}|\bat\b)\s*([a-zA-Z0-9.-]+)\s*(?:\[dot\]|\(dot\)|{dot}|\.)\s*([a-zA-Z]{2,})'
            obfuscated = re.findall(obfuscated_pattern, text, re.IGNORECASE)
            for parts in obfuscated:
                email = f"{parts[0]}@{parts[1]}.{parts[2]}".lower().replace(' ', '')
                if '@' in email:
                    result['emails'].append(email)
            
            # Extract mailto links
            mailto_links = soup.find_all('a', href=re.compile(r'^mailto:'))
            for link in mailto_links:
                email = link['href'].replace('mailto:', '').split('?')[0].strip().lower()
                if email and '@' in email:
                    result['emails'].append(email)
            
            # Extract social links
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link['href']
                
                if any(agg in href.lower() for agg in self.LINK_AGGREGATORS):
                    result['linktree_url'] = href
                
                for platform, pattern in self.SOCIAL_PATTERNS.items():
                    if platform in href.lower():
                        match = re.search(pattern, href, re.IGNORECASE)
                        if match:
                            result['social_links'][platform] = href.split('?')[0]
            
            return result
            
        except Exception as e:
            print(f"[SOCIAL_GRAPH] Error scraping website {url}: {e}")
            return result
    
    def _scrape_link_aggregator(self, url: str) -> Dict:
        """Scrape Linktree/Beacons for social links"""
        result = {
            'social_links': {},
            'emails': [],
            'personal_website': None
        }
        
        try:
            response = self.session.get(url, timeout=10, allow_redirects=True)
            if not response.ok:
                return result
            
            soup = BeautifulSoup(response.text, 'html.parser')
            all_links = soup.find_all('a', href=True)
            
            skip_hosts = ['instagram.com', 'youtube.com', 'twitter.com', 'x.com',
                         'facebook.com', 'tiktok.com', 'linkedin.com', 'twitch.tv',
                         'patreon.com', 'spotify.com', 'apple.com', 'google.com']
            
            for link in all_links:
                href = link['href']
                
                for platform, pattern in self.SOCIAL_PATTERNS.items():
                    if platform in href.lower():
                        match = re.search(pattern, href, re.IGNORECASE)
                        if match:
                            result['social_links'][platform] = href.split('?')[0]
                
                if href.startswith('http') and not any(skip in href.lower() for skip in skip_hosts):
                    if not result['personal_website']:
                        result['personal_website'] = href
            
            return result
            
        except Exception as e:
            print(f"[SOCIAL_GRAPH] Error scraping aggregator {url}: {e}")
            return result
