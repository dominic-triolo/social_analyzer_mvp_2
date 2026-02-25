import os
import json
import requests
import tempfile
import base64
import hashlib
import time
from typing import Dict, List, Any, Tuple, Optional
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

# BDR Round-Robin assignment map  (name → HubSpot email value)
BDR_OWNER_IDS = {
    'Miriam Plascencia':   '83266567',
    'Majo Juarez':         '79029958',
    'Nicole Roma':         '83266570',
    'Salvatore Renteria':  '81500975',
    'Sofia Gonzalez':      '79029956',
    'Tanya Pina':          '83266565',
}

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
    elif full_score >= 0.49:
        priority_tier = "auto_enroll"
        expected_precision = 0.705
        tier_reasoning = "Full score ≥0.80 (70% precision)"
    elif full_score >= 0.25:
        priority_tier = "standard_priority_review"
        expected_precision = 0.681
        tier_reasoning = "Full score ≥0.45 (68% precision)"
    else:
        priority_tier = "auto_enroll"
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
    def _full_name_fallback() -> str:
        """Return first token of full_name, first token of username, or 'there'."""
        if full_name and full_name.strip():
            first = full_name.strip().split(' ')[0]
            if first:
                return first
        # Try to derive something human-readable from the username
        # e.g. "morgandrinkscoffee" → "morgandrinkscoffee" (still better than 'there')
        if username and username.strip():
            return username.strip().lstrip('@')
        return "there"

    if not client:
        print("OpenAI client not initialized, using full_name fallback")
        return _full_name_fallback()

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
        
        # If empty or a known non-answer, fall back to full_name
        if not first_names or first_names.lower() in ['', 'none', 'unknown', 'n/a', 'not provided', 'there']:
            first_names = _full_name_fallback()

        print(f"[FIRST_NAME] @{username} → '{first_names}'")

        return first_names

    except Exception as e:
        print(f"[FIRST_NAME] Error for @{username}: {e}")
        return _full_name_fallback()

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
        "items_analyzed": len(content_analyses),
    }

    # BDR: auto_enroll contacts are handled directly and don't need a BDR owner.
    # Clear the value that was pre-assigned at import time.
    # standard/low_priority contacts keep their pre-assigned BDR (already on the contact).
    if priority_tier == "auto_enroll":
        payload["bdr_"] = ""
    
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
            # Derive best available first name before early exit
            _early_profile = social_data.get('data', [{}])[0].get('profile', {})
            _early_full_name = (_early_profile.get('full_name', '')
                                or _early_profile.get('fullName', '')
                                or _early_profile.get('name', '')).strip()
            _early_username = _early_profile.get('platform_username', '') or profile_url.rstrip('/').split('/')[-1]
            _early_first_name = (_early_full_name.split(' ')[0] if _early_full_name else None) or _early_username or 'there'
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
                first_name=_early_first_name
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
            # Derive best available first name before early exit
            _ps_full_name = (profile_info.get('full_name', '')
                             or profile_info.get('fullName', '')
                             or profile_info.get('name', '')).strip()
            _ps_username = profile_data.get('username', '') or profile_url.rstrip('/').split('/')[-1]
            _ps_first_name = (_ps_full_name.split(' ')[0] if _ps_full_name else None) or _ps_username or 'there'
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
                first_name=_ps_first_name
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
        
        # Profile lives at data[0].profile (same structure used by create_profile_snapshot above)
        _profile_info = social_data.get('data', [{}])[0].get('profile', {})
        ig_username = (_profile_info.get('platform_username', '')
                       or _profile_info.get('username', '')
                       or profile_url.rstrip('/').split('/')[-1])
        # InsightIQ uses snake_case on some endpoints, camelCase on others
        ig_full_name = (_profile_info.get('full_name', '')
                        or _profile_info.get('fullName', '')
                        or _profile_info.get('name', ''))
        ig_bio = bio if bio else (_profile_info.get('introduction', '')
                                  or _profile_info.get('biography', '')
                                  or _profile_info.get('bio', ''))
        
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

        # Bio phrase filtering (Instagram only) — API treats these as mutually exclusive:
        # send bio_phrase_advanced if advanced clauses are present, otherwise bio_phrase.
        bio_phrase = (user_filters.get('bio_phrase') or '').strip()
        bio_phrase_advanced = user_filters.get('bio_phrase_advanced') or []

        if bio_phrase_advanced and isinstance(bio_phrase_advanced, list):
            # Sanitise: keep only well-formed entries, cap at 14
            valid_actions = {'AND', 'OR', 'NOT'}
            cleaned = [
                {'bio_phrase': str(e['bio_phrase']).strip(), 'action': e['action']}
                for e in bio_phrase_advanced
                if isinstance(e, dict)
                and e.get('bio_phrase', '').strip()
                and e.get('action') in valid_actions
            ][:14]
            if cleaned:
                parameters['bio_phrase_advanced'] = cleaned
                print(f"Bio phrase advanced: {len(cleaned)} clause(s)")
        elif bio_phrase:
            parameters['bio_phrase'] = bio_phrase
            print(f"Bio phrase: {bio_phrase}")

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
                    
                    # Metadata / channel tracking
                    'flagship_social_platform': 'instagram',
                    'channel':                  'Outbound',
                    'channel_host_prospected':  'Phyllo',
                    'funnel':                   'Creator',

                    # Triggers the HubSpot workflow → /api/webhook/enrich → AI scoring
                    'enrichment_status': 'pending',
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
# BDR Round-Robin Helper
# ============================================================================

def assign_bdr_round_robin(profiles: List[Dict], bdr_names: List[str]) -> List[Dict]:
    """
    Assign bdr_ (HubSpot owner ID) to each profile in round-robin order.

    Only names present in BDR_OWNER_IDS are used; unrecognised names are silently
    skipped so a bad frontend value can never crash the pipeline.

    Args:
        profiles:  List of profile dicts (modified in-place AND returned).
        bdr_names: Ordered list of BDR display names selected by the user.

    Returns:
        The same profiles list with 'bdr_' set on every item.
    """
    owner_ids = [BDR_OWNER_IDS[n] for n in bdr_names if n in BDR_OWNER_IDS]
    if not owner_ids:
        print("[BDR] No valid BDR names supplied – skipping round-robin assignment")
        return profiles
    for i, profile in enumerate(profiles):
        profile['bdr_'] = owner_ids[i % len(owner_ids)]
        # Mark for BDR review — cleared later for auto_enroll contacts by send_to_hubspot
        profile['lead_list_fit'] = 'Not_reviewed'
    print(f"[BDR] Assigned {len(owner_ids)} BDR(s) round-robin across {len(profiles)} profiles")
    return profiles


# ============================================================================
# Discovery Tasks
# ============================================================================

@celery_app.task(name='tasks.discover_instagram_profiles', time_limit=7200, soft_time_limit=7100)
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

        # BDR round-robin assignment (pre-assign before HubSpot import;
        # send_to_hubspot clears bdr_ for auto_enroll contacts after scoring)
        bdr_names = user_filters.get('bdr_names', list(BDR_OWNER_IDS.keys()))
        profiles = assign_bdr_round_robin(profiles, bdr_names)

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


# ============================================================================
# ENVIRONMENT - COMMUNITY DISCOVERY & ENRICHMENT
# ============================================================================

MILLIONVERIFIER_API_KEY = os.getenv('MILLIONVERIFIER_API_KEY')

# ============================================================================
# DISCOVERY TASK: PATREON
# ============================================================================

@celery_app.task(name='tasks.discover_patreon_profiles', time_limit=7200, soft_time_limit=7100)
def discover_patreon_profiles(user_filters=None, job_id=None):
    """
    Run Patreon profile discovery via Apify scraper with full enrichment pipeline.

    Pipeline:
      1. Apify Patreon scraper → raw profiles
      2. NSFW filter
      3. enrich_profiles_full_pipeline (social graph → Apollo → Leads Finder → MillionVerifier)
      4. standardize_patreon_profiles → HubSpot import
    """
    if job_id is None:
        job_id = discover_patreon_profiles.request.id

    try:
        update_discovery_job_status(job_id, status='discovering')

        if not APIFY_API_TOKEN:
            raise ValueError("APIFY_API_TOKEN must be set in environment")

        user_filters = user_filters or {}
        search_keywords = user_filters.get('search_keywords', [])
        max_results     = user_filters.get('max_results', 100)
        location        = (user_filters.get('location') or 'United States').strip()
        min_patrons     = int(user_filters.get('min_patrons') or 0)
        max_patrons     = int(user_filters.get('max_patrons') or 0)
        min_posts       = int(user_filters.get('min_posts') or 0)

        if not search_keywords:
            raise ValueError("search_keywords required for Patreon discovery")

        # Append location to each keyword so the Apify actor surfaces geo-relevant results
        if location:
            search_queries = [f"{kw} {location}" for kw in search_keywords]
        else:
            search_queries = list(search_keywords)

        print(f"Starting Patreon discovery: queries={search_queries}, max={max_results}, "
              f"min_patrons={min_patrons}, max_patrons={max_patrons}, min_posts={min_posts}")

        from apify_client import ApifyClient
        apify = ApifyClient(APIFY_API_TOKEN)

        run_input = {
            "searchQueries": search_queries,
            "maxRequestsPerCrawl": max_results,
            "proxyConfiguration": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"],
            },
            "maxConcurrency": 1,
            "maxRequestRetries": 5,
            "requestHandlerTimeoutSecs": 180,
        }

        print("Starting Apify Patreon scraper (may take a few minutes)...")
        update_job_stage(job_id, 'discovery', 'running')
        run = apify.actor("mJiXU9PT4eLHuY0pi").call(run_input=run_input)
        print(f"Apify run complete: {run['id']}")

        all_items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"Apify returned {len(all_items)} total items")
        update_job_stage(job_id, 'discovery', 'completed', raw_results=len(all_items))

        # Filter NSFW
        update_job_stage(job_id, 'filtering', 'running')
        profiles = []
        nsfw_count = 0
        for item in all_items:
            if item.get('is_nsfw', 0) == 1:
                nsfw_count += 1
                continue
            profiles.append(item)

        print(f"After NSFW filter: {len(profiles)} profiles (excluded {nsfw_count} NSFW)")

        # Apply patron count filter
        patron_filtered = 0
        if min_patrons > 0 or max_patrons > 0:
            before = len(profiles)
            filtered = []
            for p in profiles:
                patron_count = int(p.get('patron_count') or p.get('total_members') or 0)
                if min_patrons > 0 and patron_count < min_patrons:
                    continue
                if max_patrons > 0 and patron_count > max_patrons:
                    continue
                filtered.append(p)
            profiles = filtered
            patron_filtered = before - len(profiles)
            print(f"After patron count filter ({min_patrons}-{max_patrons}): {len(profiles)} profiles "
                  f"(excluded {patron_filtered})")

        # Apply minimum posts filter
        posts_filtered = 0
        if min_posts > 0:
            before = len(profiles)
            profiles = [
                p for p in profiles
                if int(p.get('post_count') or p.get('total_posts') or p.get('posts_count') or 0) >= min_posts
            ]
            posts_filtered = before - len(profiles)
            print(f"After min_posts filter (>={min_posts}): {len(profiles)} profiles "
                  f"(excluded {posts_filtered})")

        update_job_stage(job_id, 'filtering', 'completed',
                         profiles_passed=len(profiles),
                         nsfw_removed=nsfw_count,
                         patron_filtered=patron_filtered,
                         posts_filtered=posts_filtered)

        if not profiles:
            warning_msg = (
                "Apify scraper returned 0 results. The scraper typically has ~5% yield. "
                "Try max_results of 100-500 to get 5-25 actual profiles."
                if not all_items else
                f"All {len(all_items)} profiles were NSFW. Try different keywords."
            )
            print(f"Warning: {warning_msg}")
            update_discovery_job_status(job_id, status='completed', profiles_found=0,
                                        new_contacts_created=0, duplicates_skipped=0)
            return {'status': 'completed', 'profiles_found': 0,
                    'new_contacts': 0, 'duplicates': 0, 'warning': warning_msg}

        # Normalise raw Patreon fields into the common internal schema used by enrichment:
        #   url, creator_name, personal_website, instagram_url, youtube_url,
        #   twitter_url, facebook_url, tiktok_url, twitch_url
        for p in profiles:
            p.setdefault('instagram_url', p.get('instagram'))
            p.setdefault('youtube_url',   p.get('youtube'))
            p.setdefault('twitter_url',   p.get('twitter'))
            p.setdefault('facebook_url',  p.get('facebook'))
            p.setdefault('tiktok_url',    p.get('tiktok'))
            p.setdefault('twitch_url',    p.get('twitch'))

        print(f"Patreon discovery complete: {len(profiles)} profiles")
        update_discovery_job_status(job_id, status='enriching', profiles_found=len(profiles))

        # Full enrichment pipeline
        enriched = enrich_profiles_full_pipeline(profiles, job_id, platform='patreon')

        # Standardise → BDR round-robin → HubSpot
        update_discovery_job_status(job_id, status='importing')
        update_job_stage(job_id, 'hubspot_import', 'running')
        standardized = standardize_patreon_profiles(enriched)
        bdr_names = user_filters.get('bdr_names', list(BDR_OWNER_IDS.keys()))
        standardized = assign_bdr_round_robin(standardized, bdr_names)
        import_results = import_profiles_to_hubspot(standardized, job_id)
        update_job_stage(job_id, 'hubspot_import', 'completed',
                         created=import_results['created'],
                         skipped=import_results['skipped'])

        update_discovery_job_status(
            job_id, status='completed',
            profiles_found=len(profiles),
            new_contacts_created=import_results['created'],
            duplicates_skipped=import_results['skipped'],
        )
        print(f"Patreon job {job_id} done: {import_results['created']} created, "
              f"{import_results['skipped']} skipped")

        return {
            'status': 'completed',
            'profiles_found': len(profiles),
            'new_contacts': import_results['created'],
            'duplicates': import_results['skipped'],
        }

    except Exception as e:
        print(f"Patreon discovery failed: {e}")
        import traceback
        traceback.print_exc()
        update_discovery_job_status(job_id, status='failed', error=str(e))
        raise


# ============================================================================
# DISCOVERY TASK: FACEBOOK GROUPS
# ============================================================================

@celery_app.task(name='tasks.discover_facebook_groups', time_limit=7200, soft_time_limit=7100)
def discover_facebook_groups(user_filters=None, job_id=None):
    """
    Discover Facebook Groups via Google Search Scraper (Apify) + full enrichment.

    Pipeline:
      1. Expand keywords → Google queries (site:facebook.com/groups "kw")
      2. Run Apify google-search-scraper
      3. Parse FB group URLs, names, member counts
      4. enrich_profiles_full_pipeline
      5. standardize_facebook_profiles → HubSpot import
    """
    if job_id is None:
        job_id = discover_facebook_groups.request.id

    try:
        update_discovery_job_status(job_id, status='discovering')

        if not APIFY_API_TOKEN:
            raise ValueError("APIFY_API_TOKEN must be set")

        user_filters = user_filters or {}
        keywords            = user_filters.get('keywords', [])
        max_results         = user_filters.get('max_results', 100)
        min_members         = int(user_filters.get('min_members') or 0)
        max_members         = int(user_filters.get('max_members') or 0)
        visibility          = user_filters.get('visibility', 'all')   # 'all'|'public'|'private'
        min_posts_per_month = int(user_filters.get('min_posts_per_month') or 0)

        if not keywords:
            raise ValueError("keywords required for Facebook Groups discovery")

        print(f"Facebook Groups discovery: keywords={keywords}, max={max_results}, "
              f"visibility={visibility}, min_posts/month={min_posts_per_month}")

        # Expand keywords into Google queries, optionally scoping by visibility
        vis_suffix = ''
        if visibility == 'public':
            vis_suffix = ' "public group"'
        elif visibility == 'private':
            vis_suffix = ' "private group"'

        google_queries = []
        for kw in keywords:
            google_queries.append(f'site:facebook.com/groups "{kw}"{vis_suffix}')
            google_queries.append(f'site:facebook.com/groups {kw} community{vis_suffix}')
            google_queries.append(f'site:facebook.com/groups {kw} group{vis_suffix}')

        # Cap at 15 queries to keep cost reasonable
        google_queries = google_queries[:15]
        print(f"Running {len(google_queries)} Google queries via Apify")

        from apify_client import ApifyClient
        apify = ApifyClient(APIFY_API_TOKEN)

        run_input = {
            'queries':           '\n'.join(google_queries),
            'maxPagesPerQuery':  5,
            'resultsPerPage':    20,
            'countryCode':       'us',
            'languageCode':      'en',
            'mobileResults':     False,
        }

        update_job_stage(job_id, 'discovery', 'running')
        run = apify.actor("apify~google-search-scraper").call(run_input=run_input)
        items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"Google search complete: {len(items)} result pages")
        update_job_stage(job_id, 'discovery', 'completed',
                         queries_run=len(google_queries), result_pages=len(items))

        # Parse results + filter into profile dicts
        update_job_stage(job_id, 'filtering', 'running')
        profiles = []
        seen_urls: set = set()

        for item in items:
            if len(profiles) >= max_results:
                break
            for result in item.get('organicResults', []):
                if len(profiles) >= max_results:
                    break

                url = result.get('url', '')
                if 'facebook.com/groups/' not in url:
                    continue

                group_url = _extract_facebook_group_url(url)
                if group_url in seen_urls:
                    continue
                seen_urls.add(group_url)

                title   = result.get('title', '')
                snippet = result.get('description', '')

                group_name = (title
                              .replace(' | Facebook', '')
                              .replace(' - Facebook', '')
                              .strip())

                member_count = _extract_member_count(f"{title} {snippet}")

                if min_members > 0 and 0 < member_count < min_members:
                    continue
                if max_members > 0 and member_count > max_members:
                    continue

                # Visibility filter — cross-check snippet text
                combined_text = (title + ' ' + snippet).lower()
                if visibility == 'public' and 'private group' in combined_text:
                    continue
                if visibility == 'private' and 'public group' in combined_text:
                    continue

                # Best-effort posts/month extraction from snippet
                posts_per_month = _extract_posts_per_month(snippet)
                if min_posts_per_month > 0 and posts_per_month is not None and posts_per_month < min_posts_per_month:
                    continue

                profiles.append({
                    'group_name':        group_name,
                    'group_url':         group_url,
                    'description':       snippet[:2000],
                    'member_count':      member_count,
                    'posts_per_month':   posts_per_month,
                    # Fields expected by enrichment pipeline
                    'url':               group_url,   # used by social graph builder
                    'creator_name':      '',          # populated by Apollo if found
                    'instagram_url':     None,
                    'youtube_url':       None,
                    'twitter_url':       None,
                    'facebook_url':      group_url,
                    'tiktok_url':        None,
                    'personal_website':  None,
                })

        print(f"Facebook Groups discovery: {len(profiles)} groups found")
        update_job_stage(job_id, 'filtering', 'completed', profiles_passed=len(profiles))
        update_discovery_job_status(job_id, status='enriching', profiles_found=len(profiles))

        # Full enrichment pipeline
        enriched = enrich_profiles_full_pipeline(profiles, job_id, platform='facebook_groups')

        # Standardise → BDR round-robin → HubSpot
        update_discovery_job_status(job_id, status='importing')
        update_job_stage(job_id, 'hubspot_import', 'running')
        standardized = standardize_facebook_profiles(enriched)
        bdr_names = user_filters.get('bdr_names', list(BDR_OWNER_IDS.keys()))
        standardized = assign_bdr_round_robin(standardized, bdr_names)
        import_results = import_profiles_to_hubspot(standardized, job_id)
        update_job_stage(job_id, 'hubspot_import', 'completed',
                         created=import_results['created'],
                         skipped=import_results['skipped'])

        update_discovery_job_status(
            job_id, status='completed',
            profiles_found=len(profiles),
            new_contacts_created=import_results['created'],
            duplicates_skipped=import_results['skipped'],
        )
        print(f"Facebook job {job_id} done: {import_results['created']} created, "
              f"{import_results['skipped']} skipped")

        return {
            'status': 'completed',
            'profiles_found': len(profiles),
            'new_contacts': import_results['created'],
            'duplicates': import_results['skipped'],
        }

    except Exception as e:
        print(f"Facebook Groups discovery failed: {e}")
        import traceback
        traceback.print_exc()
        update_discovery_job_status(job_id, status='failed', error=str(e))
        raise


# ============================================================================
# APOLLO.IO ENRICHMENT CLIENT
# ============================================================================

class ApolloEnrichment:
    """
    Apollo.io API client for professional email lookup.

    Strategy (matching colleague's implementation):
      - Attempt 1: full query (name, domain, org, linkedin)
      - Attempt 2 (if first fails and name looks like a real person):
          firstName + lastName + domain only
      - 300 ms delay between attempts
      - Input dedup via SHA-256 hash
    """

    BASE_URL = "https://api.apollo.io/api/v1"

    # Domains that are never useful for Apollo lookup
    SKIP_DOMAINS = {
        "meetup.com", "eventbrite.com", "youtube.com", "youtu.be",
        "reddit.com", "facebook.com", "instagram.com", "twitter.com",
        "x.com", "linkedin.com", "patreon.com", "tiktok.com",
        "google.com", "yelp.com", "tripadvisor.com", "wikipedia.org",
        "amazon.com", "substack.com", "discord.com", "discord.gg",
        "github.com", "medium.com",
    }

    # Name must look like a real human name (letters, spaces, hyphens, ≥2 chars)
    _VALID_NAME_RE = re.compile(r'^[A-Za-z\s\-]{2,}$')

    def __init__(self, api_key: str):
        self.api_key = api_key

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def person_match(self, name: str = None, domain: str = None,
                     org_name: str = None, linkedin_url: str = None) -> Optional[Dict]:
        """
        Find email + contact info for a person.

        Returns dict with: email, first_name, last_name, full_name, title,
        linkedin, twitter, facebook, phone, location, headline, organization.
        Returns None if nothing found.
        """
        if not self.api_key:
            return None

        # Attempt 1: full query
        result = self._call_match(
            name=name, domain=domain,
            org_name=org_name, linkedin_url=linkedin_url
        )
        if result and result.get('email'):
            return result

        # Attempt 2: simplified (firstName + lastName + domain)
        if name and domain and self._is_valid_candidate(name):
            time.sleep(0.3)
            parts = name.strip().split()
            if len(parts) >= 2:
                result2 = self._call_match(
                    first_name=parts[0],
                    last_name=' '.join(parts[1:]),
                    domain=domain,
                )
                if result2 and result2.get('email'):
                    return result2

        return result  # return whatever we got (may have linkedin even without email)

    @staticmethod
    def extract_domain(url: str) -> str:
        """Extract bare domain from URL, e.g. 'example.com'."""
        try:
            return urlparse(url).netloc.replace('www.', '').lower()
        except Exception:
            return ''

    @staticmethod
    def is_enrichable_domain(domain: str) -> bool:
        """Return True if domain is not a social platform / known skip domain."""
        if not domain:
            return False
        return not any(skip in domain for skip in ApolloEnrichment.SKIP_DOMAINS)

    @staticmethod
    def make_input_hash(**kwargs) -> str:
        """SHA-256 hash of Apollo query params for dedup."""
        payload = json.dumps(kwargs, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    @staticmethod
    def _is_valid_candidate(name: str) -> bool:
        return bool(ApolloEnrichment._VALID_NAME_RE.match(name.strip()))

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _call_match(self, name=None, first_name=None, last_name=None,
                    domain=None, org_name=None, linkedin_url=None) -> Optional[Dict]:
        """Single Apollo /people/match call."""
        data: Dict = {'reveal_personal_emails': True}
        if name:        data['name']              = name
        if first_name:  data['first_name']        = first_name
        if last_name:   data['last_name']         = last_name
        if domain:      data['domain']            = domain
        if org_name:    data['organization_name'] = org_name
        if linkedin_url: data['linkedin_url']     = linkedin_url

        try:
            resp = requests.post(
                f"{self.BASE_URL}/people/match",
                headers={
                    'Content-Type':  'application/json',
                    'x-api-key':     self.api_key,
                    'Cache-Control': 'no-cache',
                },
                json=data,
                timeout=15,
            )

            if resp.status_code == 429:
                print("[APOLLO] Rate limited — backing off 2s")
                time.sleep(2)
                return None
            if resp.status_code in (401, 403):
                print(f"[APOLLO] Auth error ({resp.status_code})")
                return None
            if resp.status_code == 422:
                print("[APOLLO] Unprocessable (422) — invalid params")
                return None
            if not resp.ok:
                print(f"[APOLLO] Error {resp.status_code}")
                return None

            person = resp.json().get('person') or {}
            if not person:
                return None

            # Extract phone
            phones = person.get('phone_numbers') or []
            phone = phones[0].get('raw_number', '') if phones else ''

            # Build location
            loc_parts = [person.get('city'), person.get('state'), person.get('country')]
            location = ', '.join(p for p in loc_parts if p)

            return {
                'email':        person.get('email', ''),
                'first_name':   person.get('first_name', ''),
                'last_name':    person.get('last_name', ''),
                'full_name':    person.get('name', ''),
                'title':        person.get('title', ''),
                'linkedin':     person.get('linkedin_url', ''),
                'twitter':      person.get('twitter_url', ''),
                'facebook':     person.get('facebook_url', ''),
                'phone':        phone,
                'location':     location,
                'headline':     person.get('headline', ''),
                'organization': (person.get('organization') or {}).get('name', ''),
            }

        except Exception as e:
            print(f"[APOLLO] Exception: {e}")
            return None


# ============================================================================
# MILLIONVERIFIER EMAIL VALIDATION CLIENT
# ============================================================================

class MillionVerifierClient:
    """
    MillionVerifier API client for email validation.

    Batch processing: groups of 10, parallel within each batch,
    100 ms delay between batches (matches colleague's implementation).
    """

    BASE_URL = "https://api.millionverifier.com/api/v3/"

    # Result mapping (API result → our status string)
    _RESULT_MAP = {
        'ok':          'valid',
        'catch_all':   'catch-all',
        'invalid':     'invalid',
        'disposable':  'invalid',
        'unknown':     'unknown',
        'error':       'unknown',
    }

    def __init__(self, api_key: str):
        self.api_key = api_key

    def verify_email(self, email: str) -> Dict:
        """Verify a single email. Returns dict with 'status' and raw fields."""
        url = (f"{self.BASE_URL}?api={requests.utils.quote(self.api_key)}"
               f"&email={requests.utils.quote(email)}&timeout=15")
        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            raw_result = data.get('result', 'unknown')
            return {
                'email':   data.get('email', email),
                'status':  self._RESULT_MAP.get(raw_result, 'unknown'),
                'quality': data.get('quality', 'unknown'),
                'free':    bool(data.get('free')),
                'role':    bool(data.get('role')),
            }
        except Exception as e:
            print(f"[MV] Error verifying {email}: {e}")
            return {'email': email, 'status': 'unknown', 'quality': 'unknown',
                    'free': False, 'role': False}

    def verify_batch(self, email_items: List[Dict]) -> Dict[str, str]:
        """
        Verify a list of {'email': str, 'profile_idx': int} dicts in parallel batches.

        Returns dict: {email -> status_string}
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results: Dict[str, str] = {}
        batch_size = 10

        for i in range(0, len(email_items), batch_size):
            batch = email_items[i:i + batch_size]

            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                future_to_email = {
                    executor.submit(self.verify_email, item['email']): item['email']
                    for item in batch
                }
                for future in as_completed(future_to_email):
                    email = future_to_email[future]
                    try:
                        result = future.result()
                        results[email] = result['status']
                    except Exception as e:
                        print(f"[MV] Future error for {email}: {e}")
                        results[email] = 'unknown'

            # 100 ms delay between batches
            if i + batch_size < len(email_items):
                time.sleep(0.1)

        return results

# ============================================================================
# SOCIAL GRAPH BUILDER
# Crawls websites and link aggregators for emails + social links.
# Uses Apify Cheerio Scraper for reliability (JS-heavy pages, bot protection).
# Falls back to direct requests for simple pages.
# ============================================================================

class SocialGraphBuilder:
    """
    Builds a social graph for a creator by:
      1. Scraping Linktree / Beacons / other link aggregators (via Apify)
      2. Crawling personal websites (/contact, /about, /about-us) (via Apify)
      3. Direct HTTP fallback for both of the above
    Extracts: emails, social profile URLs, personal website URL.
    """

    LINK_AGGREGATORS = [
        'linktr.ee', 'beacons.ai', 'linkin.bio', 'linkpop.com',
        'hoo.be', 'campsite.bio', 'lnk.bio', 'tap.bio', 'solo.to',
        'bio.link', 'carrd.co',
    ]

    # Domains treated as social platforms (not personal websites)
    _SOCIAL_HOSTS = {
        'youtube.com', 'youtu.be', 'instagram.com', 'twitter.com', 'x.com',
        'discord.gg', 'discord.com', 'facebook.com', 'tiktok.com', 'twitch.tv',
        'linkedin.com', 'patreon.com', 'google.com', 'apple.com', 'spotify.com',
        'amazon.com', 'reddit.com', 'tumblr.com', 'pinterest.com', 'github.com',
        'medium.com', 'wordpress.com', 'linktr.ee', 'beacons.ai', 'ko-fi.com',
        'buymeacoffee.com', 'gumroad.com', 'substack.com', 'bit.ly',
        'meetup.com', 'eventbrite.com',
    }

    SOCIAL_PATTERNS: Dict[str, str] = {
        'instagram_url': r'instagram\.com/(?!p/|reel/|explore/)([a-zA-Z0-9._]+)',
        'youtube_url':   r'youtube\.com/(?:c/|channel/|@)?([a-zA-Z0-9_\-]+)',
        'twitter_url':   r'(?:twitter|x)\.com/([a-zA-Z0-9_]+)',
        'linkedin_url':  r'linkedin\.com/in/([a-zA-Z0-9\-]+)',
        'tiktok_url':    r'tiktok\.com/@([a-zA-Z0-9._]+)',
        'facebook_url':  r'facebook\.com/(?!groups/)([a-zA-Z0-9.]+)',
        'twitch_url':    r'twitch\.tv/([a-zA-Z0-9_]+)',
        'discord_url':   r'discord\.(?:gg|com)/([a-zA-Z0-9]+)',
    }

    # Email patterns
    _EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    _OBFUSCATED_PATTERNS = [
        re.compile(r'([a-zA-Z0-9._%+-]+)\s*\[\s*at\s*\]\s*([a-zA-Z0-9.-]+)\s*\[\s*dot\s*\]\s*([a-zA-Z]{2,})', re.I),
        re.compile(r'([a-zA-Z0-9._%+-]+)\s*\(\s*at\s*\)\s*([a-zA-Z0-9.-]+)\s*\(\s*dot\s*\)\s*([a-zA-Z]{2,})', re.I),
        re.compile(r'([a-zA-Z0-9._%+-]+)\s*\{\s*at\s*\}\s*([a-zA-Z0-9.-]+)\s*\{\s*dot\s*\}\s*([a-zA-Z]{2,})', re.I),
        re.compile(r'([a-zA-Z0-9._%+-]+)\s+at\s+([a-zA-Z0-9.-]+)\s+dot\s+([a-zA-Z]{2,})\b', re.I),
    ]
    _BLOCKED_EMAIL_PATTERNS = [
        '.png', '.jpg', '.gif', '.jpeg', '.webp', '.svg',
        'sentry.io', 'example.com', 'cloudfront', 'amazonaws',
        'patreon.com', 'w3.org', 'schema.org', 'googleapis.com', 'gstatic.com',
        'substackcdn', 'cdninstagram', 'fbcdn',
    ]

    # Email priority tier 2: personal providers beat unknown domains
    _PERSONAL_EMAIL_DOMAINS = {
        'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
        'icloud.com', 'me.com', 'protonmail.com', 'proton.me', 'live.com',
        'msn.com', 'mail.com', 'zoho.com', 'ymail.com', 'gmx.com',
    }

    # Extended subpage list for website contact crawl (26 paths)
    _CRAWL_SUBPAGES = [
        '', '/contact', '/about', '/about-us', '/contact-us', '/team',
        '/staff', '/bio', '/press', '/people', '/our-team', '/meet-the-team',
        '/leadership', '/board', '/board-of-directors', '/officers',
        '/connect', '/get-in-touch', '/reach-out', '/organizers',
        '/hosts', '/founders', '/who-we-are', '/our-story', '/info', '/support',
    ]

    # Glob keywords — Apify uses these to also follow dynamically routed pages
    _CRAWL_GLOB_KEYWORDS = [
        'contact', 'about', 'team', 'staff', 'people', 'board',
        'leadership', 'connect', 'organizer', 'host', 'founder',
        'info', 'support', 'who-we-are',
    ]

    # Name extraction from Google result titles / LinkedIn slugs
    _NAME_FROM_TITLE_RE = re.compile(
        r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s*[-|–·,]', re.UNICODE
    )
    _NAME_CONTEXT_RES = [
        re.compile(
            r'(?:admin|administrator|owner|manager|founder|created\s+by|'
            r'managed\s+by|run\s+by|led\s+by|organized\s+by)\s*[:\-–]?\s*'
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})', re.I
        ),
        re.compile(
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s*'
            r'(?:is the|,\s*(?:admin|founder|owner|manager|organizer|leader))', re.I
        ),
    ]
    _LINKEDIN_SLUG_RE = re.compile(r'linkedin\.com/in/([a-zA-Z0-9\-]+)')

    def __init__(self, apify_token: str = None):
        self.apify_token = apify_token
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            )
        })

    # ------------------------------------------------------------------
    # Public: batch operations (Apify-backed, called by enrichment pipeline)
    # ------------------------------------------------------------------

    def scrape_link_aggregators_batch(self, urls: List[str]) -> Dict[str, Dict]:
        """
        Scrape a batch of Linktree/Beacons URLs via Apify Cheerio Scraper.
        Returns {url: {emails, social_links, personal_website}}
        Falls back to direct scraping if Apify unavailable.
        """
        if not urls:
            return {}

        if self.apify_token:
            try:
                return self._apify_scrape_pages(urls, page_type='aggregator')
            except Exception as e:
                print(f"[SGB] Apify aggregator scrape failed, falling back: {e}")

        # Direct fallback
        results = {}
        for url in urls:
            results[url] = self._direct_scrape_aggregator(url)
        return results

    def crawl_websites_batch(self, websites: List[str]) -> Dict[str, Dict]:
        """
        Crawl personal websites via Apify Cheerio Scraper.

        Hits the root page plus all _CRAWL_SUBPAGES (26 paths) and uses
        glob patterns to also follow any dynamically-routed pages whose URLs
        contain contact/about/team/leadership keywords.

        Returns {domain: {emails, social_links}}
        Falls back to direct scraping if Apify unavailable.
        """
        if not websites:
            return {}

        # Build start URLs: root + all subpages
        start_urls = []
        domain_map: Dict[str, str] = {}  # url → domain key
        for site in websites:
            site = site.rstrip('/')
            domain = ApolloEnrichment.extract_domain(site)
            for path in self._CRAWL_SUBPAGES:
                full_url = site + path
                start_urls.append(full_url)
                domain_map[full_url] = domain

        if self.apify_token:
            try:
                return self._apify_crawl_websites(start_urls, domain_map, websites)
            except Exception as e:
                print(f"[SGB] Apify website crawl failed, falling back: {e}")

        # Direct fallback (shorter list — direct HTTP is rate-limited anyway)
        results: Dict[str, Dict] = {}
        for site in websites:
            domain = ApolloEnrichment.extract_domain(site)
            combined: Dict = {'emails': [], 'social_links': {}}
            for path in ['', '/contact', '/about', '/about-us', '/team']:
                page_data = self._direct_scrape_page(site.rstrip('/') + path)
                combined['emails'].extend(page_data.get('emails', []))
                combined['social_links'].update(page_data.get('social_links', {}))
            combined['emails'] = list(set(combined['emails']))
            results[domain] = combined
        return results

    # ------------------------------------------------------------------
    # Apify-backed scrapers
    # ------------------------------------------------------------------

    def _apify_scrape_pages(self, urls: List[str], page_type: str) -> Dict[str, Dict]:
        """Generic Apify Cheerio Scraper call.

        For website crawls, also extracts mailto: links from the page so we
        don't have to rely solely on regex matching of raw text.
        """
        from apify_client import ApifyClient
        apify = ApifyClient(self.apify_token)

        PAGE_FUNCTION = r"""
async function pageFunction(context) {
    const { $, request } = context;
    const mailtos = [];
    $('a[href^="mailto:"]').each(function() {
        const h = $(this).attr('href');
        if (h) mailtos.push(h.replace('mailto:', '').split('?')[0].trim().toLowerCase());
    });
    const text = $('body').text();
    const links = [];
    $('a[href]').each(function() { links.push($(this).attr('href')); });
    return {
        url: request.url,
        text: text.substring(0, 8000),
        links: links.slice(0, 200),
        mailtos: mailtos
    };
}
"""
        run_input = {
            'startUrls':          [{'url': u} for u in urls],
            'maxCrawlPages':      len(urls),
            'maxConcurrency':     10,
            'requestTimeoutSecs': 30,
            'pageFunction':       PAGE_FUNCTION,
        }
        run = apify.actor("apify~cheerio-scraper").call(run_input=run_input, timeout_secs=120)
        items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())

        results: Dict[str, Dict] = {}
        for item in items:
            url     = item.get('url', '')
            text    = item.get('text', '')
            links   = item.get('links', [])
            mailtos = item.get('mailtos', [])
            parsed  = self._parse_page_content(text, links)
            # Merge explicit mailto links — these are more reliable than regex
            if mailtos:
                all_emails = list(dict.fromkeys(mailtos + parsed.get('emails', [])))
                parsed['emails'] = [
                    e for e in all_emails
                    if not any(b in e for b in self._BLOCKED_EMAIL_PATTERNS)
                ]
            results[url] = parsed
        return results

    def _apify_crawl_websites(self, start_urls: List[str],
                              domain_map: Dict[str, str],
                              original_sites: List[str] = None) -> Dict[str, Dict]:
        """
        Crawl website pages via Apify and group results by domain.

        Uses glob patterns so Apify also follows any page whose URL contains
        contact/about/team/leadership keywords (catches dynamically-routed sites).

        Email selection priority:
          1. Domain-matching email (hi@theirdomain.com)
          2. Personal email provider (gmail, yahoo, etc.)
          3. Any other email found
        """
        from apify_client import ApifyClient
        apify = ApifyClient(self.apify_token)

        # Build glob patterns for each site so Apify follows relevant sub-pages
        globs = []
        if original_sites:
            for site in original_sites:
                site = site.rstrip('/')
                domain = ApolloEnrichment.extract_domain(site)
                for kw in self._CRAWL_GLOB_KEYWORDS:
                    globs.append({'glob': f'https://{domain}/**/*{kw}*'})
                    globs.append({'glob': f'https://www.{domain}/**/*{kw}*'})

        PAGE_FUNCTION = r"""
async function pageFunction(context) {
    const { $, request } = context;
    const mailtos = [];
    $('a[href^="mailto:"]').each(function() {
        const h = $(this).attr('href');
        if (h) mailtos.push(h.replace('mailto:', '').split('?')[0].trim().toLowerCase());
    });
    const text = $('body').text();
    const links = [];
    $('a[href]').each(function() { links.push($(this).attr('href')); });
    return {
        url: request.url,
        text: text.substring(0, 8000),
        links: links.slice(0, 200),
        mailtos: mailtos
    };
}
"""
        run_input = {
            'startUrls':          [{'url': u} for u in start_urls],
            'maxCrawlPages':      len(start_urls) + (len(globs) // 2 if globs else 0),
            'maxConcurrency':     6,
            'requestTimeoutSecs': 30,
            'pageFunction':       PAGE_FUNCTION,
        }
        if globs:
            run_input['globs'] = globs[:200]  # Apify cap

        run = apify.actor("apify~cheerio-scraper").call(run_input=run_input, timeout_secs=300)
        items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())

        # Aggregate raw emails by domain
        by_domain: Dict[str, Dict] = {}
        for item in items:
            url     = item.get('url', '')
            text    = item.get('text', '')
            links   = item.get('links', [])
            mailtos = item.get('mailtos', [])

            domain = domain_map.get(url) or ApolloEnrichment.extract_domain(url)
            if domain not in by_domain:
                by_domain[domain] = {'all_emails': [], 'social_links': {}}

            parsed = self._parse_page_content(text, links)

            # Collect all candidate emails (mailtos first — more reliable)
            candidate_emails = list(dict.fromkeys(
                [e for e in mailtos if '@' in e] + parsed.get('emails', [])
            ))
            candidate_emails = [
                e for e in candidate_emails
                if not any(b in e for b in self._BLOCKED_EMAIL_PATTERNS)
            ]
            by_domain[domain]['all_emails'].extend(candidate_emails)
            by_domain[domain]['social_links'].update(parsed.get('social_links', {}))

        # For each domain, pick the best email using priority logic
        results: Dict[str, Dict] = {}
        for domain, data in by_domain.items():
            best = self._select_best_email(
                list(dict.fromkeys(data['all_emails'])), domain
            )
            results[domain] = {
                'emails':      [best] if best else [],
                'social_links': data['social_links'],
            }

        return results

    # ------------------------------------------------------------------
    # Google Bridge  (Facebook / Meetup → find organizer via Google)
    # ------------------------------------------------------------------

    def google_bridge_enrich(self, profiles: List[Dict], job_id: str) -> List[Dict]:
        """
        For Facebook Groups (and future Meetup) profiles that have NO email,
        website, or LinkedIn, run Google searches to surface the organizer's
        contact information.

        Two Google queries per group:
          1. "<group_name>" website contact email
          2. "<group_name>" organizer OR founder OR leader site:linkedin.com

        Parses organic results for:
          - LinkedIn /in/ profiles  → linkedin_url
          - Instagram / Twitter / YouTube URLs  → respective social fields
          - Non-social URLs  → personal_website
          - Email addresses in snippets  → email
          - Organizer name from result title / snippet / LinkedIn slug

        Only processes profiles where platform is 'facebook_group' (or 'meetup')
        AND profile currently has no email AND no website AND no linkedin_url.
        """
        if not APIFY_API_TOKEN:
            print("[GOOGLE_BRIDGE] APIFY_API_TOKEN not set — skipping")
            return profiles

        # Filter to profiles that actually need it
        needs_bridge = [
            p for p in profiles
            if p.get('platform') in ('facebook_group', 'meetup')
            and not p.get('email')
            and not p.get('personal_website')
            and not p.get('linkedin_url')
        ]

        if not needs_bridge:
            print("[GOOGLE_BRIDGE] No profiles need bridging — skipping")
            return profiles

        print(f"[GOOGLE_BRIDGE] Running for {len(needs_bridge)} profiles")

        # Build query list
        queries: List[Dict] = []
        for p in needs_bridge:
            group_name = p.get('group_name') or p.get('community_name') or p.get('creator_name') or ''
            if not group_name:
                continue
            gn = group_name.replace('"', '')
            queries.append({
                'term': f'"{gn}" website contact email',
                '_profile_ref': id(p),
            })
            queries.append({
                'term': f'"{gn}" organizer OR founder OR leader site:linkedin.com',
                '_profile_ref': id(p),
            })

        if not queries:
            return profiles

        # Build profile_id → profile mapping
        id_to_profile = {id(p): p for p in needs_bridge}

        # Run in batches of 20 queries
        BATCH_SIZE = 20
        all_results: List[Dict] = []
        for batch_start in range(0, len(queries), BATCH_SIZE):
            batch = queries[batch_start: batch_start + BATCH_SIZE]
            search_queries = [{'term': q['term']} for q in batch]
            actor_input = {
                'queries': search_queries,
                'resultsPerPage': 5,
                'maxPagesPerQuery': 1,
                'outputAsJSON': True,
                'saveHtml': False,
                'saveMarkdown': False,
            }
            try:
                from apify_client import ApifyClient
                apify = ApifyClient(self.apify_token)
                run = apify.actor("apify~google-search-scraper").call(
                    run_input=actor_input, timeout_secs=120
                )
                raw = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
                all_results.extend(raw or [])
            except Exception as e:
                print(f"[GOOGLE_BRIDGE] Apify error (batch {batch_start}): {e}")

        if not all_results:
            return profiles

        # Map query term → list of organic results
        query_to_results: Dict[str, List[Dict]] = {}
        for item in all_results:
            term = (item.get('searchQuery') or {}).get('term', '')
            organics = item.get('organicResults') or []
            query_to_results.setdefault(term, []).extend(organics)

        # Parse results and merge back into profiles
        for q_entry in queries:
            term = q_entry['term']
            profile = id_to_profile.get(q_entry['_profile_ref'])
            if not profile:
                continue

            organics = query_to_results.get(term, [])
            for result in organics:
                title    = result.get('title', '')
                snippet  = result.get('description', '') or result.get('snippet', '')
                url      = result.get('url', '') or result.get('link', '')

                if not url:
                    continue

                parsed = urlparse(url)
                netloc = parsed.netloc.lower()

                # LinkedIn profile
                if 'linkedin.com/in/' in url and not profile.get('linkedin_url'):
                    profile['linkedin_url'] = url.split('?')[0]

                # Instagram
                elif re.search(r'instagram\.com/', url, re.I) and not profile.get('instagram_url'):
                    profile['instagram_url'] = url.split('?')[0]

                # Twitter / X
                elif re.search(r'(twitter|x)\.com/', url, re.I) and not profile.get('twitter_url'):
                    profile['twitter_url'] = url.split('?')[0]

                # YouTube
                elif re.search(r'youtube\.com/(channel|c/|@)', url, re.I) and not profile.get('youtube_url'):
                    profile['youtube_url'] = url.split('?')[0]

                # Non-social personal website
                elif (
                    url.startswith('http')
                    and not any(s in netloc for s in self._SOCIAL_HOSTS)
                    and not any(agg in netloc for agg in ('linktree', 'beacons', 'linktr'))
                    and not profile.get('personal_website')
                ):
                    profile['personal_website'] = url.split('?')[0]

                # Email from snippet
                if not profile.get('email'):
                    snippet_emails = self._extract_emails(snippet + ' ' + title)
                    if snippet_emails:
                        profile['email'] = snippet_emails[0]

                # Organizer / creator name
                if not profile.get('creator_name') or profile.get('creator_name') == profile.get('group_name'):
                    name = self._extract_name_from_text(title, snippet, url)
                    if name:
                        profile['creator_name'] = name

        print(f"[GOOGLE_BRIDGE] Enriched {len(needs_bridge)} profiles")
        return profiles

    # ------------------------------------------------------------------
    # YouTube About Pages
    # ------------------------------------------------------------------

    @staticmethod
    def _youtube_about_url(url: str) -> str:
        """Normalise any YouTube channel URL to its /about page URL."""
        url = url.rstrip('/')
        for suffix in ('/videos', '/shorts', '/community', '/playlists', '/about'):
            if url.endswith(suffix):
                url = url[:-len(suffix)]
        return url + '/about'

    def scrape_youtube_about_pages_batch(self, profiles: List[Dict]) -> List[Dict]:
        """
        Scrape YouTube /about pages for profiles that have a youtube_url.

        The /about page exposes the creator's email (click-to-reveal in real
        browsers, but plain-text in the initial HTML Apify captures), their
        website link, and any social links in the channel description.

        Updates profiles in-place with: email, linkedin_url, personal_website,
        linktree_url, creator_name (fallback from channel name in page title).

        Runs for ALL profiles with a youtube_url regardless of whether they
        already have an email (YT is cheap + fast and may give a better email).
        """
        if not self.apify_token:
            print("[YT_ABOUT] APIFY_API_TOKEN not set — skipping")
            return profiles

        yt_profiles = [p for p in profiles if p.get('youtube_url')]
        if not yt_profiles:
            print("[YT_ABOUT] No profiles with youtube_url — skipping")
            return profiles

        print(f"[YT_ABOUT] Scraping {len(yt_profiles)} YouTube About pages")

        # Build URL → profile index map
        url_to_idxs: Dict[str, List[int]] = {}
        about_urls: List[str] = []
        for i, p in enumerate(profiles):
            if not p.get('youtube_url'):
                continue
            about_url = self._youtube_about_url(p['youtube_url'])
            about_urls.append(about_url)
            url_to_idxs.setdefault(about_url, []).append(i)

        PAGE_FUNCTION = r"""
async function pageFunction(context) {
    const { $, request } = context;
    const mailtos = [];
    $('a[href^="mailto:"]').each(function() {
        const h = $(this).attr('href');
        if (h) mailtos.push(h.replace('mailto:', '').split('?')[0].trim().toLowerCase());
    });
    const text = $('body').text();
    const links = [];
    $('a[href]').each(function() { links.push($(this).attr('href')); });
    // Channel name from <title>
    const title = $('title').text().trim();
    return {
        url: request.url,
        text: text.substring(0, 10000),
        links: links.slice(0, 300),
        mailtos: mailtos,
        pageTitle: title
    };
}
"""
        try:
            from apify_client import ApifyClient
            apify = ApifyClient(self.apify_token)
            run_input = {
                'startUrls':          [{'url': u} for u in about_urls],
                'maxCrawlPages':      len(about_urls),
                'maxConcurrency':     30,
                'requestTimeoutSecs': 90,
                'pageFunction':       PAGE_FUNCTION,
            }
            run   = apify.actor("apify~cheerio-scraper").call(run_input=run_input, timeout_secs=300)
            items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
        except Exception as e:
            print(f"[YT_ABOUT] Apify error: {e}")
            return profiles

        _LINK_AGG_HOSTS = set(self.LINK_AGGREGATORS)

        for item in items:
            page_url   = item.get('url', '')
            text       = item.get('text', '')
            links      = item.get('links', []) or []
            mailtos    = item.get('mailtos', []) or []
            page_title = item.get('pageTitle', '')

            idxs = url_to_idxs.get(page_url, [])
            if not idxs:
                continue

            # Collect emails — mailtos first, then regex
            candidate_emails = list(dict.fromkeys(
                [e for e in mailtos if '@' in e] + self._extract_emails(text)
            ))
            candidate_emails = [
                e for e in candidate_emails
                if not self._is_blocked_email(e)
            ]

            parsed = self._parse_page_content(text, links)

            for i in idxs:
                p = profiles[i]

                # Email (only fill if missing)
                if candidate_emails and not p.get('email'):
                    p['email'] = candidate_emails[0]

                # LinkedIn
                for href in links:
                    if href and re.search(r'linkedin\.com/in/', href, re.I):
                        if not p.get('linkedin_url'):
                            p['linkedin_url'] = href.split('?')[0]

                # Link aggregators (feeds Pass 2)
                for href in links:
                    if href and any(agg in href.lower() for agg in _LINK_AGG_HOSTS):
                        if not p.get('linktree_url'):
                            p['linktree_url'] = href.split('?')[0]

                # Personal website
                if parsed.get('personal_website') and not p.get('personal_website'):
                    p['personal_website'] = parsed['personal_website']

                # Creator name fallback from page title ("Channel Name - YouTube")
                if page_title and not p.get('creator_name'):
                    name = page_title.split(' - ')[0].strip()
                    if name and name.lower() not in ('youtube', ''):
                        p['creator_name'] = name

        print(f"[YT_ABOUT] Completed scraping {len(yt_profiles)} YouTube pages")
        return profiles

    # ------------------------------------------------------------------
    # Instagram Bios
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_instagram_handle(url: str) -> Optional[str]:
        """Extract the username from an Instagram profile URL."""
        m = re.search(r'instagram\.com/([a-zA-Z0-9._]+)/?', url or '')
        if m:
            handle = m.group(1).lower()
            if handle not in ('p', 'reel', 'stories', 'explore', 'tv'):
                return handle
        return None

    def scrape_instagram_bios_batch(self, profiles: List[Dict]) -> List[Dict]:
        """
        Use apify~instagram-profile-scraper to enrich profiles that have an
        instagram_url.

        Returns structured data (biography, externalUrl, externalUrls[],
        followersCount, fullName) — much more reliable than Cheerio for IG.

        Updates profiles in-place with: email (from bio), personal_website,
        linktree_url, instagram_followers.
        """
        if not self.apify_token:
            print("[IG_BIO] APIFY_API_TOKEN not set — skipping")
            return profiles

        ig_profiles = [p for p in profiles if p.get('instagram_url')]
        if not ig_profiles:
            print("[IG_BIO] No profiles with instagram_url — skipping")
            return profiles

        print(f"[IG_BIO] Scraping {len(ig_profiles)} Instagram bios")

        # Extract handles; build handle → profile indices map
        handle_to_idxs: Dict[str, List[int]] = {}
        for i, p in enumerate(profiles):
            if not p.get('instagram_url'):
                continue
            handle = self._extract_instagram_handle(p['instagram_url'])
            if handle:
                handle_to_idxs.setdefault(handle, []).append(i)

        if not handle_to_idxs:
            return profiles

        _LINK_AGG_HOSTS = set(self.LINK_AGGREGATORS)

        try:
            from apify_client import ApifyClient
            apify = ApifyClient(self.apify_token)
            run_input = {
                'usernames':    list(handle_to_idxs.keys()),
                'resultsLimit': 1,
            }
            run   = apify.actor("apify~instagram-profile-scraper").call(
                run_input=run_input, timeout_secs=300
            )
            items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
        except Exception as e:
            print(f"[IG_BIO] Apify error: {e}")
            return profiles

        for item in items:
            username = (item.get('username') or '').lower()
            idxs = handle_to_idxs.get(username, [])
            if not idxs:
                continue

            bio          = item.get('biography', '') or ''
            external_url = item.get('externalUrl', '') or ''
            extra_urls   = [u.get('url', '') for u in (item.get('externalUrls') or [])]
            followers    = item.get('followersCount') or 0
            full_name    = item.get('fullName', '') or ''

            # Collect all outbound URLs from the profile
            all_external = [u for u in [external_url] + extra_urls if u]

            # Extract email from bio text
            bio_emails = self._extract_emails(bio)

            for i in idxs:
                p = profiles[i]

                if bio_emails and not p.get('email'):
                    p['email'] = bio_emails[0]

                # Follower count
                if followers and not p.get('instagram_followers'):
                    p['instagram_followers'] = followers

                # Creator name from IG full_name
                if full_name and not p.get('creator_name'):
                    p['creator_name'] = full_name

                for ext_url in all_external:
                    if not ext_url:
                        continue
                    # Link aggregator (feeds Pass 2)
                    if any(agg in ext_url.lower() for agg in _LINK_AGG_HOSTS):
                        if not p.get('linktree_url'):
                            p['linktree_url'] = ext_url.split('?')[0]
                    # LinkedIn
                    elif re.search(r'linkedin\.com/in/', ext_url, re.I):
                        if not p.get('linkedin_url'):
                            p['linkedin_url'] = ext_url.split('?')[0]
                    # Personal website (non-social)
                    elif ext_url.startswith('http'):
                        netloc = urlparse(ext_url).netloc.lower()
                        if not any(s in netloc for s in self._SOCIAL_HOSTS):
                            if not p.get('personal_website'):
                                p['personal_website'] = ext_url.split('?')[0]

        print(f"[IG_BIO] Completed scraping {len(ig_profiles)} Instagram profiles")
        return profiles

    # ------------------------------------------------------------------
    # Twitter / X Bios
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_twitter_handle(url: str) -> Optional[str]:
        """Extract the handle from a Twitter / X profile URL."""
        m = re.search(r'(?:twitter|x)\.com/([a-zA-Z0-9_]+)/?', url or '')
        if m:
            handle = m.group(1).lower()
            if handle not in ('home', 'search', 'explore', 'notifications',
                              'messages', 'i', 'intent', 'hashtag', 'share'):
                return handle
        return None

    def scrape_twitter_bios_batch(self, profiles: List[Dict]) -> List[Dict]:
        """
        Use apidojo~twitter-user-scraper to enrich profiles that have a
        twitter_url.

        Returns structured data (description, entities.url.urls,
        entities.description.urls, followersCount, name).

        Updates profiles in-place with: email (from bio), personal_website,
        linkedin_url, linktree_url, twitter_followers.
        """
        if not self.apify_token:
            print("[TW_BIO] APIFY_API_TOKEN not set — skipping")
            return profiles

        tw_profiles = [p for p in profiles if p.get('twitter_url')]
        if not tw_profiles:
            print("[TW_BIO] No profiles with twitter_url — skipping")
            return profiles

        print(f"[TW_BIO] Scraping {len(tw_profiles)} Twitter bios")

        handle_to_idxs: Dict[str, List[int]] = {}
        for i, p in enumerate(profiles):
            if not p.get('twitter_url'):
                continue
            handle = self._extract_twitter_handle(p['twitter_url'])
            if handle:
                handle_to_idxs.setdefault(handle, []).append(i)

        if not handle_to_idxs:
            return profiles

        _LINK_AGG_HOSTS = set(self.LINK_AGGREGATORS)

        try:
            from apify_client import ApifyClient
            apify = ApifyClient(self.apify_token)
            run_input = {
                'handles': list(handle_to_idxs.keys()),
            }
            run   = apify.actor("apidojo~twitter-user-scraper").call(
                run_input=run_input, timeout_secs=300
            )
            items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
        except Exception as e:
            print(f"[TW_BIO] Apify error: {e}")
            return profiles

        for item in items:
            # Actor may return handle under 'username', 'screen_name', or nested
            username = (
                item.get('username')
                or item.get('screen_name')
                or (item.get('legacy') or {}).get('screen_name', '')
            ).lower().lstrip('@')

            idxs = handle_to_idxs.get(username, [])
            if not idxs:
                continue

            description = item.get('description', '') or ''
            followers   = (
                item.get('followersCount')
                or item.get('followers_count')
                or (item.get('legacy') or {}).get('followers_count', 0)
                or 0
            )
            full_name   = (
                item.get('name')
                or (item.get('legacy') or {}).get('name', '')
                or ''
            )

            # Collect entity URLs (Twitter's t.co expansion)
            entity_urls: List[str] = []
            entities = item.get('entities') or {}
            for url_entry in (entities.get('url') or {}).get('urls', []):
                expanded = url_entry.get('expanded_url', '')
                if expanded:
                    entity_urls.append(expanded)
            for url_entry in (entities.get('description') or {}).get('urls', []):
                expanded = url_entry.get('expanded_url', '')
                if expanded:
                    entity_urls.append(expanded)

            # Also check legacy.entities if present
            legacy = item.get('legacy') or {}
            leg_entities = legacy.get('entities') or {}
            for url_entry in (leg_entities.get('url') or {}).get('urls', []):
                expanded = url_entry.get('expanded_url', '')
                if expanded:
                    entity_urls.append(expanded)

            bio_emails = self._extract_emails(description)

            for i in idxs:
                p = profiles[i]

                if bio_emails and not p.get('email'):
                    p['email'] = bio_emails[0]

                if followers and not p.get('twitter_followers'):
                    p['twitter_followers'] = followers

                if full_name and not p.get('creator_name'):
                    p['creator_name'] = full_name

                for ext_url in entity_urls:
                    if not ext_url:
                        continue
                    if any(agg in ext_url.lower() for agg in _LINK_AGG_HOSTS):
                        if not p.get('linktree_url'):
                            p['linktree_url'] = ext_url.split('?')[0]
                    elif re.search(r'linkedin\.com/in/', ext_url, re.I):
                        if not p.get('linkedin_url'):
                            p['linkedin_url'] = ext_url.split('?')[0]
                    elif ext_url.startswith('http'):
                        netloc = urlparse(ext_url).netloc.lower()
                        if not any(s in netloc for s in self._SOCIAL_HOSTS):
                            if not p.get('personal_website'):
                                p['personal_website'] = ext_url.split('?')[0]

        print(f"[TW_BIO] Completed scraping {len(tw_profiles)} Twitter profiles")
        return profiles

    # ------------------------------------------------------------------
    # RSS Feed Parsing
    # ------------------------------------------------------------------

    def parse_rss_feeds_batch(self, profiles: List[Dict]) -> List[Dict]:
        """
        Scrape RSS / podcast feed URLs for profiles that have an rss_url.

        Podcast feeds (generated by Buzzsprout, Anchor, Podbean, etc.) embed
        rich contact metadata in iTunes namespace tags:
          <itunes:owner><itunes:email>   – most reliable contact email
          <itunes:author>                – human-readable author name
          <itunes:name>                  – owner name in <itunes:owner> block
          <link>                         – canonical website URL

        Falls back to regex email extraction from the raw feed XML text.

        Updates profiles in-place with: email, creator_name, personal_website.
        """
        if not self.apify_token:
            print("[RSS] APIFY_API_TOKEN not set — skipping")
            return profiles

        rss_profiles = [p for p in profiles if p.get('rss_url')]
        if not rss_profiles:
            print("[RSS] No profiles with rss_url — skipping")
            return profiles

        print(f"[RSS] Parsing {len(rss_profiles)} RSS feeds")

        url_to_idxs: Dict[str, List[int]] = {}
        rss_urls: List[str] = []
        for i, p in enumerate(profiles):
            rss = p.get('rss_url', '')
            if rss:
                rss_urls.append(rss)
                url_to_idxs.setdefault(rss, []).append(i)

        # RSS pageFunction — parse iTunes namespace tags from raw XML
        PAGE_FUNCTION = r"""
async function pageFunction(context) {
    const { $, request } = context;
    // iTunes email lives inside <itunes:owner><itunes:email>...</itunes:email>
    const itunesEmail = $('itunes\\:owner itunes\\:email').first().text().trim()
        || $('itunes\\:email').first().text().trim();
    const itunesAuthor = $('itunes\\:author').first().text().trim()
        || $('itunes\\:owner itunes\\:name').first().text().trim();
    // Podcast / channel website link
    const channelLink = $('channel > link').first().text().trim()
        || $('channel > link').first().attr('href') || '';
    // Raw text for fallback email regex
    const rawText = $('body').length ? $('body').text() : $.html();
    return {
        url: request.url,
        itunesEmail: itunesEmail,
        itunesAuthor: itunesAuthor,
        channelLink: channelLink,
        text: rawText.substring(0, 6000)
    };
}
"""
        try:
            from apify_client import ApifyClient
            apify = ApifyClient(self.apify_token)
            run_input = {
                'startUrls':          [{'url': u} for u in rss_urls],
                'maxCrawlPages':      len(rss_urls),
                'maxConcurrency':     20,
                'requestTimeoutSecs': 30,
                'pageFunction':       PAGE_FUNCTION,
            }
            run   = apify.actor("apify~cheerio-scraper").call(run_input=run_input, timeout_secs=180)
            items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
        except Exception as e:
            print(f"[RSS] Apify error: {e}")
            return profiles

        for item in items:
            page_url      = item.get('url', '')
            itunes_email  = item.get('itunesEmail', '') or ''
            itunes_author = item.get('itunesAuthor', '') or ''
            channel_link  = item.get('channelLink', '') or ''
            text          = item.get('text', '') or ''

            idxs = url_to_idxs.get(page_url, [])
            if not idxs:
                continue

            # Fallback: regex email from raw feed text
            fallback_emails = self._extract_emails(text) if not itunes_email else []

            for i in idxs:
                p = profiles[i]

                # Email — iTunes tag is most authoritative
                if itunes_email and not p.get('email'):
                    if not self._is_blocked_email(itunes_email):
                        p['email'] = itunes_email.lower().strip()
                elif fallback_emails and not p.get('email'):
                    p['email'] = fallback_emails[0]

                # Creator name from iTunes author
                if itunes_author and not p.get('creator_name'):
                    p['creator_name'] = itunes_author

                # Website from channel <link>
                if channel_link and channel_link.startswith('http'):
                    netloc = urlparse(channel_link).netloc.lower()
                    if not any(s in netloc for s in self._SOCIAL_HOSTS):
                        if not p.get('personal_website'):
                            p['personal_website'] = channel_link.split('?')[0]

        print(f"[RSS] Completed parsing {len(rss_profiles)} RSS feeds")
        return profiles

    # ------------------------------------------------------------------
    # Google Contact Search  (last-resort, all platforms)
    # ------------------------------------------------------------------

    def google_contact_search(self, profiles: List[Dict], job_id: str) -> List[Dict]:
        """
        Last-resort Google search for profiles that made it through the full
        social-scraping pipeline with still no email, website, or LinkedIn.

        Generates 3 queries per profile using the creator name and/or their
        Patreon/platform slug:
          1. "<name>" email website
          2. site:linkedin.com "<name>"
          3. "<name>" "contact" OR "reach me" OR "get in touch"

        Uses the same `apify~google-search-scraper` + URL parsing logic as
        google_bridge_enrich.
        """
        if not self.apify_token:
            print("[CONTACT_SEARCH] APIFY_API_TOKEN not set — skipping")
            return profiles

        needs_search = [
            p for p in profiles
            if not p.get('email')
            and not p.get('personal_website')
            and not p.get('linkedin_url')
        ]

        if not needs_search:
            print("[CONTACT_SEARCH] No profiles need contact search — skipping")
            return profiles

        print(f"[CONTACT_SEARCH] Running for {len(needs_search)} profiles")

        queries: List[Dict] = []
        for p in needs_search:
            name = p.get('creator_name', '').strip()
            # Derive a slug from the primary platform URL as fallback
            slug = ''
            for url_field in ('url', 'instagram_url', 'youtube_url', 'twitter_url'):
                raw_url = p.get(url_field, '')
                if raw_url:
                    slug = raw_url.rstrip('/').split('/')[-1].split('?')[0]
                    break
            search_name = name or slug
            if not search_name:
                continue

            sn = search_name.replace('"', '')
            queries.append({'term': f'"{sn}" email website contact',       '_profile_ref': id(p)})
            queries.append({'term': f'site:linkedin.com "{sn}"',            '_profile_ref': id(p)})
            queries.append({'term': f'"{sn}" "contact" OR "reach me" OR "get in touch"', '_profile_ref': id(p)})

        if not queries:
            return profiles

        id_to_profile = {id(p): p for p in needs_search}

        BATCH_SIZE = 20
        all_results: List[Dict] = []
        for batch_start in range(0, len(queries), BATCH_SIZE):
            batch = queries[batch_start: batch_start + BATCH_SIZE]
            actor_input = {
                'queries':        '\n'.join(q['term'] for q in batch),
                'resultsPerPage': 5,
                'maxPagesPerQuery': 1,
                'outputAsJSON':   True,
                'saveHtml':       False,
                'saveMarkdown':   False,
            }
            try:
                from apify_client import ApifyClient
                apify = ApifyClient(self.apify_token)
                run = apify.actor("apify~google-search-scraper").call(
                    run_input=actor_input, timeout_secs=120
                )
                raw = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
                all_results.extend(raw or [])
            except Exception as e:
                print(f"[CONTACT_SEARCH] Apify error (batch {batch_start}): {e}")

        if not all_results:
            return profiles

        query_to_results: Dict[str, List[Dict]] = {}
        for item in all_results:
            term     = (item.get('searchQuery') or {}).get('term', '')
            organics = item.get('organicResults') or []
            query_to_results.setdefault(term, []).extend(organics)

        for q_entry in queries:
            term    = q_entry['term']
            profile = id_to_profile.get(q_entry['_profile_ref'])
            if not profile:
                continue

            for result in query_to_results.get(term, []):
                title   = result.get('title', '')
                snippet = result.get('description', '') or result.get('snippet', '')
                url     = result.get('url', '') or result.get('link', '')
                if not url:
                    continue

                netloc = urlparse(url).netloc.lower()

                if 'linkedin.com/in/' in url and not profile.get('linkedin_url'):
                    profile['linkedin_url'] = url.split('?')[0]
                elif re.search(r'instagram\.com/', url, re.I) and not profile.get('instagram_url'):
                    profile['instagram_url'] = url.split('?')[0]
                elif re.search(r'(twitter|x)\.com/', url, re.I) and not profile.get('twitter_url'):
                    profile['twitter_url'] = url.split('?')[0]
                elif re.search(r'youtube\.com/(channel|c/|@)', url, re.I) and not profile.get('youtube_url'):
                    profile['youtube_url'] = url.split('?')[0]
                elif (
                    url.startswith('http')
                    and not any(s in netloc for s in self._SOCIAL_HOSTS)
                    and not any(agg in netloc for agg in ('linktree', 'beacons', 'linktr'))
                    and not profile.get('personal_website')
                ):
                    profile['personal_website'] = url.split('?')[0]

                if not profile.get('email'):
                    emails = self._extract_emails(snippet + ' ' + title)
                    if emails:
                        profile['email'] = emails[0]

                if not profile.get('creator_name'):
                    name = self._extract_name_from_text(title, snippet, url)
                    if name:
                        profile['creator_name'] = name

        print(f"[CONTACT_SEARCH] Enriched {len(needs_search)} profiles")
        return profiles

    # ------------------------------------------------------------------
    # Direct HTTP fallbacks
    # ------------------------------------------------------------------

    def _direct_scrape_page(self, url: str) -> Dict:
        """Direct HTTP GET + BeautifulSoup parse."""
        result: Dict = {'emails': [], 'social_links': {}, 'personal_website': None}
        try:
            resp = self._session.get(url, timeout=10, allow_redirects=True)
            if not resp.ok:
                return result
            soup = BeautifulSoup(resp.text, 'html.parser')

            # mailto links (most reliable)
            for a in soup.find_all('a', href=re.compile(r'^mailto:', re.I)):
                email = a['href'].replace('mailto:', '').split('?')[0].strip().lower()
                if email and '@' in email:
                    result['emails'].append(email)

            text = soup.get_text(' ', strip=True)
            result['emails'].extend(self._extract_emails(text))

            # Social links from <a> tags
            for a in soup.find_all('a', href=True):
                href = a['href']
                for key, pattern in self.SOCIAL_PATTERNS.items():
                    if re.search(pattern, href, re.I):
                        result['social_links'][key] = href.split('?')[0]

            result['emails'] = list(set(result['emails']))
        except Exception as e:
            print(f"[SGB] Direct scrape error {url}: {e}")
        return result

    def _direct_scrape_aggregator(self, url: str) -> Dict:
        """Direct HTTP scrape of a Linktree/Beacons page."""
        result: Dict = {'emails': [], 'social_links': {}, 'personal_website': None}
        try:
            resp = self._session.get(url, timeout=10, allow_redirects=True)
            if not resp.ok:
                return result
            soup = BeautifulSoup(resp.text, 'html.parser')
            text = soup.get_text(' ', strip=True)
            result['emails'] = self._extract_emails(text)

            for a in soup.find_all('a', href=True):
                href = a['href']
                for key, pattern in self.SOCIAL_PATTERNS.items():
                    if re.search(pattern, href, re.I):
                        result['social_links'][key] = href.split('?')[0]
                # Personal website: external, non-social link
                if href.startswith('http'):
                    host = urlparse(href).netloc.replace('www.', '').lower()
                    if not any(s in host for s in self._SOCIAL_HOSTS):
                        if not result['personal_website']:
                            result['personal_website'] = href

        except Exception as e:
            print(f"[SGB] Direct aggregator scrape error {url}: {e}")
        return result

    # ------------------------------------------------------------------
    # Single-URL convenience (used for inline per-profile enrichment)
    # ------------------------------------------------------------------

    def build_graph(self, url: str, name: str = None) -> Dict:
        """Build social graph from a single URL (aggregator or website)."""
        is_agg = any(agg in url.lower() for agg in self.LINK_AGGREGATORS)
        if is_agg:
            data = self._direct_scrape_aggregator(url)
            result = {
                'emails':           data.get('emails', []),
                'social_links':     data.get('social_links', {}),
                'personal_website': data.get('personal_website'),
                'linktree_url':     url,
            }
            # If aggregator links to a personal website, crawl that too
            if result['personal_website']:
                site_data = self._direct_scrape_page(result['personal_website'])
                result['emails'].extend(site_data.get('emails', []))
                result['social_links'].update(site_data.get('social_links', {}))
        else:
            data = self._direct_scrape_page(url)
            result = {
                'emails':           data.get('emails', []),
                'social_links':     data.get('social_links', {}),
                'personal_website': None,
                'linktree_url':     None,
            }
        result['emails'] = list(set(result['emails']))
        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _parse_page_content(self, text: str, links: List[str]) -> Dict:
        """Parse Apify Cheerio page result into emails + social links."""
        emails = self._extract_emails(text)
        social_links: Dict[str, str] = {}
        personal_website = None

        for href in links:
            if not href:
                continue
            for key, pattern in self.SOCIAL_PATTERNS.items():
                if re.search(pattern, href, re.I):
                    social_links[key] = href.split('?')[0]
            # Detect personal website from links
            if href.startswith('http'):
                host = urlparse(href).netloc.replace('www.', '').lower()
                if not any(s in host for s in self._SOCIAL_HOSTS):
                    if not personal_website:
                        personal_website = href

        return {'emails': emails, 'social_links': social_links,
                'personal_website': personal_website}

    def _extract_emails(self, text: str) -> List[str]:
        """Extract both standard and obfuscated email addresses from text."""
        emails = []

        # Standard regex
        for match in self._EMAIL_RE.finditer(text):
            email = match.group(0).lower()
            if not self._is_blocked_email(email):
                emails.append(email)

        # Obfuscated patterns  (name [at] domain [dot] com)
        for pattern in self._OBFUSCATED_PATTERNS:
            for match in pattern.finditer(text):
                try:
                    email = f"{match.group(1)}@{match.group(2)}.{match.group(3)}".lower().replace(' ', '')
                    if '@' in email and not self._is_blocked_email(email):
                        emails.append(email)
                except Exception:
                    pass

        return list(set(emails))

    def _is_blocked_email(self, email: str) -> bool:
        return any(p in email.lower() for p in self._BLOCKED_EMAIL_PATTERNS)

    def _select_best_email(self, emails: List[str], site_domain: str) -> Optional[str]:
        """
        Pick the highest-priority email from a list of candidates.

        Priority order (matches colleague's logic):
          1. Email whose domain matches the site domain  →  e.g. hi@theircreatorwebsite.com
          2. Personal email provider (gmail, yahoo, etc.)
          3. Any non-blocked email

        Returns None if the list is empty.
        """
        if not emails:
            return None

        site_domain = site_domain.lower().lstrip('www.')

        tier1 = [e for e in emails if e.split('@')[-1].lstrip('www.') == site_domain]
        if tier1:
            return tier1[0]

        tier2 = [e for e in emails if e.split('@')[-1].lstrip('www.') in self._PERSONAL_EMAIL_DOMAINS]
        if tier2:
            return tier2[0]

        return emails[0]

    def _extract_name_from_text(self, title: str, snippet: str, url: str) -> Optional[str]:
        """
        Attempt to extract an organizer/creator name from a Google result.

        Tries (in order):
          1. Title regex patterns (e.g. "John Smith – Facebook Group Admin")
          2. Snippet context patterns (e.g. "managed by John Smith")
          3. LinkedIn URL slug → humanise slug
        """
        for text in (title, snippet):
            if not text:
                continue
            m = self._NAME_FROM_TITLE_RE.match(text.strip())
            if m:
                return m.group(1).strip()
            for pat in self._NAME_CONTEXT_RES:
                m = pat.search(text)
                if m:
                    return m.group(1).strip()

        # LinkedIn slug fallback
        m = self._LINKEDIN_SLUG_RE.search(url or '')
        if m:
            slug = m.group(1)
            # Convert "john-doe-123abc" → "John Doe"
            parts = [p.capitalize() for p in slug.split('-') if p and not p.isdigit() and len(p) > 1]
            if len(parts) >= 2:
                return ' '.join(parts[:3])

        return None

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _extract_facebook_group_url(raw_url: str) -> str:
    """Extract a clean, canonical Facebook group URL."""
    try:
        url = raw_url.split('?')[0]
        if '/groups/' in url:
            parts = url.split('/groups/')
            if len(parts) > 1:
                group_id = parts[1].split('/')[0]
                return f"https://www.facebook.com/groups/{group_id}"
        return url
    except Exception:
        return raw_url


def _extract_posts_per_month(text: str):
    """
    Best-effort extraction of posts per month from a Google snippet.

    Handles phrases like:
      '10 posts a month', '3 posts per week', '2 posts per day', '50 posts this month',
      '5 posts a week', '1 post per day'

    Returns an integer estimate of posts per month, or None if not found.
    """
    if not text:
        return None

    text_lower = text.lower()

    # Try "X posts a/per month" or "X posts this month"
    m = re.search(r'(\d+(?:[\.,]\d+)?)\s+posts?\s+(?:a|per|this)\s+month', text_lower)
    if m:
        try:
            return int(float(m.group(1).replace(',', '.')))
        except ValueError:
            pass

    # Try "X posts a/per week" → multiply by ~4.3
    m = re.search(r'(\d+(?:[\.,]\d+)?)\s+posts?\s+(?:a|per)\s+week', text_lower)
    if m:
        try:
            return int(float(m.group(1).replace(',', '.')) * 4.3)
        except ValueError:
            pass

    # Try "X posts a/per day" → multiply by ~30
    m = re.search(r'(\d+(?:[\.,]\d+)?)\s+posts?\s+(?:a|per)\s+day', text_lower)
    if m:
        try:
            return int(float(m.group(1).replace(',', '.')) * 30)
        except ValueError:
            pass

    return None


def _extract_member_count(text: str) -> int:
    """Parse member count from strings like '5.2K members', '1,234 members'."""
    patterns = [
        r'([\d,]+\.?\d*[KkMm]?)\s+members?',
        r'([\d,]+\.?\d*[KkMm]?)\s+people',
        r'([\d,]+)\s+in\s+group',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.I)
        if m:
            raw = m.group(1).replace(',', '').upper()
            try:
                if 'K' in raw:
                    return int(float(raw.replace('K', '')) * 1_000)
                if 'M' in raw:
                    return int(float(raw.replace('M', '')) * 1_000_000)
                return int(raw)
            except ValueError:
                pass
    return 0


# ============================================================================
# LEADS FINDER ENRICHMENT (Apify code_crafter~leads-finder)
# ============================================================================

def enrich_with_leads_finder(profiles: List[Dict], job_id: str) -> List[Dict]:
    """
    Use Apify code_crafter~leads-finder to find emails by domain for profiles
    that still lack an email address.

    Matches results back to profiles by domain.
    """
    if not APIFY_API_TOKEN:
        print("[LEADS_FINDER] APIFY_API_TOKEN not set — skipping")
        return profiles

    apollo = ApolloEnrichment('')  # static methods only

    # Collect unique enrichable domains from profiles with no email
    domain_to_profile_idxs: Dict[str, List[int]] = {}
    for i, p in enumerate(profiles):
        if p.get('email'):
            continue
        website = p.get('personal_website') or p.get('url', '')
        if not website:
            continue
        domain = apollo.extract_domain(website)
        if domain and apollo.is_enrichable_domain(domain):
            domain_to_profile_idxs.setdefault(domain, []).append(i)

    domains = list(domain_to_profile_idxs.keys())
    if not domains:
        print("[LEADS_FINDER] No enrichable domains — skipping")
        return profiles

    print(f"[LEADS_FINDER] Looking up {len(domains)} domains...")

    try:
        from apify_client import ApifyClient
        apify = ApifyClient(APIFY_API_TOKEN)

        run_input = {
            'company_domain': domains,
            'email_status':   ['validated'],
            'fetch_count':    min(len(domains) * 5, 200),
        }
        run = apify.actor("code_crafter~leads-finder").call(
            run_input=run_input, timeout_secs=120
        )
        items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"[LEADS_FINDER] Got {len(items)} results")

        # Index results by domain
        best_by_domain: Dict[str, Dict] = {}
        for item in items:
            domain = (item.get('company_domain') or item.get('domain') or '').lower()
            if domain and domain not in best_by_domain:
                best_by_domain[domain] = item

        # Apply to profiles
        matched = 0
        for domain, idxs in domain_to_profile_idxs.items():
            match = best_by_domain.get(domain.lower())
            if not match:
                continue
            email = match.get('email') or match.get('work_email') or match.get('personal_email', '')
            if not email:
                continue
            for i in idxs:
                p = profiles[i]
                p['email'] = email
                if match.get('phone') and not p.get('phone'):
                    p['phone'] = match['phone']
                if match.get('linkedin_url') and not p.get('linkedin_url'):
                    p['linkedin_url'] = match['linkedin_url']
                if not p.get('creator_name') and match.get('first_name') and match.get('last_name'):
                    p['creator_name'] = f"{match['first_name']} {match['last_name']}"
                matched += 1

        print(f"[LEADS_FINDER] Matched emails for {matched} profiles")

    except Exception as e:
        print(f"[LEADS_FINDER] Error: {e}")
        import traceback
        traceback.print_exc()

    return profiles


# ============================================================================
# UNIFIED ENRICHMENT PIPELINE
# ============================================================================

def enrich_profiles_full_pipeline(profiles: List[Dict], job_id: str,
                                  platform: str) -> List[Dict]:
    """
    Full enrichment pipeline (platform-agnostic).

    Expects each profile dict to have at minimum:
      url           – primary URL (Patreon page / FB group URL)
      creator_name  – person name (may be empty)
      instagram_url, youtube_url, twitter_url, facebook_url, tiktok_url
      personal_website (may be None)
      rss_url       – optional podcast RSS feed URL

    Execution plan (mirrors colleague's 12-step parallel architecture):

      ── GROUP 1 (parallel Apify calls) ─────────────────────────────────
      A. Google Bridge        – facebook_group/meetup only; surfaces organizer
      B. RSS Feed Parsing     – profiles with rss_url (podcasts)
      C. Link Aggregators P1  – Linktree/Beacons URLs already in profile fields

      ── GROUP 2 (parallel Apify calls) ─────────────────────────────────
      D. YouTube About Pages  – profiles with youtube_url
      E. Instagram Bios       – profiles with instagram_url
      F. Twitter/X Bios       – profiles with twitter_url

      ── SEQUENTIAL ──────────────────────────────────────────────────────
      G. Link Aggregators P2  – NEW aggregator URLs surfaced in Group 2
      H. Google Contact Search– last resort: no email + no website + no LinkedIn
      I. Website Crawl        – 26 subpages + glob patterns + email priority logic
      J. Apollo.io            – person match for profiles still missing email
      K. Leads Finder         – domain-based Apify lookup
      L. MillionVerifier      – validate all discovered emails
    """
    if not profiles:
        return profiles

    from concurrent.futures import ThreadPoolExecutor, as_completed as futures_as_completed

    print(f"[ENRICH] Starting full pipeline for {len(profiles)} {platform} profiles")

    sgb    = SocialGraphBuilder(apify_token=APIFY_API_TOKEN)
    apollo = ApolloEnrichment(APOLLO_API_KEY) if APOLLO_API_KEY else None
    mv     = MillionVerifierClient(MILLIONVERIFIER_API_KEY) if MILLIONVERIFIER_API_KEY else None

    _LINK_AGG_HOSTS = set(SocialGraphBuilder.LINK_AGGREGATORS)

    def _is_aggregator(url: str) -> bool:
        return bool(url) and any(agg in url.lower() for agg in _LINK_AGG_HOSTS)

    # Track Pass 1 URLs so Pass 2 only processes genuinely new ones
    agg_url_to_idx_p1: Dict[str, List[int]] = {}

    # ------------------------------------------------------------------ #
    # GROUP 1 (parallel): Google Bridge | RSS | Link Agg Pass 1          #
    # ------------------------------------------------------------------ #
    print("[ENRICH] Group 1 (parallel): Google Bridge | RSS | Link Agg Pass 1")
    update_job_stage(job_id, 'enriching_core', 'running')

    def _g1_google_bridge() -> str:
        if platform in ('facebook_group', 'meetup'):
            sgb.google_bridge_enrich(profiles, job_id)
        return 'Google Bridge'

    def _g1_rss() -> str:
        sgb.parse_rss_feeds_batch(profiles)
        return 'RSS'

    def _g1_link_agg_p1() -> str:
        agg_urls: List[str] = []
        local_map: Dict[str, List[int]] = {}
        for i, p in enumerate(profiles):
            for field in ('personal_website', 'instagram_url', 'youtube_url',
                          'twitter_url', 'url'):
                val = p.get(field, '')
                if val and _is_aggregator(val):
                    agg_urls.append(val)
                    local_map.setdefault(val, []).append(i)
        if agg_urls:
            print(f"[ENRICH]   Link Agg P1: {len(set(agg_urls))} URLs")
            results = sgb.scrape_link_aggregators_batch(list(set(agg_urls)))
            for url, data in results.items():
                for i in local_map.get(url, []):
                    p = profiles[i]
                    if data.get('emails') and not p.get('email'):
                        p['email'] = data['emails'][0]
                    for key, val in data.get('social_links', {}).items():
                        if val and not p.get(key):
                            p[key] = val
                    if data.get('personal_website') and not p.get('personal_website'):
                        p['personal_website'] = data['personal_website']
        # Expose to outer scope for Pass 2 dedup (single write after Apify returns)
        agg_url_to_idx_p1.update(local_map)
        return 'Link Agg P1'

    with ThreadPoolExecutor(max_workers=3) as pool:
        g1_tasks = {
            pool.submit(_g1_google_bridge): 'Google Bridge',
            pool.submit(_g1_rss):           'RSS',
            pool.submit(_g1_link_agg_p1):   'Link Agg P1',
        }
        for fut in futures_as_completed(g1_tasks):
            label = g1_tasks[fut]
            try:
                fut.result()
                print(f"[ENRICH]   {label} done")
            except Exception as e:
                print(f"[ENRICH]   {label} error: {e}")

    update_job_stage(job_id, 'enriching_core', 'completed')

    # ------------------------------------------------------------------ #
    # GROUP 2 (parallel): YouTube | Instagram | Twitter bios             #
    # ------------------------------------------------------------------ #
    print("[ENRICH] Group 2 (parallel): YouTube About Pages | Instagram | Twitter bios")
    update_job_stage(job_id, 'enriching_social', 'running')

    def _g2_youtube() -> str:
        sgb.scrape_youtube_about_pages_batch(profiles)
        return 'YouTube'

    def _g2_instagram() -> str:
        sgb.scrape_instagram_bios_batch(profiles)
        return 'Instagram'

    def _g2_twitter() -> str:
        sgb.scrape_twitter_bios_batch(profiles)
        return 'Twitter'

    with ThreadPoolExecutor(max_workers=3) as pool:
        g2_tasks = {
            pool.submit(_g2_youtube):   'YouTube',
            pool.submit(_g2_instagram): 'Instagram',
            pool.submit(_g2_twitter):   'Twitter',
        }
        for fut in futures_as_completed(g2_tasks):
            label = g2_tasks[fut]
            try:
                fut.result()
                print(f"[ENRICH]   {label} done")
            except Exception as e:
                print(f"[ENRICH]   {label} error: {e}")

    update_job_stage(job_id, 'enriching_social', 'completed')

    # ------------------------------------------------------------------ #
    # Link Aggregators Pass 2                                             #
    # Scrape NEW aggregator URLs surfaced during Group 2.                #
    # Only for profiles that still have no email.                        #
    # ------------------------------------------------------------------ #
    print("[ENRICH] Link Aggregators Pass 2")
    update_job_stage(job_id, 'link_agg_p2', 'running')

    agg_urls_p2: List[str] = []
    agg_url_to_idx_p2: Dict[str, List[int]] = {}
    for i, p in enumerate(profiles):
        if p.get('email'):
            continue
        lt = p.get('linktree_url', '')
        if lt and _is_aggregator(lt) and lt not in agg_url_to_idx_p1:
            agg_urls_p2.append(lt)
            agg_url_to_idx_p2.setdefault(lt, []).append(i)

    if agg_urls_p2:
        print(f"[ENRICH]   Pass 2: {len(set(agg_urls_p2))} new aggregator URLs")
        agg_results_p2 = sgb.scrape_link_aggregators_batch(list(set(agg_urls_p2)))
        for url, data in agg_results_p2.items():
            for i in agg_url_to_idx_p2.get(url, []):
                p = profiles[i]
                if data.get('emails') and not p.get('email'):
                    p['email'] = data['emails'][0]
                for key, val in data.get('social_links', {}).items():
                    if val and not p.get(key):
                        p[key] = val
                if data.get('personal_website') and not p.get('personal_website'):
                    p['personal_website'] = data['personal_website']

    update_job_stage(job_id, 'link_agg_p2', 'completed',
                     new_urls_scraped=len(set(agg_urls_p2)))

    # ------------------------------------------------------------------ #
    # Google Contact Search  (last resort)                               #
    # ------------------------------------------------------------------ #
    print("[ENRICH] Google Contact Search")
    update_job_stage(job_id, 'contact_search', 'running')
    profiles = sgb.google_contact_search(profiles, job_id)
    update_job_stage(job_id, 'contact_search', 'completed')

    # ------------------------------------------------------------------ #
    # Website Crawl  (26 subpages + glob patterns + email priority)      #
    # ------------------------------------------------------------------ #
    print("[ENRICH] Website Crawl")
    update_job_stage(job_id, 'website_crawl', 'running')

    websites_to_crawl: List[str] = []
    website_to_idx: Dict[str, List[int]] = {}
    for i, p in enumerate(profiles):
        site = p.get('personal_website', '')
        if (site and not _is_aggregator(site)
                and ApolloEnrichment.is_enrichable_domain(
                    ApolloEnrichment.extract_domain(site))):
            websites_to_crawl.append(site)
            website_to_idx.setdefault(site, []).append(i)

    if websites_to_crawl:
        print(f"[ENRICH]   Crawling {len(set(websites_to_crawl))} websites")
        website_results = sgb.crawl_websites_batch(list(set(websites_to_crawl)))
        for domain, data in website_results.items():
            for site, idxs in website_to_idx.items():
                if ApolloEnrichment.extract_domain(site) == domain:
                    for i in idxs:
                        p = profiles[i]
                        if data.get('emails') and not p.get('email'):
                            p['email'] = data['emails'][0]
                        for key, val in data.get('social_links', {}).items():
                            if val and not p.get(key):
                                p[key] = val

    update_job_stage(job_id, 'website_crawl', 'completed',
                     sites_crawled=len(set(websites_to_crawl)))

    # ------------------------------------------------------------------ #
    # Apollo.io                                                           #
    # ------------------------------------------------------------------ #
    print("[ENRICH] Apollo.io")
    update_job_stage(job_id, 'apollo', 'running')

    if apollo:
        apollo_hits = 0
        seen_hashes: set = set()

        for p in profiles:
            if p.get('email'):
                continue  # already have email

            name     = p.get('creator_name', '').strip()
            site     = p.get('personal_website', '')
            domain   = ApolloEnrichment.extract_domain(site) if site else ''
            org      = p.get('group_name', '')
            linkedin = p.get('linkedin_url', '')

            if not (name or domain):
                continue
            if domain and not ApolloEnrichment.is_enrichable_domain(domain):
                continue

            input_hash = ApolloEnrichment.make_input_hash(
                name=name, domain=domain, org=org, linkedin=linkedin
            )
            if input_hash in seen_hashes:
                continue
            seen_hashes.add(input_hash)

            result = apollo.person_match(
                name=name or None,
                domain=domain or None,
                org_name=org or None,
                linkedin_url=linkedin or None,
            )

            if result:
                if result.get('email'):
                    p['email'] = result['email']
                    apollo_hits += 1
                    print(f"[APOLLO] Found email for {name or domain}")
                if result.get('first_name') and not p.get('creator_name'):
                    p['creator_name'] = (
                        f"{result['first_name']} {result.get('last_name', '')}".strip()
                    )
                if result.get('phone') and not p.get('phone'):
                    p['phone'] = result['phone']
                if result.get('linkedin') and not p.get('linkedin_url'):
                    p['linkedin_url'] = result['linkedin']
                if result.get('twitter') and not p.get('twitter_url'):
                    p['twitter_url'] = result['twitter']

            time.sleep(0.3)

        print(f"[ENRICH]   Apollo found {apollo_hits} emails")
        update_job_stage(job_id, 'apollo', 'completed', emails_found=apollo_hits)
    else:
        print("[ENRICH]   Apollo skipped (no API key)")
        update_job_stage(job_id, 'apollo', 'skipped')

    # ------------------------------------------------------------------ #
    # Leads Finder                                                        #
    # ------------------------------------------------------------------ #
    print("[ENRICH] Leads Finder")
    update_job_stage(job_id, 'leads_finder', 'running')
    profiles = enrich_with_leads_finder(profiles, job_id)
    update_job_stage(job_id, 'leads_finder', 'completed')

    # ------------------------------------------------------------------ #
    # MillionVerifier  (validate all discovered emails)                   #
    # ------------------------------------------------------------------ #
    print("[ENRICH] MillionVerifier email validation")
    update_job_stage(job_id, 'email_validation', 'running')

    if mv:
        email_items = [
            {'email': p['email'], 'idx': i}
            for i, p in enumerate(profiles)
            if p.get('email')
        ]
        if email_items:
            print(f"[ENRICH]   Validating {len(email_items)} emails")
            validation_results = mv.verify_batch(email_items)
            for item in email_items:
                status = validation_results.get(item['email'], 'unknown')
                profiles[item['idx']]['email_validation_status'] = status
            valid_count = sum(1 for s in validation_results.values() if s == 'valid')
            print(f"[ENRICH]   {valid_count}/{len(email_items)} emails valid")
            update_job_stage(job_id, 'email_validation', 'completed',
                             emails_validated=len(email_items), valid=valid_count)
        else:
            print("[ENRICH]   No emails to validate")
            update_job_stage(job_id, 'email_validation', 'skipped')
    else:
        print("[ENRICH]   MillionVerifier skipped (no API key)")
        update_job_stage(job_id, 'email_validation', 'skipped')

    print(f"[ENRICH] Pipeline complete for {len(profiles)} profiles")
    return profiles


# ============================================================================
# STANDARDIZE: PATREON → HUBSPOT
# ============================================================================

def standardize_patreon_profiles(profiles: List[Dict]) -> List[Dict]:
    """
    Map enriched Patreon profiles to the exact HubSpot contact property names
    required for batch-create. Unknown / None / empty values are dropped.
    """
    standardized = []

    for i, profile in enumerate(profiles):
        try:
            props = {
                # ── Universal social links ──────────────────────────────
                'email':                profile.get('email'),
                'instagram_handle':     profile.get('instagram_url'),
                'youtube_profile_link': profile.get('youtube_url'),
                'tiktok_handle':        profile.get('tiktok_url'),
                'website':              profile.get('personal_website'),
                'twitterhandle':        profile.get('twitter_url'),
                'facebook_profile_link': profile.get('facebook_url'),

                # ── Patreon-specific ───────────────────────────────────
                'patreon_link':         profile.get('url'),
                'patreon_title':        profile.get('creator_name') or profile.get('name'),
                'total_patrons':        (profile.get('patron_count')
                                         or profile.get('total_members')),
                'paid_patrons':         (profile.get('paid_members')
                                         or profile.get('paid_patrons')),
                'patreon_description':  (profile.get('about')
                                         or profile.get('description')),

                # ── Metadata / channel tracking ───────────────────────
                'flagship_social_platform': 'patreon',
                'channel':                  'Outbound',
                'channel_host_prospected':  'Phyllo',
                'funnel':                   'Community',
            }

            # Drop None / empty-string / zero values
            props = {k: v for k, v in props.items()
                     if v is not None and v != '' and v != 0}

            standardized.append(props)

        except Exception as e:
            print(f"[STANDARDIZE] Patreon profile #{i+1} error: {e}")
            continue

    print(f"[STANDARDIZE] {len(standardized)} Patreon profiles ready for HubSpot")
    return standardized


# ============================================================================
# STANDARDIZE: FACEBOOK GROUPS → HUBSPOT
# ============================================================================

def standardize_facebook_profiles(profiles: List[Dict]) -> List[Dict]:
    """
    Map enriched Facebook Group profiles to the exact HubSpot contact property
    names required for batch-create. Unknown / None / empty values are dropped.
    """
    standardized = []

    for i, profile in enumerate(profiles):
        try:
            props = {
                # ── Universal social links ──────────────────────────────
                'email':                profile.get('email'),
                'instagram_handle':     profile.get('instagram_url'),
                'youtube_profile_link': profile.get('youtube_url'),
                'tiktok_handle':        profile.get('tiktok_url'),
                'website':              profile.get('personal_website'),
                'twitterhandle':        profile.get('twitter_url'),
                'facebook_profile_link': profile.get('facebook_url'),

                # ── Facebook Groups-specific ──────────────────────────
                'facebook_group_link':        profile.get('group_url'),
                'facebook_group_name':        profile.get('group_name'),
                'facebook_group_size':        profile.get('member_count') or None,
                'facebook_group_description': profile.get('description'),

                # ── Metadata / channel tracking ───────────────────────
                'flagship_social_platform': 'facebook_group',
                'channel':                  'Outbound',
                'channel_host_prospected':  'Phyllo',
                'funnel':                   'Community',
            }

            # Drop None / empty-string / zero values
            props = {k: v for k, v in props.items()
                     if v is not None and v != '' and v != 0}

            standardized.append(props)

        except Exception as e:
            print(f"[STANDARDIZE] Facebook profile #{i+1} error: {e}")
            continue

    print(f"[STANDARDIZE] {len(standardized)} Facebook profiles ready for HubSpot")
    return standardized


# ============================================================================
# REDIS JOB STATUS TRACKER
# ============================================================================

def update_discovery_job_status(job_id, status, **kwargs):
    """Update discovery job status in Redis (24-hour TTL)."""
    try:
        import redis as redis_lib
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        r = redis_lib.from_url(redis_url, decode_responses=True)

        job_key  = f'discovery_job:{job_id}'
        job_data = r.get(job_key)
        job_data = json.loads(job_data) if job_data else {'job_id': job_id}

        job_data['status']     = status
        job_data['updated_at'] = datetime.now().isoformat()
        job_data.update(kwargs)

        r.setex(job_key, 86400, json.dumps(job_data))
        print(f"Job {job_id} → {status}")
    except Exception as e:
        print(f"Failed to update job status: {e}")


def update_job_stage(job_id: str, stage: str, status: str, **metrics):
    """
    Update a specific pipeline stage within a discovery job's Redis record.

    Stages: discovery | filtering | enriching_core | enriching_social |
            link_agg_p2 | contact_search | website_crawl | apollo |
            leads_finder | email_validation | hubspot_import

    status: 'running' | 'completed' | 'failed' | 'skipped'
    metrics: arbitrary key/value data recorded on the stage (e.g. profiles_found=47)
    """
    try:
        import redis as redis_lib
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        r = redis_lib.from_url(redis_url, decode_responses=True)

        job_key  = f'discovery_job:{job_id}'
        job_data = r.get(job_key)
        if not job_data:
            return
        job = json.loads(job_data)

        if 'stages' not in job:
            job['stages'] = {}

        now = datetime.now().isoformat()
        stage_rec = job['stages'].get(stage, {})
        stage_rec['status'] = status

        if status == 'running':
            stage_rec['started_at'] = now
            job['current_stage'] = stage
        elif status in ('completed', 'failed', 'skipped'):
            stage_rec['completed_at'] = now
            started = stage_rec.get('started_at')
            if started:
                try:
                    from datetime import datetime as _dt
                    delta = _dt.fromisoformat(now) - _dt.fromisoformat(started)
                    stage_rec['duration_s'] = round(delta.total_seconds(), 1)
                except Exception:
                    pass

        stage_rec.update(metrics)
        job['stages'][stage] = stage_rec
        job['updated_at'] = now

        r.setex(job_key, 86400, json.dumps(job))
    except Exception as e:
        print(f"[STAGE_UPDATE] Failed to update stage '{stage}': {e}")


# ============================================================================
# HUBSPOT BATCH IMPORT
# ============================================================================

def import_profiles_to_hubspot(profiles: List[Dict], job_id: str) -> Dict:
    """
    Import standardized profiles to HubSpot via batch contacts API.

    Profiles must already be in HubSpot property key format (output of
    standardize_patreon_profiles / standardize_facebook_profiles). This
    function passes them through directly with no additional field mapping.

    Batch size: 100 contacts per request (HubSpot limit).
    Returns: {'created': int, 'skipped': int}
    """
    if not HUBSPOT_API_KEY:
        raise ValueError("HUBSPOT_API_KEY must be set in environment")

    # Build contact inputs — profile dict IS the properties dict
    contacts = []
    for idx, profile in enumerate(profiles):
        properties = {k: v for k, v in profile.items()
                      if v is not None and v != ''}
        contacts.append({
            'properties':           properties,
            'objectWriteTraceId':   f"{job_id}_{idx}",
        })

    created_count = 0
    skipped_count = 0
    total_batches = (len(contacts) + 99) // 100

    print(f"[HUBSPOT] Importing {len(contacts)} contacts in {total_batches} batches")

    for i in range(0, len(contacts), 100):
        batch     = contacts[i:i + 100]
        batch_num = (i // 100) + 1

        try:
            resp = requests.post(
                f"{HUBSPOT_API_URL}/crm/v3/objects/contacts/batch/create",
                headers={
                    'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                    'Content-Type':  'application/json',
                },
                json={'inputs': batch},
                timeout=30,
            )

            if resp.status_code == 201:
                created_count += len(batch)
                print(f"[HUBSPOT] Batch {batch_num}/{total_batches}: {len(batch)} created")

            elif resp.status_code == 207:
                result        = resp.json()
                batch_created = len(result.get('results', []))
                batch_errors  = result.get('errors', [])
                batch_skipped = len(batch_errors)

                created_count += batch_created
                skipped_count += batch_skipped

                print(f"[HUBSPOT] Batch {batch_num}/{total_batches}: "
                      f"{batch_created} created, {batch_skipped} duplicates/errors")
                for err in batch_errors[:3]:
                    print(f"  Error: {err.get('message', 'Unknown')}")

            else:
                print(f"[HUBSPOT] Batch {batch_num} error: "
                      f"{resp.status_code} — {resp.text[:200]}")
                skipped_count += len(batch)

        except Exception as e:
            print(f"[HUBSPOT] Exception on batch {batch_num}: {e}")
            skipped_count += len(batch)

        if i + 100 < len(contacts):
            time.sleep(0.5)

    print(f"[HUBSPOT] Import complete: {created_count} created, {skipped_count} skipped")
    return {'created': created_count, 'skipped': skipped_count}
