import os
import json
import requests
import tempfile
import base64
import hashlib
from typing import Dict, List, Any, Tuple
from datetime import datetime, timedelta
import boto3
from botocore.client import Config
from celery_app import celery_app
from openai import OpenAI
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO

# Configuration from environment variables
INSIGHTIQ_USERNAME = os.getenv('INSIGHTIQ_USERNAME')
INSIGHTIQ_PASSWORD = os.getenv('INSIGHTIQ_PASSWORD')
INSIGHTIQ_WORK_PLATFORM_ID = os.getenv('INSIGHTIQ_WORK_PLATFORM_ID')
INSIGHTIQ_API_URL = os.getenv('INSIGHTIQ_API_URL', 'https://api.staging.insightiq.ai')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
HUBSPOT_WEBHOOK_URL = os.getenv('HUBSPOT_WEBHOOK_URL')

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
        print("OpenAI client initialized successfully")
    except Exception as e:
        print(f"ERROR initializing OpenAI client: {e}")


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
        'retreat', 'workshop', 'trip', 'tour', 'travel', 'getaway', 'adventure',
        'join me', 'join us', 'book now', 'spaces available', 'registration open',
        'destination', 'experience', 'journey', 'expedition', 'immersion',
        'hosted', 'hosting', 'leading', 'guiding'
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
- Watersports: surfing, kitesurfing, scuba diving
- Hunting
- Traveling with children (family travel accounts/family vloggers)

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

UNSUPPORTED PROFILE TYPES (disqualify if HIGH CONFIDENCE):
- Brand accounts (no personal creator)
- Meme accounts / content aggregators
- Accounts that only repost content
- Explicit or offensive content
- Content focused on firearms
- Family accounts / family travel accounts (PRIMARY focus on kids/family)
- Creator appears under age 18
- Non-English speaking creator (primary language is not English)

PASS TO NEXT STAGE if:
- Is not an UNSUPPORTED PROFILE TYPE and does not show any UNSUPPORTED ACTIVITIES
- ANY uncertainty about whether to disqualify

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
  "selected_content_indices": [0, 3, 7]  // ONLY if decision is "continue", otherwise empty array
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


def transcribe_video_with_whisper(video_url: str) -> str:
    """Transcribe video using Whisper"""
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


def analyze_content_item(media_url: str, media_format: str) -> Dict[str, Any]:
    """Analyze a single content item"""
    
    if media_format == 'IMAGE':
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": """Analyze this social media image covering: theme/topic, what creator shares, visual style, text/captions, creator visibility, monetization signs, CTAs, audience engagement style.

Respond in JSON: {"summary": "3-4 sentence summary"}"""
                }, {
                    "type": "image_url",
                    "image_url": {"url": media_url}
                }]
            }],
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        return {"type": "IMAGE", "url": media_url, "summary": result['summary']}
    
    else:  # VIDEO
        transcript = transcribe_video_with_whisper(media_url)
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "user",
                "content": f"""Based on this transcription, provide detailed summary covering: theme, what creator shares, how they address audience, monetization, CTAs, presence, tone.

TRANSCRIPTION: {transcript}

Respond in JSON: {{"summary": "3-4 sentence summary"}}"""
            }],
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        return {"type": "VIDEO", "url": media_url, "summary": result['summary']}


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
            "content": "You analyze creators to profile their content strategy, audience engagement, and monetization."
        }, {
            "role": "user",
            "content": f"""Create structured creator profile covering: content category, content types, audience engagement, creator presence, monetization, community building.

CONTENT: {combined}

JSON format with those 6 fields as arrays/strings."""
        }],
        response_format={"type": "json_object"}
    )
    
    return json.loads(response.choices[0].message.content)


def generate_lead_score(content_analyses: List[Dict[str, Any]], creator_profile: Dict[str, Any]) -> Dict[str, Any]:
    """Generate TrovaTrip lead score based on ICP criteria - v1.1 (Production)"""
    summaries = [f"Content {idx} ({item['type']}): {item['summary']}" for idx, item in enumerate(content_analyses, 1)]
    combined = "\n\n".join(summaries)
    
    profile_context = f"""PROFILE:
- Category: {creator_profile.get('content_category')}
- Types: {creator_profile.get('content_types')}
- Engagement: {creator_profile.get('audience_engagement')}
- Presence: {creator_profile.get('creator_presence')}
- Monetization: {creator_profile.get('monetization')}
- Community: {creator_profile.get('community_building')}"""
    
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{
            "role": "system",
            "content": """You score creators for TrovaTrip, a group travel platform where creators host trips with their communities.

CRITICAL: A good fit is someone whose AUDIENCE wants to meet each other AND the host in real life. Examples: book clubs traveling to Ireland, widow communities on healing retreats, food bloggers doing culinary tours, wellness enthusiasts doing fitness / wellness retreats

BAD FITS to avoid:
- Pure artists/performers with fan bases (not communities)
- Very niche specialists where audience doesn't want group travel
- Pastors / religious figures
- Politicians
- Creators who do only post transactional content (no invitation/CTA for audience to contribute/comment)
- Creators without clear monetization (not business-minded)

SCORING CRITERIA (0.0-1.0 each):"""
        }, {
            "role": "user",
            "content": f"""{profile_context}

CONTENT: {combined}

Score these 5 sections (0.0 to 1.0):

1. **niche_and_audience_identity** (0.0-1.0)
   HIGH scores (0.7-1.0): Clear lifestyle niche where audience shares identity (including but not limited to: widows, DINKs, book lovers, history nerds, foodies, wellness seekers). People want to connect with EACH OTHER in addition to the host.
   LOW scores (0.0-0.4): Generic content, pure performance/art fans, religious-primary content, very technical/specialized, or unclear who the audience is.

2. **host_likeability_and_content_style** (0.0-1.0)
   HIGH scores (0.7-1.0): Face-forward, appears regularly on camera, warm/conversational tone, shares experiences, content facilitates connection with audience through vulnerability and authenticity, genuine interest in knowing their audience and having their audience know them.
   LOW scores (0.0-0.4): Behind-the-camera content, aesthetic-only, formal/sterile tone, doesn't show personality, pure expertise without relatability.

3. **monetization_and_business_mindset** (0.0-1.0)
   HIGH scores (0.7-1.0): Already selling something (coaching, courses, products, Patreon, brand deals, services). Audience pays for access. Comfortable with sales/launches.
   LOW scores (0.0-0.4): No monetization, only donations, free content only, or explicitly states "no monetization."

4. **community_infrastructure** (0.0-1.0)
   HIGH scores (0.7-1.0): Has owned channels (email list, podcast, YouTube, Patreon, Discord, membership, in-person groups, private Facebook group). Can reach audience directly.
   LOW scores (0.0-0.4): Only social media presence, no owned channels mentioned, purely algorithm-dependent.

5. **trip_fit_and_travelability** (0.0-1.0)
   HIGH scores (0.7-1.0): Content naturally fits a trip (including but not limited to food/wine tours, history tours, retreats of any kind, adventure travel, cultural experiences). Audience has money/time for travel (including but not limited to professionals, DINKs, older audiences). Already travels or audience asks to travel together.
   LOW scores (0.0-0.4): No natural trip concept, very young/broke audience, content doesn't translate to group experiences, highly specialized/technical focus.

Also provide:
- **combined_lead_score**: Weighted average: (niche × 0.25) + (likeability × 0.20) + (monetization × 0.25) + (community × 0.15) + (trip_fit × 0.15)
- **score_reasoning**: 2-3 sentences on fit for group travel with their community.

RESPOND ONLY with JSON:
{{
  "niche_and_audience_identity": 0.0,
  "host_likeability_and_content_style": 0.0,
  "monetization_and_business_mindset": 0.0,
  "community_infrastructure": 0.0,
  "trip_fit_and_travelability": 0.0,
  "combined_lead_score": 0.0,
  "score_reasoning": "..."
}}"""
        }],
        response_format={"type": "json_object"}
    )
    
    result = json.loads(response.choices[0].message.content)
    print(f"GPT Lead Score Response: {json.dumps(result, indent=2)}")
    
    # Extract section scores
    section_scores = {
        "niche_and_audience_identity": result.get('niche_and_audience_identity', 0.0),
        "host_likeability_and_content_style": result.get('host_likeability_and_content_style', 0.0),
        "monetization_and_business_mindset": result.get('monetization_and_business_mindset', 0.0),
        "community_infrastructure": result.get('community_infrastructure', 0.0),
        "trip_fit_and_travelability": result.get('trip_fit_and_travelability', 0.0)
    }
    
    return {
        "section_scores": section_scores,
        "lead_score": result.get('combined_lead_score', 0.0),
        "score_reasoning": result.get('score_reasoning', '')
    }


def send_to_hubspot(contact_id: str, lead_score: float, section_scores: Dict, score_reasoning: str, 
                    creator_profile: Dict, content_analyses: List[Dict]):
    """Send results to HubSpot with validation"""
    content_summaries = [f"Content {idx} ({item['type']}): {item['summary']}" 
                        for idx, item in enumerate(content_analyses, 1)]
    
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
    
    payload = {
        "contact_id": contact_id,
        "lead_score": lead_score,
        "score_reasoning": score_reasoning,
        "score_niche_and_audience": section_scores.get('niche_and_audience_identity', 0.0),
        "score_host_likeability": section_scores.get('host_likeability_and_content_style', 0.0),
        "score_monetization": section_scores.get('monetization_and_business_mindset', 0.0),
        "score_community_infrastructure": section_scores.get('community_infrastructure', 0.0),
        "score_trip_fit": section_scores.get('trip_fit_and_travelability', 0.0),
        "content_summary_structured": "\n\n".join(content_summaries),
        "profile_category": safe_str(creator_profile.get('content_category')),
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
                content_analyses=[]
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
                    'host_likeability_and_content_style': 0.20,
                    'monetization_and_business_mindset': 0.20,
                    'community_infrastructure': 0.20,
                    'trip_fit_and_travelability': 0.20
                },
                score_reasoning=f"Pre-screen rejected: {pre_screen_result.get('reasoning')}",
                creator_profile={'content_category': 'Pre-screened out'},
                content_analyses=[]
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
                content_analyses.append(analysis)
                print(f"Item {idx}: Successfully analyzed")
                
            except Exception as e:
                print(f"Item {idx}: Error analyzing: {e}")
                continue
        
        if not content_analyses:
            return {"status": "error", "message": "Could not analyze any selected content items"}
        
        print(f"Successfully analyzed {len(content_analyses)} items")
        
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
            'has_travel_experience': has_travel_experience,
            'timestamp': datetime.now().isoformat(),
            'items_analyzed': len(content_analyses)
        }
        save_analysis_cache(contact_id, cache_data)
        
        # Step 8: Calculate lead score
        self.update_state(state='PROGRESS', meta={'stage': 'Calculating lead score'})
        lead_analysis = generate_lead_score(content_analyses, creator_profile)
        
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
            content_analyses
        )
        
        print(f"=== COMPLETE: {contact_id} - Score: {lead_analysis['lead_score']} ===")
        
        return {
            "status": "success",
            "contact_id": contact_id,
            "lead_score": lead_analysis['lead_score'],
            "section_scores": lead_analysis.get('section_scores', {}),
            "creator_profile": creator_profile,
            "items_analyzed": len(content_analyses),
            "pre_screen_passed": True,
            "travel_experience_detected": has_travel_experience
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
