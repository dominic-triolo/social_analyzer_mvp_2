import os
import json
import requests
from flask import Flask, request, jsonify
from typing import Dict, List, Any
from datetime import datetime
import tempfile
import base64
import hashlib
import boto3
from botocore.client import Config

app = Flask(__name__)

# Configuration from environment variables
INSIGHTIQ_USERNAME = os.getenv('INSIGHTIQ_USERNAME')
INSIGHTIQ_PASSWORD = os.getenv('INSIGHTIQ_PASSWORD')
INSIGHTIQ_WORK_PLATFORM_ID = os.getenv('INSIGHTIQ_WORK_PLATFORM_ID')
INSIGHTIQ_API_URL = os.getenv('INSIGHTIQ_API_URL', 'https://api.sandbox.insightiq.ai')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
HUBSPOT_WEBHOOK_URL = os.getenv('HUBSPOT_WEBHOOK_URL')

# R2 Configuration
R2_ACCESS_KEY_ID = os.getenv('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.getenv('R2_SECRET_ACCESS_KEY')
R2_BUCKET_NAME = os.getenv('R2_BUCKET_NAME')
R2_ENDPOINT_URL = os.getenv('R2_ENDPOINT_URL')
R2_PUBLIC_URL = os.getenv('R2_PUBLIC_URL')

# Check required environment variables
required_vars = {
    'INSIGHTIQ_USERNAME': INSIGHTIQ_USERNAME,
    'INSIGHTIQ_PASSWORD': INSIGHTIQ_PASSWORD,
    'INSIGHTIQ_WORK_PLATFORM_ID': INSIGHTIQ_WORK_PLATFORM_ID,
    'OPENAI_API_KEY': OPENAI_API_KEY,
    'HUBSPOT_WEBHOOK_URL': HUBSPOT_WEBHOOK_URL,
    'R2_ACCESS_KEY_ID': R2_ACCESS_KEY_ID,
    'R2_SECRET_ACCESS_KEY': R2_SECRET_ACCESS_KEY,
    'R2_BUCKET_NAME': R2_BUCKET_NAME,
    'R2_ENDPOINT_URL': R2_ENDPOINT_URL,
    'R2_PUBLIC_URL': R2_PUBLIC_URL
}

missing_vars = [k for k, v in required_vars.items() if not v]
if missing_vars:
    print(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}")
    print("App cannot function without these variables!")

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
        r2_client = None
else:
    print("WARNING: R2 credentials not set - re-hosting will be skipped")

# Initialize OpenAI client - try/except for different versions
client = None
if not OPENAI_API_KEY:
    print("CRITICAL ERROR: OPENAI_API_KEY is not set!")
else:
    try:
        # Try newer import style first
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        print("OpenAI client initialized successfully (new style)")
    except (ImportError, TypeError) as e:
        print(f"New style failed: {e}, trying old style...")
        try:
            # Fallback to older import style
            import openai
            openai.api_key = OPENAI_API_KEY
            client = openai
            print("OpenAI client initialized successfully (old style)")
        except Exception as e2:
            print(f"ERROR initializing OpenAI client: {e2}")
            client = None


def fetch_social_content(profile_url: str) -> Dict[str, Any]:
    """Fetch content from InsightIQ API"""
    url = f"{INSIGHTIQ_API_URL}/v1/social/creators/contents/fetch"
    
    # Create Basic Auth header
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
    
    # Debug logging
    print(f"InsightIQ Request URL: {url}")
    print(f"Payload: {payload}")
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        print(f"InsightIQ Response Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Error Response Body: {response.text}")
            
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response content: {response.text}")
        raise


def rehost_media_on_r2(media_url: str, contact_id: str, media_format: str) -> str:
    """Download media from Instagram CDN and upload to R2, return public URL"""
    if not r2_client:
        print("R2 client not available, returning original URL")
        return media_url
    
    try:
        # Download media
        print(f"Downloading media from: {media_url[:100]}...")
        media_response = requests.get(media_url, timeout=30)
        media_response.raise_for_status()
        
        # Generate unique filename
        url_hash = hashlib.md5(media_url.encode()).hexdigest()
        
        # Determine extension based on media format
        if media_format == 'VIDEO':
            extension = 'mp4'
        else:
            # Try to get extension from URL
            extension = media_url.split('.')[-1].split('?')[0]
            if extension not in ['jpg', 'jpeg', 'png', 'gif', 'webp']:
                extension = 'jpg'  # Default to jpg
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        object_key = f"social_content/{contact_id}/{timestamp}_{url_hash}.{extension}"
        
        # Determine content type
        if media_format == 'VIDEO':
            content_type = 'video/mp4'
        else:
            content_type = media_response.headers.get('content-type', 'image/jpeg')
        
        # Upload to R2
        print(f"Uploading to R2: {object_key}")
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=object_key,
            Body=media_response.content,
            ContentType=content_type
        )
        
        # Return public URL
        public_url = f"{R2_PUBLIC_URL}/{object_key}"
        print(f"Media re-hosted successfully: {public_url}")
        return public_url
        
    except Exception as e:
        print(f"ERROR re-hosting media: {e}")
        print("Falling back to original URL")
        return media_url


def determine_media_format(media_url: str) -> str:
    """Determine if media is IMAGE or VIDEO"""
    url_lower = media_url.lower()
    
    video_extensions = ['.mp4', '.mov', '.avi', '.webm', '.m4v', '.mkv']
    image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp']
    
    for ext in video_extensions:
        if ext in url_lower:
            return 'VIDEO'
    
    for ext in image_extensions:
        if ext in url_lower:
            return 'IMAGE'
    
    # Default to IMAGE if unclear
    return 'IMAGE'


def transcribe_video_with_whisper(video_url: str) -> str:
    """Download video temporarily and transcribe using OpenAI Whisper"""
    # Download video to temp file
    video_response = requests.get(video_url, timeout=30)
    video_response.raise_for_status()
    
    # Create temp file with proper extension
    with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as temp_video:
        temp_video.write(video_response.content)
        temp_video_path = temp_video.name
    
    try:
        # Transcribe with Whisper
        with open(temp_video_path, 'rb') as audio_file:
            transcript = client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file
            )
        return transcript.text
    finally:
        # Clean up temp file
        os.unlink(temp_video_path)


def analyze_content_item(media_url: str, media_format: str) -> Dict[str, Any]:
    """Analyze a single content item and return a summary"""
    
    if media_format == 'IMAGE':
        # Use GPT-4 Vision for images
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": """Analyze this social media image and provide a detailed summary covering:

1. Content theme/topic (e.g., fitness, fashion, food, travel, lifestyle, etc.)
2. What the creator is sharing (advice, personal update, product showcase, tutorial, entertainment, storytelling, etc.)
3. Visual composition and style
4. Any text, captions, or messaging visible
5. Whether the creator is visible in the image (and if so, how prominently)
6. Signs of monetization (product placements, brand mentions, sponsored content indicators)
7. Any calls-to-action or community building efforts (subscribe, join, follow, link in bio, etc.)
8. How the creator addresses the audience (directly speaking to viewers, casual tone, professional, etc.)

Respond in JSON format:
{
  "summary": "comprehensive 3-4 sentence summary covering the points above"
}"""
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": media_url
                            }
                        }
                    ]
                }
            ],
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        return {
            "type": "IMAGE",
            "url": media_url,
            "summary": result['summary']
        }
    
    else:  # VIDEO
        # Step 1: Transcribe video with Whisper
        print(f"Transcribing video: {media_url}")
        transcript = transcribe_video_with_whisper(media_url)
        
        # Step 2: Summarize the video using the transcription
        print(f"Summarizing video based on transcription")
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": f"""Based on this video transcription, provide a detailed summary covering:

1. Content theme/topic (e.g., fitness advice, personal vlog, product review, tutorial, storytelling, etc.)
2. What the creator is sharing (advice/expertise, personal experience, entertainment, education, product promotion, etc.)
3. Main points and key messages
4. How the creator addresses the audience (speaking directly to camera, using "you", asking questions, casual vs professional tone, etc.)
5. Signs of monetization (mentions of sponsors, products, affiliate links, paid partnerships, etc.)
6. Any calls-to-action or community building (subscribe, join mailing list/Patreon/Discord, visit website, etc.)
7. The creator's presence and on-camera style
8. Overall tone and approach

TRANSCRIPTION:
{transcript}

Respond in JSON format:
{{
  "summary": "comprehensive 3-4 sentence summary covering the points above"
}}"""
                }
            ],
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        return {
            "type": "VIDEO",
            "url": media_url,
            "summary": result['summary']
        }


def generate_creator_profile(content_analyses: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate a structured creator profile based on content summaries"""
    
    # Extract all the individual summaries AND descriptions
    summaries = []
    for idx, item in enumerate(content_analyses, 1):
        summary_text = f"Content {idx} ({item['type']}): {item['summary']}"
        if item.get('description'):
            summary_text += f"\nOriginal Post Description: {item['description']}"
        summaries.append(summary_text)
    
    combined_summaries = "\n\n".join(summaries)
    
    # Generate structured creator profile
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": """You are a social media analyst who creates detailed creator profiles. Analyze the content summaries to understand the creator's content strategy, audience engagement, and monetization approach."""
            },
            {
                "role": "user",
                "content": f"""Based on these content summaries, create a structured creator profile covering these aspects:

1. Content Category/Theme: What is the primary category or theme of content this creator posts? (e.g., hiking/backpacking, fitness, fashion/beauty, books/literature, food/cooking, travel, technology, etc.)

2. Content Types: What types of things does the creator share in their content? (e.g., advice/expertise, personal updates, entertaining content, educational content, product reviews, tutorials, storytelling, etc.)

3. Audience Engagement: To what degree does the creator address their audience directly or invite them to join the discourse? (e.g., frequently speaks directly to camera, uses second-person language in captions, asks questions, encourages comments, rarely addresses audience directly, etc.)

4. Creator Presence: How frequently is the creator in front of the camera or present in their content? (e.g., always visible, frequently visible, occasionally visible, rarely visible, never visible)

5. Monetization: Does the creator monetize their audience by advertising products or services in their content? (e.g., yes with frequent ads, yes with occasional sponsorships, subtle product placements, no visible monetization)

6. Community Building: Does the creator invite their audience to join a mailing list, Patreon, Discord, Substack, Facebook group, or other community platform in their content? (e.g., yes with specific calls-to-action, mentions community platforms, no community building efforts visible)

CONTENT SUMMARIES:
{combined_summaries}

Respond in JSON format:
{{
  "content_category": "primary category/theme",
  "content_types": ["type1", "type2", "type3"],
  "audience_engagement": "description of how they engage audience",
  "creator_presence": "description of their on-camera presence",
  "monetization": "description of monetization approach",
  "community_building": "description of community building efforts"
}}"""
            }
        ],
        response_format={"type": "json_object"}
    )
    
    result = json.loads(response.choices[0].message.content)
    return result


def generate_lead_analysis(content_analyses: List[Dict[str, Any]], creator_profile: Dict[str, Any]) -> Dict[str, Any]:
    """Combine all content summaries and creator profile to generate a TrovaTrip-specific lead score"""
    
    # Extract all the individual summaries AND descriptions
    summaries = []
    for idx, item in enumerate(content_analyses, 1):
        # Include both the AI-generated summary and the original description
        summary_text = f"Content {idx} ({item['type']}): {item['summary']}"
        
        # Add the description if it exists and is not empty
        if item.get('description'):
            summary_text += f"\nOriginal Post Description: {item['description']}"
        
        summaries.append(summary_text)
    
    combined_summaries = "\n\n".join(summaries)
    
    # Format creator profile for context
    profile_context = f"""CREATOR PROFILE:
- Content Category: {creator_profile.get('content_category', 'Unknown')}
- Content Types: {', '.join(creator_profile.get('content_types', []))}
- Audience Engagement: {creator_profile.get('audience_engagement', 'Unknown')}
- Creator Presence: {creator_profile.get('creator_presence', 'Unknown')}
- Monetization: {creator_profile.get('monetization', 'Unknown')}
- Community Building: {creator_profile.get('community_building', 'Unknown')}"""
    
    # TrovaTrip-specific scoring
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": """You are a social media lead scoring analyst and you score creators for TrovaTrip.

COMPANY CONTEXT:
TrovaTrip is a platform that allows content creators, community leaders, and entrepreneurs to "host" group travel experiences with their audience/community around the globe. The Host is responsible for generating the bookings for their trip by marketing it to their audience/community. The Host gets paid for each trip based on bookings sold and travels for free, attending the trip with their travelers.

YOUR ROLE:
You score creators based on the likelihood that they will be interested in hosting a trip with their audience/community and the likelihood that they will be successful selling a trip to their audience/community. i.e. the degree to which the creator's audience/community is likely to be interested in connecting with each other and the creator in real life.

SCORING GUIDE:
- 0.0-0.3: Low quality, poor engagement potential
- 0.3-0.6: Moderate quality, some potential
- 0.6-0.8: Good quality, strong potential
- 0.8-1.0: Excellent quality, high-value lead"""
            },
            {
                "role": "user",
                "content": f"""Based on this creator's profile and content summaries, score them using the TrovaTrip rubric.

{profile_context}

CONTENT SUMMARIES:
{combined_summaries}

SCORING RUBRIC - Score each section 0.0 to 1.0:

1. NICHE & AUDIENCE IDENTITY (0.0-1.0)
- Clear, specific niche (e.g., history nerds, bookish romance readers, widows, queer women, nurses, DINKs, vanlife, interior design, outdoor science, dating/relationship)
- Followers share a recognizable identity or interest that could shape a trip: "my widows community," "queer girls," "book club," "DINK finance/lifestyle," "royal gossip/history nerds"
- People know exactly what they go to the Host for

2. HOST LIKEABILITY & CONTENT STYLE (0.0-1.0)
- Face-forward: Host regularly appears on camera, talks directly to their audience
- Warm, fun, safe, inclusive vibes → "I'd travel with them"
- Tone is inclusive, warm, and conversational (not sterile or purely aesthetic)
- Content already features experiences, trips, or "come with me" energy

3. MONETIZATION & BUSINESS MINDSET (0.0-1.0)
- Already monetizing in at least one way: Coaching/consults, interior design sessions, readings (tarot, astrology), courses/workshops, digital products, shop/Amazon storefront, merch, paid communities, Patreon, brand deals
- Audience is conditioned to pay for access or expertise
- Host is comfortable selling and running launches (deadlines, limited spots, bonuses, etc.)

4. COMMUNITY INFRASTRUCTURE/OWNED CHANNELS (0.0-1.0)
- Has at least one "depth" or owned channel: Email list, podcast, YouTube, FB group, Discord, Patreon, in-person groups, membership/sisterhood/book club, etc.
- Can reach audience without relying solely on one social platform's algorithm
- Ideally already communicates on a cadence (e.g., weekly newsletter or podcast)

5. TRIP FIT & AUDIENCE TRAVELABILITY (0.0-1.0)
- Niche naturally aligns with a trip concept (history in Europe, archaeology in Egypt, bookish tours in Ireland, vanlife/adventure trips, food & wine in Italy, grief/empowerment retreats, etc.)
- Audience life stage and finances make group travel realistic: DINKs, mid-career professionals, older wellness audiences, nurses, etc.
- Host already travels and shares it, or audience has expressed desire to travel with them

Respond in JSON format:
{{
  "section_scores": {{
    "niche_and_audience_identity": 0.0-1.0,
    "host_likeability_and_content_style": 0.0-1.0,
    "monetization_and_business_mindset": 0.0-1.0,
    "community_infrastructure": 0.0-1.0,
    "trip_fit_and_travelability": 0.0-1.0
  }},
  "combined_lead_score": 0.0-1.0,
  "score_reasoning": "Brief explanation of the combined score based on the five sections"
}}"""
            }
        ],
        response_format={"type": "json_object"}
    )
    
    result = json.loads(response.choices[0].message.content)
    return {
        "section_scores": result.get('section_scores', {}),
        "lead_score": result.get('combined_lead_score', 0.0),
        "score_reasoning": result.get('score_reasoning', '')
    }


def send_to_hubspot(contact_id: str, lead_score: float, section_scores: Dict[str, float], score_reasoning: str, creator_profile: Dict[str, Any], content_analyses: List[Dict[str, Any]]):
    """Send results back to HubSpot via webhook"""
    
    # Extract personalization hooks from content
    content_summaries_structured = []
    
    for idx, item in enumerate(content_analyses, 1):
        content_summaries_structured.append(f"Content {idx} ({item['type']}): {item['summary']}")
    
    # Detect community platforms mentioned
    community_text = creator_profile.get('community_building', '').lower()
    community_platforms = []
    platform_keywords = {
        'email list': 'Email List',
        'newsletter': 'Newsletter', 
        'mailing list': 'Mailing List',
        'patreon': 'Patreon',
        'discord': 'Discord',
        'substack': 'Substack',
        'facebook group': 'Facebook Group',
        'community': 'Community Platform'
    }
    
    for keyword, platform_name in platform_keywords.items():
        if keyword in community_text:
            if platform_name not in community_platforms:
                community_platforms.append(platform_name)
    
    has_community_platform = len(community_platforms) > 0
    
    payload = {
        "contact_id": contact_id,
        "lead_score": lead_score,
        "score_reasoning": score_reasoning,
        
        # Section scores
        "score_niche_and_audience": section_scores.get('niche_and_audience_identity', 0.0),
        "score_host_likeability": section_scores.get('host_likeability_and_content_style', 0.0),
        "score_monetization": section_scores.get('monetization_and_business_mindset', 0.0),
        "score_community_infrastructure": section_scores.get('community_infrastructure', 0.0),
        "score_trip_fit": section_scores.get('trip_fit_and_travelability', 0.0),
        
        # Structured content summaries
        "content_summary_structured": "\n\n".join(content_summaries_structured),
        
        # Flattened creator profile for easy access
        "profile_category": creator_profile.get('content_category'),
        "profile_content_types": ", ".join(creator_profile.get('content_types', [])),
        "profile_tone": creator_profile.get('audience_engagement', ''),
        "profile_engagement": creator_profile.get('audience_engagement', ''),
        "profile_presence": creator_profile.get('creator_presence', ''),
        "profile_monetization": creator_profile.get('monetization', ''),
        "profile_community_building": creator_profile.get('community_building', ''),
        
        # Community platform detection
        "has_community_platform": has_community_platform,
        "community_platforms_detected": ", ".join(community_platforms) if community_platforms else "None detected",
        
        "analyzed_at": datetime.now().isoformat()
    }
    
    response = requests.post(HUBSPOT_WEBHOOK_URL, json=payload)
    response.raise_for_status()
    
    return response.json()


from tasks import process_creator_profile

@app.route('/webhook/async', methods=['POST'])
def handle_webhook_async():
    """Async webhook handler - returns immediately, processes in background"""
    try:
        data = request.get_json()
        
        contact_id = data.get('contact_id')
        profile_url = data.get('profile_url')
        bio = data.get('bio', '')
        follower_count = data.get('follower_count', 0)
        
        if not all([contact_id, profile_url]):
            return jsonify({"error": "Missing required fields: contact_id, profile_url"}), 400
        
        print(f"=== QUEUEING: {contact_id} ===")
        if bio:
            print(f"Bio: {bio[:100]}...")
        if follower_count:
            print(f"Follower count: {follower_count:,}")
        
        # Queue the task with profile data
        task = process_creator_profile.delay(contact_id, profile_url, bio, follower_count)
        
        print(f"=== QUEUED: {contact_id} - Task ID: {task.id} ===")
        
        return jsonify({
            "status": "queued",
            "contact_id": contact_id,
            "task_id": task.id,
            "message": "Profile queued for processing"
        }), 202
        
    except Exception as e:
        print(f"Error queuing task: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/webhook/status/<task_id>', methods=['GET'])
def check_task_status(task_id):
    """Check status of a queued task"""
    from celery.result import AsyncResult
    
    task = AsyncResult(task_id, app=process_creator_profile.app)
    
    if task.state == 'PENDING':
        response = {
            'state': task.state,
            'status': 'Task is waiting in queue...'
        }
    elif task.state == 'PROGRESS':
        response = {
            'state': task.state,
            'status': task.info.get('stage', 'Processing...'),
        }
    elif task.state == 'SUCCESS':
        response = {
            'state': task.state,
            'result': task.result
        }
    else:  # FAILURE or other
        response = {
            'state': task.state,
            'status': str(task.info),
        }
    
    return jsonify(response)


@app.route('/webhook', methods=['POST'])
def handle_webhook():
    """Main webhook handler"""
    try:
        print("=== WEBHOOK RECEIVED ===")
        
        # Parse incoming webhook
        data = request.get_json()
        print(f"Received data: {data}")
        
        contact_id = data.get('contact_id')
        profile_url = data.get('profile_url')
        
        if not all([contact_id, profile_url]):
            return jsonify({"error": "Missing required fields: contact_id, profile_url"}), 400
        
        print(f"Processing: contact_id={contact_id}, profile_url={profile_url}")
        
        # Step 1: Fetch social content
        print(f"STEP 1: Fetching content from InsightIQ...")
        social_data = fetch_social_content(profile_url)
        print(f"STEP 1 COMPLETE: Received data from InsightIQ")
        
        # Debug: Log the response structure
        print(f"InsightIQ Response Type: {type(social_data)}")
        print(f"InsightIQ Response Keys: {social_data.keys() if isinstance(social_data, dict) else 'Not a dict'}")
        print(f"InsightIQ Full Response: {json.dumps(social_data, indent=2)[:1000]}...")  # First 1000 chars
        
        # Step 2: Process each piece of content
        content_analyses = []
        
        # Assuming the API returns content in a 'content' or 'posts' array
        content_items = social_data.get('content', social_data.get('posts', social_data.get('contents', social_data.get('data', []))))
        
        print(f"STEP 2: Found {len(content_items) if content_items else 0} content items to process")
        
        if not content_items:
            print(f"ERROR: No content items found. Available keys: {list(social_data.keys()) if isinstance(social_data, dict) else 'N/A'}")
            return jsonify({"error": "No content found in InsightIQ response", "response_keys": list(social_data.keys()) if isinstance(social_data, dict) else []}), 404
        
        for idx, item in enumerate(content_items[:5], 1):  # Process up to 5 items for MVP
            print(f"STEP 2.{idx}: Processing content item {idx}/{min(5, len(content_items))}")
            
            # Extract content type and format
            content_format = item.get('format')  # VIDEO, COLLECTION, etc.
            content_type = item.get('type')  # REELS, FEED, etc.
            description = item.get('description', '')
            
            print(f"STEP 2.{idx}: Format={content_format}, Type={content_type}")
            
            # Determine media URL based on format
            media_url = None
            media_format = None
            
            if content_format == 'VIDEO':
                # For videos, use media_url
                media_url = item.get('media_url')
                media_format = 'VIDEO'
                print(f"STEP 2.{idx}: VIDEO - Using media_url")
            elif content_format == 'COLLECTION':
                # For collections, get the first image from content_group_media
                content_group_media = item.get('content_group_media', [])
                print(f"STEP 2.{idx}: COLLECTION - Found {len(content_group_media)} items in content_group_media")
                
                if content_group_media and len(content_group_media) > 0:
                    media_url = content_group_media[0].get('media_url')
                    print(f"STEP 2.{idx}: Using first media from collection")
                else:
                    # Fallback to thumbnail_url
                    media_url = item.get('thumbnail_url')
                    print(f"STEP 2.{idx}: No content_group_media, using thumbnail_url")
                
                media_format = 'IMAGE'
            else:
                # Fallback for other formats
                media_url = item.get('media_url') or item.get('thumbnail_url')
                print(f"STEP 2.{idx}: Other format, using media_url or thumbnail")
                if media_url:
                    media_format = determine_media_format(media_url)
            
            # Clean up the URL - remove trailing periods
            if media_url:
                media_url = media_url.rstrip('.')
            
            if not media_url:
                print(f"STEP 2.{idx}: No media URL found, skipping")
                continue
            
            # Re-host media on R2
            print(f"STEP 2.{idx}: Original URL: {media_url[:100]}...")
            rehosted_url = rehost_media_on_r2(media_url, contact_id, media_format)
            print(f"STEP 2.{idx}: Re-hosted URL: {rehosted_url[:100]}...")
            
            try:
                print(f"STEP 2.{idx}: Analyzing {media_format}...")
                analysis = analyze_content_item(rehosted_url, media_format)
                
                # Add the description from InsightIQ to the analysis
                analysis['description'] = description
                
                content_analyses.append(analysis)
                print(f"STEP 2.{idx}: Analysis complete")
                
            except Exception as e:
                print(f"STEP 2.{idx} ERROR: {str(e)}")
                import traceback
                print(f"Traceback: {traceback.format_exc()}")
                continue
        
        # Step 3: Check if we have content to analyze
        print(f"STEP 3: Completed processing. Analyzed {len(content_analyses)} items")
        
        if not content_analyses:
            return jsonify({"error": "No content found to analyze"}), 404
        
        # Step 4: Generate creator profile
        print(f"STEP 4: Generating creator profile...")
        creator_profile = generate_creator_profile(content_analyses)
        print(f"STEP 4 COMPLETE: Creator profile generated")
        print(f"  - Category: {creator_profile.get('content_category')}")
        print(f"  - Monetization: {creator_profile.get('monetization')}")
        
        # Step 5: Generate lead score using creator profile and content summaries
        print(f"STEP 5: Generating TrovaTrip lead analysis...")
        lead_analysis = generate_lead_analysis(content_analyses, creator_profile)
        print(f"STEP 5 COMPLETE: Lead score: {lead_analysis['lead_score']}")
        print(f"  - Section scores: {lead_analysis.get('section_scores', {})}")
        
        # Step 6: Send to HubSpot
        print(f"STEP 6: Sending results to HubSpot...")
        send_to_hubspot(
            contact_id,
            lead_analysis['lead_score'],
            lead_analysis.get('section_scores', {}),
            lead_analysis.get('score_reasoning', ''),
            creator_profile,
            content_analyses
        )
        print(f"STEP 6 COMPLETE: Results sent to HubSpot")
        
        print("=== WEBHOOK COMPLETE ===")
        
        return jsonify({
            "status": "success",
            "contact_id": contact_id,
            "items_processed": len(content_analyses),
            "lead_score": lead_analysis['lead_score'],
            "section_scores": lead_analysis.get('section_scores', {}),
            "score_reasoning": lead_analysis.get('score_reasoning', ''),
            "creator_profile": creator_profile
        }), 200
        
    except Exception as e:
        print(f"=== WEBHOOK ERROR ===")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
