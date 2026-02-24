import os
import json
import requests
from flask import Flask, request, jsonify, render_template
from typing import Dict, List, Any
from datetime import datetime
import tempfile
import base64
import hashlib
import boto3
from botocore.client import Config

app = Flask(__name__)

# Initialize Redis connection (needed by discovery_routes)
import redis
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
r = redis.from_url(redis_url, decode_responses=True)

# Configuration from environment variables
INSIGHTIQ_USERNAME = os.getenv('INSIGHTIQ_USERNAME')
INSIGHTIQ_PASSWORD = os.getenv('INSIGHTIQ_PASSWORD')
INSIGHTIQ_WORK_PLATFORM_ID = os.getenv('INSIGHTIQ_WORK_PLATFORM_ID')
INSIGHTIQ_API_URL = os.getenv('INSIGHTIQ_API_URL', 'https://api.sandbox.insightiq.ai')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
HUBSPOT_WEBHOOK_URL = os.getenv('HUBSPOT_WEBHOOK_URL')

# BDR Round-Robin — valid display names and their corresponding HubSpot email values
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
    """
    Analyze a single content item (image or video)
    Returns summary and metadata
    """
    
    if media_format == 'IMAGE':
        # Analyze image with GPT-4 Vision
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": """Analyze this social media image. Cover these aspects:
- Theme/topic of the content
- What the creator is showcasing or sharing
- Visual style and composition
- Any visible text or captions
- Creator's visibility and presence in the image
- Any signs of monetization (product placement, sponsorships, etc.)
- Call-to-action elements
- How the creator engages with their audience through this content

Respond in JSON format with a single "summary" field containing a 3-4 sentence analysis."""
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
            "summary": result.get('summary', 'No summary available')
        }
    
    else:  # VIDEO
        # First, transcribe the video
        transcript = transcribe_video_with_whisper(media_url)
        
        # Then analyze the transcript
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": f"""Based on this video transcription, provide a detailed summary covering:
- Theme and main topic
- What the creator is sharing or teaching
- How the creator addresses their audience
- Any monetization elements (products, services, sponsorships)
- Calls-to-action
- Creator's on-camera presence and personality
- Overall tone and style

TRANSCRIPTION:
{transcript}

Respond in JSON format with a single "summary" field containing a 3-4 sentence analysis."""
                }
            ],
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        return {
            "type": "VIDEO",
            "url": media_url,
            "summary": result.get('summary', 'No summary available'),
            "transcript": transcript
        }


def generate_creator_profile(content_analyses: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate comprehensive creator profile from content summaries
    Returns structured profile data
    """
    # Combine all content summaries
    summaries = []
    for idx, item in enumerate(content_analyses, 1):
        summary_text = f"Content {idx} ({item['type']}): {item['summary']}"
        if item.get('description'):
            summary_text += f"\nOriginal caption: {item['description']}"
        summaries.append(summary_text)
    
    combined_summaries = "\n\n".join(summaries)
    
    # Generate profile with GPT-4
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": "You are an expert at analyzing social media creators to understand their content strategy, audience engagement approach, and monetization."
            },
            {
                "role": "user",
                "content": f"""Based on these content summaries, create a structured creator profile covering:
1. Content category (main niche/topic)
2. Content types (formats they use)
3. Audience engagement style
4. Creator presence (on-camera personality)
5. Monetization approach
6. Community building methods

CONTENT SUMMARIES:
{combined_summaries}

Respond in JSON format with those 6 fields. Make each field descriptive but concise."""
            }
        ],
        response_format={"type": "json_object"}
    )
    
    return json.loads(response.choices[0].message.content)


def generate_lead_score(content_analyses: List[Dict[str, Any]], creator_profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate TrovaTrip lead score based on content and profile
    Returns section scores and combined score with reasoning
    """
    # Combine content summaries for scoring
    summaries = [f"Content {idx} ({item['type']}): {item['summary']}" 
                for idx, item in enumerate(content_analyses, 1)]
    combined_summaries = "\n\n".join(summaries)
    
    # Format profile for scoring
    profile_context = f"""CREATOR PROFILE:
- Content Category: {creator_profile.get('content_category', 'Unknown')}
- Content Types: {creator_profile.get('content_types', 'Unknown')}
- Audience Engagement: {creator_profile.get('audience_engagement', 'Unknown')}
- Creator Presence: {creator_profile.get('creator_presence', 'Unknown')}
- Monetization: {creator_profile.get('monetization', 'Unknown')}
- Community Building: {creator_profile.get('community_building', 'Unknown')}"""
    
    # Generate score with GPT-4
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": """You are a TrovaTrip lead scorer. TrovaTrip is a group travel platform where creators host trips for their audiences.

Score creators on 5 sections (each 0.0-1.0):
1. niche_and_audience_identity: Clear niche, engaged audience, travel-compatible audience
2. host_likeability_and_content_style: Authentic, personable, quality production
3. monetization_and_business_mindset: Revenue streams, business-savvy, asks for money
4. community_infrastructure: Email list, Patreon, Discord, ways to communicate
5. trip_fit_and_travelability: Travel content, adventure-oriented, shows destinations

Also provide a combined_lead_score (weighted average) and score_reasoning."""
            },
            {
                "role": "user",
                "content": f"""{profile_context}

CONTENT SUMMARIES:
{combined_summaries}

Score each of the 5 TrovaTrip sections (0.0-1.0), provide combined score, and explain your reasoning.
Respond in JSON with "section_scores" object, "combined_lead_score" number, and "score_reasoning" string."""
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


def send_to_hubspot(contact_id: str, lead_score: float, section_scores: Dict, score_reasoning: str, 
                    creator_profile: Dict, content_analyses: List[Dict]):
    """Send enrichment results to HubSpot via webhook"""
    # Format content summaries
    content_summaries = [f"Content {idx} ({item['type']}): {item['summary']}" 
                        for idx, item in enumerate(content_analyses, 1)]
    
    # Extract community platforms from profile
    community_text = creator_profile.get('community_building', '').lower()
    platforms = []
    platform_keywords = [
        ('email', 'Email List'),
        ('patreon', 'Patreon'),
        ('discord', 'Discord'),
        ('substack', 'Substack')
    ]
    
    for keyword, platform_name in platform_keywords:
        if keyword in community_text and platform_name not in platforms:
            platforms.append(platform_name)
    
    # Build payload
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
        "profile_category": creator_profile.get('content_category'),
        "profile_content_types": ", ".join(creator_profile.get('content_types', [])) if isinstance(creator_profile.get('content_types'), list) else creator_profile.get('content_types', ''),
        "profile_engagement": creator_profile.get('audience_engagement', ''),
        "profile_presence": creator_profile.get('creator_presence', ''),
        "profile_monetization": creator_profile.get('monetization', ''),
        "profile_community_building": creator_profile.get('community_building', ''),
        "has_community_platform": len(platforms) > 0,
        "community_platforms_detected": ", ".join(platforms) if platforms else "None",
        "analyzed_at": datetime.now().isoformat()
    }
    
    # Send to HubSpot
    try:
        response = requests.post(HUBSPOT_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        print(f"Successfully sent data to HubSpot for contact {contact_id}")
    except Exception as e:
        print(f"Error sending to HubSpot: {e}")
        raise


@app.route('/')
def index():
    """Home hub"""
    return render_template('home.html')


@app.route('/api/webhook/enrich', methods=['POST'])
def enrich_webhook():
    """
    Webhook endpoint triggered by HubSpot workflow
    Receives contact data and queues enrichment task
    """
    try:
        data = request.json
        print(f"=== WEBHOOK RECEIVED ===")
        print(f"Data: {json.dumps(data, indent=2)}")
        
        # Extract contact data
        contact_id = data.get('contact_id')
        profile_url = data.get('profile_url')
        
        if not contact_id or not profile_url:
            print("ERROR: Missing required fields (contact_id or profile_url)")
            return jsonify({
                'status': 'error',
                'message': 'Missing contact_id or profile_url'
            }), 400
        
        # Queue the enrichment task
        from tasks import process_creator_profile
        task = process_creator_profile.delay(contact_id, profile_url)
        
        print(f"✅ Task queued: {task.id}")
        
        return jsonify({
            'status': 'success',
            'task_id': task.id,
            'message': f'Enrichment task queued for contact {contact_id}'
        }), 200
        
    except Exception as e:
        print(f"ERROR in webhook: {e}")
        import traceback
        print(traceback.format_exc())
        
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/api/stats')
def get_stats():
    """API endpoint for dashboard stats"""
    try:
        # Get queue size
        queue_size = r.llen('celery') or 0
        
        # Get active workers (approximate)
        active_workers = min(queue_size, 8) if queue_size > 0 else 0
        
        # Get result type counts from Redis
        result_counts = r.hgetall('trovastats:results') or {}
        
        post_frequency = int(result_counts.get('post_frequency', 0))
        pre_screened = int(result_counts.get('pre_screened', 0))
        enriched = int(result_counts.get('enriched', 0))
        errors = int(result_counts.get('error', 0))
        
        # Get priority tier counts
        tier_counts = r.hgetall('trovastats:priority_tiers') or {}
        auto_enroll = int(tier_counts.get('auto_enroll', 0))
        high_priority = int(tier_counts.get('high_priority_review', 0))
        standard_priority = int(tier_counts.get('standard_priority_review', 0))
        low_priority = int(tier_counts.get('low_priority_review', 0))
        
        # Calculate totals
        total_completed = post_frequency + pre_screened + enriched
        total_errors = errors
        total_processed = total_completed + total_errors
        
        # Calculate average duration
        durations = r.lrange('trovastats:durations', 0, -1)
        avg_duration = 0
        if durations:
            durations_int = [int(d) for d in durations]
            avg_duration = sum(durations_int) / len(durations_int)
        
        # Calculate estimated time remaining (in minutes)
        est_time_remaining = 0
        if avg_duration > 0 and queue_size > 0:
            # Estimate based on queue size and average duration
            # Assuming 2 workers processing in parallel
            workers = 2
            est_time_remaining = (queue_size / workers) * avg_duration / 60
        
        # Calculate percentages for batch quality
        total_passed = enriched
        pass_rate = (total_passed / total_processed * 100) if total_processed > 0 else 0
        
        # Priority tier percentages (of those that passed pre-screening)
        tier_percentages = {}
        if total_passed > 0:
            tier_percentages = {
                'auto_enroll': (auto_enroll / total_passed * 100),
                'high_priority_review': (high_priority / total_passed * 100),
                'standard_priority_review': (standard_priority / total_passed * 100),
                'low_priority_review': (low_priority / total_passed * 100)
            }
        else:
            tier_percentages = {
                'auto_enroll': 0,
                'high_priority_review': 0,
                'standard_priority_review': 0,
                'low_priority_review': 0
            }
        
        return jsonify({
            'queue_size': queue_size,
            'active_workers': active_workers,
            'total_completed': total_completed,
            'total_errors': total_errors,
            'avg_duration': round(avg_duration, 1),
            'est_time_remaining': round(est_time_remaining, 1),
            'breakdown': {
                'post_frequency': post_frequency,
                'pre_screened': pre_screened,
                'enriched': enriched,
                'errors': errors
            },
            'pre_screening': {
                'total_pre_screened': post_frequency + pre_screened,
                'low_post_frequency': post_frequency,
                'outside_icp': pre_screened
            },
            'priority_tiers': {
                'auto_enroll': auto_enroll,
                'high_priority_review': high_priority,
                'standard_priority_review': standard_priority,
                'low_priority_review': low_priority,
                'total': total_passed
            },
            'batch_quality': {
                'pass_rate': round(pass_rate, 1),
                'tier_percentages': {
                    'auto_enroll': round(tier_percentages['auto_enroll'], 1),
                    'high_priority_review': round(tier_percentages['high_priority_review'], 1),
                    'standard_priority_review': round(tier_percentages['standard_priority_review'], 1),
                    'low_priority_review': round(tier_percentages['low_priority_review'], 1)
                }
            }
        })
        
    except Exception as e:
        print(f"Error generating stats: {e}")
        import traceback
        print(traceback.format_exc())
        
        # Return zeros if Redis isn't available
        return jsonify({
            'queue_size': 0,
            'active_workers': 0,
            'total_completed': 0,
            'total_errors': 0,
            'avg_duration': 0,
            'est_time_remaining': 0,
            'breakdown': {
                'post_frequency': 0,
                'pre_screened': 0,
                'enriched': 0,
                'errors': 0
            },
            'pre_screening': {
                'total_pre_screened': 0,
                'low_post_frequency': 0,
                'outside_icp': 0
            },
            'priority_tiers': {
                'auto_enroll': 0,
                'high_priority_review': 0,
                'standard_priority_review': 0,
                'low_priority_review': 0,
                'total': 0
            },
            'batch_quality': {
                'pass_rate': 0,
                'tier_percentages': {
                    'auto_enroll': 0,
                    'high_priority_review': 0,
                    'standard_priority_review': 0,
                    'low_priority_review': 0
                }
            }
        }), 200


@app.route('/api/stats/reset', methods=['POST'])
def reset_stats():
    """Reset dashboard stats (useful for starting a new batch)"""
    try:
        # Delete all stats keys
        r.delete('trovastats:results')
        r.delete('trovastats:priority_tiers')
        r.delete('trovastats:durations')
        
        return jsonify({
            'status': 'success',
            'message': 'Stats reset successfully'
        })
        
    except Exception as e:
        print(f"Error resetting stats: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200

@app.route('/webhook/async', methods=['POST'])
def handle_webhook_async():
    """Async webhook handler - returns immediately, processes in background"""
    try:
        from tasks import process_creator_profile
        
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
        print(f"ERROR in webhook: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/webhook/status/<task_id>', methods=['GET'])
def check_task_status(task_id):
    """Check status of a background task"""
    from celery.result import AsyncResult
    from celery_app import celery_app
    
    task = AsyncResult(task_id, app=celery_app)
    
    if task.state == 'PENDING':
        response = {
            'state': task.state,
            'status': 'Task is waiting in queue...'
        }
    elif task.state == 'PROGRESS':
        response = {
            'state': task.state,
            'status': task.info.get('stage', 'Processing...'),
            'details': task.info
        }
    elif task.state == 'SUCCESS':
        response = {
            'state': task.state,
            'result': task.result
        }
    else:
        response = {
            'state': task.state,
            'status': str(task.info)
        }
    
    return jsonify(response)

# ============================================================================
# DISCOVERY ROUTES
# ============================================================================

@app.route('/discovery')
def discovery_page():
    """Discovery UI page"""
    return render_template('discovery.html')


@app.route('/api/discovery/instagram', methods=['POST'])
def start_instagram_discovery():
    """Start Instagram discovery job"""
    try:
        from tasks import discover_instagram_profiles
        
        user_filters = request.json or {}
        
        # Validate max_results
        max_results = user_filters.get('max_results', 500)
        if not isinstance(max_results, int) or max_results < 1:
            return jsonify({'error': 'max_results must be a positive integer'}), 400
        if max_results > 4000:
            return jsonify({'error': 'max_results cannot exceed 4000'}), 400
        
        # Validate follower count
        follower_count = user_filters.get('follower_count', {})
        if follower_count:
            min_followers = follower_count.get('min')
            max_followers = follower_count.get('max')
            
            if min_followers and not isinstance(min_followers, int):
                return jsonify({'error': 'follower_count.min must be an integer'}), 400
            if max_followers and not isinstance(max_followers, int):
                return jsonify({'error': 'follower_count.max must be an integer'}), 400
            
            if min_followers and max_followers and min_followers >= max_followers:
                return jsonify({'error': 'follower_count.min must be less than max'}), 400
        
        # Validate lookalike (mutually exclusive)
        lookalike_type = user_filters.get('lookalike_type')
        lookalike_username = user_filters.get('lookalike_username', '').strip()
        
        if lookalike_type and lookalike_type not in ('creator', 'audience'):
            return jsonify({'error': 'lookalike_type must be "creator" or "audience"'}), 400
        
        if lookalike_type and not lookalike_username:
            return jsonify({'error': 'lookalike_username required when lookalike_type is set'}), 400

        # Validate BDR names
        bdr_names = user_filters.get('bdr_names', list(BDR_OWNER_IDS.keys()))
        if not isinstance(bdr_names, list):
            bdr_names = []
        invalid_bdrs = [n for n in bdr_names if n not in BDR_OWNER_IDS]
        if invalid_bdrs:
            return jsonify({'error': f'Unknown BDR name(s): {invalid_bdrs}'}), 400
        user_filters['bdr_names'] = bdr_names

        # Validate bio_phrase_advanced
        VALID_ACTIONS = {'AND', 'OR', 'NOT'}
        bio_advanced = user_filters.get('bio_phrase_advanced') or []
        if not isinstance(bio_advanced, list):
            return jsonify({'error': 'bio_phrase_advanced must be a list'}), 400
        for clause in bio_advanced:
            if not isinstance(clause, dict) or not clause.get('bio_phrase') or clause.get('action') not in VALID_ACTIONS:
                return jsonify({'error': 'Each bio_phrase_advanced entry must have a non-empty bio_phrase and action (AND/OR/NOT)'}), 400
        if len(bio_advanced) > 14:
            return jsonify({'error': 'bio_phrase_advanced is capped at 14 clauses (15 total including bio_phrase)'}), 400
        user_filters['bio_phrase_advanced'] = bio_advanced

        # Queue discovery task
        task = discover_instagram_profiles.delay(user_filters=user_filters)
        job_id = str(task.id)
        
        # Initialize job tracking in Redis
        r.setex(
            f'discovery_job:{job_id}',
            86400,
            json.dumps({
                'job_id': job_id,
                'platform': 'instagram',
                'status': 'queued',
                'started_at': datetime.now().isoformat(),
                'filters': user_filters,
                'profiles_found': 0,
                'new_contacts_created': 0,
                'duplicates_skipped': 0
            })
        )
        
        return jsonify({'job_id': job_id, 'status': 'queued'}), 202
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/discovery/facebook', methods=['POST'])
def start_facebook_discovery():
    """
    Start Facebook Groups discovery job
    
    POST body:
    {
        "keywords": ["running", "fitness"],
        "max_results": 100,
        "min_members": 500,  # optional
        "max_members": 50000  # optional
    }
    
    Returns:
        202 Accepted with job_id
        400 Bad Request if validation fails
    """
    try:
        from tasks import discover_facebook_groups
        
        user_filters = request.json or {}
        
        # Validate keywords
        keywords = user_filters.get('keywords', [])
        if not isinstance(keywords, list) or len(keywords) == 0:
            return jsonify({'error': 'keywords must be a non-empty array'}), 400
        
        # Validate max_results
        max_results = user_filters.get('max_results', 100)
        if not isinstance(max_results, int) or max_results < 1:
            return jsonify({'error': 'max_results must be a positive integer'}), 400
        if max_results > 500:
            return jsonify({'error': 'max_results cannot exceed 500'}), 400
        
        # Validate member counts (optional)
        min_members = user_filters.get('min_members', 0)
        max_members = user_filters.get('max_members', 0)

        if min_members and not isinstance(min_members, int):
            return jsonify({'error': 'min_members must be an integer'}), 400
        if max_members and not isinstance(max_members, int):
            return jsonify({'error': 'max_members must be an integer'}), 400
        if min_members and max_members and min_members >= max_members:
            return jsonify({'error': 'min_members must be less than max_members'}), 400

        # Validate visibility (optional: 'all', 'public', 'private')
        visibility = user_filters.get('visibility', 'all')
        if visibility not in ('all', 'public', 'private'):
            return jsonify({'error': "visibility must be one of: all, public, private"}), 400
        user_filters['visibility'] = visibility

        # Validate min_posts_per_month (optional)
        min_posts_per_month = user_filters.get('min_posts_per_month', 0)
        if min_posts_per_month and not isinstance(min_posts_per_month, int):
            return jsonify({'error': 'min_posts_per_month must be an integer'}), 400

        # Validate BDR names
        bdr_names = user_filters.get('bdr_names', list(BDR_OWNER_IDS.keys()))
        if not isinstance(bdr_names, list):
            bdr_names = []
        invalid_bdrs = [n for n in bdr_names if n not in BDR_OWNER_IDS]
        if invalid_bdrs:
            return jsonify({'error': f'Unknown BDR name(s): {invalid_bdrs}'}), 400
        user_filters['bdr_names'] = bdr_names

        # Queue discovery task
        task = discover_facebook_groups.delay(user_filters=user_filters)
        job_id = str(task.id)
        
        # Initialize job tracking in Redis
        r.setex(
            f'discovery_job:{job_id}',
            86400,  # 24 hour TTL
            json.dumps({
                'job_id': job_id,
                'platform': 'facebook',
                'status': 'queued',
                'started_at': datetime.now().isoformat(),
                'filters': user_filters,
                'profiles_found': 0,
                'new_contacts_created': 0,
                'duplicates_skipped': 0
            })
        )
        
        # Index job by platform for monitor pages
        r.lpush('discovery_jobs:facebook', job_id)
        r.ltrim('discovery_jobs:facebook', 0, 19)

        return jsonify({
            'job_id': job_id,
            'status': 'queued'
        }), 202

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/discovery/patreon', methods=['POST'])
def start_patreon_discovery():
    """
    Start Patreon discovery job via Apify scraper
    
    POST body:
    {
        "search_keywords": ["art", "gaming"],
        "max_results": 100
    }
    
    Returns:
        202 Accepted with job_id
        400 Bad Request if validation fails
    """
    try:
        from tasks import discover_patreon_profiles
        
        user_filters = request.json or {}
        
        # Validate search_keywords
        search_keywords = user_filters.get('search_keywords', [])
        if not isinstance(search_keywords, list) or len(search_keywords) == 0:
            return jsonify({'error': 'search_keywords must be a non-empty array'}), 400

        # Validate max_results
        max_results = user_filters.get('max_results', 100)
        if not isinstance(max_results, int) or max_results < 1:
            return jsonify({'error': 'max_results must be a positive integer'}), 400
        if max_results > 500:
            return jsonify({'error': 'max_results cannot exceed 500'}), 400

        # Validate location (optional string)
        location = user_filters.get('location', 'United States')
        if not isinstance(location, str):
            return jsonify({'error': 'location must be a string'}), 400
        user_filters['location'] = location.strip()

        # Validate patron count range (optional)
        min_patrons = user_filters.get('min_patrons', 0)
        max_patrons = user_filters.get('max_patrons', 0)
        if min_patrons and not isinstance(min_patrons, int):
            return jsonify({'error': 'min_patrons must be an integer'}), 400
        if max_patrons and not isinstance(max_patrons, int):
            return jsonify({'error': 'max_patrons must be an integer'}), 400
        if min_patrons and max_patrons and min_patrons >= max_patrons:
            return jsonify({'error': 'min_patrons must be less than max_patrons'}), 400

        # Validate min_posts (optional)
        min_posts = user_filters.get('min_posts', 0)
        if min_posts and not isinstance(min_posts, int):
            return jsonify({'error': 'min_posts must be an integer'}), 400

        # Validate BDR names
        bdr_names = user_filters.get('bdr_names', list(BDR_OWNER_IDS.keys()))
        if not isinstance(bdr_names, list):
            bdr_names = []
        invalid_bdrs = [n for n in bdr_names if n not in BDR_OWNER_IDS]
        if invalid_bdrs:
            return jsonify({'error': f'Unknown BDR name(s): {invalid_bdrs}'}), 400
        user_filters['bdr_names'] = bdr_names

        # Queue discovery task
        task = discover_patreon_profiles.delay(user_filters=user_filters)
        job_id = str(task.id)
        
        # Initialize job tracking in Redis
        r.setex(
            f'discovery_job:{job_id}',
            86400,  # 24 hour TTL
            json.dumps({
                'job_id': job_id,
                'platform': 'patreon',
                'status': 'queued',
                'started_at': datetime.now().isoformat(),
                'filters': user_filters,
                'profiles_found': 0,
                'new_contacts_created': 0,
                'duplicates_skipped': 0
            })
        )
        
        # Index job by platform for monitor pages
        r.lpush('discovery_jobs:patreon', job_id)
        r.ltrim('discovery_jobs:patreon', 0, 19)

        return jsonify({
            'job_id': job_id,
            'status': 'queued'
        }), 202

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/discovery/jobs/<job_id>')
def get_discovery_job(job_id):
    """Get discovery job status"""
    job_data = r.get(f'discovery_job:{job_id}')
    if not job_data:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(json.loads(job_data))


@app.route('/api/discovery/jobs')
def list_discovery_jobs():
    """List recent discovery jobs"""
    job_keys = r.keys('discovery_job:*')
    jobs = []
    for key in job_keys:
        job_data = r.get(key)
        if job_data:
            try:
                jobs.append(json.loads(job_data))
            except json.JSONDecodeError:
                continue
    jobs.sort(key=lambda x: x.get('started_at', ''), reverse=True)
    return jsonify(jobs)


# ── New UI pages ──────────────────────────────────────────────────────────────

@app.route('/monitor/instagram')
def monitor_instagram():
    return render_template('monitor_instagram.html')

@app.route('/monitor/patreon')
def monitor_patreon():
    return render_template('monitor_patreon.html', platform='patreon',
                           platform_label='Patreon', platform_icon='🎨')

@app.route('/monitor/facebook')
def monitor_facebook():
    return render_template('monitor_patreon.html', platform='facebook',
                           platform_label='Facebook Groups', platform_icon='👥')

# ── Monitor API endpoints ─────────────────────────────────────────────────────

@app.route('/api/monitor/jobs/<platform>')
def get_platform_jobs(platform):
    """Return up to 20 most recent discovery jobs for a given platform."""
    if platform not in ('patreon', 'facebook'):
        return jsonify({'error': 'Unknown platform'}), 400
    job_ids = r.lrange(f'discovery_jobs:{platform}', 0, 19)
    jobs = []
    for jid in job_ids:
        raw = r.get(f'discovery_job:{jid}')
        if raw:
            try:
                jobs.append(json.loads(raw))
            except json.JSONDecodeError:
                continue
    return jsonify(jobs)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
