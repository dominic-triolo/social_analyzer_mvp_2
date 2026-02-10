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
from discovery_routes import *

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


@app.route('/rescore/<contact_id>', methods=['POST'])
def rescore_profile(contact_id):
    """
    Re-score a profile using cached analysis data
    This allows iterating on scoring without re-running expensive API calls
    """
    try:
        print(f"=== RE-SCORING: {contact_id} ===")
        
        # Import here to avoid circular imports
        from tasks import load_analysis_cache, generate_lead_score, send_to_hubspot
        
        # Load cached analysis data
        try:
            cache_data = load_analysis_cache(contact_id)
        except Exception as e:
            return jsonify({
                "error": "Cache not found",
                "message": f"No cached analysis found for contact {contact_id}. Profile must be analyzed first.",
                "details": str(e)
            }), 404
        
        # Extract cached data
        content_analyses = cache_data.get('content_analyses', [])
        creator_profile = cache_data.get('creator_profile', {})
        has_travel_experience = cache_data.get('has_travel_experience', False)
        
        if not content_analyses or not creator_profile:
            return jsonify({
                "error": "Invalid cache data",
                "message": "Cached data is missing required fields"
            }), 400
        
        print(f"Loaded cache: {len(content_analyses)} content analyses")
        
        # Re-run scoring with current prompt
        lead_analysis = generate_lead_score(content_analyses, creator_profile)
        
        # Apply travel experience boost if applicable
        if has_travel_experience and lead_analysis['lead_score'] < 0.50:
            original_score = lead_analysis['lead_score']
            lead_analysis['lead_score'] = 0.50
            lead_analysis['score_reasoning'] = f"{lead_analysis.get('score_reasoning', '')} | TRAVEL EXPERIENCE BOOST: Creator has hosted or marketed group travel experiences (original score: {original_score:.2f}, boosted to 0.50 for manual review)"
            print(f"SCORE BOOSTED: {original_score:.2f} → 0.50 (travel experience detected)")
        
        # Send updated score to HubSpot
        send_to_hubspot(
            contact_id,
            lead_analysis['lead_score'],
            lead_analysis.get('section_scores', {}),
            lead_analysis.get('score_reasoning', ''),
            creator_profile,
            content_analyses
        )
        
        print(f"=== RE-SCORE COMPLETE: {contact_id} - New Score: {lead_analysis['lead_score']} ===")
        
        return jsonify({
            "status": "success",
            "contact_id": contact_id,
            "lead_score": lead_analysis['lead_score'],
            "section_scores": lead_analysis.get('section_scores', {}),
            "score_reasoning": lead_analysis.get('score_reasoning', ''),
            "cached_from": cache_data.get('timestamp'),
            "items_analyzed": len(content_analyses),
            "message": "Profile re-scored successfully using cached analysis"
        }), 200
        
    except Exception as e:
        print(f"Error re-scoring {contact_id}: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "error": "Re-scoring failed",
            "message": str(e)
        }), 500


@app.route('/rescore/batch', methods=['POST'])
def rescore_batch():
    """
    Queue multiple profiles for async re-scoring
    Accepts: {"contact_ids": ["123", "456", "789"]}
    Returns immediately with task info - processing happens in background
    """
    try:
        data = request.get_json()
        contact_ids = data.get('contact_ids', [])
        
        if not contact_ids:
            return jsonify({"error": "contact_ids array is required"}), 400
        
        # Convert to strings
        contact_ids = [str(cid) for cid in contact_ids]
        
        print(f"=== QUEUING BATCH RE-SCORE: {len(contact_ids)} profiles ===")
        
        # Import the Celery task
        from tasks import rescore_single_profile
        
        # Queue all profiles as async Celery tasks
        task_ids = []
        for contact_id in contact_ids:
            task = rescore_single_profile.delay(contact_id)
            task_ids.append(str(task.id))
        
        print(f"=== QUEUED {len(task_ids)} RE-SCORE TASKS ===")
        
        return jsonify({
            "status": "queued",
            "message": f"Queued {len(contact_ids)} profiles for re-scoring",
            "total": len(contact_ids),
            "task_ids_sample": task_ids[:10],  # First 10 for reference
            "note": "Re-scoring is happening in background. Check Railway worker logs to monitor progress."
        }), 202  # 202 Accepted
        
    except Exception as e:
        print(f"Error queuing batch re-score: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/cache/<contact_id>', methods=['GET'])
def view_cache(contact_id):
    """View cached analysis data for a profile"""
    try:
        from tasks import load_analysis_cache
        
        cache_data = load_analysis_cache(contact_id)
        
        # Return summary info (not full content analyses to keep response small)
        return jsonify({
            "status": "found",
            "contact_id": contact_id,
            "cached_at": cache_data.get('timestamp'),
            "profile_url": cache_data.get('profile_url'),
            "bio": cache_data.get('bio', '')[:100] + '...' if len(cache_data.get('bio', '')) > 100 else cache_data.get('bio', ''),
            "follower_count": cache_data.get('follower_count'),
            "items_analyzed": cache_data.get('items_analyzed'),
            "has_travel_experience": cache_data.get('has_travel_experience'),
            "creator_profile": cache_data.get('creator_profile'),
            "content_count": len(cache_data.get('content_analyses', []))
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "not_found",
            "contact_id": contact_id,
            "error": str(e)
        }), 404


@app.route('/cache/list', methods=['GET'])
def list_cached_profiles():
    """List all cached profiles in R2"""
    try:
        from tasks import r2_client, R2_BUCKET_NAME
        
        if not r2_client:
            return jsonify({"error": "R2 client not available"}), 500
        
        # List objects in analysis-cache/ prefix
        response = r2_client.list_objects_v2(
            Bucket=R2_BUCKET_NAME,
            Prefix='analysis-cache/'
        )
        
        cached_profiles = []
        if 'Contents' in response:
            for obj in response['Contents']:
                # Extract contact_id from key (analysis-cache/123456.json)
                key = obj['Key']
                contact_id = key.replace('analysis-cache/', '').replace('.json', '')
                
                cached_profiles.append({
                    'contact_id': contact_id,
                    'last_modified': obj['LastModified'].isoformat(),
                    'size_bytes': obj['Size']
                })
        
        return jsonify({
            "status": "success",
            "total_cached": len(cached_profiles),
            "profiles": cached_profiles
        }), 200
        
    except Exception as e:
        return jsonify({
            "error": "Failed to list cached profiles",
            "message": str(e)
        }), 500


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



# OLD SYNCHRONOUS WEBHOOK - DEPRECATED (use /webhook/async instead)
# @app.route('/webhook', methods=['POST'])
# def handle_webhook():
#     ... (commented out - uses old generate_lead_analysis function)


@app.route('/')
def dashboard():
    """Main dashboard page with batch quality metrics"""
    return '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TrovaTrip Enrichment Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        
        .header {
            text-align: center;
            color: white;
            margin-bottom: 30px;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }
        
        .header p {
            font-size: 1.2em;
            opacity: 0.9;
        }
        
        .loading {
            text-align: center;
            color: white;
            font-size: 1.5em;
            margin-top: 100px;
        }
        
        /* Top KPIs Grid */
        .kpis-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }
        
        .kpi-card {
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            text-align: center;
            transition: transform 0.2s;
        }
        
        .kpi-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 12px rgba(0,0,0,0.15);
        }
        
        .kpi-label {
            font-size: 0.9em;
            color: #666;
            margin-bottom: 8px;
            font-weight: 500;
        }
        
        .kpi-value {
            font-size: 2.2em;
            font-weight: bold;
            color: #333;
        }
        
        .kpi-value.queue { color: #ff9800; }
        .kpi-value.processing { color: #2196f3; }
        .kpi-value.completed { color: #4caf50; }
        .kpi-value.errors { color: #f44336; }
        
        /* Three Column Layout */
        .sections-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 20px;
            margin-bottom: 20px;
        }
        
        .section {
            background: white;
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        
        .section-title {
            font-size: 1.3em;
            font-weight: bold;
            color: #333;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #667eea;
        }
        
        .metric-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px 0;
            border-bottom: 1px solid #eee;
        }
        
        .metric-row:last-child {
            border-bottom: none;
        }
        
        .metric-label {
            font-size: 0.95em;
            color: #666;
            font-weight: 500;
        }
        
        .metric-value {
            font-size: 1.3em;
            font-weight: bold;
            color: #333;
        }
        
        .metric-value.primary { color: #667eea; }
        .metric-value.success { color: #4caf50; }
        .metric-value.warning { color: #ff9800; }
        .metric-value.danger { color: #f44336; }
        
        .tier-badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
            margin-right: 8px;
        }
        
        .tier-badge.auto { background: #e8f5e9; color: #2e7d32; }
        .tier-badge.high { background: #e3f2fd; color: #1565c0; }
        .tier-badge.standard { background: #fff3e0; color: #e65100; }
        .tier-badge.low { background: #fce4ec; color: #c2185b; }
        
        .refresh-info {
            text-align: center;
            color: white;
            font-size: 0.9em;
            margin-top: 20px;
            opacity: 0.9;
        }
        
        @media (max-width: 1200px) {
            .sections-grid {
                grid-template-columns: 1fr;
            }
        }
        
        @media (max-width: 768px) {
            .kpis-grid {
                grid-template-columns: repeat(2, 1fr);
            }
            .header h1 {
                font-size: 1.8em;
            }
            .kpi-value {
                font-size: 1.8em;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 TrovaTrip Enrichment Dashboard</h1>
            <p>Real-time profile processing & batch quality metrics</p>
        </div>
        
        <div id="loading" class="loading">
            <p>Loading stats...</p>
        </div>
        
        <div id="dashboard" style="display: none;">
            <!-- Top KPIs -->
            <div class="kpis-grid">
                <div class="kpi-card">
                    <div class="kpi-label">In Queue</div>
                    <div class="kpi-value queue" id="queue-count">-</div>
                </div>
                
                <div class="kpi-card">
                    <div class="kpi-label">Processing</div>
                    <div class="kpi-value processing" id="processing-count">-</div>
                </div>
                
                <div class="kpi-card">
                    <div class="kpi-label">Total Completed</div>
                    <div class="kpi-value completed" id="total-completed">-</div>
                </div>
                
                <div class="kpi-card">
                    <div class="kpi-label">Total Errors</div>
                    <div class="kpi-value errors" id="total-errors">-</div>
                </div>
                
                <div class="kpi-card">
                    <div class="kpi-label">Avg Duration (sec)</div>
                    <div class="kpi-value" id="avg-duration">-</div>
                </div>
                
                <div class="kpi-card">
                    <div class="kpi-label">Est. Time Left (min)</div>
                    <div class="kpi-value" id="est-time">-</div>
                </div>
            </div>
            
            <!-- Three Column Sections -->
            <div class="sections-grid">
                <!-- Pre-screening Section -->
                <div class="section">
                    <div class="section-title">🔍 Pre-screening</div>
                    
                    <div class="metric-row">
                        <div class="metric-label">Pre-screened Out</div>
                        <div class="metric-value danger" id="total-prescreened">-</div>
                    </div>
                    
                    <div class="metric-row">
                        <div class="metric-label">↳ Low Post Frequency</div>
                        <div class="metric-value warning" id="low-frequency">-</div>
                    </div>
                    
                    <div class="metric-row">
                        <div class="metric-label">↳ Outside ICP</div>
                        <div class="metric-value warning" id="outside-icp">-</div>
                    </div>
                </div>
                
                <!-- Enriched & Scored Section -->
                <div class="section">
                    <div class="section-title">✨ Enriched & Scored</div>
                    
                    <div class="metric-row">
                        <div class="metric-label">Total Enriched & Scored</div>
                        <div class="metric-value success" id="total-enriched">-</div>
                    </div>
                    
                    <div class="metric-row">
                        <div class="metric-label">
                            <span class="tier-badge auto">●</span> Auto Enroll
                        </div>
                        <div class="metric-value" id="tier-auto">-</div>
                    </div>
                    
                    <div class="metric-row">
                        <div class="metric-label">
                            <span class="tier-badge high">●</span> High Priority
                        </div>
                        <div class="metric-value" id="tier-high">-</div>
                    </div>
                    
                    <div class="metric-row">
                        <div class="metric-label">
                            <span class="tier-badge standard">●</span> Standard Priority
                        </div>
                        <div class="metric-value" id="tier-standard">-</div>
                    </div>
                    
                    <div class="metric-row">
                        <div class="metric-label">
                            <span class="tier-badge low">●</span> Low Priority
                        </div>
                        <div class="metric-value" id="tier-low">-</div>
                    </div>
                </div>
                
                <!-- Batch Quality Section -->
                <div class="section">
                    <div class="section-title">📈 Batch Quality</div>
                    
                    <div class="metric-row">
                        <div class="metric-label">Passed Pre-screening</div>
                        <div class="metric-value success" id="pass-rate">-</div>
                    </div>
                    
                    <div style="margin-top: 15px; padding-top: 15px; border-top: 2px solid #eee;">
                        <div style="font-size: 0.85em; color: #999; margin-bottom: 10px; font-weight: 600;">TIER BREAKDOWN</div>
                        
                        <div class="metric-row">
                            <div class="metric-label">
                                <span class="tier-badge auto">●</span> Auto Enroll
                            </div>
                            <div class="metric-value" id="tier-pct-auto">-</div>
                        </div>
                        
                        <div class="metric-row">
                            <div class="metric-label">
                                <span class="tier-badge high">●</span> High Priority
                            </div>
                            <div class="metric-value" id="tier-pct-high">-</div>
                        </div>
                        
                        <div class="metric-row">
                            <div class="metric-label">
                                <span class="tier-badge standard">●</span> Standard
                            </div>
                            <div class="metric-value" id="tier-pct-standard">-</div>
                        </div>
                        
                        <div class="metric-row">
                            <div class="metric-label">
                                <span class="tier-badge low">●</span> Low
                            </div>
                            <div class="metric-value" id="tier-pct-low">-</div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="refresh-info">
                <p>🔄 Auto-refreshing every 5 seconds</p>
                <p style="font-size: 0.9em; margin-top: 5px;">Last updated: <span id="last-update">-</span></p>
            </div>
        </div>
    </div>
    
    <script>
        async function fetchStats() {
            try {
                const response = await fetch('/api/stats');
                const data = await response.json();
                
                // Update top KPIs
                document.getElementById('queue-count').textContent = data.queue_size || 0;
                document.getElementById('processing-count').textContent = data.active_workers || 0;
                document.getElementById('total-completed').textContent = data.total_completed || 0;
                document.getElementById('total-errors').textContent = data.total_errors || 0;
                document.getElementById('avg-duration').textContent = data.avg_duration || 0;
                document.getElementById('est-time').textContent = data.est_time_remaining || 0;
                
                // Update pre-screening
                document.getElementById('total-prescreened').textContent = data.pre_screening.total_pre_screened || 0;
                document.getElementById('low-frequency').textContent = data.pre_screening.low_post_frequency || 0;
                document.getElementById('outside-icp').textContent = data.pre_screening.outside_icp || 0;
                
                // Update enriched & scored
                document.getElementById('total-enriched').textContent = data.priority_tiers.total || 0;
                document.getElementById('tier-auto').textContent = data.priority_tiers.auto_enroll || 0;
                document.getElementById('tier-high').textContent = data.priority_tiers.high_priority_review || 0;
                document.getElementById('tier-standard').textContent = data.priority_tiers.standard_priority_review || 0;
                document.getElementById('tier-low').textContent = data.priority_tiers.low_priority_review || 0;
                
                // Update batch quality
                document.getElementById('pass-rate').textContent = (data.batch_quality.pass_rate || 0) + '%';
                document.getElementById('tier-pct-auto').textContent = (data.batch_quality.tier_percentages.auto_enroll || 0) + '%';
                document.getElementById('tier-pct-high').textContent = (data.batch_quality.tier_percentages.high_priority_review || 0) + '%';
                document.getElementById('tier-pct-standard').textContent = (data.batch_quality.tier_percentages.standard_priority_review || 0) + '%';
                document.getElementById('tier-pct-low').textContent = (data.batch_quality.tier_percentages.low_priority_review || 0) + '%';
                
                // Update timestamp
                document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
                
                // Show dashboard
                document.getElementById('loading').style.display = 'none';
                document.getElementById('dashboard').style.display = 'block';
                
            } catch (error) {
                console.error('Error fetching stats:', error);
                document.getElementById('loading').innerHTML = '<p>⚠️ Error loading stats. Retrying...</p>';
            }
        }
        
        // Initial fetch
        fetchStats();
        
        // Auto-refresh every 5 seconds
        setInterval(fetchStats, 5000);
    </script>
</body>
</html>
    '''
@app.route('/api/stats')
def get_stats():
    """API endpoint for dashboard stats"""
    try:
        import redis
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        r = redis.from_url(redis_url, decode_responses=True)
        
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
        import redis
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        r = redis.from_url(redis_url, decode_responses=True)
        
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
