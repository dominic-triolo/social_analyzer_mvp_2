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
    Re-score multiple profiles at once
    Accepts: {"contact_ids": ["123", "456", "789"]}
    """
    try:
        data = request.get_json()
        contact_ids = data.get('contact_ids', [])
        
        if not contact_ids:
            return jsonify({"error": "contact_ids array is required"}), 400
        
        print(f"=== BATCH RE-SCORING: {len(contact_ids)} profiles ===")
        
        results = []
        errors = []
        
        from tasks import load_analysis_cache, generate_lead_score, send_to_hubspot
        
        for contact_id in contact_ids:
            try:
                # Load cache
                cache_data = load_analysis_cache(contact_id)
                content_analyses = cache_data.get('content_analyses', [])
                creator_profile = cache_data.get('creator_profile', {})
                has_travel_experience = cache_data.get('has_travel_experience', False)
                
                # Re-score
                lead_analysis = generate_lead_score(content_analyses, creator_profile)
                
                # Apply travel boost
                if has_travel_experience and lead_analysis['lead_score'] < 0.50:
                    original_score = lead_analysis['lead_score']
                    lead_analysis['lead_score'] = 0.50
                    lead_analysis['score_reasoning'] = f"{lead_analysis.get('score_reasoning', '')} | TRAVEL EXPERIENCE BOOST (original: {original_score:.2f})"
                
                # Send to HubSpot
                send_to_hubspot(
                    contact_id,
                    lead_analysis['lead_score'],
                    lead_analysis.get('section_scores', {}),
                    lead_analysis.get('score_reasoning', ''),
                    creator_profile,
                    content_analyses
                )
                
                results.append({
                    "contact_id": contact_id,
                    "status": "success",
                    "lead_score": lead_analysis['lead_score']
                })
                
            except Exception as e:
                errors.append({
                    "contact_id": contact_id,
                    "error": str(e)
                })
        
        print(f"=== BATCH COMPLETE: {len(results)} success, {len(errors)} errors ===")
        
        return jsonify({
            "status": "complete",
            "total": len(contact_ids),
            "success": len(results),
            "errors": len(errors),
            "results": results,
            "error_details": errors
        }), 200
        
    except Exception as e:
        print(f"Batch re-scoring error: {str(e)}")
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
    """Simple dashboard showing queue status and processing stats"""
    return '''
<!DOCTYPE html>
<html>
<head>
    <title>TrovaTrip Enrichment Dashboard</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        
        .header {
            text-align: center;
            color: white;
            margin-bottom: 40px;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            font-weight: 600;
        }
        
        .header p {
            font-size: 1.1em;
            opacity: 0.9;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .stat-card {
            background: white;
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            transition: transform 0.2s;
        }
        
        .stat-card:hover {
            transform: translateY(-5px);
        }
        
        .stat-label {
            font-size: 0.9em;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }
        
        .stat-value {
            font-size: 2.5em;
            font-weight: 700;
            color: #333;
        }
        
        .stat-icon {
            font-size: 2em;
            margin-bottom: 10px;
        }
        
        .queue { color: #667eea; }
        .processing { color: #f093fb; }
        .success { color: #4facfe; }
        .rejected { color: #fa709a; }
        .error { color: #ff6b6b; }
        
        .breakdown-section {
            background: white;
            border-radius: 12px;
            padding: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            margin-bottom: 30px;
        }
        
        .breakdown-section h2 {
            font-size: 1.5em;
            margin-bottom: 20px;
            color: #333;
        }
        
        .breakdown-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }
        
        .breakdown-item {
            padding: 15px;
            border-radius: 8px;
            background: #f8f9fa;
            border-left: 4px solid;
        }
        
        .breakdown-item.frequency { border-color: #feca57; }
        .breakdown-item.prescreen { border-color: #ff6b6b; }
        .breakdown-item.enriched { border-color: #48dbfb; }
        .breakdown-item.error { border-color: #ff9ff3; }
        
        .breakdown-label {
            font-size: 0.85em;
            color: #666;
            margin-bottom: 5px;
        }
        
        .breakdown-value {
            font-size: 1.8em;
            font-weight: 600;
            color: #333;
        }
        
        .refresh-info {
            text-align: center;
            color: white;
            margin-top: 20px;
            opacity: 0.8;
        }
        
        .loading {
            text-align: center;
            color: white;
            font-size: 1.2em;
            margin: 50px 0;
        }
        
        .progress-bar {
            background: rgba(255,255,255,0.3);
            height: 8px;
            border-radius: 4px;
            margin-top: 15px;
            overflow: hidden;
        }
        
        .progress-fill {
            background: white;
            height: 100%;
            border-radius: 4px;
            transition: width 0.3s ease;
        }
        
        @media (max-width: 768px) {
            .header h1 {
                font-size: 1.8em;
            }
            .stat-value {
                font-size: 2em;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 TrovaTrip Enrichment Dashboard</h1>
            <p>Real-time profile processing status</p>
        </div>
        
        <div id="loading" class="loading">
            <p>Loading stats...</p>
        </div>
        
        <div id="dashboard" style="display: none;">
            <!-- Main Stats Grid -->
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-icon queue">⏳</div>
                    <div class="stat-label">In Queue</div>
                    <div class="stat-value" id="queue-count">-</div>
                </div>
                
                <div class="stat-card">
                    <div class="stat-icon processing">⚙️</div>
                    <div class="stat-label">Processing</div>
                    <div class="stat-value" id="processing-count">-</div>
                </div>
                
                <div class="stat-card">
                    <div class="stat-icon success">✅</div>
                    <div class="stat-label">Total Completed</div>
                    <div class="stat-value" id="total-completed">-</div>
                </div>
                
                <div class="stat-card">
                    <div class="stat-icon error">❌</div>
                    <div class="stat-label">Total Errors</div>
                    <div class="stat-value" id="total-errors">-</div>
                </div>
            </div>
            
            <!-- Breakdown Section -->
            <div class="breakdown-section">
                <h2>📊 Processing Breakdown</h2>
                <div class="breakdown-grid">
                    <div class="breakdown-item frequency">
                        <div class="breakdown-label">Post Frequency</div>
                        <div class="breakdown-value" id="frequency-count">-</div>
                    </div>
                    
                    <div class="breakdown-item prescreen">
                        <div class="breakdown-label">Pre-screened Out</div>
                        <div class="breakdown-value" id="prescreen-count">-</div>
                    </div>
                    
                    <div class="breakdown-item enriched">
                        <div class="breakdown-label">Enriched & Scored</div>
                        <div class="breakdown-value" id="enriched-count">-</div>
                    </div>
                    
                    <div class="breakdown-item error">
                        <div class="breakdown-label">Errors</div>
                        <div class="breakdown-value" id="error-count">-</div>
                    </div>
                </div>
                
                <div class="progress-bar">
                    <div class="progress-fill" id="progress-bar" style="width: 0%"></div>
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
                
                // Update main stats
                document.getElementById('queue-count').textContent = data.queue_size || 0;
                document.getElementById('processing-count').textContent = data.active_workers || 0;
                document.getElementById('total-completed').textContent = data.total_completed || 0;
                document.getElementById('total-errors').textContent = data.total_errors || 0;
                
                // Update breakdown
                document.getElementById('frequency-count').textContent = data.breakdown.post_frequency || 0;
                document.getElementById('prescreen-count').textContent = data.breakdown.pre_screened || 0;
                document.getElementById('enriched-count').textContent = data.breakdown.enriched || 0;
                document.getElementById('error-count').textContent = data.breakdown.errors || 0;
                
                // Update progress bar
                const total = data.total_completed + data.total_errors;
                const progress = total > 0 ? (total / (total + data.queue_size)) * 100 : 0;
                document.getElementById('progress-bar').style.width = progress + '%';
                
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
        
        total_completed = post_frequency + pre_screened + enriched
        total_errors = errors
        
        return jsonify({
            'queue_size': queue_size,
            'active_workers': active_workers,
            'total_completed': total_completed,
            'total_errors': total_errors,
            'breakdown': {
                'post_frequency': post_frequency,
                'pre_screened': pre_screened,
                'enriched': enriched,
                'errors': errors
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
            'breakdown': {
                'post_frequency': 0,
                'pre_screened': 0,
                'enriched': 0,
                'errors': 0
            }
        }), 200




@app.route('/api/stats/reset', methods=['POST'])
def reset_stats():
    """Reset dashboard stats (useful for starting a new batch)"""
    try:
        import redis
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        r = redis.from_url(redis_url, decode_responses=True)
        
        # Delete the stats hash
        r.delete('trovastats:results')
        
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
