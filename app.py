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
    """Main dashboard"""
    return '''
<!DOCTYPE html>
<html>
<head>
    <title>TrovaTrip Lead Enrichment Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        
        .header {
            background: white;
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }
        
        .header h1 {
            font-size: 32px;
            color: #667eea;
            margin-bottom: 10px;
        }
        
        .header p {
            color: #666;
            font-size: 16px;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .stat-card {
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            transition: transform 0.2s;
        }
        
        .stat-card:hover {
            transform: translateY(-5px);
        }
        
        .stat-label {
            color: #888;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }
        
        .stat-value {
            font-size: 36px;
            font-weight: bold;
            color: #667eea;
        }
        
        .section {
            background: white;
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }
        
        .section h2 {
            color: #667eea;
            margin-bottom: 20px;
            font-size: 24px;
        }
        
        .breakdown-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }
        
        .breakdown-item {
            padding: 15px;
            background: #f8f9fa;
            border-radius: 10px;
            border-left: 4px solid #667eea;
        }
        
        .breakdown-label {
            color: #666;
            font-size: 13px;
            margin-bottom: 5px;
        }
        
        .breakdown-value {
            font-size: 24px;
            font-weight: bold;
            color: #333;
        }
        
        .timestamp {
            text-align: center;
            color: white;
            margin-top: 20px;
            font-size: 14px;
        }
        
        #loading {
            text-align: center;
            padding: 40px;
            color: white;
            font-size: 18px;
        }
        
        .progress-bar {
            height: 8px;
            background: #e0e0e0;
            border-radius: 10px;
            margin-top: 15px;
            overflow: hidden;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            transition: width 0.3s;
        }
    </style>
</head>
<body>
    <div class="container">
        <div id="loading">
            <h2>⏳ Loading Dashboard...</h2>
            <p style="margin-top: 10px;">Fetching latest stats from Redis...</p>
        </div>
        
        <div id="dashboard" style="display: none;">
            <div class="header">
                <h1>🚀 TrovaTrip Lead Enrichment</h1>
                <p>Real-time monitoring of creator profile processing and scoring</p>
            </div>
            
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">Queue Size</div>
                    <div class="stat-value" id="queue-size">0</div>
                    <div class="stat-label" style="margin-top: 10px;">Active Workers: <span id="active-workers">0</span></div>
                </div>
                
                <div class="stat-card">
                    <div class="stat-label">Total Completed</div>
                    <div class="stat-value" id="total-completed">0</div>
                </div>
                
                <div class="stat-card">
                    <div class="stat-label">Total Errors</div>
                    <div class="stat-value" id="total-errors" style="color: #dc3545;">0</div>
                </div>
                
                <div class="stat-card">
                    <div class="stat-label">Avg Duration</div>
                    <div class="stat-value" id="avg-duration">0</div>
                    <div class="stat-label" style="margin-top: 5px;">seconds</div>
                </div>
                
                <div class="stat-card">
                    <div class="stat-label">Est. Time Remaining</div>
                    <div class="stat-value" id="est-time">0</div>
                    <div class="stat-label" style="margin-top: 5px;">minutes</div>
                </div>
            </div>
            
            <div class="section">
                <h2>📊 Pre-Screening Results</h2>
                <div class="breakdown-grid">
                    <div class="breakdown-item" style="border-left-color: #dc3545;">
                        <div class="breakdown-label">Low Post Frequency</div>
                        <div class="breakdown-value" id="low-post-freq">0</div>
                    </div>
                    <div class="breakdown-item" style="border-left-color: #ffc107;">
                        <div class="breakdown-label">Outside ICP</div>
                        <div class="breakdown-value" id="outside-icp">0</div>
                    </div>
                    <div class="breakdown-item" style="border-left-color: #28a745;">
                        <div class="breakdown-label">Passed to Enrichment</div>
                        <div class="breakdown-value" id="passed-enrichment">0</div>
                    </div>
                </div>
            </div>
            
            <div class="section">
                <h2>🎯 Priority Tiers (After Enrichment)</h2>
                <div class="breakdown-grid">
                    <div class="breakdown-item" style="border-left-color: #28a745;">
                        <div class="breakdown-label">Auto-Enroll</div>
                        <div class="breakdown-value" id="tier-auto">0</div>
                    </div>
                    <div class="breakdown-item" style="border-left-color: #17a2b8;">
                        <div class="breakdown-label">High Priority Review</div>
                        <div class="breakdown-value" id="tier-high">0</div>
                    </div>
                    <div class="breakdown-item" style="border-left-color: #ffc107;">
                        <div class="breakdown-label">Standard Priority Review</div>
                        <div class="breakdown-value" id="tier-standard">0</div>
                    </div>
                    <div class="breakdown-item" style="border-left-color: #6c757d;">
                        <div class="breakdown-label">Low Priority Review</div>
                        <div class="breakdown-value" id="tier-low">0</div>
                    </div>
                </div>
            </div>
            
            <div class="section">
                <h2>✅ Batch Quality Metrics</h2>
                <div class="breakdown-grid">
                    <div class="breakdown-item" style="border-left-color: #28a745;">
                        <div class="breakdown-label">Pass Rate (Pre-screen → Enrichment)</div>
                        <div class="breakdown-value" id="pass-rate">0%</div>
                    </div>
                </div>
                
                <div style="margin-top: 20px;">
                    <div class="breakdown-label">Tier Distribution (% of Enriched Leads)</div>
                    <div class="breakdown-grid" style="margin-top: 10px;">
                        <div class="breakdown-item" style="border-left-color: #28a745;">
                            <div class="breakdown-label">Auto-Enroll</div>
                            <div class="breakdown-value" style="font-size: 20px;" id="tier-pct-auto">0%</div>
                        </div>
                        <div class="breakdown-item" style="border-left-color: #17a2b8;">
                            <div class="breakdown-label">High Priority</div>
                            <div class="breakdown-value" style="font-size: 20px;" id="tier-pct-high">0%</div>
                        </div>
                        <div class="breakdown-item" style="border-left-color: #ffc107;">
                            <div class="breakdown-label">Standard Priority</div>
                            <div class="breakdown-value" style="font-size: 20px;" id="tier-pct-standard">0%</div>
                        </div>
                        <div class="breakdown-item" style="border-left-color: #6c757d;">
                            <div class="breakdown-label">Low Priority</div>
                            <div class="breakdown-value" style="font-size: 20px;" id="tier-pct-low">0%</div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="timestamp">
                Last updated: <span id="last-update">--:--:--</span>
            </div>
        </div>
    </div>
    
    <script>
        async function fetchStats() {
            try {
                const response = await fetch('/api/stats');
                const data = await response.json();
                
                // Update main stats
                document.getElementById('queue-size').textContent = data.queue_size;
                document.getElementById('active-workers').textContent = data.active_workers;
                document.getElementById('total-completed').textContent = data.total_completed;
                document.getElementById('total-errors').textContent = data.total_errors;
                document.getElementById('avg-duration').textContent = data.avg_duration;
                document.getElementById('est-time').textContent = data.est_time_remaining;
                
                // Update pre-screening
                document.getElementById('low-post-freq').textContent = data.pre_screening.low_post_frequency;
                document.getElementById('outside-icp').textContent = data.pre_screening.outside_icp;
                document.getElementById('passed-enrichment').textContent = data.breakdown.enriched;
                
                // Update priority tiers
                document.getElementById('tier-auto').textContent = data.priority_tiers.auto_enroll;
                document.getElementById('tier-high').textContent = data.priority_tiers.high_priority_review;
                document.getElementById('tier-standard').textContent = data.priority_tiers.standard_priority_review;
                document.getElementById('tier-low').textContent = data.priority_tiers.low_priority_review;
                
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


# Import discovery routes at the END to avoid circular imports
import discovery_routes


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
