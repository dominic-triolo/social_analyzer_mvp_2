import os
import json
import requests
import tempfile
import base64
import hashlib
from typing import Dict, List, Any
from datetime import datetime
import boto3
from botocore.client import Config
from celery_app import celery_app
from openai import OpenAI

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


# Import all the functions from app.py
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
    
    response = requests.post(url, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def rehost_media_on_r2(media_url: str, contact_id: str, media_format: str) -> str:
    """Download media from Instagram CDN and upload to R2"""
    if not r2_client:
        return media_url
    
    try:
        media_response = requests.get(media_url, timeout=30)
        media_response.raise_for_status()
        
        url_hash = hashlib.md5(media_url.encode()).hexdigest()
        extension = 'mp4' if media_format == 'VIDEO' else 'jpg'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        object_key = f"social_content/{contact_id}/{timestamp}_{url_hash}.{extension}"
        
        content_type = 'video/mp4' if media_format == 'VIDEO' else 'image/jpeg'
        
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=object_key,
            Body=media_response.content,
            ContentType=content_type
        )
        
        return f"{R2_PUBLIC_URL}/{object_key}"
    except Exception as e:
        print(f"ERROR re-hosting: {e}")
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
    """Generate TrovaTrip lead score"""
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
            "content": "You score creators for TrovaTrip (group travel platform). Score 5 sections 0-1: niche/audience, likeability, monetization, community infrastructure, trip fit."
        }, {
            "role": "user",
            "content": f"""{profile_context}

CONTENT: {combined}

Score each TrovaTrip section and provide combined score + reasoning in JSON."""
        }],
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
    """Send results to HubSpot"""
    content_summaries = [f"Content {idx} ({item['type']}): {item['summary']}" 
                        for idx, item in enumerate(content_analyses, 1)]
    
    community_text = creator_profile.get('community_building', '').lower()
    platforms = []
    for keyword, name in [('email', 'Email List'), ('patreon', 'Patreon'), 
                         ('discord', 'Discord'), ('substack', 'Substack')]:
        if keyword in community_text and name not in platforms:
            platforms.append(name)
    
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
        "profile_content_types": ", ".join(creator_profile.get('content_types', [])),
        "profile_engagement": creator_profile.get('audience_engagement', ''),
        "profile_presence": creator_profile.get('creator_presence', ''),
        "profile_monetization": creator_profile.get('monetization', ''),
        "profile_community_building": creator_profile.get('community_building', ''),
        "has_community_platform": len(platforms) > 0,
        "community_platforms_detected": ", ".join(platforms) if platforms else "None",
        "analyzed_at": datetime.now().isoformat()
    }
    
    requests.post(HUBSPOT_WEBHOOK_URL, json=payload, timeout=10)


@celery_app.task(bind=True, name='tasks.process_creator_profile')
def process_creator_profile(self, contact_id: str, profile_url: str):
    """
    Background task to process a creator profile
    Returns task result that can be checked via task_id
    """
    try:
        print(f"=== PROCESSING: {contact_id} ===")
        
        # Update task state
        self.update_state(state='PROGRESS', meta={'stage': 'Fetching content from InsightIQ'})
        
        # Fetch content
        social_data = fetch_social_content(profile_url)
        content_items = social_data.get('data', [])
        
        if not content_items:
            return {"status": "error", "message": "No content found"}
        
        # Process content items
        self.update_state(state='PROGRESS', meta={'stage': 'Analyzing content', 'total': min(3, len(content_items))})
        
        content_analyses = []
        for idx, item in enumerate(content_items[:3], 1):
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
            
            if media_url:
                media_url = media_url.rstrip('.')
                rehosted_url = rehost_media_on_r2(media_url, contact_id, media_format)
                analysis = analyze_content_item(rehosted_url, media_format)
                analysis['description'] = item.get('description', '')
                content_analyses.append(analysis)
        
        # Generate profile
        self.update_state(state='PROGRESS', meta={'stage': 'Generating creator profile'})
        creator_profile = generate_creator_profile(content_analyses)
        
        # Generate score
        self.update_state(state='PROGRESS', meta={'stage': 'Calculating lead score'})
        lead_analysis = generate_lead_score(content_analyses, creator_profile)
        
        # Send to HubSpot
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
            "creator_profile": creator_profile
        }
        
    except Exception as e:
        print(f"=== ERROR: {contact_id} - {str(e)} ===")
        return {
            "status": "error",
            "contact_id": contact_id,
            "message": str(e)
        }
