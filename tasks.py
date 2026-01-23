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
    """Generate TrovaTrip lead score based on ICP criteria - v1.2 Revised"""
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
            "content": """You score creators for TrovaTrip, a group travel platform where hosts lead trips with their communities.

CRITICAL DISTINCTION - Community vs Fan Base:
- COMMUNITY (GOOD): Followers want to meet EACH OTHER, share an identity, form friendships. Example: Book club members, widows supporting each other, DINKs with similar lifestyle, specific cultural/ethnic communities.
- FAN BASE (BAD): Followers admire the creator but don't want to connect with each other. Example: Fitness coach's clients, motivational speaker's followers, fashion influencer's admirers.

COMMON FALSE POSITIVES TO AVOID (look like good fits but aren't):
1. Generic Fitness Coaches - Unless they have a specific community (e.g., "moms who lift," "nurses fitness group"), they have clients/fans, not communities.
2. Personal Development Coaches - Motivational speakers create fans, not communities that want to travel together.
3. Fashion/Beauty Influencers (generic) - Admirers, not a community with shared identity.
4. Generic Lifestyle Content - Pretty photos don't create travel communities.

ACTUAL GOOD FITS (may not look obvious):
1. Niche Lifestyle Communities - Urban cultural communities, specific ethnic/regional groups, particular life stages (widows, DINKs, empty nesters).
2. Specialized Wellness - Specific health conditions (chronic illness, TMJ, PCOS), holistic health communities.
3. Food/Culinary Creators - With specific cuisine focus or cultural angle.
4. Travel Creators - Obviously, if they already do group experiences.
5. Hobby-Based - Book clubs, history nerds, specific craft/art forms (not performance art).

SCORING CRITERIA (0.0-1.0 each):"""
        }, {
            "role": "user",
            "content": f"""{profile_context}

CONTENT: {combined}

Score these 5 sections (0.0 to 1.0):

1. **niche_and_audience_identity** (0.0-1.0) - MOST IMPORTANT
   HIGH scores (0.7-1.0): 
   - Clear sub-community with shared identity: "widows," "DINKs," "book lovers of X genre," "chronic illness warriors," specific ethnic/cultural groups, "nurses," "empty nesters"
   - Evidence followers connect with EACH OTHER, not just the creator
   - Specific lifestyle angle: urban professionals, cultural community, life stage group
   
   MID scores (0.4-0.6):
   - Somewhat defined but broad: "wellness seekers," "travelers," "foodies" (unless very specific cuisine/region)
   - Lifestyle/fashion with cultural/regional identity
   - Fitness with specific community angle (moms, nurses, specific age group)
   
   LOW scores (0.0-0.3):
   - Generic categories: "fitness," "personal development," "motivation," "lifestyle," "fashion"
   - No evidence of shared identity among followers
   - Pure fan/client relationship (coaching clients, course students, fitness clients)
   - Generic motivational/inspirational content

2. **host_likeability_and_content_style** (0.0-1.0)
   HIGH scores (0.7-1.0): Face-forward, warm/conversational, shares experiences, "come with me" energy
   MID scores (0.5-0.6): Sometimes on camera OR strong aesthetic/lifestyle content that builds connection
   LOW scores (0.0-0.4): No personal presence, purely educational/transactional, cold/formal tone

3. **monetization_and_business_mindset** (0.0-1.0) - LESS IMPORTANT than before
   HIGH scores (0.7-1.0): Sells products/services, comfortable with business, launches
   MID scores (0.4-0.6): Some monetization OR strong community building even without monetization yet
   LOW scores (0.0-0.3): No monetization AND no community infrastructure

4. **community_infrastructure** (0.0-1.0)
   HIGH scores (0.7-1.0): Email list, podcast, Discord, Patreon, membership, in-person meetups mentioned
   MID scores (0.4-0.6): Evidence of wanting deeper connection, asks followers to connect, strong engagement
   LOW scores (0.0-0.3): Pure social media, no evidence of community building

5. **trip_fit_and_travelability** (0.0-1.0)
   HIGH scores (0.7-1.0): 
   - Natural trip concept: culinary tours, wellness retreats, cultural immersion, adventure travel, creative workshops
   - Audience likely has resources: professionals, DINKs, established careers, older audiences
   - Already travels or followers express wanting to meet
   
   MID scores (0.4-0.6):
   - Could work as lifestyle/personality-driven trip
   - Specific wellness niche could be retreat
   - Urban/cultural community trip potential
   
   LOW scores (0.0-0.3):
   - Generic fitness/coaching (no travel angle)
   - Very young/student audience
   - Content doesn't translate to group experiences
   - Pure motivation/inspiration (not experiential)

CRITICAL: Heavily penalize these red flags:
- "Fitness and personal development" without specific community → Max 0.50 total score
- "Coaching" or "clients" mentioned without community building → Niche score max 0.40
- Generic "motivation," "inspiration," "personal growth" → Niche score max 0.40
- No evidence followers want to meet each other → Niche score max 0.50

Also provide:
- **combined_lead_score**: NEW WEIGHTED FORMULA: (niche × 0.35) + (likeability × 0.20) + (monetization × 0.15) + (community × 0.15) + (trip_fit × 0.15)
- **score_reasoning**: 2-3 sentences. EXPLICITLY state if this is a "fan base" vs "community" and why.

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
def process_creator_profile(self, contact_id: str, profile_url: str):
    """Background task to process a creator profile"""
    try:
        print(f"=== PROCESSING: {contact_id} ===")
        
        self.update_state(state='PROGRESS', meta={'stage': 'Fetching content from InsightIQ'})
        
        social_data = fetch_social_content(profile_url)
        content_items = social_data.get('data', [])
        
        if not content_items:
            return {"status": "error", "message": "No content found"}
        
        self.update_state(state='PROGRESS', meta={'stage': 'Analyzing content'})
        
        content_analyses = []
        items_to_try = min(10, len(content_items))
        
        for idx, item in enumerate(content_items[:items_to_try], 1):
            if len(content_analyses) >= 3:
                break
            
            print(f"Processing item {idx}/{items_to_try} (have {len(content_analyses)} successful so far)")
            
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
                print(f"Item {idx}: No media URL, skipping")
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
                error_msg = str(e)
                if '413' in error_msg or 'Maximum content size' in error_msg:
                    print(f"Item {idx}: Video too large, skipping")
                elif 'Timeout while downloading' in error_msg:
                    print(f"Item {idx}: R2 timeout, skipping")
                else:
                    print(f"Item {idx}: Analysis failed - {error_msg}")
                continue
        
        if len(content_analyses) < 1:
            return {"status": "error", "message": "Could not analyze any content items"}
        
        print(f"Successfully analyzed {len(content_analyses)} items")
        
        self.update_state(state='PROGRESS', meta={'stage': 'Generating creator profile'})
        creator_profile = generate_creator_profile(content_analyses)
        
        self.update_state(state='PROGRESS', meta={'stage': 'Calculating lead score'})
        lead_analysis = generate_lead_score(content_analyses, creator_profile)
        
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
            "items_analyzed": len(content_analyses)
        }
        
    except Exception as e:
        print(f"=== ERROR: {contact_id} - {str(e)} ===")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return {
            "status": "error",
            "contact_id": contact_id,
            "message": str(e)
        }
