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
    """Generate TrovaTrip lead score based on ICP criteria"""
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

CRITICAL: A good fit is someone whose AUDIENCE wants to meet each other AND the host in real life. Think: book clubs traveling to Ireland, widow communities on healing retreats, food bloggers doing culinary tours.

AUTOMATIC DISQUALIFIERS - Score ALL sections 0.0-0.2 if ANY apply:
1. UNSUPPORTED SPORTS/ACTIVITIES (requires specialized equipment/facilities we can't provide):
   - Gymnastics (needs specialized equipment like bars, beams, vault)
   - Golf (needs golf courses and equipment)
   - Watersports (surfing, diving, fishing - requires specialized equipment/boats)
   - Snowsports (skiing, snowboarding - requires mountains/resorts)
   - Hunting, mountain biking, horseback riding
   - Note: Dance, ballet, yoga, pilates, barre, pole dance ARE supported (we can secure studio space)
   - Note: Hiking, camping, swimming, meditation ARE supported
   
2. INAPPROPRIATE CONTENT:
   - Offensive content (racist, misogynistic, xenophobic)
   - Sexual/explicit content (OnlyFans promotion, adult content)
   - Firearms-focused content
   
3. NOT A PERSONAL CREATOR:
   - Business accounts (restaurant, store, brand pages)
   - Accounts that only repost others' content (meme pages, aggregators)
   - No identifiable individual creator/host

OTHER BAD FITS (score low but not automatic disqualification):
- Pure artists/performers with fan bases (not communities)
- Very niche specialists where audience doesn't want group travel together
- Religious/spiritual content as PRIMARY EXCLUSIVE focus (BUT: faith + lifestyle/travel/wellness is GOOD)
- Creators without clear monetization AND without strong face-forward presence (need at least one)

SCORING CRITERIA (0.0-1.0 each):"""
        }, {
            "role": "user",
            "content": f"""{profile_context}

CONTENT: {combined}

FIRST: Check for AUTOMATIC DISQUALIFIERS above. If ANY apply, score all sections 0.0-0.2 and explain why in reasoning.

OTHERWISE, score these 5 sections (0.0 to 1.0):

1. **niche_and_audience_identity** (0.0-1.0)
   HIGH scores (0.7-1.0): Clear lifestyle niche where audience shares identity (widows, DINKs, book lovers, history nerds, foodies, wellness seekers, outdoor enthusiasts, faith + lifestyle communities). People want to connect with EACH OTHER. Niche can be supported by standard group travel (no specialized equipment needed).
   MID scores (0.5-0.6): Somewhat broad (humor, general lifestyle) BUT if host is very face-forward and engaging, audience likely wants to meet them.
   LOW scores (0.0-0.4): Generic content, pure performance/art fans, religious-ONLY content (no lifestyle mix), very technical/specialized, requires unsupported sports/equipment, or unclear who the audience is.
   
2. **host_likeability_and_content_style** (0.0-1.0)
   HIGH scores (0.7-1.0): Face-forward, appears regularly on camera, warm/conversational tone, shares experiences, "come with me" energy, feels like someone you'd travel with. Strong personality can compensate for less defined niche.
   MID scores (0.5-0.6): Sometimes on camera, shows some personality but not consistently engaging.
   LOW scores (0.0-0.4): Behind-the-camera content, aesthetic-only, formal/sterile tone, doesn't show personality, pure expertise without relatability, business account without personal host.

3. **monetization_and_business_mindset** (0.0-1.0)
   HIGH scores (0.7-1.0): Already selling something (coaching, courses, products, Patreon, brand deals, services). Audience pays for access. Comfortable with sales/launches.
   MID scores (0.4-0.6): Some monetization signs (affiliate links, sponsorships, building audience for future monetization) OR no clear monetization BUT very strong face-forward presence and audience engagement (shows business potential).
   LOW scores (0.0-0.3): No monetization, only donations, free content only, AND not face-forward/engaging enough to suggest future business capability.
   
4. **community_infrastructure** (0.0-1.0)
   HIGH scores (0.7-1.0): Has owned channels beyond social media (email list, podcast, YouTube, Patreon, Discord, membership, in-person groups). Can reach audience directly.
   MID scores (0.4-0.6): Active social media with strong engagement, mentions of wanting to connect deeper with audience, building toward owned channels.
   LOW scores (0.0-0.3): Only social media presence, no owned channels mentioned, purely algorithm-dependent.

5. **trip_fit_and_travelability** (0.0-1.0)
   HIGH scores (0.7-1.0): Content naturally fits a trip we can support (food/wine tours, history tours, wellness retreats including specific health focuses, adventure travel with hiking/camping, cultural experiences, creative workshops, faith + travel experiences). Audience has money/time for travel (professionals, DINKs, older audiences, health-focused communities). Already travels or audience asks to travel together.
   MID scores (0.5-0.6): Somewhat broad content (humor, general lifestyle) but host personality could anchor a "come hang with me" style trip. Or specific wellness niche (like TMJ, chronic illness) that fits wellness retreat model.
   LOW scores (0.0-0.4): No natural trip concept, requires unsupported equipment/activities, very young/broke audience, content doesn't translate to group experiences, highly specialized/technical focus that wouldn't work as group travel.

Also provide:
- **combined_lead_score**: Weighted average: (niche × 0.25) + (likeability × 0.20) + (monetization × 0.25) + (community × 0.15) + (trip_fit × 0.15)
- **score_reasoning**: 2-3 sentences on fit for group travel with their community. If disqualified, explain why.

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
    """Send results to HubSpot"""
    content_summaries = [f"Content {idx} ({item['type']}): {item['summary']}" 
                        for idx, item in enumerate(content_analyses, 1)]
    
    # Helper function to safely convert values to strings
    def safe_str(value):
        if value is None:
            return ''
        if isinstance(value, list):
            # Filter out None values and convert all items to strings
            str_items = [str(item) for item in value if item is not None]
            return ', '.join(str_items)
        if isinstance(value, dict):
            # Convert dict to JSON string
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
    
    # Check if we got valid data
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
        "enrichment_status": enrichment_status,  # NEW: success, warning, or error
        "enrichment_error_details": "; ".join(error_details) if error_details else "",  # NEW: what went wrong
        "items_analyzed": len(content_analyses)  # NEW: how many content items were analyzed
    }
    
    print(f"Sending to HubSpot: {HUBSPOT_WEBHOOK_URL}")
    print(f"Enrichment Status: {enrichment_status}")
    if error_details:
        print(f"Error Details: {'; '.join(error_details)}")
    
    response = requests.post(HUBSPOT_WEBHOOK_URL, json=payload, timeout=10)
    print(f"HubSpot response: {response.status_code}")


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
        
        # Process content items - try up to 10 items to get 3 successful analyses
        self.update_state(state='PROGRESS', meta={'stage': 'Analyzing content'})
        
        content_analyses = []
        items_to_try = min(10, len(content_items))  # Try up to 10 items
        
        for idx, item in enumerate(content_items[:items_to_try], 1):
            if len(content_analyses) >= 3:
                break  # Stop once we have 3 successful analyses
            
            print(f"Processing item {idx}/{items_to_try} (have {len(content_analyses)} successful so far)")
            
            content_format = item.get('format')
            media_url = None
            media_format = None
            
            # Determine media URL and format
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
            
            # Check video file size before processing
            if media_format == 'VIDEO':
                try:
                    head_response = requests.head(media_url, timeout=10)
                    content_length = int(head_response.headers.get('content-length', 0))
                    max_size = 25 * 1024 * 1024  # 25MB in bytes
                    
                    if content_length > max_size:
                        print(f"Item {idx}: Video too large ({content_length / 1024 / 1024:.1f}MB), skipping")
                        continue
                except Exception as e:
                    print(f"Item {idx}: Could not check video size: {e}, attempting anyway")
            
            try:
                # Re-host media on R2
                rehosted_url = rehost_media_on_r2(media_url, contact_id, media_format)
                
                # Analyze content
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
        
        # Check if we have enough content to analyze
        if len(content_analyses) < 1:
            return {"status": "error", "message": "Could not analyze any content items"}
        
        print(f"Successfully analyzed {len(content_analyses)} items")
        
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
