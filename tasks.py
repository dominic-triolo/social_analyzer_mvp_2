import os
import json
import requests
import tempfile
import base64
import hashlib
import time  # Added for discovery
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

# Discovery configuration (NEW)
INSIGHTIQ_CLIENT_ID = os.getenv('INSIGHTIQ_CLIENT_ID')
INSIGHTIQ_SECRET = os.getenv('INSIGHTIQ_SECRET')
HUBSPOT_API_KEY = os.getenv('HUBSPOT_API_KEY')
HUBSPOT_API_URL = 'https://api.hubapi.com'

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


# ============================================================================
# EXISTING ENRICHMENT FUNCTIONS
# ============================================================================

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


# ============================================================================
# EXISTING ENRICHMENT TASK
# ============================================================================

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


# ============================================================================
# NEW: DISCOVERY MODULE
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
    
    # Fixed parameters applied to ALL searches
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
        """
        Search for creator profiles with fixed base parameters
        
        Args:
            platform: 'instagram', 'youtube', 'tiktok', or 'facebook'
            user_filters: dict with ONLY user-configurable parameters:
                - max_results: int (1-4000)
                - follower_count: dict with min/max
                - lookalike_type: 'creator' or 'audience' (mutually exclusive)
                - lookalike_username: str (required if lookalike_type set)
                - creator_interests: list of str
                - hashtags: list of dicts [{"name": "travel"}, ...]
        
        Returns:
            List of profile dicts with standardized fields
        """
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
            # YouTube uses subscriber_count
            parameters['subscriber_count'] = {
                'min': follower_filter.get('min', 20000),
                'max': follower_filter.get('max', 900000)
            }
        else:
            parameters['follower_count'] = {
                'min': follower_filter.get('min', 20000),
                'max': follower_filter.get('max', 900000)
            }
        
        # Add lookalike (mutually exclusive - enforce this!)
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
        
        if 'hashtags' in user_filters and user_filters['hashtags']:
            parameters['hashtags'] = user_filters['hashtags']
            print(f"Hashtags: {parameters['hashtags']}")
        
        # Start export job
        print(f"Starting {platform} discovery with fixed parameters...")
        print(f"Follower range: {parameters.get('follower_count', parameters.get('subscriber_count'))}")
        print(f"Max results: {parameters['max_results']}")
        
        job_id = self._start_job(parameters)
        
        # Wait for results
        print(f"Waiting for results (job_id: {job_id})...")
        raw_results = self._fetch_results(job_id)
        
        # Standardize output
        print(f"Processing {len(raw_results)} profiles...")
        return self._standardize_results(raw_results, platform)
    
    def _start_job(self, parameters):
        """Start InsightIQ export job"""
        url = 'https://api.insightiq.ai/v1/social/creators/profiles/search-export'
        
        print(f"API parameters: {parameters}")
        
        try:
            response = requests.post(url=url, headers=self.headers, json=parameters, timeout=30)
            
            if response.status_code != 200:
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
        
        max_wait_time = 600  # 10 minutes max
        start_time = time.time()
        poll_count = 0
        
        while True:
            # Check timeout
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
                    print(f"Fetch error: {response.status_code} - {response.text}")
                    raise Exception(f"Failed to fetch results: {response.text}")
                
                data = response.json()
                
                # Check if job is still processing
                if data.get('status') == 'IN_PROGRESS':
                    print(f"Job still processing (poll #{poll_count}, elapsed: {int(elapsed)}s), waiting 60 seconds...")
                    time.sleep(60)
                    continue
                
                # Check for errors
                if data.get('status') == 'FAILED':
                    error_msg = data.get('error', 'Unknown error')
                    raise Exception(f"Job failed: {error_msg}")
                
                # Job completed, collect results
                batch_results = data.get('data', [])
                all_results.extend(batch_results)
                
                total_results = data.get('metadata', {}).get('total_results', 0)
                
                print(f"Fetched {len(all_results)}/{total_results} profiles")
                
                # Check if we've got all results
                if offset + limit >= total_results or len(batch_results) == 0:
                    break
                
                offset += limit
                
            except requests.exceptions.RequestException as e:
                print(f"Request failed during fetch: {e}")
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
                
                # Parse name
                full_name = profile.get('full_name', '')
                name_parts = [n.capitalize() for n in full_name.split()] if full_name else []
                first_name = name_parts[0] if name_parts else ''
                last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''
                
                # Get location
                location = profile.get('creator_location', {})
                
                # Standardized output
                standardized_profile = {
                    'profile_url': profile.get('url', ''),
                    'handle': profile.get('platform_username', ''),
                    'display_name': full_name,
                    'first_name': first_name,
                    'last_name': last_name,
                    'platform': platform,
                    'follower_count': profile.get('follower_count') or profile.get('subscriber_count', 0),
                    'engagement_rate': profile.get('engagement_rate', 0),
                    'bio': profile.get('bio', ''),
                    'email': contact_details.get('email'),
                    'phone': contact_details.get('phone'),
                    'city': location.get('city'),
                    'state': location.get('state'),
                    'country': location.get('country'),
                    'audience_credibility': profile.get('audience_credibility_category'),
                    'last_post_date': profile.get('last_post_timestamp'),
                    
                    # Additional contact URLs
                    **{k: v for k, v in contact_details.items() 
                       if k not in ('email', 'phone')}
                }
                
                standardized.append(standardized_profile)
                
            except Exception as e:
                print(f"Failed to process profile #{i+1}: {e}")
                continue
        
        print(f"Successfully processed {len(standardized)} profiles")
        return standardized
    
    def _extract_contact_details(self, contact_details):
        """Extract and format contact details"""
        contacts = {}
        
        for detail in contact_details:
            contact_type = detail.get('type', '').lower()
            contact_value = detail.get('value', '')
            
            if contact_type and contact_value:
                if contact_type in ('email', 'phone'):
                    contacts[contact_type] = contact_value
                else:
                    # Other contact types (twitter, linkedin, etc.)
                    contacts[f'{contact_type}_url'] = contact_value
        
        return contacts


# ============================================================================
# NEW: DISCOVERY TASKS
# ============================================================================

@celery_app.task(name='tasks.discover_instagram_profiles')
def discover_instagram_profiles(user_filters=None, job_id=None):
    """
    Run Instagram profile discovery with fixed base parameters
    
    Args:
        user_filters: dict with ONLY user-configurable parameters:
            - max_results: int (1-4000)
            - follower_count: {min: int, max: int}
            - lookalike_type: 'creator' or 'audience' (mutually exclusive!)
            - lookalike_username: str
            - creator_interests: list of str
            - hashtags: list of dicts [{"name": "travel"}, ...]
        job_id: optional job tracking ID
    
    Returns:
        dict with results summary
    """
    if job_id is None:
        job_id = discover_instagram_profiles.request.id
    
    try:
        # Update status
        update_discovery_job_status(job_id, status='discovering')
        
        # Get credentials from environment
        client_id = INSIGHTIQ_CLIENT_ID
        secret = INSIGHTIQ_SECRET
        
        if not client_id or not secret:
            raise ValueError("INSIGHTIQ_CLIENT_ID and INSIGHTIQ_SECRET must be set in environment")
        
        # Validate lookalikes are mutually exclusive
        user_filters = user_filters or {}
        lookalike_type = user_filters.get('lookalike_type')
        lookalike_username = user_filters.get('lookalike_username', '').strip()
        
        if lookalike_type and lookalike_type not in ('creator', 'audience'):
            raise ValueError("lookalike_type must be 'creator' or 'audience'")
        
        if lookalike_type and not lookalike_username:
            raise ValueError("lookalike_username required when lookalike_type is set")
        
        print(f"Starting discovery with filters: {user_filters}")
        
        # Initialize client
        discovery_client = InsightIQDiscovery(client_id, secret)
        
        # Run discovery
        profiles = discovery_client.search_profiles(platform='instagram', user_filters=user_filters)
        
        print(f"Discovery complete: {len(profiles)} profiles found")
        
        # Update status
        update_discovery_job_status(job_id, status='importing', profiles_found=len(profiles))
        
        # Import to HubSpot
        import_results = import_profiles_to_hubspot(profiles, job_id)
        
        # Final status
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


def update_discovery_job_status(job_id, status, **kwargs):
    """
    Update discovery job status in Redis
    
    Args:
        job_id: Job ID
        status: Job status (queued, discovering, importing, completed, failed)
        **kwargs: Additional fields to update
    """
    try:
        # Import Redis here to avoid issues if it's defined elsewhere
        from app import r
        
        job_key = f'discovery_job:{job_id}'
        
        # Get existing job data
        job_data = r.get(job_key)
        if job_data:
            job_data = json.loads(job_data)
        else:
            job_data = {'job_id': job_id}
        
        # Update fields
        job_data['status'] = status
        job_data['updated_at'] = datetime.now().isoformat()
        job_data.update(kwargs)
        
        # Save with 24 hour TTL
        r.setex(job_key, 86400, json.dumps(job_data))
        
        print(f"Job {job_id} status updated: {status}")
    except Exception as e:
        print(f"Failed to update job status: {e}")


def import_profiles_to_hubspot(profiles, job_id):
    """
    Import discovered profiles to HubSpot via batch API
    
    Args:
        profiles: List of profile dicts from InsightIQDiscovery
        job_id: Discovery job ID for tracking
    
    Returns:
        dict with {'created': int, 'skipped': int}
    """
    if not HUBSPOT_API_KEY:
        raise ValueError("HUBSPOT_API_KEY must be set in environment")
    
    contacts = []
    
    print(f"Preparing {len(profiles)} profiles for HubSpot import")
    
    for profile in profiles:
        # Map discovery fields to HubSpot properties
        properties = {
            # Core identity
            'platform': profile.get('platform', 'instagram'),
            'profile_url': profile['profile_url'],
            'instagram_handle': profile.get('handle', ''),
            
            # Name
            'firstname': profile.get('first_name', ''),
            'lastname': profile.get('last_name', ''),
            
            # Metrics
            'followers': profile.get('follower_count', 0),
            'engagement_rate': profile.get('engagement_rate', 0),
            
            # Contact info
            'email': profile.get('email'),
            'phone': profile.get('phone'),
            
            # Location
            'city': profile.get('city'),
            'state': profile.get('state'),
            'country': profile.get('country'),
            
            # Bio/description (truncate for HubSpot limits)
            'bio': profile.get('bio', '')[:5000] if profile.get('bio') else '',
            
            # Discovery metadata
            'discovery_source': 'insightiq_discovery',
            'discovery_job_id': job_id,
            'discovery_date': datetime.now().isoformat(),
            'enrichment_status': 'pending',
            
            # Lead qualification
            'lifecycle_stage': 'lead',
            'audience_credibility': profile.get('audience_credibility'),
            'last_post_date': profile.get('last_post_date')
        }
        
        # Add any additional contact URLs (twitter, linkedin, etc.)
        for key, value in profile.items():
            if key.endswith('_url') and key not in ('profile_url',):
                properties[key] = value
        
        # Remove None values (HubSpot API doesn't like them)
        properties = {k: v for k, v in properties.items() if v is not None}
        
        contacts.append({'properties': properties})
    
    # Batch import (max 100 per request)
    created_count = 0
    skipped_count = 0
    total_batches = (len(contacts) + 99) // 100
    
    print(f"Importing in {total_batches} batches...")
    
    for i in range(0, len(contacts), 100):
        batch = contacts[i:i+100]
        batch_num = (i // 100) + 1
        
        try:
            print(f"Importing batch {batch_num}/{total_batches} ({len(batch)} contacts)...")
            
            response = requests.post(
                f"{HUBSPOT_API_URL}/crm/v3/objects/contacts/batch/create",
                headers={
                    'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                    'Content-Type': 'application/json'
                },
                json={'inputs': batch},
                timeout=30
            )
            
            if response.status_code == 201:
                # All created successfully
                created_count += len(batch)
                print(f"Batch {batch_num}: {len(batch)} contacts created")
                
            elif response.status_code == 207:
                # Multi-status: some created, some duplicates
                result = response.json()
                batch_created = len(result.get('results', []))
                batch_errors = result.get('errors', [])
                batch_skipped = len(batch_errors)
                
                created_count += batch_created
                skipped_count += batch_skipped
                
                print(f"Batch {batch_num}: {batch_created} created, {batch_skipped} duplicates/errors")
                
                # Log first few errors for debugging
                for error in batch_errors[:3]:
                    print(f"Error: {error.get('message', 'Unknown error')}")
                
            else:
                # Error
                print(f"Batch import error: {response.status_code} - {response.text}")
                skipped_count += len(batch)
        
        except Exception as e:
            print(f"Exception importing batch {batch_num}: {e}")
            skipped_count += len(batch)
        
        # Small delay between batches to avoid rate limits
        if i + 100 < len(contacts):
            time.sleep(0.5)
    
    print(f"Import complete: {created_count} created, {skipped_count} skipped/errors")
    
    return {
        'created': created_count,
        'skipped': skipped_count
    }
