import os
import json
import requests
from flask import Flask, request, jsonify
from typing import Dict, List, Any
from datetime import datetime
import tempfile
import base64

app = Flask(__name__)

# Configuration from environment variables
INSIGHTIQ_USERNAME = os.getenv('INSIGHTIQ_USERNAME')
INSIGHTIQ_PASSWORD = os.getenv('INSIGHTIQ_PASSWORD')
INSIGHTIQ_WORK_PLATFORM_ID = os.getenv('INSIGHTIQ_WORK_PLATFORM_ID')  # Set this manually
INSIGHTIQ_API_URL = os.getenv('INSIGHTIQ_API_URL', 'https://api.sandbox.insightiq.ai')  # Defaults to sandbox
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
HUBSPOT_WEBHOOK_URL = os.getenv('HUBSPOT_WEBHOOK_URL')

# Check required environment variables
required_vars = {
    'INSIGHTIQ_USERNAME': INSIGHTIQ_USERNAME,
    'INSIGHTIQ_PASSWORD': INSIGHTIQ_PASSWORD,
    'INSIGHTIQ_WORK_PLATFORM_ID': INSIGHTIQ_WORK_PLATFORM_ID,
    'OPENAI_API_KEY': OPENAI_API_KEY,
    'HUBSPOT_WEBHOOK_URL': HUBSPOT_WEBHOOK_URL
}

missing_vars = [k for k, v in required_vars.items() if not v]
if missing_vars:
    print(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}")
    print("App cannot function without these variables!")

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
                            "text": """Analyze this social media image and provide a brief summary (2-3 sentences) covering:
1. Visual content and composition
2. Any text or messaging visible
3. Overall tone and style

Respond in JSON format:
{
  "summary": "your summary here"
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
                    "content": f"""Based on this video transcription, provide a brief summary (2-3 sentences) of the video content, main points, and tone.

TRANSCRIPTION:
{transcript}

Respond in JSON format:
{{
  "summary": "your summary here"
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


def generate_lead_analysis(content_analyses: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Combine all content summaries and generate a single lead score"""
    
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
    
    # Single comprehensive analysis
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": """You are a social media lead scoring analyst. Based on a creator's content summaries, you assess:
- Content quality and professionalism
- Engagement potential and audience appeal
- Brand alignment and messaging consistency
- Overall value as a potential business lead

Provide a lead score from 0.0 to 1.0 where:
- 0.0-0.3: Low quality, poor engagement potential
- 0.3-0.6: Moderate quality, some potential
- 0.6-0.8: Good quality, strong potential
- 0.8-1.0: Excellent quality, high-value lead"""
            },
            {
                "role": "user",
                "content": f"""Based on these individual content summaries (which include both AI analysis and original post descriptions), provide:

1. The combined content summaries (concatenate them with separators like " | " or similar)
2. A single lead score from 0.0 to 1.0 based on their overall potential as a lead

INDIVIDUAL CONTENT SUMMARIES:
{combined_summaries}

Respond in JSON format:
{{
  "combined_summary": "Content 1: [summary] | Content 2: [summary] | ...",
  "lead_score": 0.85
}}"""
            }
        ],
        response_format={"type": "json_object"}
    )
    
    result = json.loads(response.choices[0].message.content)
    return {
        "summary": result['combined_summary'],
        "lead_score": result['lead_score']
    }


def send_to_hubspot(contact_id: str, summary: str, lead_score: float):
    """Send results back to HubSpot via webhook"""
    payload = {
        "contact_id": contact_id,
        "summary": summary,
        "lead_score": lead_score,
        "analyzed_at": datetime.now().isoformat()
    }
    
    response = requests.post(HUBSPOT_WEBHOOK_URL, json=payload)
    response.raise_for_status()
    
    return response.json()


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
            
            if not media_url:
                print(f"STEP 2.{idx}: No media URL found, skipping")
                continue
            
            print(f"STEP 2.{idx}: Final URL (first 100 chars): {media_url[:100]}...")
            
            try:
                print(f"STEP 2.{idx}: Analyzing {media_format}...")
                analysis = analyze_content_item(media_url, media_format)
                
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
        
        # Step 4: Generate single lead score and combined summary based on all content
        print(f"STEP 4: Generating lead analysis...")
        lead_analysis = generate_lead_analysis(content_analyses)
        print(f"STEP 4 COMPLETE: Lead score: {lead_analysis['lead_score']}")
        
        # Step 5: Send to HubSpot
        print(f"STEP 5: Sending results to HubSpot...")
        send_to_hubspot(
            contact_id,
            lead_analysis['summary'],
            lead_analysis['lead_score']
        )
        print(f"STEP 5 COMPLETE: Results sent to HubSpot")
        
        print("=== WEBHOOK COMPLETE ===")
        
        return jsonify({
            "status": "success",
            "contact_id": contact_id,
            "items_processed": len(content_analyses),
            "lead_score": lead_analysis['lead_score'],
            "summary": lead_analysis['summary']
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
