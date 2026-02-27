"""
Shared client instances — Redis, R2 (boto3), OpenAI.

Lazily initialized on first access so importing this module is always safe
(even when env vars are missing during tests).
"""
import logging
import redis
import boto3
from botocore.client import Config

from app.config import (
    REDIS_URL,
    R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME,
    R2_ENDPOINT_URL, R2_PUBLIC_URL,
    OPENAI_API_KEY,
)

logger = logging.getLogger('app.extensions')

# ── Redis ─────────────────────────────────────────────────────────────────────
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# ── R2 (Cloudflare) ──────────────────────────────────────────────────────────
r2_client = None
if R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY and R2_ENDPOINT_URL:
    try:
        r2_client = boto3.client(
            's3',
            endpoint_url=R2_ENDPOINT_URL,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            config=Config(signature_version='s3v4'),
            region_name='auto',
        )
        logger.info("R2 client initialized successfully")
    except Exception as e:
        logger.error("Error initializing R2 client: %s", e)
else:
    logger.warning("R2 credentials not set — re-hosting will be skipped")

# ── OpenAI ────────────────────────────────────────────────────────────────────
openai_client = None
if OPENAI_API_KEY:
    try:
        from openai import OpenAI
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        logger.info("OpenAI client initialized successfully")
    except Exception as e:
        logger.error("Error initializing OpenAI client: %s", e)
else:
    logger.warning("OPENAI_API_KEY not set")
