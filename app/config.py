"""
Centralized configuration — all env vars, constants, BDR map.
"""
import os


# ── Logging ──────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = os.getenv('LOG_FORMAT', 'text')

# ── Redis ─────────────────────────────────────────────────────────────────────
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# ── PostgreSQL ────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///local.db')

# ── InsightIQ ─────────────────────────────────────────────────────────────────
INSIGHTIQ_USERNAME = os.getenv('INSIGHTIQ_USERNAME')
INSIGHTIQ_PASSWORD = os.getenv('INSIGHTIQ_PASSWORD')
INSIGHTIQ_WORK_PLATFORM_ID = os.getenv('INSIGHTIQ_WORK_PLATFORM_ID')
INSIGHTIQ_API_URL = os.getenv('INSIGHTIQ_API_URL', 'https://api.staging.insightiq.ai')
INSIGHTIQ_CLIENT_ID = os.getenv('INSIGHTIQ_CLIENT_ID')
INSIGHTIQ_SECRET = os.getenv('INSIGHTIQ_SECRET')

# ── OpenAI ────────────────────────────────────────────────────────────────────
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# ── Anthropic ─────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')

# ── Ollama (local LLM) ──────────────────────────────────────────────────────
OLLAMA_URL = os.getenv('OLLAMA_URL', 'http://localhost:11434')
OLLAMA_MODEL = os.getenv('OLLAMA_MODEL', 'gemma3:1b')

# ── HubSpot ───────────────────────────────────────────────────────────────────
HUBSPOT_WEBHOOK_URL = os.getenv('HUBSPOT_WEBHOOK_URL')
HUBSPOT_API_KEY = os.getenv('HUBSPOT_API_KEY')
HUBSPOT_API_URL = 'https://api.hubapi.com'

# ── Cloudflare R2 ─────────────────────────────────────────────────────────────
R2_ACCESS_KEY_ID = os.getenv('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.getenv('R2_SECRET_ACCESS_KEY')
R2_BUCKET_NAME = os.getenv('R2_BUCKET_NAME')
R2_ENDPOINT_URL = os.getenv('R2_ENDPOINT_URL')
R2_PUBLIC_URL = os.getenv('R2_PUBLIC_URL')

# ── External services ─────────────────────────────────────────────────────────
APIFY_API_TOKEN = os.getenv('APIFY_API_TOKEN')
APOLLO_API_KEY = os.getenv('APOLLO_API_KEY')
MILLIONVERIFIER_API_KEY = os.getenv('MILLIONVERIFIER_API_KEY')

# ── Slack notifications ──────────────────────────────────────────────────────
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')

# ── Auth ─────────────────────────────────────────────────────────────────────
DASHBOARD_PASSWORD = os.getenv('DASHBOARD_PASSWORD')

# ── BDR Round-Robin — display name → HubSpot owner ID ────────────────────────
BDR_OWNER_IDS = {
    'Miriam Plascencia':   '83266567',
    'Majo Juarez':         '79029958',
    'Nicole Roma':         '83266570',
    'Salvatore Renteria':  '81500975',
    'Sofia Gonzalez':      '79029956',
    'Tanya Pina':          '83266565',
}

# ── Pipeline stage definitions ────────────────────────────────────────────────
PIPELINE_STAGES = [
    'discovery',
    'pre_screen',
    'enrichment',
    'analysis',
    'scoring',
    'crm_sync',
]

# ── Run status values ─────────────────────────────────────────────────────────
RUN_STATUSES = [
    'queued',
    'discovering',
    'pre_screening',
    'enriching',
    'analyzing',
    'scoring',
    'syncing',
    'completed',
    'failed',
]
