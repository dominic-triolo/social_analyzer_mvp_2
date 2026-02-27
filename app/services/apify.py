"""
Apify actor management, social graph building, Apollo enrichment,
and all platform-agnostic enrichment pipeline logic.
"""
import logging
import os
import re
import json
import time
import hashlib
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime
from urllib.parse import urlparse
from bs4 import BeautifulSoup

logger = logging.getLogger('services.apify')

from app.config import (
    APIFY_API_TOKEN, APOLLO_API_KEY, MILLIONVERIFIER_API_KEY,
    BDR_OWNER_IDS, HUBSPOT_API_KEY, HUBSPOT_API_URL,
    R2_BUCKET_NAME, R2_PUBLIC_URL,
)
from app.extensions import redis_client, r2_client


# ============================================================================
# APOLLO.IO ENRICHMENT CLIENT
# ============================================================================

class ApolloEnrichment:
    """
    Apollo.io API client for professional email lookup.

    Strategy (matching colleague's implementation):
      - Attempt 1: full query (name, domain, org, linkedin)
      - Attempt 2 (if first fails and name looks like a real person):
          firstName + lastName + domain only
      - 300 ms delay between attempts
      - Input dedup via SHA-256 hash
    """

    BASE_URL = "https://api.apollo.io/api/v1"

    # Domains that are never useful for Apollo lookup
    SKIP_DOMAINS = {
        "meetup.com", "eventbrite.com", "youtube.com", "youtu.be",
        "reddit.com", "facebook.com", "instagram.com", "twitter.com",
        "x.com", "linkedin.com", "patreon.com", "tiktok.com",
        "google.com", "yelp.com", "tripadvisor.com", "wikipedia.org",
        "amazon.com", "substack.com", "discord.com", "discord.gg",
        "github.com", "medium.com",
    }

    # Name must look like a real human name (letters, spaces, hyphens, ≥2 chars)
    _VALID_NAME_RE = re.compile(r'^[A-Za-z\s\-]{2,}$')

    def __init__(self, api_key: str):
        self.api_key = api_key

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def person_match(self, name: str = None, domain: str = None,
                     org_name: str = None, linkedin_url: str = None) -> Optional[Dict]:
        """
        Find email + contact info for a person.

        Returns dict with: email, first_name, last_name, full_name, title,
        linkedin, twitter, facebook, phone, location, headline, organization.
        Returns None if nothing found.
        """
        if not self.api_key:
            return None

        # Attempt 1: full query
        result = self._call_match(
            name=name, domain=domain,
            org_name=org_name, linkedin_url=linkedin_url
        )
        if result and result.get('email'):
            return result

        # Attempt 2: simplified (firstName + lastName + domain)
        if name and domain and self._is_valid_candidate(name):
            time.sleep(0.3)
            parts = name.strip().split()
            if len(parts) >= 2:
                result2 = self._call_match(
                    first_name=parts[0],
                    last_name=' '.join(parts[1:]),
                    domain=domain,
                )
                if result2 and result2.get('email'):
                    return result2

        return result  # return whatever we got (may have linkedin even without email)

    @staticmethod
    def extract_domain(url: str) -> str:
        """Extract bare domain from URL, e.g. 'example.com'."""
        try:
            return urlparse(url).netloc.replace('www.', '').lower()
        except Exception:
            return ''

    @staticmethod
    def is_enrichable_domain(domain: str) -> bool:
        """Return True if domain is not a social platform / known skip domain."""
        if not domain:
            return False
        return not any(skip in domain for skip in ApolloEnrichment.SKIP_DOMAINS)

    @staticmethod
    def make_input_hash(**kwargs) -> str:
        """SHA-256 hash of Apollo query params for dedup."""
        payload = json.dumps(kwargs, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    @staticmethod
    def _is_valid_candidate(name: str) -> bool:
        return bool(ApolloEnrichment._VALID_NAME_RE.match(name.strip()))

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _call_match(self, name=None, first_name=None, last_name=None,
                    domain=None, org_name=None, linkedin_url=None) -> Optional[Dict]:
        """Single Apollo /people/match call."""
        data: Dict = {'reveal_personal_emails': True}
        if name:        data['name']              = name
        if first_name:  data['first_name']        = first_name
        if last_name:   data['last_name']         = last_name
        if domain:      data['domain']            = domain
        if org_name:    data['organization_name'] = org_name
        if linkedin_url: data['linkedin_url']     = linkedin_url

        try:
            resp = requests.post(
                f"{self.BASE_URL}/people/match",
                headers={
                    'Content-Type':  'application/json',
                    'x-api-key':     self.api_key,
                    'Cache-Control': 'no-cache',
                },
                json=data,
                timeout=15,
            )

            if resp.status_code == 429:
                logger.warning("Rate limited — backing off 2s")
                time.sleep(2)
                return None
            if resp.status_code in (401, 403):
                logger.error("Auth error (%s)", resp.status_code)
                return None
            if resp.status_code == 422:
                logger.warning("Unprocessable (422) — invalid params")
                return None
            if not resp.ok:
                logger.error("Error %s", resp.status_code)
                return None

            person = resp.json().get('person') or {}
            if not person:
                return None

            # Extract phone
            phones = person.get('phone_numbers') or []
            phone = phones[0].get('raw_number', '') if phones else ''

            # Build location
            loc_parts = [person.get('city'), person.get('state'), person.get('country')]
            location = ', '.join(p for p in loc_parts if p)

            return {
                'email':        person.get('email', ''),
                'first_name':   person.get('first_name', ''),
                'last_name':    person.get('last_name', ''),
                'full_name':    person.get('name', ''),
                'title':        person.get('title', ''),
                'linkedin':     person.get('linkedin_url', ''),
                'twitter':      person.get('twitter_url', ''),
                'facebook':     person.get('facebook_url', ''),
                'phone':        phone,
                'location':     location,
                'headline':     person.get('headline', ''),
                'organization': (person.get('organization') or {}).get('name', ''),
            }

        except Exception as e:
            logger.error("Exception: %s", e)
            return None


# ============================================================================
# MILLIONVERIFIER EMAIL VALIDATION CLIENT
# ============================================================================

class MillionVerifierClient:
    """
    MillionVerifier API client for email validation.

    Batch processing: groups of 10, parallel within each batch,
    100 ms delay between batches (matches colleague's implementation).
    """

    BASE_URL = "https://api.millionverifier.com/api/v3/"

    # Result mapping (API result → our status string)
    _RESULT_MAP = {
        'ok':          'valid',
        'catch_all':   'catch-all',
        'invalid':     'invalid',
        'disposable':  'invalid',
        'unknown':     'unknown',
        'error':       'unknown',
    }

    def __init__(self, api_key: str):
        self.api_key = api_key

    def verify_email(self, email: str) -> Dict:
        """Verify a single email. Returns dict with 'status' and raw fields."""
        url = (f"{self.BASE_URL}?api={requests.utils.quote(self.api_key)}"
               f"&email={requests.utils.quote(email)}&timeout=15")
        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            raw_result = data.get('result', 'unknown')
            return {
                'email':   data.get('email', email),
                'status':  self._RESULT_MAP.get(raw_result, 'unknown'),
                'quality': data.get('quality', 'unknown'),
                'free':    bool(data.get('free')),
                'role':    bool(data.get('role')),
            }
        except Exception as e:
            logger.error("Error verifying %s: %s", email, e)
            return {'email': email, 'status': 'unknown', 'quality': 'unknown',
                    'free': False, 'role': False}

    def verify_batch(self, email_items: List[Dict]) -> Dict[str, str]:
        """
        Verify a list of {'email': str, 'profile_idx': int} dicts in parallel batches.

        Returns dict: {email -> status_string}
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results: Dict[str, str] = {}
        batch_size = 10

        for i in range(0, len(email_items), batch_size):
            batch = email_items[i:i + batch_size]

            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                future_to_email = {
                    executor.submit(self.verify_email, item['email']): item['email']
                    for item in batch
                }
                for future in as_completed(future_to_email):
                    email = future_to_email[future]
                    try:
                        result = future.result()
                        results[email] = result['status']
                    except Exception as e:
                        logger.error("Future error for %s: %s", email, e)
                        results[email] = 'unknown'

            # 100 ms delay between batches
            if i + batch_size < len(email_items):
                time.sleep(0.1)

        return results

# ============================================================================
# SOCIAL GRAPH BUILDER
# Crawls websites and link aggregators for emails + social links.
# Uses Apify Cheerio Scraper for reliability (JS-heavy pages, bot protection).
# Falls back to direct requests for simple pages.
# ============================================================================

class SocialGraphBuilder:
    """
    Builds a social graph for a creator by:
      1. Scraping Linktree / Beacons / other link aggregators (via Apify)
      2. Crawling personal websites (/contact, /about, /about-us) (via Apify)
      3. Direct HTTP fallback for both of the above
    Extracts: emails, social profile URLs, personal website URL.
    """

    LINK_AGGREGATORS = [
        'linktr.ee', 'beacons.ai', 'linkin.bio', 'linkpop.com',
        'hoo.be', 'campsite.bio', 'lnk.bio', 'tap.bio', 'solo.to',
        'bio.link', 'carrd.co',
    ]

    # Domains treated as social platforms (not personal websites)
    _SOCIAL_HOSTS = {
        'youtube.com', 'youtu.be', 'instagram.com', 'twitter.com', 'x.com',
        'discord.gg', 'discord.com', 'facebook.com', 'tiktok.com', 'twitch.tv',
        'linkedin.com', 'patreon.com', 'google.com', 'apple.com', 'spotify.com',
        'amazon.com', 'reddit.com', 'tumblr.com', 'pinterest.com', 'github.com',
        'medium.com', 'wordpress.com', 'linktr.ee', 'beacons.ai', 'ko-fi.com',
        'buymeacoffee.com', 'gumroad.com', 'substack.com', 'bit.ly',
        'meetup.com', 'eventbrite.com',
    }

    SOCIAL_PATTERNS: Dict[str, str] = {
        'instagram_url': r'instagram\.com/(?!p/|reel/|explore/)([a-zA-Z0-9._]+)',
        'youtube_url':   r'youtube\.com/(?:c/|channel/|@)?([a-zA-Z0-9_\-]+)',
        'twitter_url':   r'(?:twitter|x)\.com/([a-zA-Z0-9_]+)',
        'linkedin_url':  r'linkedin\.com/in/([a-zA-Z0-9\-]+)',
        'tiktok_url':    r'tiktok\.com/@([a-zA-Z0-9._]+)',
        'facebook_url':  r'facebook\.com/(?!groups/)([a-zA-Z0-9.]+)',
        'twitch_url':    r'twitch\.tv/([a-zA-Z0-9_]+)',
        'discord_url':   r'discord\.(?:gg|com)/([a-zA-Z0-9]+)',
    }

    # Email patterns
    _EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    _OBFUSCATED_PATTERNS = [
        re.compile(r'([a-zA-Z0-9._%+-]+)\s*\[\s*at\s*\]\s*([a-zA-Z0-9.-]+)\s*\[\s*dot\s*\]\s*([a-zA-Z]{2,})', re.I),
        re.compile(r'([a-zA-Z0-9._%+-]+)\s*\(\s*at\s*\)\s*([a-zA-Z0-9.-]+)\s*\(\s*dot\s*\)\s*([a-zA-Z]{2,})', re.I),
        re.compile(r'([a-zA-Z0-9._%+-]+)\s*\{\s*at\s*\}\s*([a-zA-Z0-9.-]+)\s*\{\s*dot\s*\}\s*([a-zA-Z]{2,})', re.I),
        re.compile(r'([a-zA-Z0-9._%+-]+)\s+at\s+([a-zA-Z0-9.-]+)\s+dot\s+([a-zA-Z]{2,})\b', re.I),
    ]
    _BLOCKED_EMAIL_PATTERNS = [
        '.png', '.jpg', '.gif', '.jpeg', '.webp', '.svg',
        'sentry.io', 'example.com', 'cloudfront', 'amazonaws',
        'patreon.com', 'w3.org', 'schema.org', 'googleapis.com', 'gstatic.com',
        'substackcdn', 'cdninstagram', 'fbcdn',
    ]

    # Email priority tier 2: personal providers beat unknown domains
    _PERSONAL_EMAIL_DOMAINS = {
        'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
        'icloud.com', 'me.com', 'protonmail.com', 'proton.me', 'live.com',
        'msn.com', 'mail.com', 'zoho.com', 'ymail.com', 'gmx.com',
    }

    # Extended subpage list for website contact crawl (26 paths)
    _CRAWL_SUBPAGES = [
        '', '/contact', '/about', '/about-us', '/contact-us', '/team',
        '/staff', '/bio', '/press', '/people', '/our-team', '/meet-the-team',
        '/leadership', '/board', '/board-of-directors', '/officers',
        '/connect', '/get-in-touch', '/reach-out', '/organizers',
        '/hosts', '/founders', '/who-we-are', '/our-story', '/info', '/support',
    ]

    # Glob keywords — Apify uses these to also follow dynamically routed pages
    _CRAWL_GLOB_KEYWORDS = [
        'contact', 'about', 'team', 'staff', 'people', 'board',
        'leadership', 'connect', 'organizer', 'host', 'founder',
        'info', 'support', 'who-we-are',
    ]

    # Name extraction from Google result titles / LinkedIn slugs
    _NAME_FROM_TITLE_RE = re.compile(
        r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s*[-|–·,]', re.UNICODE
    )
    _NAME_CONTEXT_RES = [
        re.compile(
            r'(?:admin|administrator|owner|manager|founder|created\s+by|'
            r'managed\s+by|run\s+by|led\s+by|organized\s+by)\s*[:\-–]?\s*'
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})', re.I
        ),
        re.compile(
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s*'
            r'(?:is the|,\s*(?:admin|founder|owner|manager|organizer|leader))', re.I
        ),
    ]
    _LINKEDIN_SLUG_RE = re.compile(r'linkedin\.com/in/([a-zA-Z0-9\-]+)')

    def __init__(self, apify_token: str = None):
        self.apify_token = apify_token
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            )
        })

    # ------------------------------------------------------------------
    # Public: batch operations (Apify-backed, called by enrichment pipeline)
    # ------------------------------------------------------------------

    def scrape_link_aggregators_batch(self, urls: List[str]) -> Dict[str, Dict]:
        """
        Scrape a batch of Linktree/Beacons URLs via Apify Cheerio Scraper.
        Returns {url: {emails, social_links, personal_website}}
        Falls back to direct scraping if Apify unavailable.
        """
        if not urls:
            return {}

        if self.apify_token:
            try:
                return self._apify_scrape_pages(urls, page_type='aggregator')
            except Exception as e:
                logger.warning("Apify aggregator scrape failed, falling back: %s", e)

        # Direct fallback
        results = {}
        for url in urls:
            results[url] = self._direct_scrape_aggregator(url)
        return results

    def crawl_websites_batch(self, websites: List[str]) -> Dict[str, Dict]:
        """
        Crawl personal websites via Apify Cheerio Scraper.

        Hits the root page plus all _CRAWL_SUBPAGES (26 paths) and uses
        glob patterns to also follow any dynamically-routed pages whose URLs
        contain contact/about/team/leadership keywords.

        Returns {domain: {emails, social_links}}
        Falls back to direct scraping if Apify unavailable.
        """
        if not websites:
            return {}

        # Build start URLs: root + all subpages
        start_urls = []
        domain_map: Dict[str, str] = {}  # url → domain key
        for site in websites:
            site = site.rstrip('/')
            domain = ApolloEnrichment.extract_domain(site)
            for path in self._CRAWL_SUBPAGES:
                full_url = site + path
                start_urls.append(full_url)
                domain_map[full_url] = domain

        if self.apify_token:
            try:
                return self._apify_crawl_websites(start_urls, domain_map, websites)
            except Exception as e:
                logger.warning("Apify website crawl failed, falling back: %s", e)

        # Direct fallback (shorter list — direct HTTP is rate-limited anyway)
        results: Dict[str, Dict] = {}
        for site in websites:
            domain = ApolloEnrichment.extract_domain(site)
            combined: Dict = {'emails': [], 'social_links': {}}
            for path in ['', '/contact', '/about', '/about-us', '/team']:
                page_data = self._direct_scrape_page(site.rstrip('/') + path)
                combined['emails'].extend(page_data.get('emails', []))
                combined['social_links'].update(page_data.get('social_links', {}))
            combined['emails'] = list(set(combined['emails']))
            results[domain] = combined
        return results

    # ------------------------------------------------------------------
    # Apify-backed scrapers
    # ------------------------------------------------------------------

    def _apify_scrape_pages(self, urls: List[str], page_type: str) -> Dict[str, Dict]:
        """Generic Apify Cheerio Scraper call.

        For website crawls, also extracts mailto: links from the page so we
        don't have to rely solely on regex matching of raw text.
        """
        from apify_client import ApifyClient
        apify = ApifyClient(self.apify_token)

        PAGE_FUNCTION = r"""
async function pageFunction(context) {
    const { $, request } = context;
    const mailtos = [];
    $('a[href^="mailto:"]').each(function() {
        const h = $(this).attr('href');
        if (h) mailtos.push(h.replace('mailto:', '').split('?')[0].trim().toLowerCase());
    });
    const text = $('body').text();
    const links = [];
    $('a[href]').each(function() { links.push($(this).attr('href')); });
    return {
        url: request.url,
        text: text.substring(0, 8000),
        links: links.slice(0, 200),
        mailtos: mailtos
    };
}
"""
        run_input = {
            'startUrls':          [{'url': u} for u in urls],
            'maxCrawlPages':      len(urls),
            'maxConcurrency':     10,
            'requestTimeoutSecs': 30,
            'pageFunction':       PAGE_FUNCTION,
        }
        run = apify.actor("apify~cheerio-scraper").call(run_input=run_input, timeout_secs=120)
        items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())

        results: Dict[str, Dict] = {}
        for item in items:
            url     = item.get('url', '')
            text    = item.get('text', '')
            links   = item.get('links', [])
            mailtos = item.get('mailtos', [])
            parsed  = self._parse_page_content(text, links)
            # Merge explicit mailto links — these are more reliable than regex
            if mailtos:
                all_emails = list(dict.fromkeys(mailtos + parsed.get('emails', [])))
                parsed['emails'] = [
                    e for e in all_emails
                    if not any(b in e for b in self._BLOCKED_EMAIL_PATTERNS)
                ]
            results[url] = parsed
        return results

    def _apify_crawl_websites(self, start_urls: List[str],
                              domain_map: Dict[str, str],
                              original_sites: List[str] = None) -> Dict[str, Dict]:
        """
        Crawl website pages via Apify and group results by domain.

        Uses glob patterns so Apify also follows any page whose URL contains
        contact/about/team/leadership keywords (catches dynamically-routed sites).

        Email selection priority:
          1. Domain-matching email (hi@theirdomain.com)
          2. Personal email provider (gmail, yahoo, etc.)
          3. Any other email found
        """
        from apify_client import ApifyClient
        apify = ApifyClient(self.apify_token)

        # Build glob patterns for each site so Apify follows relevant sub-pages
        globs = []
        if original_sites:
            for site in original_sites:
                site = site.rstrip('/')
                domain = ApolloEnrichment.extract_domain(site)
                for kw in self._CRAWL_GLOB_KEYWORDS:
                    globs.append({'glob': f'https://{domain}/**/*{kw}*'})
                    globs.append({'glob': f'https://www.{domain}/**/*{kw}*'})

        PAGE_FUNCTION = r"""
async function pageFunction(context) {
    const { $, request } = context;
    const mailtos = [];
    $('a[href^="mailto:"]').each(function() {
        const h = $(this).attr('href');
        if (h) mailtos.push(h.replace('mailto:', '').split('?')[0].trim().toLowerCase());
    });
    const text = $('body').text();
    const links = [];
    $('a[href]').each(function() { links.push($(this).attr('href')); });
    return {
        url: request.url,
        text: text.substring(0, 8000),
        links: links.slice(0, 200),
        mailtos: mailtos
    };
}
"""
        run_input = {
            'startUrls':          [{'url': u} for u in start_urls],
            'maxCrawlPages':      len(start_urls) + (len(globs) // 2 if globs else 0),
            'maxConcurrency':     6,
            'requestTimeoutSecs': 30,
            'pageFunction':       PAGE_FUNCTION,
        }
        if globs:
            run_input['globs'] = globs[:200]  # Apify cap

        run = apify.actor("apify~cheerio-scraper").call(run_input=run_input, timeout_secs=300)
        items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())

        # Aggregate raw emails by domain
        by_domain: Dict[str, Dict] = {}
        for item in items:
            url     = item.get('url', '')
            text    = item.get('text', '')
            links   = item.get('links', [])
            mailtos = item.get('mailtos', [])
            domain = domain_map.get(url) or ApolloEnrichment.extract_domain(url)
            if domain not in by_domain:
                by_domain[domain] = {'all_emails': [], 'social_links': {}}

            parsed = self._parse_page_content(text, links)

            # Collect all candidate emails (mailtos first — more reliable)
            candidate_emails = list(dict.fromkeys(
                [e for e in mailtos if '@' in e] + parsed.get('emails', [])
            ))
            candidate_emails = [
                e for e in candidate_emails
                if not any(b in e for b in self._BLOCKED_EMAIL_PATTERNS)
            ]
            by_domain[domain]['all_emails'].extend(candidate_emails)
            by_domain[domain]['social_links'].update(parsed.get('social_links', {}))

        # For each domain, pick the best email using priority logic
        results: Dict[str, Dict] = {}
        for domain, data in by_domain.items():
            best = self._select_best_email(
                list(dict.fromkeys(data['all_emails'])), domain
            )
            results[domain] = {
                'emails':      [best] if best else [],
                'social_links': data['social_links'],
            }

        return results

    # ------------------------------------------------------------------
    # Google Bridge  (Facebook / Meetup → find organizer via Google)
    # ------------------------------------------------------------------

    def google_bridge_enrich(self, profiles: List[Dict], job_id: str) -> List[Dict]:
        """
        For Facebook Groups (and future Meetup) profiles that have NO email,
        website, or LinkedIn, run Google searches to surface the organizer's
        contact information.

        Two Google queries per group:
          1. "<group_name>" website contact email
          2. "<group_name>" organizer OR founder OR leader site:linkedin.com

        Parses organic results for:
          - LinkedIn /in/ profiles  → linkedin_url
          - Instagram / Twitter / YouTube URLs  → respective social fields
          - Non-social URLs  → personal_website
          - Email addresses in snippets  → email
          - Organizer name from result title / snippet / LinkedIn slug

        Only processes profiles where platform is 'facebook_group' (or 'meetup')
        AND profile currently has no email AND no website AND no linkedin_url.
        """
        if not APIFY_API_TOKEN:
            logger.warning("APIFY_API_TOKEN not set — skipping Google bridge")
            return profiles

        # Filter to profiles that actually need it
        needs_bridge = [
            p for p in profiles
            if p.get('platform') in ('facebook_group', 'meetup')
            and not p.get('email')
            and not p.get('personal_website')
            and not p.get('linkedin_url')
        ]

        if not needs_bridge:
            logger.info("No profiles need bridging — skipping")
            return profiles

        logger.info("Google bridge running for %d profiles", len(needs_bridge))

        # Build query list
        queries: List[Dict] = []
        for p in needs_bridge:
            group_name = p.get('group_name') or p.get('community_name') or p.get('creator_name') or ''
            if not group_name:
                continue
            gn = group_name.replace('"', '')
            queries.append({
                'term': f'"{gn}" website contact email',
                '_profile_ref': id(p),
            })
            queries.append({
                'term': f'"{gn}" organizer OR founder OR leader site:linkedin.com',
                '_profile_ref': id(p),
            })

        if not queries:
            return profiles

        # Build profile_id → profile mapping
        id_to_profile = {id(p): p for p in needs_bridge}

        # Run in batches of 20 queries
        BATCH_SIZE = 20
        all_results: List[Dict] = []
        for batch_start in range(0, len(queries), BATCH_SIZE):
            batch = queries[batch_start: batch_start + BATCH_SIZE]
            search_queries = [{'term': q['term']} for q in batch]
            actor_input = {
                'queries': search_queries,
                'resultsPerPage': 5,
                'maxPagesPerQuery': 1,
                'outputAsJSON': True,
                'saveHtml': False,
                'saveMarkdown': False,
            }
            try:
                from apify_client import ApifyClient
                apify = ApifyClient(self.apify_token)
                run = apify.actor("apify~google-search-scraper").call(
                    run_input=actor_input, timeout_secs=120
                )
                raw = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
                all_results.extend(raw or [])
            except Exception as e:
                logger.error("Google bridge Apify error (batch %d): %s", batch_start, e)

        if not all_results:
            return profiles

        # Map query term → list of organic results
        query_to_results: Dict[str, List[Dict]] = {}
        for item in all_results:
            term = (item.get('searchQuery') or {}).get('term', '')
            organics = item.get('organicResults') or []
            query_to_results.setdefault(term, []).extend(organics)

        # Parse results and merge back into profiles
        for q_entry in queries:
            term = q_entry['term']
            profile = id_to_profile.get(q_entry['_profile_ref'])
            if not profile:
                continue

            organics = query_to_results.get(term, [])
            for result in organics:
                title    = result.get('title', '')
                snippet  = result.get('description', '') or result.get('snippet', '')
                url      = result.get('url', '') or result.get('link', '')

                if not url:
                    continue

                parsed = urlparse(url)
                netloc = parsed.netloc.lower()

                # LinkedIn profile
                if 'linkedin.com/in/' in url and not profile.get('linkedin_url'):
                    profile['linkedin_url'] = url.split('?')[0]

                # Instagram
                elif re.search(r'instagram\.com/', url, re.I) and not profile.get('instagram_url'):
                    profile['instagram_url'] = url.split('?')[0]

                # Twitter / X
                elif re.search(r'(twitter|x)\.com/', url, re.I) and not profile.get('twitter_url'):
                    profile['twitter_url'] = url.split('?')[0]

                # YouTube
                elif re.search(r'youtube\.com/(channel|c/|@)', url, re.I) and not profile.get('youtube_url'):
                    profile['youtube_url'] = url.split('?')[0]

                # Non-social personal website
                elif (
                    url.startswith('http')
                    and not any(s in netloc for s in self._SOCIAL_HOSTS)
                    and not any(agg in netloc for agg in ('linktree', 'beacons', 'linktr'))
                    and not profile.get('personal_website')
                ):
                    profile['personal_website'] = url.split('?')[0]

                # Email from snippet
                if not profile.get('email'):
                    snippet_emails = self._extract_emails(snippet + ' ' + title)
                    if snippet_emails:
                        profile['email'] = snippet_emails[0]

                # Organizer / creator name
                if not profile.get('creator_name') or profile.get('creator_name') == profile.get('group_name'):
                    name = self._extract_name_from_text(title, snippet, url)
                    if name:
                        profile['creator_name'] = name

        logger.info("Google bridge enriched %d profiles", len(needs_bridge))
        return profiles

    # ------------------------------------------------------------------
    # YouTube About Pages
    # ------------------------------------------------------------------

    @staticmethod
    def _youtube_about_url(url: str) -> str:
        """Normalise any YouTube channel URL to its /about page URL."""
        url = url.rstrip('/')
        for suffix in ('/videos', '/shorts', '/community', '/playlists', '/about'):
            if url.endswith(suffix):
                url = url[:-len(suffix)]
        return url + '/about'

    def scrape_youtube_about_pages_batch(self, profiles: List[Dict]) -> List[Dict]:
        """
        Scrape YouTube /about pages for profiles that have a youtube_url.

        The /about page exposes the creator's email (click-to-reveal in real
        browsers, but plain-text in the initial HTML Apify captures), their
        website link, and any social links in the channel description.

        Updates profiles in-place with: email, linkedin_url, personal_website,
        linktree_url, creator_name (fallback from channel name in page title).

        Runs for ALL profiles with a youtube_url regardless of whether they
        already have an email (YT is cheap + fast and may give a better email).
        """
        if not self.apify_token:
            logger.warning("APIFY_API_TOKEN not set — skipping YouTube about")
            return profiles

        yt_profiles = [p for p in profiles if p.get('youtube_url')]
        if not yt_profiles:
            logger.info("No profiles with youtube_url — skipping")
            return profiles

        logger.info("Scraping %d YouTube About pages", len(yt_profiles))

        # Build URL → profile index map
        url_to_idxs: Dict[str, List[int]] = {}
        about_urls: List[str] = []
        for i, p in enumerate(profiles):
            if not p.get('youtube_url'):
                continue
            about_url = self._youtube_about_url(p['youtube_url'])
            about_urls.append(about_url)
            url_to_idxs.setdefault(about_url, []).append(i)

        PAGE_FUNCTION = r"""
async function pageFunction(context) {
    const { $, request } = context;
    const mailtos = [];
    $('a[href^="mailto:"]').each(function() {
        const h = $(this).attr('href');
        if (h) mailtos.push(h.replace('mailto:', '').split('?')[0].trim().toLowerCase());
    });
    const text = $('body').text();
    const links = [];
    $('a[href]').each(function() { links.push($(this).attr('href')); });
    // Channel name from <title>
    const title = $('title').text().trim();
    return {
        url: request.url,
        text: text.substring(0, 10000),
        links: links.slice(0, 300),
        mailtos: mailtos,
        pageTitle: title
    };
}
"""
        try:
            from apify_client import ApifyClient
            apify = ApifyClient(self.apify_token)
            run_input = {
                'startUrls':          [{'url': u} for u in about_urls],
                'maxCrawlPages':      len(about_urls),
                'maxConcurrency':     30,
                'requestTimeoutSecs': 90,
                'pageFunction':       PAGE_FUNCTION,
            }
            run   = apify.actor("apify~cheerio-scraper").call(run_input=run_input, timeout_secs=300)
            items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
        except Exception as e:
            logger.error("YouTube about Apify error: %s", e)
            return profiles

        _LINK_AGG_HOSTS = set(self.LINK_AGGREGATORS)

        for item in items:
            page_url   = item.get('url', '')
            text       = item.get('text', '')
            links      = item.get('links', []) or []
            mailtos    = item.get('mailtos', []) or []
            page_title = item.get('pageTitle', '')

            idxs = url_to_idxs.get(page_url, [])
            if not idxs:
                continue

            # Collect emails — mailtos first, then regex
            candidate_emails = list(dict.fromkeys(
                [e for e in mailtos if '@' in e] + self._extract_emails(text)
            ))
            candidate_emails = [
                e for e in candidate_emails
                if not self._is_blocked_email(e)
            ]

            parsed = self._parse_page_content(text, links)

            for i in idxs:
                p = profiles[i]

                # Email (only fill if missing)
                if candidate_emails and not p.get('email'):
                    p['email'] = candidate_emails[0]

                # LinkedIn
                for href in links:
                    if href and re.search(r'linkedin\.com/in/', href, re.I):
                        if not p.get('linkedin_url'):
                            p['linkedin_url'] = href.split('?')[0]

                # Link aggregators (feeds Pass 2)
                for href in links:
                    if href and any(agg in href.lower() for agg in _LINK_AGG_HOSTS):
                        if not p.get('linktree_url'):
                            p['linktree_url'] = href.split('?')[0]

                # Personal website
                if parsed.get('personal_website') and not p.get('personal_website'):
                    p['personal_website'] = parsed['personal_website']

                # Creator name fallback from page title ("Channel Name - YouTube")
                if page_title and not p.get('creator_name'):
                    name = page_title.split(' - ')[0].strip()
                    if name and name.lower() not in ('youtube', ''):
                        p['creator_name'] = name

        logger.info("Completed scraping %d YouTube pages", len(yt_profiles))
        return profiles

    # ------------------------------------------------------------------
    # Instagram Bios
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_instagram_handle(url: str) -> Optional[str]:
        """Extract the username from an Instagram profile URL."""
        m = re.search(r'instagram\.com/([a-zA-Z0-9._]+)/?', url or '')
        if m:
            handle = m.group(1).lower()
            if handle not in ('p', 'reel', 'stories', 'explore', 'tv'):
                return handle
        return None

    def scrape_instagram_bios_batch(self, profiles: List[Dict]) -> List[Dict]:
        """
        Use apify~instagram-profile-scraper to enrich profiles that have an
        instagram_url.

        Returns structured data (biography, externalUrl, externalUrls[],
        followersCount, fullName) — much more reliable than Cheerio for IG.

        Updates profiles in-place with: email (from bio), personal_website,
        linktree_url, instagram_followers.
        """
        if not self.apify_token:
            logger.warning("APIFY_API_TOKEN not set — skipping IG bio")
            return profiles

        ig_profiles = [p for p in profiles if p.get('instagram_url')]
        if not ig_profiles:
            logger.info("No profiles with instagram_url — skipping")
            return profiles

        logger.info("Scraping %d Instagram bios", len(ig_profiles))

        # Extract handles; build handle → profile indices map
        handle_to_idxs: Dict[str, List[int]] = {}
        for i, p in enumerate(profiles):
            if not p.get('instagram_url'):
                continue
            handle = self._extract_instagram_handle(p['instagram_url'])
            if handle:
                handle_to_idxs.setdefault(handle, []).append(i)

        if not handle_to_idxs:
            return profiles

        _LINK_AGG_HOSTS = set(self.LINK_AGGREGATORS)

        try:
            from apify_client import ApifyClient
            apify = ApifyClient(self.apify_token)
            run_input = {
                'usernames':    list(handle_to_idxs.keys()),
                'resultsLimit': 1,
            }
            run   = apify.actor("apify~instagram-profile-scraper").call(
                run_input=run_input, timeout_secs=300
            )
            items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
        except Exception as e:
            logger.error("IG bio Apify error: %s", e)
            return profiles

        for item in items:
            username = (item.get('username') or '').lower()
            idxs = handle_to_idxs.get(username, [])
            if not idxs:
                continue

            bio          = item.get('biography', '') or ''
            external_url = item.get('externalUrl', '') or ''
            extra_urls   = [u.get('url', '') for u in (item.get('externalUrls') or [])]
            followers    = item.get('followersCount') or 0
            full_name    = item.get('fullName', '') or ''

            # Collect all outbound URLs from the profile
            all_external = [u for u in [external_url] + extra_urls if u]

            # Extract email from bio text
            bio_emails = self._extract_emails(bio)

            for i in idxs:
                p = profiles[i]

                if bio_emails and not p.get('email'):
                    p['email'] = bio_emails[0]

                # Follower count
                if followers and not p.get('instagram_followers'):
                    p['instagram_followers'] = followers

                # Creator name from IG full_name
                if full_name and not p.get('creator_name'):
                    p['creator_name'] = full_name

                for ext_url in all_external:
                    if not ext_url:
                        continue
                    # Link aggregator (feeds Pass 2)
                    if any(agg in ext_url.lower() for agg in _LINK_AGG_HOSTS):
                        if not p.get('linktree_url'):
                            p['linktree_url'] = ext_url.split('?')[0]
                    # LinkedIn
                    elif re.search(r'linkedin\.com/in/', ext_url, re.I):
                        if not p.get('linkedin_url'):
                            p['linkedin_url'] = ext_url.split('?')[0]
                    # Personal website (non-social)
                    elif ext_url.startswith('http'):
                        netloc = urlparse(ext_url).netloc.lower()
                        if not any(s in netloc for s in self._SOCIAL_HOSTS):
                            if not p.get('personal_website'):
                                p['personal_website'] = ext_url.split('?')[0]

        logger.info("Completed scraping %d Instagram profiles", len(ig_profiles))
        return profiles

    # ------------------------------------------------------------------
    # Twitter / X Bios
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_twitter_handle(url: str) -> Optional[str]:
        """Extract the handle from a Twitter / X profile URL."""
        m = re.search(r'(?:twitter|x)\.com/([a-zA-Z0-9_]+)/?', url or '')
        if m:
            handle = m.group(1).lower()
            if handle not in ('home', 'search', 'explore', 'notifications',
                              'messages', 'i', 'intent', 'hashtag', 'share'):
                return handle
        return None

    def scrape_twitter_bios_batch(self, profiles: List[Dict]) -> List[Dict]:
        """
        Use apidojo~twitter-user-scraper to enrich profiles that have a
        twitter_url.

        Returns structured data (description, entities.url.urls,
        entities.description.urls, followersCount, name).

        Updates profiles in-place with: email (from bio), personal_website,
        linkedin_url, linktree_url, twitter_followers.
        """
        if not self.apify_token:
            logger.warning("APIFY_API_TOKEN not set — skipping Twitter bio")
            return profiles

        tw_profiles = [p for p in profiles if p.get('twitter_url')]
        if not tw_profiles:
            logger.info("No profiles with twitter_url — skipping")
            return profiles

        logger.info("Scraping %d Twitter bios", len(tw_profiles))

        handle_to_idxs: Dict[str, List[int]] = {}
        for i, p in enumerate(profiles):
            if not p.get('twitter_url'):
                continue
            handle = self._extract_twitter_handle(p['twitter_url'])
            if handle:
                handle_to_idxs.setdefault(handle, []).append(i)

        if not handle_to_idxs:
            return profiles

        _LINK_AGG_HOSTS = set(self.LINK_AGGREGATORS)

        try:
            from apify_client import ApifyClient
            apify = ApifyClient(self.apify_token)
            run_input = {
                'handles': list(handle_to_idxs.keys()),
            }
            run   = apify.actor("apidojo~twitter-user-scraper").call(
                run_input=run_input, timeout_secs=300
            )
            items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
        except Exception as e:
            logger.error("Twitter bio Apify error: %s", e)
            return profiles

        for item in items:
            # Actor may return handle under 'username', 'screen_name', or nested
            username = (
                item.get('username')
                or item.get('screen_name')
                or (item.get('legacy') or {}).get('screen_name', '')
            ).lower().lstrip('@')

            idxs = handle_to_idxs.get(username, [])
            if not idxs:
                continue

            description = item.get('description', '') or ''
            followers   = (
                item.get('followersCount')
                or item.get('followers_count')
                or (item.get('legacy') or {}).get('followers_count', 0)
                or 0
            )
            full_name   = (
                item.get('name')
                or (item.get('legacy') or {}).get('name', '')
                or ''
            )

            # Collect entity URLs (Twitter's t.co expansion)
            entity_urls: List[str] = []
            entities = item.get('entities') or {}
            for url_entry in (entities.get('url') or {}).get('urls', []):
                expanded = url_entry.get('expanded_url', '')
                if expanded:
                    entity_urls.append(expanded)
            for url_entry in (entities.get('description') or {}).get('urls', []):
                expanded = url_entry.get('expanded_url', '')
                if expanded:
                    entity_urls.append(expanded)

            # Also check legacy.entities if present
            legacy = item.get('legacy') or {}
            leg_entities = legacy.get('entities') or {}
            for url_entry in (leg_entities.get('url') or {}).get('urls', []):
                expanded = url_entry.get('expanded_url', '')
                if expanded:
                    entity_urls.append(expanded)

            bio_emails = self._extract_emails(description)

            for i in idxs:
                p = profiles[i]

                if bio_emails and not p.get('email'):
                    p['email'] = bio_emails[0]

                if followers and not p.get('twitter_followers'):
                    p['twitter_followers'] = followers

                if full_name and not p.get('creator_name'):
                    p['creator_name'] = full_name

                for ext_url in entity_urls:
                    if not ext_url:
                        continue
                    if any(agg in ext_url.lower() for agg in _LINK_AGG_HOSTS):
                        if not p.get('linktree_url'):
                            p['linktree_url'] = ext_url.split('?')[0]
                    elif re.search(r'linkedin\.com/in/', ext_url, re.I):
                        if not p.get('linkedin_url'):
                            p['linkedin_url'] = ext_url.split('?')[0]
                    elif ext_url.startswith('http'):
                        netloc = urlparse(ext_url).netloc.lower()
                        if not any(s in netloc for s in self._SOCIAL_HOSTS):
                            if not p.get('personal_website'):
                                p['personal_website'] = ext_url.split('?')[0]

        logger.info("Completed scraping %d Twitter profiles", len(tw_profiles))
        return profiles

    # ------------------------------------------------------------------
    # RSS Feed Parsing
    # ------------------------------------------------------------------

    def parse_rss_feeds_batch(self, profiles: List[Dict]) -> List[Dict]:
        """
        Scrape RSS / podcast feed URLs for profiles that have an rss_url.

        Podcast feeds (generated by Buzzsprout, Anchor, Podbean, etc.) embed
        rich contact metadata in iTunes namespace tags:
          <itunes:owner><itunes:email>   – most reliable contact email
          <itunes:author>                – human-readable author name
          <itunes:name>                  – owner name in <itunes:owner> block
          <link>                         – canonical website URL

        Falls back to regex email extraction from the raw feed XML text.

        Updates profiles in-place with: email, creator_name, personal_website.
        """
        if not self.apify_token:
            logger.warning("APIFY_API_TOKEN not set — skipping RSS")
            return profiles

        rss_profiles = [p for p in profiles if p.get('rss_url')]
        if not rss_profiles:
            logger.info("No profiles with rss_url — skipping")
            return profiles

        logger.info("Parsing %d RSS feeds", len(rss_profiles))

        url_to_idxs: Dict[str, List[int]] = {}
        rss_urls: List[str] = []
        for i, p in enumerate(profiles):
            rss = p.get('rss_url', '')
            if rss:
                rss_urls.append(rss)
                url_to_idxs.setdefault(rss, []).append(i)

        # RSS pageFunction — parse iTunes namespace tags from raw XML
        PAGE_FUNCTION = r"""
async function pageFunction(context) {
    const { $, request } = context;
    // iTunes email lives inside <itunes:owner><itunes:email>...</itunes:email>
    const itunesEmail = $('itunes\\:owner itunes\\:email').first().text().trim()
        || $('itunes\\:email').first().text().trim();
    const itunesAuthor = $('itunes\\:author').first().text().trim()
        || $('itunes\\:owner itunes\\:name').first().text().trim();
    // Podcast / channel website link
    const channelLink = $('channel > link').first().text().trim()
        || $('channel > link').first().attr('href') || '';
    // Raw text for fallback email regex
    const rawText = $('body').length ? $('body').text() : $.html();
    return {
        url: request.url,
        itunesEmail: itunesEmail,
        itunesAuthor: itunesAuthor,
        channelLink: channelLink,
        text: rawText.substring(0, 6000)
    };
}
"""
        try:
            from apify_client import ApifyClient
            apify = ApifyClient(self.apify_token)
            run_input = {
                'startUrls':          [{'url': u} for u in rss_urls],
                'maxCrawlPages':      len(rss_urls),
                'maxConcurrency':     20,
                'requestTimeoutSecs': 30,
                'pageFunction':       PAGE_FUNCTION,
            }
            run   = apify.actor("apify~cheerio-scraper").call(run_input=run_input, timeout_secs=180)
            items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
        except Exception as e:
            logger.error("RSS Apify error: %s", e)
            return profiles

        for item in items:
            page_url      = item.get('url', '')
            itunes_email  = item.get('itunesEmail', '') or ''
            itunes_author = item.get('itunesAuthor', '') or ''
            channel_link  = item.get('channelLink', '') or ''
            text          = item.get('text', '') or ''

            idxs = url_to_idxs.get(page_url, [])
            if not idxs:
                continue

            # Fallback: regex email from raw feed text
            fallback_emails = self._extract_emails(text) if not itunes_email else []

            for i in idxs:
                p = profiles[i]

                # Email — iTunes tag is most authoritative
                if itunes_email and not p.get('email'):
                    if not self._is_blocked_email(itunes_email):
                        p['email'] = itunes_email.lower().strip()
                elif fallback_emails and not p.get('email'):
                    p['email'] = fallback_emails[0]

                # Creator name from iTunes author
                if itunes_author and not p.get('creator_name'):
                    p['creator_name'] = itunes_author

                # Website from channel <link>
                if channel_link and channel_link.startswith('http'):
                    netloc = urlparse(channel_link).netloc.lower()
                    if not any(s in netloc for s in self._SOCIAL_HOSTS):
                        if not p.get('personal_website'):
                            p['personal_website'] = channel_link.split('?')[0]

        logger.info("Completed parsing %d RSS feeds", len(rss_profiles))
        return profiles

    # ------------------------------------------------------------------
    # Google Contact Search  (last-resort, all platforms)
    # ------------------------------------------------------------------

    def google_contact_search(self, profiles: List[Dict], job_id: str) -> List[Dict]:
        """
        Last-resort Google search for profiles that made it through the full
        social-scraping pipeline with still no email, website, or LinkedIn.

        Generates 3 queries per profile using the creator name and/or their
        Patreon/platform slug:
          1. "<name>" email website
          2. site:linkedin.com "<name>"
          3. "<name>" "contact" OR "reach me" OR "get in touch"

        Uses the same `apify~google-search-scraper` + URL parsing logic as
        google_bridge_enrich.
        """
        if not self.apify_token:
            logger.warning("APIFY_API_TOKEN not set — skipping contact search")
            return profiles

        needs_search = [
            p for p in profiles
            if not p.get('email')
            and not p.get('personal_website')
            and not p.get('linkedin_url')
        ]

        if not needs_search:
            logger.info("No profiles need contact search — skipping")
            return profiles

        logger.info("Contact search running for %d profiles", len(needs_search))

        queries: List[Dict] = []
        for p in needs_search:
            name = p.get('creator_name', '').strip()
            # Derive a slug from the primary platform URL as fallback
            slug = ''
            for url_field in ('url', 'instagram_url', 'youtube_url', 'twitter_url'):
                raw_url = p.get(url_field, '')
                if raw_url:
                    slug = raw_url.rstrip('/').split('/')[-1].split('?')[0]
                    break
            search_name = name or slug
            if not search_name:
                continue

            sn = search_name.replace('"', '')
            queries.append({'term': f'"{sn}" email website contact',       '_profile_ref': id(p)})
            queries.append({'term': f'site:linkedin.com "{sn}"',            '_profile_ref': id(p)})
            queries.append({'term': f'"{sn}" "contact" OR "reach me" OR "get in touch"', '_profile_ref': id(p)})

        if not queries:
            return profiles

        id_to_profile = {id(p): p for p in needs_search}

        BATCH_SIZE = 20
        all_results: List[Dict] = []
        for batch_start in range(0, len(queries), BATCH_SIZE):
            batch = queries[batch_start: batch_start + BATCH_SIZE]
            actor_input = {
                'queries':        '\n'.join(q['term'] for q in batch),
                'resultsPerPage': 5,
                'maxPagesPerQuery': 1,
                'outputAsJSON':   True,
                'saveHtml':       False,
                'saveMarkdown':   False,
            }
            try:
                from apify_client import ApifyClient
                apify = ApifyClient(self.apify_token)
                run = apify.actor("apify~google-search-scraper").call(
                    run_input=actor_input, timeout_secs=120
                )
                raw = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
                all_results.extend(raw or [])
            except Exception as e:
                logger.error("Contact search Apify error (batch %d): %s", batch_start, e)

        if not all_results:
            return profiles

        query_to_results: Dict[str, List[Dict]] = {}
        for item in all_results:
            term     = (item.get('searchQuery') or {}).get('term', '')
            organics = item.get('organicResults') or []
            query_to_results.setdefault(term, []).extend(organics)

        for q_entry in queries:
            term    = q_entry['term']
            profile = id_to_profile.get(q_entry['_profile_ref'])
            if not profile:
                continue

            for result in query_to_results.get(term, []):
                title   = result.get('title', '')
                snippet = result.get('description', '') or result.get('snippet', '')
                url     = result.get('url', '') or result.get('link', '')
                if not url:
                    continue

                netloc = urlparse(url).netloc.lower()

                if 'linkedin.com/in/' in url and not profile.get('linkedin_url'):
                    profile['linkedin_url'] = url.split('?')[0]
                elif re.search(r'instagram\.com/', url, re.I) and not profile.get('instagram_url'):
                    profile['instagram_url'] = url.split('?')[0]
                elif re.search(r'(twitter|x)\.com/', url, re.I) and not profile.get('twitter_url'):
                    profile['twitter_url'] = url.split('?')[0]
                elif re.search(r'youtube\.com/(channel|c/|@)', url, re.I) and not profile.get('youtube_url'):
                    profile['youtube_url'] = url.split('?')[0]
                elif (
                    url.startswith('http')
                    and not any(s in netloc for s in self._SOCIAL_HOSTS)
                    and not any(agg in netloc for agg in ('linktree', 'beacons', 'linktr'))
                    and not profile.get('personal_website')
                ):
                    profile['personal_website'] = url.split('?')[0]

                if not profile.get('email'):
                    emails = self._extract_emails(snippet + ' ' + title)
                    if emails:
                        profile['email'] = emails[0]

                if not profile.get('creator_name'):
                    name = self._extract_name_from_text(title, snippet, url)
                    if name:
                        profile['creator_name'] = name

        logger.info("Contact search enriched %d profiles", len(needs_search))
        return profiles

    # ------------------------------------------------------------------
    # Direct HTTP fallbacks
    # ------------------------------------------------------------------

    def _direct_scrape_page(self, url: str) -> Dict:
        """Direct HTTP GET + BeautifulSoup parse."""
        result: Dict = {'emails': [], 'social_links': {}, 'personal_website': None}
        try:
            resp = self._session.get(url, timeout=10, allow_redirects=True)
            if not resp.ok:
                return result
            soup = BeautifulSoup(resp.text, 'html.parser')

            # mailto links (most reliable)
            for a in soup.find_all('a', href=re.compile(r'^mailto:', re.I)):
                email = a['href'].replace('mailto:', '').split('?')[0].strip().lower()
                if email and '@' in email:
                    result['emails'].append(email)

            text = soup.get_text(' ', strip=True)
            result['emails'].extend(self._extract_emails(text))

            # Social links from <a> tags
            for a in soup.find_all('a', href=True):
                href = a['href']
                for key, pattern in self.SOCIAL_PATTERNS.items():
                    if re.search(pattern, href, re.I):
                        result['social_links'][key] = href.split('?')[0]

            result['emails'] = list(set(result['emails']))
        except Exception as e:
            logger.error("Direct scrape error %s: %s", url, e)
        return result

    def _direct_scrape_aggregator(self, url: str) -> Dict:
        """Direct HTTP scrape of a Linktree/Beacons page."""
        result: Dict = {'emails': [], 'social_links': {}, 'personal_website': None}
        try:
            resp = self._session.get(url, timeout=10, allow_redirects=True)
            if not resp.ok:
                return result
            soup = BeautifulSoup(resp.text, 'html.parser')
            text = soup.get_text(' ', strip=True)
            result['emails'] = self._extract_emails(text)

            for a in soup.find_all('a', href=True):
                href = a['href']
                for key, pattern in self.SOCIAL_PATTERNS.items():
                    if re.search(pattern, href, re.I):
                        result['social_links'][key] = href.split('?')[0]
                # Personal website: external, non-social link
                if href.startswith('http'):
                    host = urlparse(href).netloc.replace('www.', '').lower()
                    if not any(s in host for s in self._SOCIAL_HOSTS):
                        if not result['personal_website']:
                            result['personal_website'] = href

        except Exception as e:
            logger.error("Direct aggregator scrape error %s: %s", url, e)
        return result

    # ------------------------------------------------------------------
    # Single-URL convenience (used for inline per-profile enrichment)
    # ------------------------------------------------------------------

    def build_graph(self, url: str, name: str = None) -> Dict:
        """Build social graph from a single URL (aggregator or website)."""
        is_agg = any(agg in url.lower() for agg in self.LINK_AGGREGATORS)
        if is_agg:
            data = self._direct_scrape_aggregator(url)
            result = {
                'emails':           data.get('emails', []),
                'social_links':     data.get('social_links', {}),
                'personal_website': data.get('personal_website'),
                'linktree_url':     url,
            }
            # If aggregator links to a personal website, crawl that too
            if result['personal_website']:
                site_data = self._direct_scrape_page(result['personal_website'])
                result['emails'].extend(site_data.get('emails', []))
                result['social_links'].update(site_data.get('social_links', {}))
        else:
            data = self._direct_scrape_page(url)
            result = {
                'emails':           data.get('emails', []),
                'social_links':     data.get('social_links', {}),
                'personal_website': None,
                'linktree_url':     None,
            }
        result['emails'] = list(set(result['emails']))
        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _parse_page_content(self, text: str, links: List[str]) -> Dict:
        """Parse Apify Cheerio page result into emails + social links."""
        emails = self._extract_emails(text)
        social_links: Dict[str, str] = {}
        personal_website = None

        for href in links:
            if not href:
                continue
            for key, pattern in self.SOCIAL_PATTERNS.items():
                if re.search(pattern, href, re.I):
                    social_links[key] = href.split('?')[0]
            # Detect personal website from links
            if href.startswith('http'):
                host = urlparse(href).netloc.replace('www.', '').lower()
                if not any(s in host for s in self._SOCIAL_HOSTS):
                    if not personal_website:
                        personal_website = href

        return {'emails': emails, 'social_links': social_links,
                'personal_website': personal_website}

    def _extract_emails(self, text: str) -> List[str]:
        """Extract both standard and obfuscated email addresses from text."""
        emails = []

        # Standard regex
        for match in self._EMAIL_RE.finditer(text):
            email = match.group(0).lower()
            if not self._is_blocked_email(email):
                emails.append(email)

        # Obfuscated patterns  (name [at] domain [dot] com)
        for pattern in self._OBFUSCATED_PATTERNS:
            for match in pattern.finditer(text):
                try:
                    email = f"{match.group(1)}@{match.group(2)}.{match.group(3)}".lower().replace(' ', '')
                    if '@' in email and not self._is_blocked_email(email):
                        emails.append(email)
                except Exception:
                    pass

        return list(set(emails))

    def _is_blocked_email(self, email: str) -> bool:
        return any(p in email.lower() for p in self._BLOCKED_EMAIL_PATTERNS)

    def _select_best_email(self, emails: List[str], site_domain: str) -> Optional[str]:
        """
        Pick the highest-priority email from a list of candidates.

        Priority order (matches colleague's logic):
          1. Email whose domain matches the site domain  →  e.g. hi@theircreatorwebsite.com
          2. Personal email provider (gmail, yahoo, etc.)
          3. Any non-blocked email

        Returns None if the list is empty.
        """
        if not emails:
            return None

        site_domain = site_domain.lower().lstrip('www.')

        tier1 = [e for e in emails if e.split('@')[-1].lstrip('www.') == site_domain]
        if tier1:
            return tier1[0]

        tier2 = [e for e in emails if e.split('@')[-1].lstrip('www.') in self._PERSONAL_EMAIL_DOMAINS]
        if tier2:
            return tier2[0]

        return emails[0]

    def _extract_name_from_text(self, title: str, snippet: str, url: str) -> Optional[str]:
        """
        Attempt to extract an organizer/creator name from a Google result.

        Tries (in order):
          1. Title regex patterns (e.g. "John Smith – Facebook Group Admin")
          2. Snippet context patterns (e.g. "managed by John Smith")
          3. LinkedIn URL slug → humanise slug
        """
        for text in (title, snippet):
            if not text:
                continue
            m = self._NAME_FROM_TITLE_RE.match(text.strip())
            if m:
                return m.group(1).strip()
            for pat in self._NAME_CONTEXT_RES:
                m = pat.search(text)
                if m:
                    return m.group(1).strip()

        # LinkedIn slug fallback
        m = self._LINKEDIN_SLUG_RE.search(url or '')
        if m:
            slug = m.group(1)
            # Convert "john-doe-123abc" → "John Doe"
            parts = [p.capitalize() for p in slug.split('-') if p and not p.isdigit() and len(p) > 1]
            if len(parts) >= 2:
                return ' '.join(parts[:3])

        return None


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _extract_facebook_group_url(raw_url: str) -> str:
    """Extract a clean, canonical Facebook group URL."""
    try:
        url = raw_url.split('?')[0]
        if '/groups/' in url:
            parts = url.split('/groups/')
            if len(parts) > 1:
                group_id = parts[1].split('/')[0]
                return f"https://www.facebook.com/groups/{group_id}"
        return url
    except Exception:
        return raw_url


def _extract_posts_per_month(text: str):
    """
    Best-effort extraction of posts per month from a Google snippet.

    Handles phrases like:
      '10 posts a month', '3 posts per week', '2 posts per day', '50 posts this month',
      '5 posts a week', '1 post per day'

    Returns an integer estimate of posts per month, or None if not found.
    """
    if not text:
        return None

    text_lower = text.lower()

    # Try "X posts a/per month" or "X posts this month"
    m = re.search(r'(\d+(?:[\.,]\d+)?)\s+posts?\s+(?:a|per|this)\s+month', text_lower)
    if m:
        try:
            return int(float(m.group(1).replace(',', '.')))
        except ValueError:
            pass

    # Try "X posts a/per week" → multiply by ~4.3
    m = re.search(r'(\d+(?:[\.,]\d+)?)\s+posts?\s+(?:a|per)\s+week', text_lower)
    if m:
        try:
            return int(float(m.group(1).replace(',', '.')) * 4.3)
        except ValueError:
            pass

    # Try "X posts a/per day" → multiply by ~30
    m = re.search(r'(\d+(?:[\.,]\d+)?)\s+posts?\s+(?:a|per)\s+day', text_lower)
    if m:
        try:
            return int(float(m.group(1).replace(',', '.')) * 30)
        except ValueError:
            pass

    return None


def _extract_member_count(text: str) -> int:
    """Parse member count from strings like '5.2K members', '1,234 members'."""
    patterns = [
        r'([\d,]+\.?\d*[KkMm]?)\s+members?',
        r'([\d,]+\.?\d*[KkMm]?)\s+people',
        r'([\d,]+)\s+in\s+group',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.I)
        if m:
            raw = m.group(1).replace(',', '').upper()
            try:
                if 'K' in raw:
                    return int(float(raw.replace('K', '')) * 1_000)
                if 'M' in raw:
                    return int(float(raw.replace('M', '')) * 1_000_000)
                return int(raw)
            except ValueError:
                pass
    return 0


# ============================================================================
# LEADS FINDER ENRICHMENT (Apify code_crafter~leads-finder)
# ============================================================================

def enrich_with_leads_finder(profiles: List[Dict], job_id: str) -> List[Dict]:
    """
    Use Apify code_crafter~leads-finder to find emails by domain for profiles
    that still lack an email address.

    Matches results back to profiles by domain.
    """
    if not APIFY_API_TOKEN:
        logger.warning("APIFY_API_TOKEN not set — skipping leads finder")
        return profiles

    apollo = ApolloEnrichment('')  # static methods only

    # Collect unique enrichable domains from profiles with no email
    domain_to_profile_idxs: Dict[str, List[int]] = {}
    for i, p in enumerate(profiles):
        if p.get('email'):
            continue
        website = p.get('personal_website') or p.get('url', '')
        if not website:
            continue
        domain = apollo.extract_domain(website)
        if domain and apollo.is_enrichable_domain(domain):
            domain_to_profile_idxs.setdefault(domain, []).append(i)

    domains = list(domain_to_profile_idxs.keys())
    if not domains:
        logger.info("No enrichable domains — skipping leads finder")
        return profiles

    logger.info("Leads finder looking up %d domains", len(domains))

    try:
        from apify_client import ApifyClient
        apify = ApifyClient(APIFY_API_TOKEN)

        run_input = {
            'company_domain': domains,
            'email_status':   ['validated'],
            'fetch_count':    min(len(domains) * 5, 200),
        }
        run = apify.actor("code_crafter~leads-finder").call(
            run_input=run_input, timeout_secs=120
        )
        items = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
        logger.info("Leads finder got %d results", len(items))

        # Index results by domain
        best_by_domain: Dict[str, Dict] = {}
        for item in items:
            domain = (item.get('company_domain') or item.get('domain') or '').lower()
            if domain and domain not in best_by_domain:
                best_by_domain[domain] = item

        # Apply to profiles
        matched = 0
        for domain, idxs in domain_to_profile_idxs.items():
            match = best_by_domain.get(domain.lower())
            if not match:
                continue
            email = match.get('email') or match.get('work_email') or match.get('personal_email', '')
            if not email:
                continue
            for i in idxs:
                p = profiles[i]
                p['email'] = email
                if match.get('phone') and not p.get('phone'):
                    p['phone'] = match['phone']
                if match.get('linkedin_url') and not p.get('linkedin_url'):
                    p['linkedin_url'] = match['linkedin_url']
                if not p.get('creator_name') and match.get('first_name') and match.get('last_name'):
                    p['creator_name'] = f"{match['first_name']} {match['last_name']}"
                matched += 1

        logger.info("Leads finder matched emails for %d profiles", matched)

    except Exception as e:
        logger.error("Leads finder error: %s", e)
        import traceback
        traceback.print_exc()

    return profiles


# ============================================================================
# UNIFIED ENRICHMENT PIPELINE
# ============================================================================

def enrich_profiles_full_pipeline(profiles: List[Dict], job_id: str,
                                  platform: str) -> List[Dict]:
    """
    Full enrichment pipeline (platform-agnostic).

    Expects each profile dict to have at minimum:
      url           – primary URL (Patreon page / FB group URL)
      creator_name  – person name (may be empty)
      instagram_url, youtube_url, twitter_url, facebook_url, tiktok_url
      personal_website (may be None)
      rss_url       – optional podcast RSS feed URL

    Execution plan (mirrors colleague's 12-step parallel architecture):

      ── GROUP 1 (parallel Apify calls) ─────────────────────────────────
      A. Google Bridge        – facebook_group/meetup only; surfaces organizer
      B. RSS Feed Parsing     – profiles with rss_url (podcasts)
      C. Link Aggregators P1  – Linktree/Beacons URLs already in profile fields

      ── GROUP 2 (parallel Apify calls) ─────────────────────────────────
      D. YouTube About Pages  – profiles with youtube_url
      E. Instagram Bios       – profiles with instagram_url
      F. Twitter/X Bios       – profiles with twitter_url

      ── SEQUENTIAL ──────────────────────────────────────────────────────
      G. Link Aggregators P2  – NEW aggregator URLs surfaced in Group 2
      H. Google Contact Search– last resort: no email + no website + no LinkedIn
      I. Website Crawl        – 26 subpages + glob patterns + email priority logic
      J. Apollo.io            – person match for profiles still missing email
      K. Leads Finder         – domain-based Apify lookup
      L. MillionVerifier      – validate all discovered emails
    """
    if not profiles:
        return profiles

    from concurrent.futures import ThreadPoolExecutor, as_completed as futures_as_completed

    logger.info("Starting full enrichment pipeline for %d %s profiles", len(profiles), platform)

    sgb    = SocialGraphBuilder(apify_token=APIFY_API_TOKEN)
    apollo = ApolloEnrichment(APOLLO_API_KEY) if APOLLO_API_KEY else None
    mv     = MillionVerifierClient(MILLIONVERIFIER_API_KEY) if MILLIONVERIFIER_API_KEY else None

    _LINK_AGG_HOSTS = set(SocialGraphBuilder.LINK_AGGREGATORS)

    def _is_aggregator(url: str) -> bool:
        return bool(url) and any(agg in url.lower() for agg in _LINK_AGG_HOSTS)

    # Track Pass 1 URLs so Pass 2 only processes genuinely new ones
    agg_url_to_idx_p1: Dict[str, List[int]] = {}

    # ------------------------------------------------------------------ #
    # GROUP 1 (parallel): Google Bridge | RSS | Link Agg Pass 1          #
    # ------------------------------------------------------------------ #
    logger.info("Group 1 (parallel): Google Bridge | RSS | Link Agg Pass 1")

    def _g1_google_bridge() -> str:
        if platform in ('facebook_group', 'meetup'):
            sgb.google_bridge_enrich(profiles, job_id)
        return 'Google Bridge'

    def _g1_rss() -> str:
        sgb.parse_rss_feeds_batch(profiles)
        return 'RSS'

    def _g1_link_agg_p1() -> str:
        agg_urls: List[str] = []
        local_map: Dict[str, List[int]] = {}
        for i, p in enumerate(profiles):
            for field in ('personal_website', 'instagram_url', 'youtube_url',
                          'twitter_url', 'url'):
                val = p.get(field, '')
                if val and _is_aggregator(val):
                    agg_urls.append(val)
                    local_map.setdefault(val, []).append(i)
        if agg_urls:
            logger.info("Link Agg P1: %d URLs", len(set(agg_urls)))
            results = sgb.scrape_link_aggregators_batch(list(set(agg_urls)))
            for url, data in results.items():
                for i in local_map.get(url, []):
                    p = profiles[i]
                    if data.get('emails') and not p.get('email'):
                        p['email'] = data['emails'][0]
                    for key, val in data.get('social_links', {}).items():
                        if val and not p.get(key):
                            p[key] = val
                    if data.get('personal_website') and not p.get('personal_website'):
                        p['personal_website'] = data['personal_website']
        # Expose to outer scope for Pass 2 dedup (single write after Apify returns)
        agg_url_to_idx_p1.update(local_map)
        return 'Link Agg P1'

    with ThreadPoolExecutor(max_workers=3) as pool:
        g1_tasks = {
            pool.submit(_g1_google_bridge): 'Google Bridge',
            pool.submit(_g1_rss):           'RSS',
            pool.submit(_g1_link_agg_p1):   'Link Agg P1',
        }
        for fut in futures_as_completed(g1_tasks):
            label = g1_tasks[fut]
            try:
                fut.result()
                logger.info("%s done", label)
            except Exception as e:
                logger.error("%s error: %s", label, e)

    # ------------------------------------------------------------------ #
    # GROUP 2 (parallel): YouTube | Instagram | Twitter bios             #
    # ------------------------------------------------------------------ #
    logger.info("Group 2 (parallel): YouTube About Pages | Instagram | Twitter bios")

    def _g2_youtube() -> str:
        sgb.scrape_youtube_about_pages_batch(profiles)
        return 'YouTube'

    def _g2_instagram() -> str:
        sgb.scrape_instagram_bios_batch(profiles)
        return 'Instagram'

    def _g2_twitter() -> str:
        sgb.scrape_twitter_bios_batch(profiles)
        return 'Twitter'

    with ThreadPoolExecutor(max_workers=3) as pool:
        g2_tasks = {
            pool.submit(_g2_youtube):   'YouTube',
            pool.submit(_g2_instagram): 'Instagram',
            pool.submit(_g2_twitter):   'Twitter',
        }
        for fut in futures_as_completed(g2_tasks):
            label = g2_tasks[fut]
            try:
                fut.result()
                logger.info("%s done", label)
            except Exception as e:
                logger.error("%s error: %s", label, e)

    # ------------------------------------------------------------------ #
    # Link Aggregators Pass 2                                             #
    # Scrape NEW aggregator URLs surfaced during Group 2.                #
    # Only for profiles that still have no email.                        #
    # ------------------------------------------------------------------ #
    logger.info("Link Aggregators Pass 2")

    agg_urls_p2: List[str] = []
    agg_url_to_idx_p2: Dict[str, List[int]] = {}
    for i, p in enumerate(profiles):
        if p.get('email'):
            continue
        lt = p.get('linktree_url', '')
        if lt and _is_aggregator(lt) and lt not in agg_url_to_idx_p1:
            agg_urls_p2.append(lt)
            agg_url_to_idx_p2.setdefault(lt, []).append(i)

    if agg_urls_p2:
        logger.info("Pass 2: %d new aggregator URLs", len(set(agg_urls_p2)))
        agg_results_p2 = sgb.scrape_link_aggregators_batch(list(set(agg_urls_p2)))
        for url, data in agg_results_p2.items():
            for i in agg_url_to_idx_p2.get(url, []):
                p = profiles[i]
                if data.get('emails') and not p.get('email'):
                    p['email'] = data['emails'][0]
                for key, val in data.get('social_links', {}).items():
                    if val and not p.get(key):
                        p[key] = val
                if data.get('personal_website') and not p.get('personal_website'):
                    p['personal_website'] = data['personal_website']

    # ------------------------------------------------------------------ #
    # Google Contact Search  (last resort)                               #
    # ------------------------------------------------------------------ #
    logger.info("Google Contact Search")
    profiles = sgb.google_contact_search(profiles, job_id)

    # ------------------------------------------------------------------ #
    # Website Crawl  (26 subpages + glob patterns + email priority)      #
    # ------------------------------------------------------------------ #
    logger.info("Website Crawl")

    websites_to_crawl: List[str] = []
    website_to_idx: Dict[str, List[int]] = {}
    for i, p in enumerate(profiles):
        site = p.get('personal_website', '')
        if (site and not _is_aggregator(site)
                and ApolloEnrichment.is_enrichable_domain(
                    ApolloEnrichment.extract_domain(site))):
            websites_to_crawl.append(site)
            website_to_idx.setdefault(site, []).append(i)

    if websites_to_crawl:
        logger.info("Crawling %d websites", len(set(websites_to_crawl)))
        website_results = sgb.crawl_websites_batch(list(set(websites_to_crawl)))
        for domain, data in website_results.items():
            for site, idxs in website_to_idx.items():
                if ApolloEnrichment.extract_domain(site) == domain:
                    for i in idxs:
                        p = profiles[i]
                        if data.get('emails') and not p.get('email'):
                            p['email'] = data['emails'][0]
                        for key, val in data.get('social_links', {}).items():
                            if val and not p.get(key):
                                p[key] = val

    # ------------------------------------------------------------------ #
    # Apollo.io                                                           #
    # ------------------------------------------------------------------ #
    logger.info("Apollo.io enrichment")

    if apollo:
        apollo_hits = 0
        seen_hashes: set = set()

        for p in profiles:
            if p.get('email'):
                continue  # already have email

            name     = p.get('creator_name', '').strip()
            site     = p.get('personal_website', '')
            domain   = ApolloEnrichment.extract_domain(site) if site else ''
            org      = p.get('group_name', '')
            linkedin = p.get('linkedin_url', '')

            if not (name or domain):
                continue
            if domain and not ApolloEnrichment.is_enrichable_domain(domain):
                continue

            input_hash = ApolloEnrichment.make_input_hash(
                name=name, domain=domain, org=org, linkedin=linkedin
            )
            if input_hash in seen_hashes:
                continue
            seen_hashes.add(input_hash)

            result = apollo.person_match(
                name=name or None,
                domain=domain or None,
                org_name=org or None,
                linkedin_url=linkedin or None,
            )

            if result:
                if result.get('email'):
                    p['email'] = result['email']
                    apollo_hits += 1
                    logger.info("Apollo found email for %s", name or domain)
                if result.get('first_name') and not p.get('creator_name'):
                    p['creator_name'] = (
                        f"{result['first_name']} {result.get('last_name', '')}".strip()
                    )
                if result.get('phone') and not p.get('phone'):
                    p['phone'] = result['phone']
                if result.get('linkedin') and not p.get('linkedin_url'):
                    p['linkedin_url'] = result['linkedin']
                if result.get('twitter') and not p.get('twitter_url'):
                    p['twitter_url'] = result['twitter']

            time.sleep(0.3)

        logger.info("Apollo found %d emails", apollo_hits)
    else:
        logger.info("Apollo skipped (no API key)")

    # ------------------------------------------------------------------ #
    # Leads Finder                                                        #
    # ------------------------------------------------------------------ #
    logger.info("Leads Finder")
    profiles = enrich_with_leads_finder(profiles, job_id)

    # ------------------------------------------------------------------ #
    # MillionVerifier  (validate all discovered emails)                   #
    # ------------------------------------------------------------------ #
    logger.info("MillionVerifier email validation")

    if mv:
        email_items = [
            {'email': p['email'], 'idx': i}
            for i, p in enumerate(profiles)
            if p.get('email')
        ]
        if email_items:
            logger.info("Validating %d emails", len(email_items))
            validation_results = mv.verify_batch(email_items)
            for item in email_items:
                status = validation_results.get(item['email'], 'unknown')
                profiles[item['idx']]['email_validation_status'] = status
            valid_count = sum(1 for s in validation_results.values() if s == 'valid')
            logger.info("%d/%d emails valid", valid_count, len(email_items))
        else:
            logger.info("No emails to validate")
    else:
        logger.info("MillionVerifier skipped (no API key)")

    logger.info("Enrichment pipeline complete for %d profiles", len(profiles))
    return profiles


# ============================================================================
# STANDARDIZE: PATREON → HUBSPOT
# ============================================================================

def standardize_patreon_profiles(profiles: List[Dict]) -> List[Dict]:
    """
    Map enriched Patreon profiles to the exact HubSpot contact property names
    required for batch-create. Unknown / None / empty values are dropped.
    """
    standardized = []

    for i, profile in enumerate(profiles):
        try:
            props = {
                # ── Universal social links ──────────────────────────────
                'email':                profile.get('email'),
                'instagram_handle':     profile.get('instagram_url'),
                'youtube_profile_link': profile.get('youtube_url'),
                'tiktok_handle':        profile.get('tiktok_url'),
                'website':              profile.get('personal_website'),
                'twitterhandle':        profile.get('twitter_url'),
                'facebook_profile_link': profile.get('facebook_url'),

                # ── Patreon-specific ───────────────────────────────────
                'patreon_link':         profile.get('url'),
                'patreon_title':        profile.get('creator_name') or profile.get('name'),
                'total_patrons':        (profile.get('patron_count')
                                         or profile.get('total_members')),
                'paid_patrons':         (profile.get('paid_members')
                                         or profile.get('paid_patrons')),
                'patreon_description':  (profile.get('about')
                                         or profile.get('description')),

                # ── Metadata / channel tracking ───────────────────────
                'flagship_social_platform': 'patreon',
                'channel':                  'Outbound',
                'channel_host_prospected':  'Phyllo',
                'funnel':                   'Community',
            }

            # Drop None / empty-string / zero values
            props = {k: v for k, v in props.items()
                     if v is not None and v != '' and v != 0}

            standardized.append(props)

        except Exception as e:
            logger.error("Patreon profile #%d error: %s", i+1, e)
            continue

    logger.info("%d Patreon profiles ready for HubSpot", len(standardized))
    return standardized


# ============================================================================
# STANDARDIZE: FACEBOOK GROUPS → HUBSPOT
# ============================================================================

def standardize_facebook_profiles(profiles: List[Dict]) -> List[Dict]:
    """
    Map enriched Facebook Group profiles to the exact HubSpot contact property
    names required for batch-create. Unknown / None / empty values are dropped.
    """
    standardized = []

    for i, profile in enumerate(profiles):
        try:
            props = {
                # ── Universal social links ──────────────────────────────
                'email':                profile.get('email'),
                'instagram_handle':     profile.get('instagram_url'),
                'youtube_profile_link': profile.get('youtube_url'),
                'tiktok_handle':        profile.get('tiktok_url'),
                'website':              profile.get('personal_website'),
                'twitterhandle':        profile.get('twitter_url'),
                'facebook_profile_link': profile.get('facebook_url'),

                # ── Facebook Groups-specific ──────────────────────────
                'facebook_group_link':        profile.get('group_url'),
                'facebook_group_name':        profile.get('group_name'),
                'facebook_group_size':        profile.get('member_count') or None,
                'facebook_group_description': profile.get('description'),

                # ── Metadata / channel tracking ───────────────────────
                'flagship_social_platform': 'facebook_group',
                'channel':                  'Outbound',
                'channel_host_prospected':  'Phyllo',
                'funnel':                   'Community',
            }

            # Drop None / empty-string / zero values
            props = {k: v for k, v in props.items()
                     if v is not None and v != '' and v != 0}

            standardized.append(props)

        except Exception as e:
            logger.error("Facebook profile #%d error: %s", i+1, e)
            continue

    logger.info("%d Facebook profiles ready for HubSpot", len(standardized))
    return standardized


# ============================================================================
# BDR Round-Robin Helper
# ============================================================================

def assign_bdr_round_robin(profiles: List[Dict], bdr_names: List[str]) -> List[Dict]:
    """
    Assign bdr_ (HubSpot owner ID) to each profile in round-robin order.

    Only names present in BDR_OWNER_IDS are used; unrecognised names are silently
    skipped so a bad frontend value can never crash the pipeline.

    Args:
        profiles:  List of profile dicts (modified in-place AND returned).
        bdr_names: Ordered list of BDR display names selected by the user.

    Returns:
        The same profiles list with 'bdr_' set on every item.
    """
    owner_ids = [BDR_OWNER_IDS[n] for n in bdr_names if n in BDR_OWNER_IDS]
    if not owner_ids:
        logger.warning("No valid BDR names supplied – skipping round-robin assignment")
        return profiles
    for i, profile in enumerate(profiles):
        profile['bdr_'] = owner_ids[i % len(owner_ids)]
        # Mark for BDR review — cleared later for auto_enroll contacts by send_to_hubspot
        profile['lead_list_fit'] = 'Not_reviewed'
    logger.info("Assigned %d BDR(s) round-robin across %d profiles", len(owner_ids), len(profiles))
    return profiles


# ============================================================================
# REDIS JOB STATUS TRACKER
# ============================================================================

def update_discovery_job_status(job_id, status, **kwargs):
    """Update discovery job status in Redis (24-hour TTL)."""
    try:
        job_key  = f'discovery_job:{job_id}'
        job_data = redis_client.get(job_key)
        job_data = json.loads(job_data) if job_data else {'job_id': job_id}

        job_data['status']     = status
        job_data['updated_at'] = datetime.now().isoformat()
        job_data.update(kwargs)

        redis_client.setex(job_key, 86400, json.dumps(job_data))
        logger.info("Job %s → %s", job_id, status)
    except Exception as e:
        logger.error("Failed to update job status: %s", e)
