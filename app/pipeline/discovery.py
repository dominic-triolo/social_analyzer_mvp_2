"""
Pipeline Stage 1: DISCOVERY — Find profiles from platform APIs.

Each adapter calls its platform's API and returns raw profiles.
Filtering, enrichment, and CRM sync happen in later stages.
"""
import logging
from typing import Dict, List, Any

from app.config import INSIGHTIQ_CLIENT_ID, INSIGHTIQ_SECRET, APIFY_API_TOKEN
from app.pipeline.base import StageAdapter, StageResult
from app.services.apify import (
    _extract_facebook_group_url,
    _extract_posts_per_month,
    _extract_member_count,
)

logger = logging.getLogger('pipeline.discovery')


# ── Adapters ──────────────────────────────────────────────────────────────────

class InstagramDiscovery(StageAdapter):
    """Discover Instagram profiles via InsightIQ API."""
    platform = 'instagram'
    stage = 'discovery'
    description = 'Search by followers, interests, and lookalike'
    apis = ['InsightIQ']

    def estimate_cost(self, count: int) -> float:
        return count * 0.02

    def run(self, profiles, run) -> StageResult:
        from app.services.insightiq import InsightIQDiscovery

        if not INSIGHTIQ_CLIENT_ID or not INSIGHTIQ_SECRET:
            raise ValueError("INSIGHTIQ_CLIENT_ID and INSIGHTIQ_SECRET must be set")

        filters = run.filters or {}
        lookalike_type = filters.get('lookalike_type')
        lookalike_username = filters.get('lookalike_username', '').strip()

        if lookalike_type and lookalike_type not in ('creator', 'audience'):
            raise ValueError("lookalike_type must be 'creator' or 'audience'")
        if lookalike_type and not lookalike_username:
            raise ValueError("lookalike_username required when lookalike_type is set")

        logger.info("Starting with filters: %s", filters)

        client = InsightIQDiscovery(INSIGHTIQ_CLIENT_ID, INSIGHTIQ_SECRET)
        found = client.search_profiles(platform='instagram', user_filters=filters)

        logger.info("Found %d profiles", len(found))

        return StageResult(
            profiles=found,
            processed=len(found),
            cost=len(found) * 0.02,
        )


class PatreonDiscovery(StageAdapter):
    """Discover Patreon creators via Apify scraper."""
    platform = 'patreon'
    stage = 'discovery'
    description = 'Keyword search + location filter'
    apis = ['Apify']

    def estimate_cost(self, count: int) -> float:
        return count * 0.01

    def run(self, profiles, run) -> StageResult:
        from apify_client import ApifyClient

        if not APIFY_API_TOKEN:
            raise ValueError("APIFY_API_TOKEN must be set")

        filters = run.filters or {}
        search_keywords = filters.get('search_keywords', [])
        max_results = filters.get('max_results', 100)
        location = (filters.get('location') or 'United States').strip()

        if not search_keywords:
            raise ValueError("search_keywords required for Patreon discovery")

        search_queries = [f"{kw} {location}" for kw in search_keywords] if location else list(search_keywords)
        logger.info("queries=%s, max=%d", search_queries, max_results)

        apify = ApifyClient(APIFY_API_TOKEN)
        run_result = apify.actor("mJiXU9PT4eLHuY0pi").call(run_input={
            "searchQueries": search_queries,
            "maxRequestsPerCrawl": max_results,
            "proxyConfiguration": {"useApifyProxy": True, "apifyProxyGroups": ["RESIDENTIAL"]},
            "maxConcurrency": 1,
            "maxRequestRetries": 5,
            "requestHandlerTimeoutSecs": 180,
        })
        all_items = list(apify.dataset(run_result["defaultDatasetId"]).iterate_items())
        logger.info("Apify returned %d items", len(all_items))

        # Normalize social URL fields
        for p in all_items:
            p.setdefault('instagram_url', p.get('instagram'))
            p.setdefault('youtube_url', p.get('youtube'))
            p.setdefault('twitter_url', p.get('twitter'))
            p.setdefault('facebook_url', p.get('facebook'))
            p.setdefault('tiktok_url', p.get('tiktok'))
            p.setdefault('twitch_url', p.get('twitch'))

        return StageResult(
            profiles=all_items,
            processed=len(all_items),
            cost=len(all_items) * 0.01,
        )


class FacebookDiscovery(StageAdapter):
    """Discover Facebook Groups via Google Search Scraper."""
    platform = 'facebook'
    stage = 'discovery'
    description = 'Google Search scraping for groups'
    apis = ['Apify']

    def estimate_cost(self, count: int) -> float:
        return count * 0.01

    def run(self, profiles, run) -> StageResult:
        from apify_client import ApifyClient

        if not APIFY_API_TOKEN:
            raise ValueError("APIFY_API_TOKEN must be set")

        filters = run.filters or {}
        keywords = filters.get('keywords', [])
        max_results = filters.get('max_results', 100)
        visibility = filters.get('visibility', 'all')

        if not keywords:
            raise ValueError("keywords required for Facebook Groups discovery")

        vis_suffix = ''
        if visibility == 'public':
            vis_suffix = ' "public group"'
        elif visibility == 'private':
            vis_suffix = ' "private group"'

        google_queries = []
        for kw in keywords:
            google_queries.append(f'site:facebook.com/groups "{kw}"{vis_suffix}')
            google_queries.append(f'site:facebook.com/groups {kw} community{vis_suffix}')
            google_queries.append(f'site:facebook.com/groups {kw} group{vis_suffix}')
        google_queries = google_queries[:15]

        logger.info("%d queries, max=%d", len(google_queries), max_results)

        apify = ApifyClient(APIFY_API_TOKEN)
        run_result = apify.actor("apify~google-search-scraper").call(run_input={
            'queries': '\n'.join(google_queries),
            'maxPagesPerQuery': 5,
            'resultsPerPage': 20,
            'countryCode': 'us',
            'languageCode': 'en',
            'mobileResults': False,
        })
        items = list(apify.dataset(run_result["defaultDatasetId"]).iterate_items())

        found = []
        seen_urls = set()
        for item in items:
            if len(found) >= max_results:
                break
            for result in item.get('organicResults', []):
                if len(found) >= max_results:
                    break
                url = result.get('url', '')
                if 'facebook.com/groups/' not in url:
                    continue
                group_url = _extract_facebook_group_url(url)
                if group_url in seen_urls:
                    continue
                seen_urls.add(group_url)

                title = result.get('title', '')
                snippet = result.get('description', '')

                found.append({
                    'group_name': title.replace(' | Facebook', '').replace(' - Facebook', '').strip(),
                    'group_url': group_url,
                    'description': snippet[:2000],
                    'member_count': _extract_member_count(f"{title} {snippet}"),
                    'posts_per_month': _extract_posts_per_month(snippet),
                    'url': group_url,
                    'creator_name': '',
                    'instagram_url': None,
                    'youtube_url': None,
                    'twitter_url': None,
                    'facebook_url': group_url,
                    'tiktok_url': None,
                    'personal_website': None,
                    '_search_title': title,
                    '_search_snippet': snippet,
                })

        logger.info("Found %d groups", len(found))

        return StageResult(
            profiles=found,
            processed=len(found),
            cost=len(found) * 0.01,
        )


# ── Adapter registry ─────────────────────────────────────────────────────────

ADAPTERS: Dict[str, type] = {
    'instagram': InstagramDiscovery,
    'patreon': PatreonDiscovery,
    'facebook': FacebookDiscovery,
}
