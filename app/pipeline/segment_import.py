"""
Pipeline Stage: SEGMENT IMPORT — Import contacts from HubSpot segments for rewarm runs.

Instagram: Pull contacts from HubSpot list, convert to canonical profile format.
"""
import logging
import re
from typing import Dict, List, Any
from urllib.parse import urlparse

from app.pipeline.base import StageAdapter, StageResult

logger = logging.getLogger('pipeline.segment_import')


def _extract_ig_username(raw_handle: str) -> str | None:
    """Extract Instagram username from a URL or raw handle string.

    Handles formats like:
        https://www.instagram.com/username/
        https://instagram.com/username
        http://instagram.com/username/?hl=en
        @username
        username
    Returns None if the input is empty or not parseable.
    """
    if not raw_handle or not raw_handle.strip():
        return None

    raw_handle = raw_handle.strip()

    # If it looks like a URL, parse out the path
    if 'instagram.com' in raw_handle:
        parsed = urlparse(raw_handle)
        path = parsed.path.strip('/')
        # Path might be "username" or "username/more/stuff"
        parts = path.split('/')
        if parts and parts[0]:
            return parts[0]
        return None

    # Strip leading @
    if raw_handle.startswith('@'):
        raw_handle = raw_handle[1:]

    return raw_handle if raw_handle else None


class SegmentImportInstagram(StageAdapter):
    """Import Instagram contacts from HubSpot list segments."""

    platform = 'instagram'
    stage = 'segment_import'
    description = 'Import contacts from HubSpot list for rewarm'
    apis = ['HubSpot']

    def run(self, profiles: List[Dict[str, Any]], run: Any) -> StageResult:
        import app.services.hubspot as hubspot_svc

        filters = run.filters or {}
        list_ids = filters.get('hubspot_list_ids', [])

        if not list_ids:
            logger.warning("No hubspot_list_ids in run filters — nothing to import")
            return StageResult(profiles=[], processed=0, skipped=0)

        imported = []
        skipped = 0
        errors = []

        for list_id in list_ids:
            try:
                contacts = hubspot_svc.hubspot_import_segment(list_id, 'instagram')
                logger.info("List %s: fetched %d contacts", list_id, len(contacts))
            except Exception as e:
                msg = f"Error fetching list {list_id}: {e}"
                logger.error(msg)
                errors.append(msg)
                continue

            for contact in contacts:
                raw_handle = contact.get('instagram_handle', '')
                username = _extract_ig_username(raw_handle)

                if not username:
                    skipped += 1
                    logger.debug(
                        "Skipped contact %s %s — no IG handle",
                        contact.get('firstname', ''),
                        contact.get('lastname', ''),
                    )
                    continue

                first = contact.get('firstname', '') or ''
                last = contact.get('lastname', '') or ''
                full_name = f"{first} {last}".strip()

                followers_raw = contact.get('instagram_followers', 0)
                try:
                    follower_count = int(followers_raw) if followers_raw else 0
                except (ValueError, TypeError):
                    follower_count = 0

                profile = {
                    'first_and_last_name': full_name,
                    'flagship_social_platform_handle': username,
                    'instagram_handle': f'https://www.instagram.com/{username}/',
                    'instagram_bio': '',
                    'instagram_followers': follower_count,
                    'average_engagement': 0,
                    'email': contact.get('email'),
                    'phone': None,
                    'tiktok_handle': None,
                    'youtube_profile_link': None,
                    'facebook_profile_link': None,
                    'patreon_link': None,
                    'pinterest_profile_link': None,
                    'city': contact.get('city', ''),
                    'state': contact.get('state', ''),
                    'country': contact.get('country', ''),
                    'flagship_social_platform': 'instagram',
                    'channel': 'Outbound',
                    'channel_host_prospected': 'HubSpot Rewarm',
                    'funnel': 'Creator',
                    'enrichment_status': 'pending',
                }

                imported.append(profile)
                run.increment_stage_progress('segment_import', 'completed')
                logger.info(
                    "Imported @%s (%s, %s followers)",
                    username, full_name, f"{follower_count:,}",
                )

        if skipped:
            logger.info("Skipped %d contacts without IG handles", skipped)

        return StageResult(
            profiles=imported,
            processed=len(imported) + skipped,
            skipped=skipped,
            errors=errors,
        )


# Adapter registry
ADAPTERS: Dict[str, type] = {
    'instagram': SegmentImportInstagram,
}
