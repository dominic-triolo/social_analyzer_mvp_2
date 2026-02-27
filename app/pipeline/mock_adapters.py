"""
Mock pipeline adapters — realistic fake data for local testing.

Activated with MOCK_PIPELINE=1 env var. Every external API call is replaced
with canned responses that produce a realistic end-to-end flow. Useful for
UI development, demo runs, and verifying pipeline orchestration.
"""
import logging
import time
import random
import uuid
from typing import Dict, List, Any

from app.pipeline.base import StageAdapter, StageResult

logger = logging.getLogger('pipeline.mock')


# ── Fake profile data ────────────────────────────────────────────────────────

MOCK_CREATORS = [
    {'username': 'wanderlust_maya', 'name': 'Maya Chen', 'bio': 'Travel photographer | 40+ countries | Host of Wanderlust Retreats | maya@email.com', 'followers': 87000, 'category': 'Travel'},
    {'username': 'fit_with_jorge', 'name': 'Jorge Ramirez', 'bio': 'Fitness coach & retreat leader | Transform your body on the road | DM for coaching', 'followers': 134000, 'category': 'Fitness'},
    {'username': 'chef_nomad_li', 'name': 'Li Wei', 'bio': 'Culinary adventures around the world | Cooking classes in 15 countries | Newsletter below', 'followers': 62000, 'category': 'Food'},
    {'username': 'yoga_with_priya', 'name': 'Priya Sharma', 'bio': 'Yoga teacher | Bali retreat host | 200hr RYT | Community of 10k+ yogis', 'followers': 95000, 'category': 'Wellness'},
    {'username': 'adventure_alex', 'name': 'Alex Thompson', 'bio': 'Hiking | Climbing | Group expeditions | Former guide at REI Adventures', 'followers': 48000, 'category': 'Travel'},
    {'username': 'sarah_creates', 'name': 'Sarah Kim', 'bio': 'Art + travel + community | Monthly creative retreats | Patreon for behind the scenes', 'followers': 72000, 'category': 'Art'},
    {'username': 'digital_nomad_dan', 'name': 'Dan Morris', 'bio': 'Building businesses from anywhere | Nomad meetups in SEA | Podcast: Remote Life', 'followers': 156000, 'category': 'Business'},
    {'username': 'wild_kitchen_nina', 'name': 'Nina Petrova', 'bio': 'Foraging + cooking in the wild | Nature retreats | Published in Bon Appetit', 'followers': 41000, 'category': 'Food'},
    {'username': 'surf_coach_kai', 'name': 'Kai Nakamura', 'bio': 'Pro surfer turned coach | Surf camps worldwide | Stoked community 🤙', 'followers': 110000, 'category': 'Sports'},
    {'username': 'photo_walks_emma', 'name': 'Emma Rodriguez', 'bio': 'Street photography | Group photo walks in 20+ cities | Online workshops', 'followers': 53000, 'category': 'Art'},
    {'username': 'mindful_mike', 'name': 'Mike Johnson', 'bio': 'Meditation teacher | Silent retreats | Bestselling author of "Still Moving"', 'followers': 89000, 'category': 'Wellness'},
    {'username': 'backpack_budget_ria', 'name': 'Ria Santos', 'bio': 'Budget travel queen 👑 | $30/day adventures | Free travel guides', 'followers': 203000, 'category': 'Travel'},
    {'username': 'music_nomad_jamal', 'name': 'Jamal Williams', 'bio': 'Producer | DJ | Music festivals worldwide | Collab: jamal@beat.co', 'followers': 167000, 'category': 'Entertainment'},
    {'username': 'eco_travel_zoe', 'name': 'Zoe Anderson', 'bio': 'Sustainable travel advocate | Eco-lodge reviews | Conservation trips', 'followers': 76000, 'category': 'Travel'},
    {'username': 'climb_together_sam', 'name': 'Sam Park', 'bio': 'Rock climbing instructor | Group trips to Yosemite & Patagonia | Certified AMGA', 'followers': 58000, 'category': 'Sports'},
]


def _simulate_delay(min_s=0.3, max_s=0.8):
    """Small delay to simulate API latency."""
    time.sleep(random.uniform(min_s, max_s))


# ── Stage 1: Discovery ───────────────────────────────────────────────────────

class MockInstagramDiscovery(StageAdapter):
    platform = 'instagram'
    stage = 'discovery'
    description = '[MOCK] Simulated profile discovery'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.02

    def run(self, profiles, run) -> StageResult:
        max_results = run.filters.get('max_results', 10)
        count = min(max_results, len(MOCK_CREATORS))
        selected = random.sample(MOCK_CREATORS, count)

        # Unique suffix per run to avoid dedup on repeated runs
        run_suffix = uuid.uuid4().hex[:4]

        discovered = []
        for creator in selected:
            _simulate_delay()
            username = f"{creator['username']}_{run_suffix}"
            profile = {
                'contact_id': str(uuid.uuid4()),
                'id': str(uuid.uuid4()),
                'url': f"https://instagram.com/{username}",
                'profile_url': f"https://instagram.com/{username}",
                'platform_username': username,
                'name': creator['name'],
                'bio': creator['bio'],
                'follower_count': creator['followers'],
                'primary_category': creator['category'],
            }
            discovered.append(profile)
            run.increment_stage_progress('discovery', 'completed')
            logger.info("Found @%s (%s followers)", username, f"{creator['followers']:,}")

        return StageResult(
            profiles=discovered,
            processed=count,
            cost=count * 0.02,
        )


class MockPatreonDiscovery(StageAdapter):
    platform = 'patreon'
    stage = 'discovery'
    description = '[MOCK] Simulated Patreon discovery'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.02

    def run(self, profiles, run) -> StageResult:
        max_results = min(run.filters.get('max_results', 5), 5)
        discovered = []
        for i in range(max_results):
            _simulate_delay()
            creator = random.choice(MOCK_CREATORS)
            profile = {
                'creator_name': creator['name'],
                'patron_count': random.randint(200, 5000),
                'url': f"https://patreon.com/{creator['username']}",
                'bio': creator['bio'],
            }
            discovered.append(profile)
            run.increment_stage_progress('discovery', 'completed')

        return StageResult(profiles=discovered, processed=max_results, cost=max_results * 0.02)


class MockFacebookDiscovery(StageAdapter):
    platform = 'facebook'
    stage = 'discovery'
    description = '[MOCK] Simulated Facebook group discovery'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.02

    def run(self, profiles, run) -> StageResult:
        max_results = min(run.filters.get('max_results', 5), 5)
        discovered = []
        for i in range(max_results):
            _simulate_delay()
            profile = {
                'group_name': f"Travel {random.choice(['Lovers', 'Addicts', 'Community', 'Explorers', 'Nomads'])} {random.choice(['Global', 'Europe', 'Asia', 'Americas'])}",
                'member_count': random.randint(1000, 50000),
                'url': f"https://facebook.com/groups/{random.randint(100000, 999999)}",
            }
            discovered.append(profile)
            run.increment_stage_progress('discovery', 'completed')

        return StageResult(profiles=discovered, processed=max_results, cost=max_results * 0.02)


# ── Stage 2: Pre-screen ──────────────────────────────────────────────────────

class MockInstagramPrescreen(StageAdapter):
    platform = 'instagram'
    stage = 'pre_screen'
    description = '[MOCK] Simulated pre-screening'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.05

    def run(self, profiles, run) -> StageResult:
        passed = []
        failed = 0

        for profile in profiles:
            _simulate_delay()
            # ~80% pass rate
            if random.random() < 0.80:
                profile['_prescreen_result'] = 'passed'
                profile['_prescreen_reason'] = 'Active content, good engagement'
                # Attach content items like the real prescreen does
                profile['_content_items'] = [
                    {
                        'type': random.choice(['image', 'reel', 'carousel']),
                        'url': f"https://instagram.com/p/{uuid.uuid4().hex[:11]}",
                        'published_at': '2026-02-10T12:00:00Z',
                        'is_pinned': False,
                        'likes_and_views_disabled': random.random() < 0.1,
                        'engagement': {
                            'like_count': random.randint(50, 5000),
                            'comment_count': random.randint(2, 200),
                        },
                    }
                    for _ in range(12)
                ]
                passed.append(profile)
                run.increment_stage_progress('pre_screen', 'completed')
                logger.info("PASS @%s", profile.get('platform_username', '?'))
            else:
                failed += 1
                run.increment_stage_progress('pre_screen', 'failed')
                logger.info("FILTERED @%s — inactive", profile.get('platform_username', '?'))

        return StageResult(
            profiles=passed,
            processed=len(profiles),
            failed=failed,
            cost=len(profiles) * 0.05,
        )


class MockPatreonPrescreen(StageAdapter):
    platform = 'patreon'
    stage = 'pre_screen'
    description = '[MOCK] Simulated Patreon pre-screen'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.03

    def run(self, profiles, run) -> StageResult:
        passed = [p for p in profiles if random.random() < 0.85]
        failed = len(profiles) - len(passed)
        for p in passed:
            run.increment_stage_progress('pre_screen', 'completed')
        return StageResult(profiles=passed, processed=len(profiles), failed=failed, cost=len(profiles) * 0.03)


class MockFacebookPrescreen(StageAdapter):
    platform = 'facebook'
    stage = 'pre_screen'
    description = '[MOCK] Simulated Facebook pre-screen'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.03

    def run(self, profiles, run) -> StageResult:
        passed = [p for p in profiles if random.random() < 0.85]
        failed = len(profiles) - len(passed)
        for p in passed:
            run.increment_stage_progress('pre_screen', 'completed')
        return StageResult(profiles=passed, processed=len(profiles), failed=failed, cost=len(profiles) * 0.03)


# ── Stage 3: Enrichment ──────────────────────────────────────────────────────

class MockInstagramEnrichment(StageAdapter):
    platform = 'instagram'
    stage = 'enrichment'
    description = '[MOCK] Simulated enrichment'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.05

    def run(self, profiles, run) -> StageResult:
        for profile in profiles:
            _simulate_delay()
            profile['_social_data'] = {
                'data': [{
                    'profile': {
                        'platform_username': profile.get('platform_username', ''),
                        'full_name': profile.get('name', ''),
                        'introduction': profile.get('bio', ''),
                    }
                }]
            }
            run.increment_stage_progress('enrichment', 'completed')

        return StageResult(profiles=profiles, processed=len(profiles), cost=len(profiles) * 0.05)


class MockPatreonEnrichment(StageAdapter):
    platform = 'patreon'
    stage = 'enrichment'
    description = '[MOCK] Simulated Patreon enrichment'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.05

    def run(self, profiles, run) -> StageResult:
        for p in profiles:
            _simulate_delay()
            run.increment_stage_progress('enrichment', 'completed')
        return StageResult(profiles=profiles, processed=len(profiles), cost=len(profiles) * 0.05)


class MockFacebookEnrichment(StageAdapter):
    platform = 'facebook'
    stage = 'enrichment'
    description = '[MOCK] Simulated Facebook enrichment'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.05

    def run(self, profiles, run) -> StageResult:
        for p in profiles:
            _simulate_delay()
            run.increment_stage_progress('enrichment', 'completed')
        return StageResult(profiles=profiles, processed=len(profiles), cost=len(profiles) * 0.05)


# ── Stage 4: Analysis ────────────────────────────────────────────────────────

class MockInstagramAnalysis(StageAdapter):
    platform = 'instagram'
    stage = 'analysis'
    description = '[MOCK] Simulated content analysis'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.15

    def run(self, profiles, run) -> StageResult:
        for profile in profiles:
            _simulate_delay(0.2, 0.5)
            username = profile.get('platform_username', 'unknown')
            category = profile.get('primary_category', 'Travel')

            niche_score = random.uniform(0.4, 0.95)
            has_events = random.random() < 0.4
            has_community = random.random() < 0.5

            profile['_bio_evidence'] = {
                'niche_signals': {'niche_identified': True, 'niche_description': category},
                'in_person_events': {'evidence_found': has_events, 'event_types': ['retreats', 'workshops'] if has_events else []},
                'community_platforms': {'evidence_found': has_community, 'platforms': ['newsletter', 'discord'] if has_community else []},
                'monetization': {'evidence_found': random.random() < 0.6, 'types': ['courses', 'coaching']},
            }
            profile['_caption_evidence'] = {
                'in_person_events': {'mention_count': random.randint(0, 5)},
                'community_platforms': {'mention_count': random.randint(0, 4)},
                'audience_engagement': {'question_count': random.randint(0, 8)},
                'authenticity_vulnerability': {'degree': random.uniform(0.2, 0.9), 'post_count': random.randint(1, 8)},
            }
            profile['_thumbnail_evidence'] = {
                'creator_visibility': {'frequency': random.choice(['high', 'medium', 'low'])},
                'niche_consistency': {'consistent_theme': random.random() < 0.7, 'niche_description': category},
                'event_promotion': {'post_count': random.randint(0, 4)},
                'audience_engagement_cues': {'post_count': random.randint(0, 5)},
                'engagement_metrics': {
                    'posts_above_threshold': random.randint(2, 8),
                    'posts_below_threshold': random.randint(0, 4),
                    'posts_hidden': random.randint(0, 2),
                },
            }
            profile['_content_analyses'] = [
                {
                    'type': random.choice(['reel', 'image', 'carousel']),
                    'summary': f'{category} content showing authentic lifestyle',
                    'shows_pov': random.random() < 0.6,
                    'shows_authenticity': random.random() < 0.7,
                    'shows_vulnerability': random.random() < 0.3,
                    'engagement': {'like_count': random.randint(100, 5000), 'comment_count': random.randint(5, 200)},
                }
                for _ in range(3)
            ]
            profile['_creator_profile'] = {
                'primary_category': category,
                'content_types': 'reels, carousels, stories',
                'creator_presence': random.choice(['high', 'medium']),
            }
            profile['_has_travel_experience'] = random.random() < 0.3

            run.increment_stage_progress('analysis', 'completed')
            logger.info("@%s: %s, niche=%.2f", username, category, niche_score)

        return StageResult(profiles=profiles, processed=len(profiles), cost=len(profiles) * 0.15)


class MockPatreonAnalysis(StageAdapter):
    platform = 'patreon'
    stage = 'analysis'
    description = '[MOCK] Simulated Patreon analysis'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.10

    def run(self, profiles, run) -> StageResult:
        for p in profiles:
            _simulate_delay(0.2, 0.5)
            p['_bio_evidence'] = {'niche_signals': {'niche_identified': True, 'niche_description': 'Creative'}, 'in_person_events': {'evidence_found': False, 'event_types': []}, 'community_platforms': {'evidence_found': True, 'platforms': ['patreon']}, 'monetization': {'evidence_found': True, 'types': ['subscriptions']}}
            p['_caption_evidence'] = {'in_person_events': {'mention_count': 0}, 'community_platforms': {'mention_count': 2}, 'audience_engagement': {'question_count': 1}, 'authenticity_vulnerability': {'degree': 0.5, 'post_count': 2}}
            p['_thumbnail_evidence'] = {'engagement_metrics': {'posts_above_threshold': 3, 'posts_below_threshold': 2, 'posts_hidden': 0}}
            p['_content_analyses'] = []
            p['_creator_profile'] = {'primary_category': 'Creative', 'content_types': 'text, images'}
            run.increment_stage_progress('analysis', 'completed')
        return StageResult(profiles=profiles, processed=len(profiles), cost=len(profiles) * 0.10)


class MockFacebookAnalysis(StageAdapter):
    platform = 'facebook'
    stage = 'analysis'
    description = '[MOCK] Simulated Facebook analysis'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.10

    def run(self, profiles, run) -> StageResult:
        for p in profiles:
            _simulate_delay(0.2, 0.5)
            p['_bio_evidence'] = {'niche_signals': {'niche_identified': True, 'niche_description': 'Community'}, 'in_person_events': {'evidence_found': True, 'event_types': ['meetups']}, 'community_platforms': {'evidence_found': True, 'platforms': ['facebook']}, 'monetization': {'evidence_found': False, 'types': []}}
            p['_caption_evidence'] = {'in_person_events': {'mention_count': 3}, 'community_platforms': {'mention_count': 1}, 'audience_engagement': {'question_count': 4}, 'authenticity_vulnerability': {'degree': 0.6, 'post_count': 3}}
            p['_thumbnail_evidence'] = {'engagement_metrics': {'posts_above_threshold': 4, 'posts_below_threshold': 1, 'posts_hidden': 0}}
            p['_content_analyses'] = []
            p['_creator_profile'] = {'primary_category': 'Community', 'content_types': 'posts, events'}
            run.increment_stage_progress('analysis', 'completed')
        return StageResult(profiles=profiles, processed=len(profiles), cost=len(profiles) * 0.10)


# ── Stage 5: Scoring ─────────────────────────────────────────────────────────

class MockInstagramScoring(StageAdapter):
    platform = 'instagram'
    stage = 'scoring'
    description = '[MOCK] Simulated scoring'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.02

    def run(self, profiles, run) -> StageResult:
        scored = []
        for profile in profiles:
            _simulate_delay()

            # Generate realistic-looking scores
            niche = random.uniform(0.3, 0.95)
            auth = random.uniform(0.3, 0.95)
            monet = random.uniform(0.2, 0.85)
            comm = random.uniform(0.1, 0.90)
            eng = random.uniform(0.2, 0.80)

            manual_score = (niche * 0.30 + auth * 0.30 + monet * 0.20 + comm * 0.15 + eng * 0.05)

            followers = profile.get('follower_count', 0)
            if followers >= 100000:
                boost = 0.15
            elif followers >= 75000:
                boost = 0.10
            elif followers >= 50000:
                boost = 0.05
            else:
                boost = 0.0

            full_score = max(0.0, min(1.0, manual_score + boost + random.uniform(-0.05, 0.05)))

            if manual_score >= 0.65:
                tier = 'auto_enroll'
            elif full_score >= 0.80:
                tier = 'auto_enroll'
            elif full_score >= 0.25:
                tier = 'standard_priority_review'
            else:
                tier = 'low_priority_review'

            profile['_lead_analysis'] = {
                'section_scores': {
                    'niche_and_audience_identity': round(niche, 3),
                    'creator_authenticity_and_presence': round(auth, 3),
                    'monetization_and_business_mindset': round(monet, 3),
                    'community_infrastructure': round(comm, 3),
                    'engagement_and_connection': round(eng, 3),
                },
                'manual_score': round(manual_score, 3),
                'lead_score': round(full_score, 3),
                'follower_boost': boost,
                'engagement_adjustment': round(random.uniform(-0.05, 0.10), 3),
                'category_penalty': 0.0,
                'priority_tier': tier,
                'expected_precision': 0.75,
                'score_reasoning': f"Strong {profile.get('primary_category', 'niche')} creator with authentic community presence.",
            }
            profile['_first_name'] = profile.get('name', 'Creator').split()[0]

            if tier in run.tier_distribution:
                run.tier_distribution[tier] += 1

            scored.append(profile)
            run.increment_stage_progress('scoring', 'completed')
            logger.info("@%s: %.3f (%s)", profile.get('platform_username', '?'), full_score, tier)

        return StageResult(profiles=scored, processed=len(profiles), cost=len(profiles) * 0.02)


class MockPatreonScoring(StageAdapter):
    platform = 'patreon'
    stage = 'scoring'
    description = '[MOCK] Simulated Patreon scoring'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.02

    def run(self, profiles, run) -> StageResult:
        for p in profiles:
            _simulate_delay()
            score = random.uniform(0.3, 0.85)
            tier = 'auto_enroll' if score >= 0.65 else 'standard_priority_review' if score >= 0.25 else 'low_priority_review'
            p['_lead_analysis'] = {'lead_score': round(score, 3), 'priority_tier': tier, 'section_scores': {}, 'score_reasoning': 'Mock score.'}
            if tier in run.tier_distribution:
                run.tier_distribution[tier] += 1
            run.increment_stage_progress('scoring', 'completed')
        return StageResult(profiles=profiles, processed=len(profiles), cost=len(profiles) * 0.02)


class MockFacebookScoring(StageAdapter):
    platform = 'facebook'
    stage = 'scoring'
    description = '[MOCK] Simulated Facebook scoring'
    apis = ['Mock']

    def estimate_cost(self, count: int) -> float:
        return count * 0.02

    def run(self, profiles, run) -> StageResult:
        for p in profiles:
            _simulate_delay()
            score = random.uniform(0.3, 0.85)
            tier = 'auto_enroll' if score >= 0.65 else 'standard_priority_review' if score >= 0.25 else 'low_priority_review'
            p['_lead_analysis'] = {'lead_score': round(score, 3), 'priority_tier': tier, 'section_scores': {}, 'score_reasoning': 'Mock score.'}
            if tier in run.tier_distribution:
                run.tier_distribution[tier] += 1
            run.increment_stage_progress('scoring', 'completed')
        return StageResult(profiles=profiles, processed=len(profiles), cost=len(profiles) * 0.02)


# ── Stage 6: CRM Sync ────────────────────────────────────────────────────────

class MockInstagramCrmSync(StageAdapter):
    platform = 'instagram'
    stage = 'crm_sync'
    description = '[MOCK] Simulated HubSpot sync'
    apis = ['Mock']

    def run(self, profiles, run) -> StageResult:
        synced = []
        for profile in profiles:
            _simulate_delay()
            profile['_synced_to_crm'] = True
            synced.append(profile)
            run.increment_stage_progress('crm_sync', 'completed')
            logger.info("Synced @%s to HubSpot", profile.get('platform_username', '?'))

        run.contacts_synced = len(synced)
        run.save()

        return StageResult(profiles=synced, processed=len(profiles))


class MockPatreonCrmSync(StageAdapter):
    platform = 'patreon'
    stage = 'crm_sync'
    description = '[MOCK] Simulated Patreon CRM sync'
    apis = ['Mock']

    def run(self, profiles, run) -> StageResult:
        for p in profiles:
            _simulate_delay()
            p['_synced_to_crm'] = True
            run.increment_stage_progress('crm_sync', 'completed')
        run.contacts_synced = len(profiles)
        run.save()
        return StageResult(profiles=profiles, processed=len(profiles))


class MockFacebookCrmSync(StageAdapter):
    platform = 'facebook'
    stage = 'crm_sync'
    description = '[MOCK] Simulated Facebook CRM sync'
    apis = ['Mock']

    def run(self, profiles, run) -> StageResult:
        for p in profiles:
            _simulate_delay()
            p['_synced_to_crm'] = True
            run.increment_stage_progress('crm_sync', 'completed')
        run.contacts_synced = len(profiles)
        run.save()
        return StageResult(profiles=profiles, processed=len(profiles))


# ── Registry (same shape as real stage modules) ──────────────────────────────

MOCK_STAGE_REGISTRY = {
    'discovery':   {'instagram': MockInstagramDiscovery, 'patreon': MockPatreonDiscovery, 'facebook': MockFacebookDiscovery},
    'pre_screen':  {'instagram': MockInstagramPrescreen, 'patreon': MockPatreonPrescreen, 'facebook': MockFacebookPrescreen},
    'enrichment':  {'instagram': MockInstagramEnrichment, 'patreon': MockPatreonEnrichment, 'facebook': MockFacebookEnrichment},
    'analysis':    {'instagram': MockInstagramAnalysis, 'patreon': MockPatreonAnalysis, 'facebook': MockFacebookAnalysis},
    'scoring':     {'instagram': MockInstagramScoring, 'patreon': MockPatreonScoring, 'facebook': MockFacebookScoring},
    'crm_sync':    {'instagram': MockInstagramCrmSync, 'patreon': MockPatreonCrmSync, 'facebook': MockFacebookCrmSync},
}
