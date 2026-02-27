"""
Run model — Redis-backed pipeline run tracking.

A Run represents a batch of leads flowing through the 6-stage pipeline.
"""
import json
import uuid
from datetime import datetime
from typing import Dict, Optional, List

from app.extensions import redis_client as r
from app.config import PIPELINE_STAGES


RUN_TTL = 86400 * 7  # 7 days


class Run:
    """
    Redis-backed Run object.

    Keys:
        run:{id}         → JSON blob of run state
        runs:list        → sorted set of run IDs by creation time
        runs:by_status   → set per status for filtering
    """

    def __init__(
        self,
        id: str = None,
        status: str = 'queued',
        platform: str = 'instagram',
        filters: Dict = None,
        bdr_assignment: str = '',
    ):
        self.id = id or str(uuid.uuid4())
        self.status = status
        self.platform = platform
        self.created_at = datetime.now().isoformat()
        self.updated_at = self.created_at
        self.current_stage = ''
        self.stage_progress = {stage: {'total': 0, 'completed': 0, 'failed': 0} for stage in PIPELINE_STAGES}
        self.filters = filters or {}
        self.profiles_found = 0
        self.profiles_pre_screened = 0
        self.profiles_enriched = 0
        self.profiles_scored = 0
        self.contacts_synced = 0
        self.duplicates_skipped = 0
        self.bdr_assignment = bdr_assignment
        self.errors: List[Dict] = []
        self.tier_distribution = {
            'auto_enroll': 0,
            'high_priority_review': 0,
            'standard_priority_review': 0,
            'low_priority_review': 0,
        }
        self.summary = ''
        self.estimated_cost = 0.0
        self.actual_cost = 0.0
        self.stage_outputs = {}

    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'status': self.status,
            'platform': self.platform,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'current_stage': self.current_stage,
            'stage_progress': self.stage_progress,
            'filters': self.filters,
            'profiles_found': self.profiles_found,
            'profiles_pre_screened': self.profiles_pre_screened,
            'profiles_enriched': self.profiles_enriched,
            'profiles_scored': self.profiles_scored,
            'contacts_synced': self.contacts_synced,
            'duplicates_skipped': self.duplicates_skipped,
            'bdr_assignment': self.bdr_assignment,
            'errors': self.errors[-20:],  # Keep last 20 errors
            'tier_distribution': self.tier_distribution,
            'summary': self.summary,
            'estimated_cost': self.estimated_cost,
            'actual_cost': self.actual_cost,
        }

    def save(self):
        """Persist run state to Redis."""
        self.updated_at = datetime.now().isoformat()
        key = f'run:{self.id}'
        r.setex(key, RUN_TTL, json.dumps(self.to_dict()))
        # Add to sorted set for listing
        r.zadd('runs:list', {self.id: datetime.fromisoformat(self.created_at).timestamp()})
        return self

    def update_stage(self, stage: str, status: str = None, **kwargs):
        """Update current stage and optionally set run status."""
        self.current_stage = stage
        if status:
            self.status = status
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        self.save()

    def increment_stage_progress(self, stage: str, field: str = 'completed', count: int = 1):
        """Increment a stage progress counter."""
        if stage in self.stage_progress:
            self.stage_progress[stage][field] = self.stage_progress[stage].get(field, 0) + count
        self.save()

    def add_error(self, stage: str, message: str, profile_id: str = ''):
        """Add an error to the run log."""
        self.errors.append({
            'stage': stage,
            'message': message,
            'profile_id': profile_id,
            'timestamp': datetime.now().isoformat(),
        })
        self.save()

    def complete(self):
        """Mark run as completed."""
        self.status = 'completed'
        self.save()

    def fail(self, reason: str = ''):
        """Mark run as failed."""
        self.status = 'failed'
        if reason:
            self.add_error(self.current_stage, reason)
        self.save()

    @classmethod
    def _from_db_run(cls, db_run) -> 'Run':
        """Build a Run from a DbRun row."""
        run = cls.__new__(cls)
        run.id = db_run.id
        run.status = db_run.status
        run.platform = db_run.platform
        run.created_at = db_run.created_at.isoformat() if db_run.created_at else ''
        run.updated_at = (db_run.finished_at or db_run.created_at or '').isoformat() if db_run.created_at else ''
        run.current_stage = db_run.current_stage or ''
        run.stage_progress = {}
        run.filters = db_run.filters or {}
        run.profiles_found = db_run.profiles_found or 0
        run.profiles_pre_screened = db_run.profiles_pre_screened or 0
        run.profiles_enriched = db_run.profiles_enriched or 0
        run.profiles_scored = db_run.profiles_scored or 0
        run.contacts_synced = db_run.contacts_synced or 0
        run.duplicates_skipped = db_run.duplicates_skipped or 0
        run.bdr_assignment = db_run.bdr_assignment or ''
        run.errors = []
        run.tier_distribution = db_run.tier_distribution or {}
        run.summary = db_run.summary or ''
        run.estimated_cost = db_run.estimated_cost or 0.0
        run.actual_cost = db_run.actual_cost or 0.0
        run.stage_outputs = db_run.stage_outputs or {}
        return run

    @classmethod
    def load(cls, run_id: str) -> Optional['Run']:
        """Load a run from Redis, falling back to the database."""
        key = f'run:{run_id}'
        data = r.get(key)
        if data:
            d = json.loads(data)
            run = cls.__new__(cls)
            run.id = d['id']
            run.status = d['status']
            run.platform = d['platform']
            run.created_at = d['created_at']
            run.updated_at = d['updated_at']
            run.current_stage = d.get('current_stage', '')
            run.stage_progress = d.get('stage_progress', {})
            run.filters = d.get('filters', {})
            run.profiles_found = d.get('profiles_found', 0)
            run.profiles_pre_screened = d.get('profiles_pre_screened', 0)
            run.profiles_enriched = d.get('profiles_enriched', 0)
            run.profiles_scored = d.get('profiles_scored', 0)
            run.contacts_synced = d.get('contacts_synced', 0)
            run.duplicates_skipped = d.get('duplicates_skipped', 0)
            run.bdr_assignment = d.get('bdr_assignment', '')
            run.errors = d.get('errors', [])
            run.tier_distribution = d.get('tier_distribution', {})
            run.summary = d.get('summary', '')
            run.estimated_cost = d.get('estimated_cost', 0.0)
            run.actual_cost = d.get('actual_cost', 0.0)
            run.stage_outputs = d.get('stage_outputs', {})
            return run

        # Fallback to DB
        try:
            from app.database import get_session
            from app.models.db_run import DbRun
            session = get_session()
            try:
                db_run = session.get(DbRun, run_id)
                if db_run:
                    return cls._from_db_run(db_run)
            finally:
                session.close()
        except Exception:
            pass
        return None

    @classmethod
    def list_recent(cls, limit: int = 20) -> List['Run']:
        """List recent runs from Redis, falling back to the database."""
        run_ids = r.zrevrange('runs:list', 0, limit - 1)
        if run_ids:
            runs = []
            for run_id in run_ids:
                run = cls.load(run_id)
                if run:
                    runs.append(run)
            return runs

        # Fallback to DB
        try:
            from app.database import get_session
            from app.models.db_run import DbRun
            session = get_session()
            try:
                db_runs = session.query(DbRun).order_by(DbRun.created_at.desc()).limit(limit).all()
                return [cls._from_db_run(r) for r in db_runs]
            finally:
                session.close()
        except Exception:
            return []

    @classmethod
    def delete(cls, run_id: str):
        """Delete a run from Redis."""
        r.delete(f'run:{run_id}')
        r.zrem('runs:list', run_id)
