"""
Postgres-backed Run record — mirrors the Redis Run for persistent storage.
"""
from sqlalchemy import Column, Text, Integer, Float, DateTime, JSON
from sqlalchemy.sql import func

from app.database import Base


class DbRun(Base):
    __tablename__ = 'runs'

    id = Column(Text, primary_key=True)
    platform = Column(Text, nullable=False)
    status = Column(Text, nullable=False, default='queued')
    current_stage = Column(Text, default='')
    filters = Column(JSON, default=dict)
    bdr_assignment = Column(Text, default='')
    profiles_found = Column(Integer, default=0)
    profiles_pre_screened = Column(Integer, default=0)
    profiles_enriched = Column(Integer, default=0)
    profiles_scored = Column(Integer, default=0)
    contacts_synced = Column(Integer, default=0)
    duplicates_skipped = Column(Integer, default=0)
    tier_distribution = Column(JSON, default=dict)
    error_count = Column(Integer, default=0)
    summary = Column(Text, nullable=True)
    estimated_cost = Column(Float, nullable=True)
    actual_cost = Column(Float, nullable=True)
    stage_outputs = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
