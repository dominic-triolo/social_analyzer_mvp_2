"""
LeadRun model — one row per lead per run (the evidence trail).

Records what happened to each lead at every pipeline stage.
"""
from sqlalchemy import Column, Integer, Text, Float, Boolean, DateTime, JSON, ForeignKey
from sqlalchemy.sql import func

from app.database import Base


class LeadRun(Base):
    __tablename__ = 'lead_runs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    lead_id = Column(Integer, ForeignKey('leads.id'), nullable=False)
    run_id = Column(Text, ForeignKey('runs.id'), nullable=False)
    stage_reached = Column(Text, default='discovery')
    prescreen_result = Column(Text, nullable=True)   # passed/disqualified/rejected
    prescreen_reason = Column(Text, nullable=True)
    analysis_evidence = Column(JSON, default=dict)
    lead_score = Column(Float, nullable=True)         # 0.0-1.0
    manual_score = Column(Float, nullable=True)       # pre-adjustment score
    section_scores = Column(JSON, default=dict)       # {niche, authenticity, ...}
    priority_tier = Column(Text, nullable=True)
    score_reasoning = Column(Text, nullable=True)
    synced_to_crm = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
