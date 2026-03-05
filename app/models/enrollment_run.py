"""
Postgres-backed enrollment dispatch run record.
"""
from sqlalchemy import Column, Integer, Text, Boolean, Date, DateTime, JSON
from sqlalchemy.sql import func

from app.database import Base


class EnrollmentRun(Base):
    __tablename__ = 'enrollment_runs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    status = Column(Text, nullable=False)           # completed | skipped | error
    reason = Column(Text, nullable=True)             # skip/error reason
    enrolled_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    active_count = Column(Integer, default=0)        # active contacts at time of run
    queued_count = Column(Integer, default=0)        # queued contacts at time of run
    total_slots = Column(Integer, default=0)         # available capacity
    allocation = Column(JSON, nullable=True)         # {segment: slot_count}
    enrolled_details = Column(JSON, nullable=True)   # [{contact_id, inbox, segment}, ...]
    errors = Column(JSON, nullable=True)             # [error strings]
    dry_run = Column(Boolean, default=False)
    run_date = Column(Date, nullable=False)          # business date this run covers
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
