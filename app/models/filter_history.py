"""
FilterHistory model — tracks filter fingerprints for staleness detection.
"""
from sqlalchemy import Column, Integer, Text, Float, DateTime
from sqlalchemy.sql import func

from app.database import Base


class FilterHistory(Base):
    __tablename__ = 'filter_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    filter_hash = Column(Text, nullable=False, index=True)
    platform = Column(Text, nullable=False)
    run_id = Column(Text, nullable=False)
    total_found = Column(Integer, default=0)
    new_found = Column(Integer, default=0)
    novelty_rate = Column(Float, default=0.0)
    ran_at = Column(DateTime(timezone=True), server_default=func.now())
