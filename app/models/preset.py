"""
Preset model — saved discovery filter presets for quick reuse.
"""
from sqlalchemy import Column, Integer, Text, DateTime, JSON
from sqlalchemy.sql import func

from app.database import Base


class Preset(Base):
    __tablename__ = 'presets'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text, nullable=False)
    platform = Column(Text, nullable=False)
    filters = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
