"""
Lead model — one row per unique creator, deduplicated by (platform, platform_id).
"""
from sqlalchemy import Column, Integer, Float, Text, DateTime, JSON, UniqueConstraint
from sqlalchemy.sql import func

from app.database import Base


class Lead(Base):
    __tablename__ = 'leads'

    id = Column(Integer, primary_key=True, autoincrement=True)
    platform = Column(Text, nullable=False)
    platform_id = Column(Text, nullable=False)  # IG handle / Patreon slug / FB group ID
    name = Column(Text, default='')
    profile_url = Column(Text, default='')
    bio = Column(Text, default='')
    follower_count = Column(Integer, default=0)
    email = Column(Text, default='')
    website = Column(Text, default='')
    social_urls = Column(JSON, default=dict)
    hubspot_contact_id = Column(Text, nullable=True)
    engagement_rate = Column(Float, nullable=True)
    media_count = Column(Integer, nullable=True)
    category = Column(Text, nullable=True)
    location = Column(Text, nullable=True)
    extra_data = Column(JSON, nullable=True)
    first_seen_at = Column(DateTime(timezone=True), server_default=func.now())
    last_seen_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint('platform', 'platform_id', name='uq_lead_platform_id'),
    )
