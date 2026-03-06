"""
HubSpot list/segment — cached in Postgres for instant combo box queries.
"""
from sqlalchemy import Column, Text, Integer

from app.database import Base


class HubSpotList(Base):
    __tablename__ = 'hubspot_lists'

    list_id = Column(Text, primary_key=True)
    name = Column(Text, nullable=False)
    size = Column(Integer, nullable=False, default=0)
    processing_type = Column(Text, nullable=True)
