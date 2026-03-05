"""
Key-value config store backed by Postgres.

Used for operational config that should persist across deploys and be
editable via the dashboard (e.g. enrollment dispatcher settings).
"""
from sqlalchemy import Column, Text, DateTime, JSON
from sqlalchemy.sql import func

from app.database import Base


class AppConfig(Base):
    __tablename__ = 'app_config'

    key = Column(Text, primary_key=True)
    value = Column(JSON, nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
