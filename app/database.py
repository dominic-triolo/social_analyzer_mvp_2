"""
Database engine + session factory.

Always initializes — defaults to SQLite for local dev, Postgres in production.
get_session() always returns a real session.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from app.config import DATABASE_URL


class Base(DeclarativeBase):
    pass


# Railway injects postgres:// but SQLAlchemy 2.x requires postgresql://
url = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

# SQLite needs different engine kwargs than Postgres
if url.startswith('sqlite'):
    engine = create_engine(url, connect_args={'check_same_thread': False})
else:
    engine = create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10)

SessionLocal = sessionmaker(bind=engine)


def get_session():
    """Return a new DB session."""
    return SessionLocal()


