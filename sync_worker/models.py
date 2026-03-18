from datetime import datetime

from sqlalchemy import Column, String, Integer, Float, DateTime
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class CachedProduct(Base):
    __tablename__ = "cached_products"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(String(255), unique=True, nullable=False, index=True)
    name = Column(String(500), nullable=False)
    price = Column(Float, nullable=False)
    last_synced = Column(DateTime, nullable=False, default=datetime.utcnow)


class CachedUser(Base):
    __tablename__ = "cached_users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(String(255), unique=True, nullable=False, index=True)
    email = Column(String(255), nullable=False)
    name = Column(String(255), nullable=False)
    last_synced = Column(DateTime, nullable=False, default=datetime.utcnow)


class SyncState(Base):
    __tablename__ = "sync_state"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_type = Column(String(50), unique=True, nullable=False, index=True)
    last_sync_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    records_synced = Column(Integer, nullable=False, default=0)
