from datetime import date, datetime

from sqlalchemy import Column, String, Integer, Float, Date, DateTime
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import declarative_base
import uuid

Base = declarative_base()


class DailyReport(Base):
    __tablename__ = "daily_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, unique=True, nullable=False, index=True)
    total_orders = Column(Integer, nullable=False, default=0)
    total_revenue = Column(Float, nullable=False, default=0.0)
    avg_order_value = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class ProductMetric(Base):
    __tablename__ = "product_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(PG_UUID(as_uuid=True), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    views = Column(Integer, nullable=False, default=0)
    purchases = Column(Integer, nullable=False, default=0)
    revenue = Column(Float, nullable=False, default=0.0)

    class Meta:
        unique_together = ("product_id", "date")


class UserCohort(Base):
    __tablename__ = "user_cohorts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cohort_date = Column(Date, nullable=False, unique=True, index=True)
    users_count = Column(Integer, nullable=False, default=0)
    retention_7d = Column(Float, nullable=True)
    retention_30d = Column(Float, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
