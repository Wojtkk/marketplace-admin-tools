import logging
from datetime import date, datetime, timedelta

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from analytics.models import Base, DailyReport, ProductMetric

logger = logging.getLogger(__name__)

app = FastAPI(title="Analytics Service", version="1.0.0")

MARKETPLACE_DB_URL = "postgresql://readonly:readonly@marketplace-db:5432/marketplace"
ANALYTICS_DB_URL = "postgresql://analytics:analytics@localhost:5432/analytics"

marketplace_engine = create_engine(MARKETPLACE_DB_URL)
analytics_engine = create_engine(ANALYTICS_DB_URL)
AnalyticsSession = sessionmaker(bind=analytics_engine)


class DailyReportRequest(BaseModel):
    date: date | None = None


class RevenueResponse(BaseModel):
    period_start: str
    period_end: str
    total_revenue: float
    order_count: int
    average_order_value: float


@app.post("/reports/daily")
def generate_daily_report(body: DailyReportRequest) -> dict:
    report_date = body.date or (datetime.utcnow() - timedelta(days=1)).date()

    with marketplace_engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT
                    COUNT(*) as total_orders,
                    COALESCE(SUM(o.total_amount), 0) as total_revenue,
                    COALESCE(AVG(o.total_amount), 0) as avg_order_value
                FROM orders o
                JOIN order_items oi ON o.id = oi.order_id
                WHERE DATE(o.created_at) = :report_date
                  AND o.status NOT IN ('cancelled', 'refunded')
                GROUP BY DATE(o.created_at)
            """),
            {"report_date": report_date},
        )
        row = result.fetchone()

    if row is None:
        return {"date": report_date.isoformat(), "total_orders": 0, "total_revenue": 0.0, "avg_order_value": 0.0}

    session = AnalyticsSession()
    try:
        existing = session.query(DailyReport).filter(DailyReport.date == report_date).first()
        if existing:
            existing.total_orders = row.total_orders
            existing.total_revenue = float(row.total_revenue)
            existing.avg_order_value = float(row.avg_order_value)
        else:
            report = DailyReport(
                date=report_date,
                total_orders=row.total_orders,
                total_revenue=float(row.total_revenue),
                avg_order_value=float(row.avg_order_value),
            )
            session.add(report)
        session.commit()
    finally:
        session.close()

    return {
        "date": report_date.isoformat(),
        "total_orders": row.total_orders,
        "total_revenue": float(row.total_revenue),
        "avg_order_value": float(row.avg_order_value),
    }


@app.get("/reports/revenue")
def get_revenue_report(
    days: int = Query(default=30, ge=1, le=365),
) -> RevenueResponse:
    period_end = datetime.utcnow().date()
    period_start = period_end - timedelta(days=days)

    session = AnalyticsSession()
    try:
        reports = (
            session.query(DailyReport)
            .filter(DailyReport.date >= period_start, DailyReport.date <= period_end)
            .all()
        )

        total_revenue = sum(r.total_revenue for r in reports)
        order_count = sum(r.total_orders for r in reports)
        avg_value = total_revenue / order_count if order_count > 0 else 0.0

        return RevenueResponse(
            period_start=period_start.isoformat(),
            period_end=period_end.isoformat(),
            total_revenue=total_revenue,
            order_count=order_count,
            average_order_value=avg_value,
        )
    finally:
        session.close()


@app.get("/reports/top-products")
def get_top_products(limit: int = Query(default=10, ge=1, le=100)) -> dict:
    with marketplace_engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT
                    oi.product_id,
                    oi.product_name,
                    COUNT(*) as purchase_count,
                    SUM(oi.quantity) as total_quantity,
                    SUM(oi.price * oi.quantity) as total_revenue
                FROM order_items oi
                JOIN orders o ON o.id = oi.order_id
                WHERE o.status NOT IN ('cancelled', 'refunded')
                  AND o.created_at >= NOW() - INTERVAL '30 days'
                GROUP BY oi.product_id, oi.product_name
                ORDER BY total_revenue DESC
                LIMIT :limit
            """),
            {"limit": limit},
        )
        rows = result.fetchall()

    products = [
        {
            "product_id": str(row.product_id),
            "product_name": row.product_name,
            "purchase_count": row.purchase_count,
            "total_quantity": row.total_quantity,
            "total_revenue": float(row.total_revenue),
        }
        for row in rows
    ]
    return {"products": products, "period": "last_30_days"}


@app.get("/metrics/orders")
def get_order_metrics() -> dict:
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)
    week_ago = today - timedelta(days=7)

    session = AnalyticsSession()
    try:
        today_report = session.query(DailyReport).filter(DailyReport.date == today).first()
        yesterday_report = session.query(DailyReport).filter(DailyReport.date == yesterday).first()

        weekly_reports = (
            session.query(DailyReport)
            .filter(DailyReport.date >= week_ago, DailyReport.date <= today)
            .all()
        )

        weekly_orders = sum(r.total_orders for r in weekly_reports)
        weekly_revenue = sum(r.total_revenue for r in weekly_reports)

        return {
            "today": {
                "orders": today_report.total_orders if today_report else 0,
                "revenue": today_report.total_revenue if today_report else 0.0,
            },
            "yesterday": {
                "orders": yesterday_report.total_orders if yesterday_report else 0,
                "revenue": yesterday_report.total_revenue if yesterday_report else 0.0,
            },
            "last_7_days": {
                "orders": weekly_orders,
                "revenue": weekly_revenue,
            },
        }
    finally:
        session.close()
