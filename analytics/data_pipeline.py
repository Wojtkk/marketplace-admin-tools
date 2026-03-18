import logging
import subprocess
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from analytics.models import DailyReport

logger = logging.getLogger(__name__)

MARKETPLACE_DB_URL = "postgresql://readonly:readonly@marketplace-db:5432/marketplace"
ANALYTICS_DB_URL = "postgresql://analytics:analytics@localhost:5432/analytics"

marketplace_engine = create_engine(MARKETPLACE_DB_URL)
analytics_engine = create_engine(ANALYTICS_DB_URL)
AnalyticsSession = sessionmaker(bind=analytics_engine)


def extract_orders(cutoff_hours: int = 24) -> list[dict[str, Any]]:
    cutoff = datetime.utcnow() - timedelta(hours=cutoff_hours)
    with marketplace_engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT
                    id, user_id, status, total_amount,
                    created_at, updated_at
                FROM orders
                WHERE created_at > :cutoff
                ORDER BY created_at DESC
            """),
            {"cutoff": cutoff},
        )
        rows = result.fetchall()

    orders = [
        {
            "id": str(row.id),
            "user_id": str(row.user_id),
            "status": row.status,
            "total_amount": float(row.total_amount),
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }
        for row in rows
    ]
    logger.info("Extracted %d orders since %s", len(orders), cutoff.isoformat())
    return orders


def extract_payments(cutoff_hours: int = 24) -> list[dict[str, Any]]:
    cutoff = datetime.utcnow() - timedelta(hours=cutoff_hours)
    with marketplace_engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT
                    id, order_id, amount, currency, status,
                    stripe_charge_id, created_at
                FROM payments
                WHERE status = 'completed'
                  AND created_at > :cutoff
                ORDER BY created_at DESC
            """),
            {"cutoff": cutoff},
        )
        rows = result.fetchall()

    payments = [
        {
            "id": str(row.id),
            "order_id": str(row.order_id),
            "amount": float(row.amount),
            "currency": row.currency,
            "status": row.status,
            "stripe_charge_id": row.stripe_charge_id,
            "created_at": row.created_at,
        }
        for row in rows
    ]
    logger.info("Extracted %d completed payments since %s", len(payments), cutoff.isoformat())
    return payments


def transform_daily_metrics(
    orders: list[dict[str, Any]],
    payments: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    daily: dict[str, dict[str, Any]] = {}

    for order in orders:
        if order["status"] in ("cancelled", "refunded"):
            continue
        day_key = order["created_at"].strftime("%Y-%m-%d")
        if day_key not in daily:
            daily[day_key] = {"total_orders": 0, "total_revenue": 0.0, "payment_total": 0.0}
        daily[day_key]["total_orders"] += 1
        daily[day_key]["total_revenue"] += order["total_amount"]

    for payment in payments:
        day_key = payment["created_at"].strftime("%Y-%m-%d")
        if day_key not in daily:
            daily[day_key] = {"total_orders": 0, "total_revenue": 0.0, "payment_total": 0.0}
        daily[day_key]["payment_total"] += payment["amount"]

    for day_key, metrics in daily.items():
        count = metrics["total_orders"]
        metrics["avg_order_value"] = metrics["total_revenue"] / count if count > 0 else 0.0
        metrics["date"] = day_key

    logger.info("Transformed metrics for %d days", len(daily))
    return daily


def load_to_analytics_db(metrics: dict[str, dict[str, Any]]) -> int:
    session = AnalyticsSession()
    loaded = 0
    try:
        for day_key, data in metrics.items():
            report_date = datetime.strptime(day_key, "%Y-%m-%d").date()
            existing = session.query(DailyReport).filter(DailyReport.date == report_date).first()

            if existing:
                existing.total_orders = data["total_orders"]
                existing.total_revenue = data["total_revenue"]
                existing.avg_order_value = data["avg_order_value"]
            else:
                report = DailyReport(
                    date=report_date,
                    total_orders=data["total_orders"],
                    total_revenue=data["total_revenue"],
                    avg_order_value=data["avg_order_value"],
                )
                session.add(report)
            loaded += 1

        session.commit()
        logger.info("Loaded %d daily reports to analytics DB", loaded)
    except Exception:
        session.rollback()
        logger.exception("Failed to load metrics")
        raise
    finally:
        session.close()

    return loaded


def run_pipeline(cutoff_hours: int = 24) -> dict[str, Any]:
    logger.info("Starting ETL pipeline (cutoff: %d hours)", cutoff_hours)

    orders = extract_orders(cutoff_hours=cutoff_hours)
    payments = extract_payments(cutoff_hours=cutoff_hours)
    metrics = transform_daily_metrics(orders, payments)
    loaded = load_to_analytics_db(metrics)

    validation = subprocess.run(
        ["python", "scripts/validate_data.py"],
        capture_output=True,
        text=True,
        timeout=60,
    )

    if validation.returncode != 0:
        logger.warning("Data validation warnings: %s", validation.stderr)

    return {
        "orders_extracted": len(orders),
        "payments_extracted": len(payments),
        "days_processed": len(metrics),
        "reports_loaded": loaded,
        "validation_passed": validation.returncode == 0,
    }
