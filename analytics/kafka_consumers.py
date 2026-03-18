import json
import logging
from datetime import datetime
from typing import Any
from uuid import UUID

from confluent_kafka import Consumer, KafkaError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from analytics.models import DailyReport, ProductMetric

logger = logging.getLogger(__name__)

ANALYTICS_DB_URL = "postgresql://analytics:analytics@localhost:5432/analytics"
analytics_engine = create_engine(ANALYTICS_DB_URL)
AnalyticsSession = sessionmaker(bind=analytics_engine)

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
CONSUMER_GROUP = "analytics-consumers"


def _parse_event(raw_value: bytes) -> dict[str, Any]:
    data = json.loads(raw_value.decode("utf-8"))
    return data.get("payload", data)


def handle_order_created(payload: dict[str, Any]) -> None:
    product_ids = []
    items = payload.get("items", [])
    for item in items:
        product_id = item.get("product_id")
        quantity = item.get("quantity", 1)
        price = item.get("price", 0.0)
        if product_id:
            product_ids.append((product_id, quantity, price))

    if not product_ids:
        return

    today = datetime.utcnow().date()
    session = AnalyticsSession()
    try:
        for pid, qty, price in product_ids:
            metric = (
                session.query(ProductMetric)
                .filter(ProductMetric.product_id == pid, ProductMetric.date == today)
                .first()
            )
            if metric:
                metric.purchases += qty
                metric.revenue += price * qty
            else:
                metric = ProductMetric(
                    product_id=pid,
                    date=today,
                    views=0,
                    purchases=qty,
                    revenue=price * qty,
                )
                session.add(metric)
        session.commit()
        logger.info("Updated product metrics for %d products from order", len(product_ids))
    except Exception:
        session.rollback()
        logger.exception("Failed to update product metrics")
    finally:
        session.close()


def handle_payment_completed(payload: dict[str, Any]) -> None:
    amount = payload.get("amount", 0.0)
    created_at = payload.get("created_at")

    if created_at:
        payment_date = datetime.fromisoformat(created_at).date()
    else:
        payment_date = datetime.utcnow().date()

    session = AnalyticsSession()
    try:
        report = session.query(DailyReport).filter(DailyReport.date == payment_date).first()
        if report:
            report.total_revenue += amount
            report.total_orders += 1
            report.avg_order_value = report.total_revenue / report.total_orders
        else:
            report = DailyReport(
                date=payment_date,
                total_orders=1,
                total_revenue=amount,
                avg_order_value=amount,
            )
            session.add(report)
        session.commit()
        logger.info("Updated daily report for %s with payment of %.2f", payment_date, amount)
    except Exception:
        session.rollback()
        logger.exception("Failed to update daily report from payment")
    finally:
        session.close()


def run_consumers() -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    topics = ["order.created", "payment.completed"]
    consumer.subscribe(topics)
    logger.info("Analytics consumers subscribed to: %s", topics)

    handlers = {
        "order.created": handle_order_created,
        "payment.completed": handle_payment_completed,
    }

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka consumer error: %s", msg.error())
                continue

            topic = msg.topic()
            handler = handlers.get(topic)
            if handler is None:
                logger.warning("No handler for topic: %s", topic)
                continue

            try:
                payload = _parse_event(msg.value())
                handler(payload)
                consumer.commit(message=msg)
            except Exception:
                logger.exception("Error processing message from %s", topic)
    finally:
        consumer.close()
        logger.info("Analytics consumers stopped")
