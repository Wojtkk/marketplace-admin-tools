import logging
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker

from admin_api.models import AuditLog, Base

logger = logging.getLogger(__name__)

DATABASE_URL = "postgresql://admin:admin@localhost:5432/admin_tools"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def log_action(
    admin_id: UUID,
    action: str,
    target_type: str,
    target_id: str,
    details: dict[str, Any] | None = None,
) -> AuditLog:
    session = SessionLocal()
    try:
        entry = AuditLog(
            admin_user_id=admin_id,
            action=action,
            target_type=target_type,
            target_id=target_id,
            details=details,
            created_at=datetime.utcnow(),
        )
        session.add(entry)
        session.commit()
        session.refresh(entry)
        logger.info(
            "Audit log: admin=%s action=%s target=%s/%s",
            admin_id, action, target_type, target_id,
        )
        return entry
    except Exception:
        session.rollback()
        logger.exception("Failed to write audit log")
        raise
    finally:
        session.close()


def get_audit_trail(
    target_type: str,
    target_id: str,
    limit: int = 100,
) -> list[dict[str, Any]]:
    session = SessionLocal()
    try:
        logs = (
            session.query(AuditLog)
            .filter(
                AuditLog.target_type == target_type,
                AuditLog.target_id == target_id,
            )
            .order_by(desc(AuditLog.created_at))
            .limit(limit)
            .all()
        )
        return [
            {
                "id": log.id,
                "admin_user_id": str(log.admin_user_id),
                "action": log.action,
                "target_type": log.target_type,
                "target_id": log.target_id,
                "details": log.details,
                "created_at": log.created_at.isoformat(),
            }
            for log in logs
        ]
    finally:
        session.close()
