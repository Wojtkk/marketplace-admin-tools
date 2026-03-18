from datetime import datetime

from sqlalchemy import Column, String, DateTime, JSON, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import declarative_base, relationship
import uuid

Base = declarative_base()


class AdminUser(Base):
    __tablename__ = "admin_users"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False, index=True)
    role = Column(String(50), nullable=False, default="viewer")
    permissions = Column(JSON, nullable=False, default=list)
    last_login = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    audit_logs = relationship("AuditLog", back_populates="admin_user", lazy="dynamic")

    def has_permission(self, permission: str) -> bool:
        if self.role == "superadmin":
            return True
        return permission in (self.permissions or [])


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_user_id = Column(
        PG_UUID(as_uuid=True),
        ForeignKey("admin_users.id"),
        nullable=False,
        index=True,
    )
    action = Column(String(100), nullable=False, index=True)
    target_type = Column(String(50), nullable=False, index=True)
    target_id = Column(String(255), nullable=False, index=True)
    details = Column(JSON, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    admin_user = relationship("AdminUser", back_populates="audit_logs")
