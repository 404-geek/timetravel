"""ORM models: Customer, Record, RecordVersion (survey-app style)."""
from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, Text
from sqlalchemy.orm import relationship

from db.base import Base


class Customer(Base):
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(Text, nullable=False)

    records = relationship("Record", back_populates="customer")
    record_versions = relationship("RecordVersion", back_populates="customer")


class Record(Base):
    __tablename__ = "records"

    id = Column(Integer, primary_key=True, index=True)
    data = Column(Text, nullable=True)  # Optional: set on create; versioned "current" is derived by replay
    latest_version = Column(Integer, nullable=True)  # When set, current state = replay to this version
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True)

    customer = relationship("Customer", back_populates="records")
    versions = relationship(
        "RecordVersion",
        back_populates="record",
        order_by="RecordVersion.version",
        cascade="all, delete-orphan",
    )


class RecordVersion(Base):
    __tablename__ = "record_versions"

    record_id = Column(Integer, ForeignKey("records.id"), primary_key=True)
    version = Column(Integer, primary_key=True)
    data = Column(Text, nullable=True)  # None when row is delta-only
    delta = Column(Text, nullable=True)  # JSON: only changed keys; value null = delete key
    is_keyframe = Column(Boolean, default=True, nullable=False)  # True = full snapshot in data
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True)

    record = relationship("Record", back_populates="versions")
    customer = relationship("Customer", back_populates="record_versions")
