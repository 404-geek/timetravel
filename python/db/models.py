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
    data = Column(Text, nullable=True)
    latest_version = Column(Integer, nullable=True)
    created_at = Column(DateTime, nullable=True)
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
    data = Column(Text, nullable=True)
    delta = Column(Text, nullable=True)
    is_keyframe = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True)

    record = relationship("Record", back_populates="versions")
    customer = relationship("Customer", back_populates="record_versions")
