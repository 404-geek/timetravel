"""Record service: current state + versioned (time-travel) operations."""
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from db.models import Record as RecordRow, RecordVersion as RecordVersionRow
from schemas.record import Record


class RecordError(Exception):
    """Raised for any record service error. Carries detail and status_code for HTTP layer."""

    def __init__(self, detail: str, status_code: int = 400, code: Optional[str] = None):
        self.detail = detail
        self.status_code = status_code
        self.code = code
        super().__init__(detail)


@dataclass
class VersionInfo:
    version: int
    created_at: str


def get_record(db: Session, id: int) -> Record:
    """Return record. Raises RecordError."""
    if id <= 0:
        raise RecordError("invalid id; id must be a positive number", code="invalid_id")
    row = db.query(RecordRow).filter(RecordRow.id == id).first()
    if row is None:
        raise RecordError(f"record of id {id} does not exist", code="not_found")
    data = json.loads(row.data)
    return Record(id=row.id, data=data)


def create_record(
    db: Session,
    id: int,
    data: dict[str, Any],
    customer_id: Optional[int] = None,
) -> None:
    """Insert record. Raises RecordError."""
    if id <= 0:
        raise RecordError("invalid id; id must be a positive number", code="invalid_id")
    row = RecordRow(id=id, data=json.dumps(data), customer_id=customer_id)
    try:
        db.add(row)
        db.commit()
    except IntegrityError:
        db.rollback()
        raise RecordError("record already exists", code="already_exists")


def update_record(db: Session, id: int, updates: dict[str, Any]) -> Record:
    """Apply updates (None = delete key). Returns full record. Merge semantics."""
    record = get_record(db, id)
    data = dict(record.data)
    for key, value in updates.items():
        if value is None:
            data.pop(key, None)
        else:
            data[key] = value
    row = db.query(RecordRow).filter(RecordRow.id == id).first()
    row.data = json.dumps(data)
    db.commit()
    return Record(id=id, data=data)


def replace_record(db: Session, id: int, data: dict[str, Any]) -> Record:
    """Set record to exactly this data (full replace). Keys not in data are removed."""
    get_record(db, id)  # ensure exists
    row = db.query(RecordRow).filter(RecordRow.id == id).first()
    row.data = json.dumps(data)
    db.commit()
    return Record(id=id, data=data)


def get_record_at_version(db: Session, id: int, version: int) -> tuple[Record, str]:
    """Return snapshot of record at given version and its created_at. Raises RecordError."""
    if id <= 0:
        raise RecordError("invalid id; id must be a positive number", code="invalid_id")
    row = (
        db.query(RecordVersionRow)
        .filter(
            RecordVersionRow.record_id == id,
            RecordVersionRow.version == version,
        )
        .first()
    )
    if row is None:
        raise RecordError(
            f"record of id {id} does not exist or has no version {version}",
            code="not_found",
        )
    if row.data is None:
        from services.record_delta import get_record_at_version_replay
        return get_record_at_version_replay(db, id, version)
    data = json.loads(row.data)
    created_at = row.created_at.isoformat() if row.created_at else ""
    return Record(id=id, data=data), created_at


def get_record_at_time(db: Session, id: int, at: datetime) -> tuple[Record, int, str]:
    """Return record state as of a given date/time (closest version with created_at <= at).
    Returns (Record, version_number, created_at_iso). Raises RecordError if no version exists at or before that time."""
    if id <= 0:
        raise RecordError("invalid id; id must be a positive number", code="invalid_id")
    at_utc = at.astimezone(timezone.utc).replace(tzinfo=None) if at.tzinfo else at
    row = (
        db.query(RecordVersionRow)
        .filter(
            RecordVersionRow.record_id == id,
            RecordVersionRow.created_at <= at_utc,
        )
        .order_by(RecordVersionRow.created_at.desc())
        .first()
    )
    if row is None:
        raise RecordError(
            f"no version of record {id} at or before the given date/time",
            status_code=404,
            code="not_found",
        )
    if row.data is None:
        from services.record_delta import get_record_at_version_replay
        record, created_at = get_record_at_version_replay(db, id, row.version)
        return record, row.version, created_at
    data = json.loads(row.data)
    created_at = row.created_at.isoformat() if row.created_at else ""
    return Record(id=id, data=data), row.version, created_at


def list_versions(db: Session, id: int) -> list[VersionInfo]:
    """List all versions for a record (version number and created_at)."""
    if id <= 0:
        raise RecordError("invalid id; id must be a positive number", code="invalid_id")
    rows = (
        db.query(RecordVersionRow.version, RecordVersionRow.created_at)
        .filter(RecordVersionRow.record_id == id)
        .order_by(RecordVersionRow.version)
        .all()
    )
    return [
        VersionInfo(
            version=r.version,
            created_at=r.created_at.isoformat() if r.created_at else "",
        )
        for r in rows
    ]


def _next_version(db: Session, record_id: int) -> int:
    r = (
        db.query(func.coalesce(func.max(RecordVersionRow.version), 0))
        .filter(RecordVersionRow.record_id == record_id)
        .scalar()
    )
    return (r or 0) + 1


def create_or_update_versioned(db: Session, id: int, body: dict[str, Any]) -> Record:
    """Create or update record and append a new snapshot to history."""
    try:
        current = get_record(db, id)
    except RecordError as e:
        if e.code == "not_found":
            current = None
        else:
            raise

    # Body = full new document (replace). Omitted keys are removed.
    new_data = dict(body)
    if current is not None:
        replace_record(db, id, new_data)
        record = Record(id=id, data=new_data)
    else:
        create_record(db, id, new_data)
        record = Record(id=id, data=new_data)

    row = db.query(RecordRow).filter(RecordRow.id == id).first()
    version = _next_version(db, id)
    rv = RecordVersionRow(
        record_id=id,
        version=version,
        data=json.dumps(record.data),
        customer_id=row.customer_id if row else None,
    )
    db.add(rv)
    db.commit()
    return record
