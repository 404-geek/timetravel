"""Record service: current state, versioned (time-travel), and delta updates. Single module."""
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from diff_match_patch import diff_match_patch
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, load_only

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


KEYFRAME_INTERVAL = 10
LARGE_VALUE_THRESHOLD = 100
_dmp = diff_match_patch()


def create_record(
    db: Session,
    id: int,
    data: dict[str, Any],
    customer_id: Optional[int] = None,
) -> None:
    """Insert record. Raises RecordError. latest_version left None until first version row."""
    if id <= 0:
        raise RecordError("invalid id; id must be a positive number", code="invalid_id")
    row = RecordRow(id=id, data=json.dumps(data), latest_version=None, customer_id=customer_id)
    try:
        db.add(row)
        db.commit()
    except IntegrityError:
        db.rollback()
        raise RecordError("record already exists", code="already_exists")


def _next_version(db: Session, record_id: int) -> int:
    r = (
        db.query(func.coalesce(func.max(RecordVersionRow.version), 0))
        .filter(RecordVersionRow.record_id == record_id)
        .scalar()
    )
    return (r or 0) + 1


def _apply_delta(state: dict[str, Any], delta: dict[str, Any]) -> None:
    for key, value in delta.items():
        if value is None:
            state.pop(key, None)
            continue
        if isinstance(value, dict) and value.get("__diff") and "patch" in value:
            base = state.get(key)
            base = base if isinstance(base, str) else ("" if base is None else str(base))
            patches = _dmp.patch_fromText(value.get("patch") or "")
            if patches:
                applied, _ = _dmp.patch_apply(patches, base)
                if applied is not None:
                    state[key] = applied
            continue
        state[key] = value


def get_record_at_version_replay(db: Session, id: int, version: int) -> tuple[Record, str]:
    """Reconstruct record at version: latest keyframe <= version, then apply deltas in order."""
    keyframe_row = (
        db.query(RecordVersionRow)
        .filter(
            RecordVersionRow.record_id == id,
            RecordVersionRow.version >= 1,
            RecordVersionRow.version <= version,
            RecordVersionRow.is_keyframe.is_(True),
            RecordVersionRow.data.isnot(None),
        )
        .order_by(RecordVersionRow.version.desc())
        .first()
    )
    if keyframe_row is None:
        raise RecordError("record or version not found", code="not_found")

    state = json.loads(keyframe_row.data or "{}")
    if keyframe_row.version == version:
        created_at = keyframe_row.created_at.isoformat() if keyframe_row.created_at else ""
        return Record(id=id, data=state), created_at

    delta_rows = (
        db.query(RecordVersionRow)
        .options(load_only(RecordVersionRow.version, RecordVersionRow.delta, RecordVersionRow.created_at))
        .filter(
            RecordVersionRow.record_id == id,
            RecordVersionRow.version > keyframe_row.version,
            RecordVersionRow.version <= version,
        )
        .order_by(RecordVersionRow.version)
        .all()
    )
    for r in delta_rows:
        delta_json = getattr(r, "delta", None)
        if delta_json:
            _apply_delta(state, json.loads(delta_json))

    last_row = delta_rows[-1] if delta_rows else keyframe_row
    created_at = last_row.created_at.isoformat() if last_row.created_at else ""
    return Record(id=id, data=state), created_at


def _apply_request_to_state(base: dict[str, Any], body: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in body.items():
        if value is None:
            out.pop(key, None)
            continue
        if isinstance(value, dict) and value.get("__diff") and "patch" in value:
            b = out.get(key)
            b = b if isinstance(b, str) else ""
            patches = _dmp.patch_fromText(value.get("patch") or "")
            if patches:
                applied, _ = _dmp.patch_apply(patches, b)
                if applied is not None:
                    out[key] = applied
            continue
        out[key] = value
    return out


def _build_stored_delta(old_state: dict[str, Any], new_state: dict[str, Any]) -> dict[str, Any]:
    minimal = {k: new_state[k] for k in new_state if k not in old_state or old_state[k] != new_state[k]}
    minimal.update({k: None for k in old_state if k not in new_state})

    out = {}
    for key, value in minimal.items():
        if value is None:
            out[key] = None
        elif not isinstance(value, str) or len(value) <= LARGE_VALUE_THRESHOLD:
            out[key] = value
        elif not isinstance(old_state.get(key), str):
            out[key] = value
        else:
            patch_text = _dmp.patch_toText(_dmp.patch_make(old_state[key], value))
            out[key] = {"__diff": True, "patch": patch_text} if len(patch_text) < len(value) else value
    return out


def create_or_update_versioned_delta(db: Session, id: int, body: dict[str, Any]) -> Record:
    """Create or update with versioning (delta path). Body {"__clear": true} clears all keys."""
    clear_all = body.get("__clear") is True
    body_clean = {k: v for k, v in body.items() if k != "__clear"}
    row = db.query(RecordRow).filter(RecordRow.id == id).first()

    if row is None:
        new_state = {} if clear_all else _apply_request_to_state({}, body_clean)
        create_record(db, id, new_state)
        old_state, customer_id = {}, None
    else:
        old_state = (
            get_record_at_version_replay(db, id, row.latest_version)[0].data
            if row.latest_version and row.latest_version >= 1
            else (json.loads(row.data) if row.data else {})
        )
        new_state = {} if clear_all else _apply_request_to_state(old_state, body_clean)
        if new_state == old_state:
            return Record(id=id, data=new_state)
        customer_id = row.customer_id

    next_ver = _next_version(db, id)
    stored_delta = _build_stored_delta(old_state, new_state)
    is_keyframe = next_ver == 1 or (next_ver % KEYFRAME_INTERVAL == 0)
    rv = RecordVersionRow(
        record_id=id,
        version=next_ver,
        data=json.dumps(new_state) if is_keyframe else None,
        delta=json.dumps(stored_delta) if stored_delta else None,
        is_keyframe=is_keyframe,
        customer_id=customer_id,
    )
    db.add(rv)
    record_row = row or db.query(RecordRow).filter(RecordRow.id == id).first()
    record_row.latest_version = next_ver
    if is_keyframe:
        record_row.data = json.dumps(new_state)
    db.commit()
    return Record(id=id, data=new_state)


def get_record(db: Session, id: int) -> Record:
    """Return record. Raises RecordError."""
    if id <= 0:
        raise RecordError("invalid id; id must be a positive number", code="invalid_id")
    row = db.query(RecordRow).filter(RecordRow.id == id).first()
    if row is None:
        raise RecordError(f"record of id {id} does not exist", code="not_found")
    if row.latest_version is None or row.latest_version < 1:
        return Record(id=row.id, data=json.loads(row.data) if row.data else {})
    if row.data and (row.latest_version == 1 or row.latest_version % KEYFRAME_INTERVAL == 0):
        return Record(id=row.id, data=json.loads(row.data))
    record, _ = get_record_at_version_replay(db, id, row.latest_version)
    return record


def get_record_at_version(db: Session, id: int, version: int) -> tuple[Record, str]:
    """Return snapshot of record at given version and its created_at. Raises RecordError."""
    if id <= 0:
        raise RecordError("invalid id; id must be a positive number", code="invalid_id")
    row = (
        db.query(RecordVersionRow)
        .filter(RecordVersionRow.record_id == id, RecordVersionRow.version == version)
        .first()
    )
    if row is None:
        raise RecordError(f"record of id {id} does not exist or has no version {version}", code="not_found")
    if row.data is None:
        return get_record_at_version_replay(db, id, version)
    created_at = row.created_at.isoformat() if row.created_at else ""
    return Record(id=id, data=json.loads(row.data)), created_at


def get_record_at_time(db: Session, id: int, at: datetime) -> tuple[Record, int, str]:
    """Return record state as of a given date/time. Raises RecordError if no version at or before that time."""
    if id <= 0:
        raise RecordError("invalid id; id must be a positive number", code="invalid_id")
    at_utc = at.astimezone(timezone.utc).replace(tzinfo=None) if at.tzinfo else at
    row = (
        db.query(RecordVersionRow)
        .filter(RecordVersionRow.record_id == id, RecordVersionRow.created_at <= at_utc)
        .order_by(RecordVersionRow.created_at.desc())
        .first()
    )
    if row is None:
        raise RecordError(f"no version of record {id} at or before the given date/time", status_code=404, code="not_found")
    if row.data is None:
        record, created_at = get_record_at_version_replay(db, id, row.version)
        return record, row.version, created_at
    created_at = row.created_at.isoformat() if row.created_at else ""
    return Record(id=id, data=json.loads(row.data)), row.version, created_at


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
        VersionInfo(version=r.version, created_at=r.created_at.isoformat() if r.created_at else "")
        for r in rows
    ]


def create_or_update_record(db: Session, id: int, body: dict[str, Any]) -> Record:
    """Create or update record only (no version history). Body = full document. For v1 API."""
    new_data = dict(body)
    row = db.query(RecordRow).filter(RecordRow.id == id).first()
    if row is None:
        create_record(db, id, new_data)
    else:
        row.data = json.dumps(new_data)
        db.commit()
    return Record(id=id, data=new_data)


def create_or_update_versioned(db: Session, id: int, body: dict[str, Any]) -> Record:
    """Create or update and append a new snapshot to history. Body = full document. For v2 POST full body."""
    new_data = dict(body)
    row = db.query(RecordRow).filter(RecordRow.id == id).first()
    if row is None:
        create_record(db, id, new_data)
    version = _next_version(db, id)
    rv = RecordVersionRow(
        record_id=id,
        version=version,
        data=json.dumps(new_data),
        customer_id=row.customer_id if row else None,
    )
    db.add(rv)
    record_row = row or db.query(RecordRow).filter(RecordRow.id == id).first()
    record_row.latest_version = version
    db.commit()
    return Record(id=id, data=new_data)


def delete_record(db: Session, id: int) -> None:
    """Delete a record and all its versions. Raises RecordError if id invalid or record not found."""
    if id <= 0:
        raise RecordError("invalid id; id must be a positive number", code="invalid_id")
    row = db.query(RecordRow).filter(RecordRow.id == id).first()
    if row is None:
        raise RecordError(f"record of id {id} does not exist", status_code=404, code="not_found")
    db.delete(row)
    db.commit()
