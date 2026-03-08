"""
Delta + keyframe storage: optional module.
- Keyframe row: full `data` stored.
- Delta row: only `delta` (changed keys) stored; value null = delete key.
Read path: replay from latest keyframe + apply deltas.
Write path: compute delta; store keyframe every N versions or when delta is large.
Existing record.py stays the default; use POST /api/v2/records/{id}/delta to write with deltas.
"""
import json
from typing import Any

from sqlalchemy.orm import Session

from db.models import Record as RecordRow, RecordVersion as RecordVersionRow
from schemas.record import Record

from services.record import (
    RecordError,
    create_record,
    get_record,
    update_record,
    _next_version,
)

KEYFRAME_INTERVAL = 10
DELTA_SIZE_RATIO_THRESHOLD = 0.5


def _apply_delta(state: dict[str, Any], delta: dict[str, Any]) -> None:
    """Apply delta to state in place. value is None => delete key; else set key."""
    for key, value in delta.items():
        if value is None:
            state.pop(key, None)
        else:
            state[key] = value


def _compute_delta(old_state: dict[str, Any], new_state: dict[str, Any]) -> dict[str, Any]:
    """Only keys that changed. new_state[key]=None in delta means delete."""
    delta = {}
    for k in set(old_state) | set(new_state):
        old_v = old_state.get(k)
        new_v = new_state.get(k)
        if new_v != old_v:
            delta[k] = new_v
    return delta


def get_record_at_version_replay(db: Session, id: int, version: int) -> tuple[Record, str]:
    """
    Get record at version by replaying: find latest keyframe <= version, then apply deltas.
    Use when row.data is None (delta-only row).
    """
    rows = (
        db.query(RecordVersionRow)
        .filter(
            RecordVersionRow.record_id == id,
            RecordVersionRow.version >= 1,
            RecordVersionRow.version <= version,
        )
        .order_by(RecordVersionRow.version)
        .all()
    )
    if not rows:
        raise RecordError("record or version not found", code="not_found")

    keyframe_row = None
    for r in reversed(rows):
        if getattr(r, "is_keyframe", True) and r.data is not None:
            keyframe_row = r
            break
    if keyframe_row is None:
        raise RecordError("no keyframe found for replay", code="not_found")

    state = json.loads(keyframe_row.data)
    for r in rows:
        if r.version <= keyframe_row.version:
            continue
        if r.version > version:
            break
        delta_json = getattr(r, "delta", None)
        if delta_json:
            _apply_delta(state, json.loads(delta_json))

    last_row = next((r for r in reversed(rows) if r.version <= version), rows[-1])
    created_at = last_row.created_at.isoformat() if last_row.created_at else ""
    return Record(id=id, data=state), created_at


def create_or_update_versioned_delta(db: Session, id: int, body: dict[str, Any]) -> Record:
    """
    Create or update record; append a keyframe or delta row.
    First version = keyframe. Then every KEYFRAME_INTERVAL or when delta is large = keyframe; else delta.
    """
    try:
        current = get_record(db, id)
        old_state = dict(current.data)
    except RecordError as e:
        if e.code != "not_found":
            raise
        old_state = {}

    new_state = dict(old_state)
    for key, value in body.items():
        if value is None:
            new_state.pop(key, None)
        else:
            new_state[key] = value

    if not old_state:
        create_record(db, id, new_state)
    else:
        update_record(db, id, body)

    next_ver = _next_version(db, id)
    row = db.query(RecordRow).filter(RecordRow.id == id).first()

    full_json = json.dumps(new_state)
    delta = _compute_delta(old_state, new_state)
    delta_json = json.dumps(delta) if delta else "{}"

    is_keyframe = (
        next_ver == 1
        or (next_ver % KEYFRAME_INTERVAL == 0)
        or (len(delta_json) >= DELTA_SIZE_RATIO_THRESHOLD * len(full_json))
    )

    # Always store delta so "what changed" is available for every version (keyframe or not)
    if is_keyframe:
        rv = RecordVersionRow(
            record_id=id,
            version=next_ver,
            data=full_json,
            delta=delta_json if delta else None,
            is_keyframe=True,
            customer_id=row.customer_id if row else None,
        )
    else:
        rv = RecordVersionRow(
            record_id=id,
            version=next_ver,
            data=None,
            delta=delta_json if delta else None,
            is_keyframe=False,
            customer_id=row.customer_id if row else None,
        )

    db.add(rv)
    db.commit()
    return Record(id=id, data=new_state)
