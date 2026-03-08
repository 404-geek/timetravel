"""
Delta + keyframe storage: optional module.
- Keyframe row: full `data` stored.
- Delta row: only `delta` (changed keys) stored; value null = delete key.
Read path: replay from latest keyframe + apply deltas.
Write path: compute delta; store keyframe every N versions or when delta is large.

Optimizations for large JSON/text:
- _compute_delta: two passes over keys only (no set union); identity check before equality.
- get_record_at_version_replay: two queries; keyframe row has full data, delta rows load_only(version, delta, created_at) so we never load full data for non-keyframe rows.
- create_or_update_versioned_delta: full_json = json.dumps(new_state) only when storing a keyframe (ver 1, ver%10, or delta size threshold); delta-only rows skip full serialization.

Use POST /api/v2/records/{id}/delta to write with deltas.
"""
import json
from typing import Any

from sqlalchemy.orm import Session, load_only

from db.models import Record as RecordRow, RecordVersion as RecordVersionRow
from schemas.record import Record

from services.record import (
    RecordError,
    create_record,
    get_record,
    replace_record,
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
    """
    Only keys that changed. new_state[key]=None in delta means delete.
    Optimized for large dicts: no full set union; two passes over keys only.
    """
    delta = {}
    for k in old_state:
        if k not in new_state:
            delta[k] = None
        elif old_state[k] is not new_state[k] and old_state[k] != new_state[k]:
            delta[k] = new_state[k]
    for k in new_state:
        if k not in old_state:
            delta[k] = new_state[k]
    return delta


def get_record_at_version_replay(db: Session, id: int, version: int) -> tuple[Record, str]:
    """
    Get record at version by replaying: find latest keyframe <= version, then apply deltas.
    Optimized for large history: one query for keyframe (full data), one for delta rows only
    (version, delta, created_at) so we never load full data for non-keyframe rows.
    """
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

    state = json.loads(keyframe_row.data)
    if keyframe_row.version == version:
        created_at = keyframe_row.created_at.isoformat() if keyframe_row.created_at else ""
        return Record(id=id, data=state), created_at

    delta_rows = (
        db.query(RecordVersionRow)
        .options(
            load_only(
                RecordVersionRow.version,
                RecordVersionRow.delta,
                RecordVersionRow.created_at,
            )
        )
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

    # Body = full new document (replace). Omitted keys are removed; delta will record deletions.
    new_state = dict(body)

    if not old_state:
        create_record(db, id, new_state)
    else:
        replace_record(db, id, new_state)

    next_ver = _next_version(db, id)
    row = db.query(RecordRow).filter(RecordRow.id == id).first()

    delta = _compute_delta(old_state, new_state)
    delta_json = json.dumps(delta) if delta else "{}"

    is_keyframe = next_ver == 1 or (next_ver % KEYFRAME_INTERVAL == 0)
    full_json = None
    if not is_keyframe:
        full_json = json.dumps(new_state)
        if len(delta_json) >= DELTA_SIZE_RATIO_THRESHOLD * len(full_json):
            is_keyframe = True
    if is_keyframe and full_json is None:
        full_json = json.dumps(new_state)

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
