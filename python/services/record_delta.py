"""
Versioned records: keyframes + deltas, minimal writes, reconstruct by replay.

See docs/VERSIONING_DESIGN.md for full design.

- v1: keyframe with full data; also set records.data once (no bulk write after that).
- Later: append only record_versions (delta = changed keys; long strings as __diff).
- Current = replay to latest_version (keyframe + apply deltas in order).
- Reconstruction: each __diff applies to the previous version's value, so "long then shorten (still long)" works.
"""
import json
from typing import Any

from diff_match_patch import diff_match_patch
from sqlalchemy.orm import Session, load_only

from db.models import Record as RecordRow, RecordVersion as RecordVersionRow
from schemas.record import Record

from services.record import RecordError, create_record, _next_version

KEYFRAME_INTERVAL = 10
LARGE_VALUE_THRESHOLD = 100  # strings longer than this in delta → store as text patch (__diff)

_dmp = diff_match_patch()


# ---- Applying patches / deltas (replay) ----

def _apply_delta(state: dict[str, Any], delta: dict[str, Any]) -> None:
    """Apply stored delta to state in place. null = delete key; __diff = apply text patch; else set key."""
    for key, value in delta.items():
        if value is None:
            state.pop(key, None)
        elif isinstance(value, dict) and value.get("__diff") and "patch" in value:
            base = state.get(key)
            if not isinstance(base, str):
                base = "" if base is None else str(base)
            patch_text = value.get("patch") or ""
            if not patch_text:
                continue
            patches = _dmp.patch_fromText(patch_text)
            if not patches:
                continue
            applied, results = _dmp.patch_apply(patches, base)
            if applied is not None and isinstance(applied, str):
                state[key] = applied
        else:
            state[key] = value


def get_record_at_version_replay(db: Session, id: int, version: int) -> tuple[Record, str]:
    """Reconstruct record at version: load latest keyframe <= version, then apply deltas in order."""
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


# ---- Building new state from request (apply request body; resolve client __diff if any) ----

def _apply_request_to_state(base: dict[str, Any], body: dict[str, Any]) -> dict[str, Any]:
    """Merge request body into base. null = delete key; {__diff, patch} = apply to base[key]; else set."""
    out = dict(base)
    for key, value in body.items():
        if value is None:
            out.pop(key, None)
        elif isinstance(value, dict) and value.get("__diff") and "patch" in value:
            b = out.get(key)
            if not isinstance(b, str):
                b = ""
            patches = _dmp.patch_fromText(value.get("patch") or "")
            if patches:
                applied, _ = _dmp.patch_apply(patches, b)
                if applied is not None and isinstance(applied, str):
                    out[key] = applied
        else:
            out[key] = value
    return out


# ---- Building stored delta (only changed keys; long strings as __diff) ----

def _build_stored_delta(old_state: dict[str, Any], new_state: dict[str, Any]) -> dict[str, Any]:
    """Delta to persist: only keys that changed. Strings over threshold → text patch."""
    minimal = {}
    for key in new_state:
        if key not in old_state or old_state[key] != new_state[key]:
            minimal[key] = new_state[key]
    for key in old_state:
        if key not in new_state:
            minimal[key] = None

    out = {}
    for key, value in minimal.items():
        if value is None:
            out[key] = None
            continue
        if not isinstance(value, str) or len(value) <= LARGE_VALUE_THRESHOLD:
            out[key] = value
            continue
        old_val = old_state.get(key)
        if not isinstance(old_val, str):
            out[key] = value
            continue
        patches = _dmp.patch_make(old_val, value)
        patch_text = _dmp.patch_toText(patches)
        if len(patch_text) < len(value):
            out[key] = {"__diff": True, "patch": patch_text}
        else:
            out[key] = value
    return out


# ---- Write path ----

def create_or_update_versioned_delta(db: Session, id: int, body: dict[str, Any]) -> Record:
    """
    Create or update record with versioning.
    - First time: create record, v1 keyframe (full data), set records.data once.
    - Later: previous state = replay to latest_version; new state = previous + body; store only delta; do not update records.data.
    """
    row = db.query(RecordRow).filter(RecordRow.id == id).first()
    if row is None:
        # Create: full document in body
        new_state = _apply_request_to_state({}, body)
        create_record(db, id, new_state)
        customer_id = None
        old_state = {}
    else:
        if row.latest_version is not None and row.latest_version >= 1:
            prev, _ = get_record_at_version_replay(db, id, row.latest_version)
            old_state = prev.data
        else:
            old_state = json.loads(row.data) if row.data else {}
        new_state = _apply_request_to_state(old_state, body)
        if new_state == old_state:
            return Record(id=id, data=new_state)
        customer_id = row.customer_id

    next_ver = _next_version(db, id)
    stored_delta = _build_stored_delta(old_state, new_state)
    delta_json = json.dumps(stored_delta) if stored_delta else "{}"
    is_keyframe = next_ver == 1 or (next_ver % KEYFRAME_INTERVAL == 0)
    full_json = json.dumps(new_state) if is_keyframe else None

    rv = RecordVersionRow(
        record_id=id,
        version=next_ver,
        data=full_json,
        delta=delta_json if stored_delta else None,
        is_keyframe=is_keyframe,
        customer_id=customer_id,
    )
    db.add(rv)
    record_row = row if row is not None else db.query(RecordRow).filter(RecordRow.id == id).first()
    record_row.latest_version = next_ver
    db.commit()
    return Record(id=id, data=new_state)
