"""V2 routes: versioned records, time-travel (snapshot) reads, list versions."""
from datetime import datetime
from typing import Annotated, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from db import get_db
from db.models import Record as RecordRow
from schemas.record import Record
from schemas.record_v2 import RecordWithVersion, VersionInfo
from services.record import (
    create_or_update_versioned,
    create_or_update_versioned_delta,
    delete_record,
    get_record,
    get_record_at_time,
    get_record_at_version,
    list_versions,
)

router = APIRouter(prefix="/api/v2")


@router.post("/health")
def health() -> dict:
    return {"ok": True}


def _parse_at(s: str) -> datetime:
    """Parse ISO 8601 datetime string (e.g. 2025-03-07T12:30:00 or 2025-03-07T12:30:00Z)."""
    s = s.strip().replace("Z", "+00:00")
    return datetime.fromisoformat(s)


@router.get("/records/{id}")
def get_record_route(
    id: int = Path(gt=0, description="Record id (positive integer)"),
    version: Annotated[
        Optional[int],
        Query(ge=1, description="Version number for time-travel; omit for latest"),
    ] = None,
    at: Annotated[
        Optional[str],
        Query(description="ISO 8601 date/time for point-in-time lookup (e.g. 2025-03-07T12:30:00Z)"),
    ] = None,
    db=Depends(get_db),
) -> Record | RecordWithVersion:
    """Get record by id. Use ?version=N for version number, ?at=... for state at a date/time, or omit for current."""
    if version is not None:
        record, created_at = get_record_at_version(db, id, version)
        return RecordWithVersion(
            id=record.id, version=version, data=record.data, created_at=created_at
        )
    if at is not None:
        try:
            dt = _parse_at(at)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid datetime for 'at': {e}")
        record, ver, created_at = get_record_at_time(db, id, dt)
        return RecordWithVersion(
            id=record.id, version=ver, data=record.data, created_at=created_at
        )
    record, latest_ver, created_at = get_record(db, id)
    if latest_ver is not None and created_at:
        return RecordWithVersion(
            id=id,
            version=latest_ver,
            data=record.data,
            created_at=created_at,
        )
    return record


@router.get("/records/{id}/versions")
def get_record_versions(
    id: int = Path(gt=0, description="Record id (positive integer)"),
    db=Depends(get_db),
) -> dict:
    """List all versions of a record (version number and created_at)."""
    versions = list_versions(db, id)
    return {
        "id": id,
        "versions": [VersionInfo(version=v.version, created_at=v.created_at) for v in versions],
    }


@router.post("/records/{id}")
def post_record(
    body: dict[str, Any],
    id: int = Path(gt=0, description="Record id (positive integer)"),
    db=Depends(get_db),
) -> Record:
    """Create or update record. New state is stored as latest version; history preserved."""
    return create_or_update_versioned(db, id, body)


@router.post("/records/{id}/delta")
def post_record_delta(
    body: dict[str, Any],
    id: int = Path(gt=0, description="Record id (positive integer)"),
    db=Depends(get_db),
) -> Record:
    """
    Apply a patch (delta only). Send only changed keys; value null = delete that key.
    Use {"__clear": true} to clear all keys. Stores only the patch per version (small, constant-size).
    First version: body = full document.
    """
    return create_or_update_versioned_delta(db, id, body)


@router.delete("/records/{id}")
def delete_record_route(
    id: int = Path(gt=0, description="Record id (positive integer)"),
    db=Depends(get_db),
) -> dict:
    """Delete a record and all its version history."""
    delete_record(db, id)
    return {"deleted": id}
