"""V2 routes: versioned records, time-travel (snapshot) reads, list versions."""
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from app.database import get_db
from app.schemas.record import Record
from app.schemas.record_v2 import RecordWithVersion, VersionInfo
from app.services.record import RecordDoesNotExist, RecordIDInvalid
from app.services.record_v2 import (
    create_or_update_versioned,
    get_record_at_version,
    get_record_current,
    list_versions,
)

router = APIRouter(prefix="/api/v2")


def _err(status: int, message: str):
    return JSONResponse(status_code=status, content={"error": message})


def _parse_id(id: str) -> tuple[int | None, JSONResponse | None]:
    try:
        n = int(id)
        return (n, None) if n > 0 else (None, _err(400, "invalid id; id must be a positive number"))
    except ValueError:
        return None, _err(400, "invalid id; id must be a positive number")


@router.post("/health")
def health():
    return {"ok": True}


@router.get("/records/{id}", response_model=None)
def get_record(
    id: str,
    db=Depends(get_db),
    version: Optional[int] = Query(None, description="Version number for time-travel; omit for latest"),
):
    """
    Get record by id. Use ?version=N for a snapshot at that version (time-travel);
    omit for current state.
    """
    id_num, err = _parse_id(id)
    if err:
        return err
    if version is not None:
        if version < 1:
            return _err(400, "version must be a positive number")
        try:
            record, created_at = get_record_at_version(db, id_num, version)
            return RecordWithVersion(
                id=record.id, version=version, data=record.data, created_at=created_at
            )
        except RecordIDInvalid:
            return _err(400, "invalid id; id must be a positive number")
        except RecordDoesNotExist:
            return _err(400, f"record of id {id_num} does not exist or has no version {version}")
    try:
        record = get_record_current(db, id_num)
        return record
    except RecordIDInvalid:
        return _err(400, "invalid id; id must be a positive number")
    except RecordDoesNotExist:
        return _err(400, f"record of id {id_num} does not exist")


@router.get("/records/{id}/versions")
def get_record_versions(id: str, db=Depends(get_db)):
    """List all versions of a record (version number and created_at)."""
    id_num, err = _parse_id(id)
    if err:
        return err
    try:
        get_record_current(db, id_num)
    except RecordIDInvalid:
        return _err(400, "invalid id; id must be a positive number")
    except RecordDoesNotExist:
        return _err(400, f"record of id {id_num} does not exist")
    try:
        versions = list_versions(db, id_num)
    except RecordIDInvalid:
        return _err(400, "invalid id; id must be a positive number")
    return {"id": id_num, "versions": [VersionInfo(version=v.version, created_at=v.created_at) for v in versions]}


@router.post("/records/{id}")
def post_record(id: str, body: dict[str, Optional[str]], db=Depends(get_db)):
    """
    Create or update record (same body as v1). New state is stored as the latest version;
    previous snapshots are preserved for time-travel.
    """
    id_num, err = _parse_id(id)
    if err:
        return err
    try:
        record = create_or_update_versioned(db, id_num, body)
        return Record(id=record.id, data=record.data)
    except RecordIDInvalid:
        return _err(400, "invalid id; id must be a positive number")
    except Exception:
        return _err(500, "internal error")
