"""V1 routes: health and records."""
from typing import Any, Optional

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from db import get_db
from schemas.record import Record
from services.record import RecordError, create_or_update_versioned, get_record

router = APIRouter(prefix="/api/v1")


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


@router.get("/records/{id}")
def get_records(id: str, db=Depends(get_db)):
    id_num, err = _parse_id(id)
    if err:
        return err
    return get_record(db, id_num)


@router.post("/records/{id}")
def post_records(id: str, body: dict[str, Any], db=Depends(get_db)):
    id_num, err = _parse_id(id)
    if err:
        return err
    record = create_or_update_versioned(db, id_num, body)
    return Record(id=record.id, data=record.data)
