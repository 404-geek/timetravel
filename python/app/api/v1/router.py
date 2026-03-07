"""V1 routes: health and records."""
from typing import Optional

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from app.database import get_db
from app.schemas.record import Record
from app.services.record import (
    RecordAlreadyExists,
    RecordDoesNotExist,
    RecordIDInvalid,
    create_record,
    get_record,
    update_record,
)

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
    try:
        return get_record(db, id_num)
    except RecordIDInvalid:
        return _err(400, "invalid id; id must be a positive number")
    except RecordDoesNotExist:
        return _err(400, f"record of id {id_num} does not exist")


@router.post("/records/{id}")
def post_records(id: str, body: dict[str, Optional[str]], db=Depends(get_db)):
    id_num, err = _parse_id(id)
    if err:
        return err
    try:
        record = get_record(db, id_num)
    except RecordDoesNotExist:
        record = None
    except RecordIDInvalid:
        return _err(400, "invalid id; id must be a positive number")

    if record is not None:
        try:
            return update_record(db, id_num, body)
        except Exception:
            return _err(500, "internal error")
    data = {k: v for k, v in body.items() if v is not None}
    try:
        create_record(db, id_num, data)
        return Record(id=id_num, data=data)
    except Exception:
        return _err(500, "internal error")
