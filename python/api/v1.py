"""V1 routes: health and records."""
from typing import Any

from fastapi import APIRouter, Depends, Path

from db import get_db
from schemas.record import Record
from services.record import create_or_update_record, get_record

router = APIRouter(prefix="/api/v1")


@router.post("/health")
def health():
    return {"ok": True}


@router.get("/records/{id}")
def get_records(
    id: int = Path(gt=0, description="Record id (positive integer)"),
    db=Depends(get_db),
) -> Record:
    return get_record(db, id)


@router.post("/records/{id}")
def post_records(
    id: int = Path(gt=0, description="Record id (positive integer)"),
    body: dict[str, Any] = ...,
    db=Depends(get_db),
) -> Record:
    return create_or_update_record(db, id, body)
