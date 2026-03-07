"""Versioned record service: snapshot history and time-travel reads."""
import json
import sqlite3
from dataclasses import dataclass
from typing import Optional

from app.schemas.record import Record
from app.services.record import (
    RecordDoesNotExist,
    RecordIDInvalid,
    create_record,
    get_record,
    update_record,
)


@dataclass
class VersionInfo:
    version: int
    created_at: str


def get_record_current(conn: sqlite3.Connection, id: int) -> Record:
    """Return current record (same as v1 get_record)."""
    return get_record(conn, id)


def get_record_at_version(conn: sqlite3.Connection, id: int, version: int) -> tuple[Record, str]:
    """Return snapshot of record at given version and its created_at. Raises RecordIDInvalid, RecordDoesNotExist."""
    if id <= 0:
        raise RecordIDInvalid()
    row = conn.execute(
        "SELECT data, created_at FROM record_versions WHERE record_id = ? AND version = ?",
        (id, version),
    ).fetchone()
    if row is None:
        raise RecordDoesNotExist()
    data = json.loads(row[0])
    created_at = row[1] or ""
    return Record(id=id, data=data), created_at


def list_versions(conn: sqlite3.Connection, id: int) -> list[VersionInfo]:
    """List all versions for a record (version number and created_at)."""
    if id <= 0:
        raise RecordIDInvalid()
    rows = conn.execute(
        "SELECT version, created_at FROM record_versions WHERE record_id = ? ORDER BY version ASC",
        (id,),
    ).fetchall()
    return [VersionInfo(version=row[0], created_at=row[1]) for row in rows]


def _next_version(conn: sqlite3.Connection, record_id: int) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) FROM record_versions WHERE record_id = ?",
        (record_id,),
    ).fetchone()
    return (row[0] or 0) + 1


def create_or_update_versioned(
    conn: sqlite3.Connection, id: int, body: dict[str, Optional[str]]
) -> Record:
    """
    Create or update record and append a new snapshot to history.
    Same semantics as v1 POST; in addition, inserts a row into record_versions.
    """
    try:
        current = get_record(conn, id)
    except RecordDoesNotExist:
        current = None
    except RecordIDInvalid:
        raise

    if current is not None:
        record = update_record(conn, id, body)
    else:
        data = {k: v for k, v in body.items() if v is not None}
        create_record(conn, id, data)
        record = Record(id=id, data=data)

    version = _next_version(conn, id)
    conn.execute(
        "INSERT INTO record_versions (record_id, version, data) VALUES (?, ?, ?)",
        (id, version, json.dumps(record.data)),
    )
    conn.commit()
    return record
