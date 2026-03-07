"""Record service: get, create, update with SQLite (matches Go service)."""
import json
import sqlite3
from typing import Optional

from app.schemas.record import Record


class RecordDoesNotExist(Exception):
    pass


class RecordIDInvalid(Exception):
    pass


class RecordAlreadyExists(Exception):
    pass


def get_record(conn: sqlite3.Connection, id: int) -> Record:
    """Return record. Raises RecordIDInvalid, RecordDoesNotExist."""
    if id <= 0:
        raise RecordIDInvalid()
    row = conn.execute("SELECT data FROM records WHERE id = ?", (id,)).fetchone()
    if row is None:
        raise RecordDoesNotExist()
    data = json.loads(row[0])
    return Record(id=id, data=data)


def create_record(conn: sqlite3.Connection, id: int, data: dict[str, str]) -> None:
    """Insert record. Raises RecordIDInvalid, RecordAlreadyExists."""
    if id <= 0:
        raise RecordIDInvalid()
    try:
        conn.execute(
            "INSERT INTO records (id, data) VALUES (?, ?)",
            (id, json.dumps(data)),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        raise RecordAlreadyExists()


def update_record(
    conn: sqlite3.Connection, id: int, updates: dict[str, Optional[str]]
) -> Record:
    """Apply updates (None = delete key). Returns full record."""
    record = get_record(conn, id)
    data = dict(record.data)
    for key, value in updates.items():
        if value is None:
            data.pop(key, None)
        else:
            data[key] = value
    conn.execute("UPDATE records SET data = ? WHERE id = ?", (json.dumps(data), id))
    conn.commit()
    return Record(id=id, data=data)
