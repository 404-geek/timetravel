"""
service/record.py — SQLite-backed record service mirroring the Go service package.

Every create or update appends a new row to ``record_versions``, giving a
complete, immutable audit trail.  The schema is identical to the Go
implementation so that the same ``records.db`` file can be used by both.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional

from entity.record import Record, VersionedRecord


# ---------------------------------------------------------------------------
# Sentinel errors (mirror Go sentinel errors)
# ---------------------------------------------------------------------------


class RecordDoesNotExistError(Exception):
    pass


class RecordIDInvalidError(Exception):
    pass


class RecordAlreadyExistsError(Exception):
    pass


class RecordVersionDoesNotExistError(Exception):
    pass


# ---------------------------------------------------------------------------
# SQLiteRecordService
# ---------------------------------------------------------------------------

_MIGRATE_SQL = """
CREATE TABLE IF NOT EXISTS record_versions (
    id         INTEGER NOT NULL,
    version    INTEGER NOT NULL,
    data       TEXT    NOT NULL,
    created_at TEXT    NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (id, version)
)
"""


def _parse_created_at(raw: str) -> datetime:
    """Parse SQLite's ``datetime('now')`` output (``YYYY-MM-DD HH:MM:SS``)."""
    dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=timezone.utc)


def _row_to_versioned_record(row: tuple) -> VersionedRecord:
    record_id, ver, data_str, created_at_str = row
    return VersionedRecord(
        id=record_id,
        version=ver,
        data=json.loads(data_str),
        created_at=_parse_created_at(created_at_str),
    )


class SQLiteRecordService:
    """
    Persistent, versioned record storage backed by SQLite.

    Thread safety: sqlite3 connections are *not* thread-safe, so each method
    acquires a lock before touching the database, matching the Go
    implementation's behaviour (single-process server with goroutine-safe
    access via database/sql's connection pool).
    """

    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.Lock()
        self._migrate()

    def close(self) -> None:
        self._conn.close()

    def _migrate(self) -> None:
        with self._lock:
            self._conn.execute(_MIGRATE_SQL)
            self._conn.commit()

    # ------------------------------------------------------------------
    # RecordService methods
    # ------------------------------------------------------------------

    def get_record(self, record_id: int) -> Record:
        """Return the latest state of the record or raise RecordDoesNotExistError."""
        vr = self.get_versioned_record(record_id, version=0)
        return Record(id=vr.id, data=vr.data)

    def create_record(self, record: Record) -> None:
        """
        Insert *record* as version 1.

        Raises:
            RecordIDInvalidError: if id <= 0.
            RecordAlreadyExistsError: if a record with that id already exists.
        """
        if record.id <= 0:
            raise RecordIDInvalidError("record id must be > 0")

        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO record_versions (id, version, data) VALUES (?, 1, ?)",
                    (record.id, json.dumps(record.data)),
                )
                self._conn.commit()
            except sqlite3.IntegrityError:
                raise RecordAlreadyExistsError(
                    f"record with id {record.id} already exists"
                )

    def update_record(
        self, record_id: int, updates: Dict[str, Optional[str]]
    ) -> Record:
        """
        Apply *updates* to the latest version of the record and persist the
        result as a new version row.

        Keys mapped to ``None`` are deleted from the record's data map.

        Raises:
            RecordIDInvalidError: if record_id <= 0.
            RecordDoesNotExistError: if no record with that id exists.
        """
        if record_id <= 0:
            raise RecordIDInvalidError("record id must be > 0")

        with self._lock:
            cur = self._conn.execute(
                "SELECT data, version FROM record_versions "
                "WHERE id = ? ORDER BY version DESC LIMIT 1",
                (record_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise RecordDoesNotExistError(
                    f"record with id {record_id} does not exist"
                )

            current_data: Dict[str, str] = json.loads(row[0])
            max_version: int = row[1]

            for key, value in updates.items():
                if value is None:
                    current_data.pop(key, None)
                else:
                    current_data[key] = value

            self._conn.execute(
                "INSERT INTO record_versions (id, version, data) VALUES (?, ?, ?)",
                (record_id, max_version + 1, json.dumps(current_data)),
            )
            self._conn.commit()

        return Record(id=record_id, data=current_data)

    # ------------------------------------------------------------------
    # VersionedRecordService methods
    # ------------------------------------------------------------------

    def get_versioned_record(self, record_id: int, version: int = 0) -> VersionedRecord:
        """
        Return the record at *version*.  If *version* <= 0, return the latest.

        Raises:
            RecordIDInvalidError: if record_id <= 0.
            RecordDoesNotExistError: if no record exists (and version <= 0).
            RecordVersionDoesNotExistError: if that specific version does not exist.
        """
        if record_id <= 0:
            raise RecordIDInvalidError("record id must be > 0")

        with self._lock:
            if version <= 0:
                cur = self._conn.execute(
                    "SELECT id, version, data, created_at FROM record_versions "
                    "WHERE id = ? ORDER BY version DESC LIMIT 1",
                    (record_id,),
                )
            else:
                cur = self._conn.execute(
                    "SELECT id, version, data, created_at FROM record_versions "
                    "WHERE id = ? AND version = ?",
                    (record_id, version),
                )

            row = cur.fetchone()

        if row is None:
            if version > 0:
                raise RecordVersionDoesNotExistError(
                    f"record {record_id} at version {version} does not exist"
                )
            raise RecordDoesNotExistError(
                f"record with id {record_id} does not exist"
            )

        return _row_to_versioned_record(row)

    def list_record_versions(self, record_id: int) -> List[VersionedRecord]:
        """
        Return all versions of *record_id* in ascending order (oldest first).

        Raises:
            RecordIDInvalidError: if record_id <= 0.
            RecordDoesNotExistError: if no record with that id exists.
        """
        if record_id <= 0:
            raise RecordIDInvalidError("record id must be > 0")

        with self._lock:
            cur = self._conn.execute(
                "SELECT id, version, data, created_at FROM record_versions "
                "WHERE id = ? ORDER BY version ASC",
                (record_id,),
            )
            rows = cur.fetchall()

        if not rows:
            raise RecordDoesNotExistError(
                f"record with id {record_id} does not exist"
            )

        return [_row_to_versioned_record(r) for r in rows]
