"""SQLite connection and table."""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent.parent / "db" / "data" / "records.db"
_conn: sqlite3.Connection | None = None


def get_db() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _conn = sqlite3.connect(str(DB_PATH))
        _conn.execute("CREATE TABLE IF NOT EXISTS records (id INTEGER PRIMARY KEY, data TEXT NOT NULL)")
        _conn.execute(
            "CREATE TABLE IF NOT EXISTS record_versions (record_id INTEGER NOT NULL, version INTEGER NOT NULL, data TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT (datetime('now')), PRIMARY KEY (record_id, version))"
        )
        _conn.commit()
    return _conn
