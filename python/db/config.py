"""Database configuration: path and settings."""
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "records.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"
