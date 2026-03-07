"""
entity/record.py — data models mirroring the Go entity package.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict


@dataclass
class Record:
    """A simple record: an integer ID with a string-to-string data map."""

    id: int
    data: Dict[str, str] = field(default_factory=dict)

    def copy(self) -> "Record":
        """Return a shallow copy so callers cannot mutate stored state."""
        return Record(id=self.id, data=dict(self.data))

    def to_dict(self) -> dict:
        return {"id": self.id, "data": self.data}


@dataclass
class VersionedRecord:
    """A Record annotated with its version number and creation timestamp."""

    id: int
    version: int
    data: Dict[str, str]
    created_at: datetime

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "version": self.version,
            "data": self.data,
            "created_at": self.created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
