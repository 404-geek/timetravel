"""V2 response models."""
from typing import Any

from pydantic import BaseModel


class VersionInfo(BaseModel):
    version: int
    created_at: str


class RecordWithVersion(BaseModel):
    id: int
    version: int
    data: dict[str, Any]
    created_at: str
