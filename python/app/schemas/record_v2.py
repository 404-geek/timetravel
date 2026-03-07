"""V2 response models: versioned record and version list."""
from pydantic import BaseModel


class VersionInfo(BaseModel):
    version: int
    created_at: str


class RecordWithVersion(BaseModel):
    """Record snapshot with version metadata (for time-travel responses)."""
    id: int
    version: int
    data: dict[str, str]
    created_at: str | None = None
