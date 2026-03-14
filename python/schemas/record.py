"""Record response (match Go)."""
from typing import Any

from pydantic import BaseModel


class Record(BaseModel):
    id: int
    data: dict[str, Any]
