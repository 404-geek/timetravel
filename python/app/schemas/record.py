"""Record response (match Go)."""
from pydantic import BaseModel


class Record(BaseModel):
    id: int
    data: dict[str, str]
