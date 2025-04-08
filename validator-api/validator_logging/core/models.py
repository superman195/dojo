from datetime import datetime
from typing import Dict, List

from pydantic import BaseModel


class LogEntry(BaseModel):
    timestamp: datetime
    level: str
    message: str
    labels: Dict[str, str] | None = None
    metadata: Dict[str, str] | None = None


class LogBatch(BaseModel):
    hotkey: str
    signature: str
    message: str
    logs: List[LogEntry]


class LogResponse(BaseModel):
    status: str
    message: str
    validator: str
    log_count: int | None = None
    error_details: Dict[str, str] | None = None
