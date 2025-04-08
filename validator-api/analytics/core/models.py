from datetime import datetime
from typing import List

from pydantic import BaseModel


class AnalyticsData(BaseModel):
    validator_task_id: str
    validator_hotkey: str
    prompt: str
    completions: List[dict]
    ground_truths: List[dict]
    miner_responses: List[dict]
    scored_hotkeys: List[str]
    absent_hotkeys: List[str]
    created_at: str
    updated_at: str
    metadata: dict | None = None


class AnalyticsPayload(BaseModel):
    tasks: List[AnalyticsData]


class ErrorDetails(BaseModel):
    error_type: str
    error_message: str
    traceback: str


class AnalyticsSuccessResponse(BaseModel):
    success: bool = True
    message: str
    timestamp: datetime
    task_count: int


class AnalyticsErrorResponse(BaseModel):
    success: bool = False
    message: str
    timestamp: datetime
    error: str
    details: ErrorDetails
