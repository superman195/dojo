"""
analytics_endpoint.py
- is the endpoint that receives task data from analytics.py
- validates incoming payload against Task schema
@to-do:
    - add proper error and success responses
    - convert payload to parquet
    - enable CORS
"""

from typing import List

import uvicorn
from fastapi import FastAPI, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel


class Task(BaseModel):
    miner_hotkey: str
    miner_coldkey: str
    validator_task_id: str
    validator_hotkey: str
    completions: List[dict]
    miner_score: List[dict]
    created_at: str
    metadata: dict | None


class AnalyticsPayload(BaseModel):
    tasks: List[Task]


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/api/v1/analytics/validators/{hotkey}/tasks")
async def create_validator_task(
    hotkey: str,
    data: AnalyticsPayload,
    x_hotkey: str | None = Header(None, alias="X-Hotkey"),
    x_signature: str | None = Header(None, alias="X-Signature"),
):
    """
    This route receives task data from analytics.py and validates against Task schema.
    """
    try:  # Validate headers
        if not x_hotkey or not x_signature:
            response = {
                "error": "Missing required headers",
                "details": {
                    "x_hotkey": "missing" if not x_hotkey else "present",
                    "x_signature": "missing" if not x_signature else "present",
                },
            }
            return JSONResponse(content=response, status_code=400)

        # Get raw JSON data from request
        request = data.model_dump()
        response = {
            "message": "Task data received",
            "hotkey": hotkey,
            "data": request,
        }
        # print(response)
        return JSONResponse(content=response, status_code=200)
    except Exception as e:
        import traceback

        response = {
            "error": "Failed to process request",
            "details": {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
            },
        }
        return JSONResponse(content=response, status_code=400)


if __name__ == "__main__":
    uvicorn.run("analytics_endpoint:app", host="0.0.0.0", port=8000, reload=True)
