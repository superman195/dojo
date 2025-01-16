"""
analytics.py
    this file is called periodically by main_validator.py to:
    1. query validator database for analytics data
    2. uploading analytics data to analytics API
"""

import datetime
import gc
import json
from datetime import datetime, timedelta, timezone
from typing import List, TypedDict

import httpx
from bittensor.utils.btlogging import logging as logger

from commons.orm import ORM
from commons.utils import datetime_as_utc
from database.client import connect_db
from dojo import TASK_DEADLINE

"""
tasks:  task[]A list of task objects 
    miner_hotkey: MinerResponse.hotkey
    miner_coldkey: MinerResponse.coldkey
    validator_task_id: ValidatorTask.id
    validator_hotkey: bt.wallet
    validator_coldkey: bt.wallet
    dojo_task_id: MinerResponse.dojo_task_id
    completions: MinerResponse.task_result?
    miner_response_score: MinerScore.scores
    synthetic_ground_truth_rank: GroundTruth.rank_id
    synthetic_ground_truth_score: GroundTruth.ground_truth_score
    miner_score: object The final miner score.
    created_at: ValidatorTask.created_at
    metadata: ValidatorTask.metadata
task_count:  int(optional, default: 1) Specifies the number of tasks in the payload.
signature:  objectA Bittensor signature from the validator that proves ownership of its hotkey.
message:  objectA message object that was signed by the signature
"""


class AnalyticsPayload(TypedDict):
    miner_hotkey: str
    miner_coldkey: str
    validator_task_id: str
    validator_hotkey: str
    completions: List[dict]
    miner_score: List[dict]
    created_at: str
    metadata: dict | None


async def _get_task_data(validator_hotkey: str) -> dict[str, list]:
    """
    - queries db for processed ValidatorTasks in a given time window
    - serializes task data as a JSON
    - returns singleton dictionary with key "tasks" and value as list of JSONs, where individual JSONs is 1 task.

    @to-do: confirm what time window we want to use in production. 6 hours?
    """
    processed_tasks = []
    await connect_db()

    from_24_hours = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(hours=24)
    from_1_hours = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(hours=1)
    from_3_mins = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(
        seconds=TASK_DEADLINE
    )
    to_now = datetime_as_utc(datetime.now(timezone.utc))

    # get processed tasks in batches from db
    async for task_batch, has_more_batches in ORM.get_processed_tasks(
        expire_from=from_24_hours, expire_to=to_now
    ):
        if task_batch is None:
            continue
        # parse each task into a dictionary, serialize any nested objects
        for task in task_batch:
            if task.miner_responses is None:
                continue
            for miner_response in task.miner_responses:
                task_payload = AnalyticsPayload(
                    miner_hotkey=miner_response.hotkey,
                    miner_coldkey=miner_response.coldkey,
                    validator_task_id=task.id,
                    validator_hotkey=validator_hotkey,
                    completions=[
                        completion.model_dump(mode="json")
                        for completion in task.completions
                    ]
                    if task.completions
                    else [],
                    miner_score=[
                        score.model_dump(mode="json") for score in miner_response.scores
                    ]
                    if miner_response.scores
                    else [],
                    created_at=task.created_at.isoformat(),
                    metadata=json.loads(task.metadata) if task.metadata else None,
                )
                processed_tasks.append(task_payload)

        if not has_more_batches:
            logger.success(
                "Finished collecting processed tasks, exiting task monitoring."
            )
            gc.collect()
            break
    # save payload to file for testing, remove in prod
    with open("payload.json", "w") as f:
        json.dump(processed_tasks, f, indent=2)

    payload = {"tasks": processed_tasks}
    return payload


async def _post_task_data(payload, hotkey, signature):
    """
    - posts task data to analytics API
    - returns response from analytics API
    """
    TIMEOUT = 15.0
    _http_client = httpx.AsyncClient()
    ANALYTICS_URL = "http://127.0.0.1:8000"

    try:
        response = await _http_client.post(
            url=f"{ANALYTICS_URL}/api/v1/analytics/validators/{hotkey}/tasks",
            json=payload,
            headers={
                "X-Hotkey": hotkey,
                "X-Signature": signature,
                "Content-Type": "application/json",
            },
            timeout=TIMEOUT,
        )
        if response.status_code == 200:
            logger.success(f"Successfully posted task data for hotkey: {hotkey}")
    except Exception as e:
        logger.error(
            f"Error occurred when trying to post task data: {str(e)}", exc_info=True
        )
        return None


async def upload_analytics_data(validator_hotkey: str):
    """
    public function to be called by main_validator.py every X hours
    triggers the querying and uploading of analytics data to analytics API
    """
    payload = await _get_task_data(validator_hotkey)
    try:
        response = await _post_task_data(
            payload=payload, hotkey=validator_hotkey, signature="signature"
        )
        return response
    except Exception as e:
        logger.error(f"Error occurred when upload_analytics_data() {e}")
        return None


if __name__ == "__main__":
    import asyncio

    async def main():
        res = await upload_analytics_data("hotkey")
        print(f"Response: {res}")

    asyncio.run(main())
