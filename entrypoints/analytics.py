"""
analytics.py
    use a job scheduler (ie. apscheduler) to periodically:
    use prisma to query validator db for task data
    use httpx library to POST data to analytics API 
"""
import httpx
import gc
from bittensor.utils.btlogging import logging as logger
from typing import TypedDict, Optional, List
from commons.orm import ORM,

# 1. schedule job to periodically upload data to API
# 2. query validator db for task data
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
class TaskPayload(TypedDict):
    miner_hotkey: str
    miner_coldkey: str
    validator_task_id: str
    validator_hotkey: str
    completions: List[dict]
    miner_score: List[dict]
    created_at: str
    metadata: Optional[dict]

async def _get_task_data():   
    # fetch validator tasks  that have been scored and are younger than 7 hours?
    # need to ensure that we do not fetch tasks that were already sent to analytics API.
    # six_hours_ago = datetime.datetime.now() - datetime.timedelta(hours=6)
    # current_time = datetime.datetime.now()

    processed_tasks = []
    async for task_batch, has_more_batches in ORM.get_processed_tasks():
        for task in task_batch:
            for miner_response in task.miner_responses:
                task_payload = TaskPayload(
                    miner_hotkey=miner_response.hotkey,
                    miner_coldkey=miner_response.coldkey,
                    validator_task_id=task.id,
                    validator_hotkey=task.validator_hotkey,
                    completions=task.completions,
                    miner_score=task.miner_responses.scores,
                    created_at=task.created_at,
                    metadata=task.metadata,
                )
                processed_tasks.append(task_payload)

        if not has_more_batches:
            logger.success(
                "Finished collecting processed tasks, exiting task monitoring."
            )
            gc.collect()
            break

    payload = {
        "tasks": processed_tasks,
    }
    return payload
    # validator_tasks = await prisma.validatortask.find_many(
    #     where={
    #         "is_processed": True,
    #     },
    #     include={
    #         "completions": {
    #             "include": {
    #                 "Criterion": True  # Include nested relationships
    #             }
    #         },
    #         "miner_responses": {
    #             "include": {
    #                 "scores": True  # Include nested relationships
    #             }
    #         },
    #         "ground_truth": True,
    #         # "parent_task": True,  # Uncomment if needed
    #         # "child_tasks": True,  # Uncomment if needed
    #     },
    #     order_by={
    #         "created_at": "desc"
    #     }
    # )

    # return validator_tasks





# async def parse_task_data(validator_tasks) -> List[TaskPayload]:
#     raw_tasks = await _get_task_data()
#     for task in raw_tasks:
#         for miner_response in task.miner_responses:
#             task_payload = TaskPayload(
#                 miner_hotkey=miner_response.hotkey,
#                 miner_coldkey=miner_response.coldkey,
#                 validator_task_id=task.id,
#                 validator_hotkey=task.validator_hotkey,
#                 completions=task.completions,
#                 miner_score=task.miner_responses.scores,
#                 created_at=task.created_at,
#                 metadata=task.metadata,
#             )
#     return []

# 3. POST to endpoint 
TIMEOUT = 15.0
_http_client = httpx.AsyncClient()
ANALYTICS_URL = "analytics_url"
async def post_task_data(payload, hotkey, signature):
    # Make request
    try:
        response = await _http_client.post(
            url=f"{ANALYTICS_URL}/api/v1/analytics/validators/{hotkey}/tasks",
            files=payload,
            headers={"X-Hotkey:": hotkey,
                     "X-Signature:": signature,
                     },
            timeout=TIMEOUT,
        )
        if response.status_code == 200:
            logger.success(f"Successfully posted task data for hotkey: {hotkey}")
    except Exception as e:
        logger.error(f"Error occurred when trying to post task data: {e}")
        return None
    

async def post_task_data_to_analytics(validator_hotkey: str):
    payload = await _get_task_data()
    await post_task_data(payload=payload, hotkey=validator_hotkey, signature="signature")