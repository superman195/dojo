"""
analytics_upload.py
    contains code to query and upload analytics data to analytics_endpoint.py
"""

import asyncio
import gc
import json
from datetime import datetime
from typing import List

import bittensor as bt
import httpx
from bittensor.utils.btlogging import logging as logger

from commons.exceptions import NoProcessedTasksYet
from commons.objects import ObjectManager
from commons.orm import ORM
from commons.utils import datetime_to_iso8601_str
from database.client import connect_db
from dojo.protocol import AnalyticsData, AnalyticsPayload
from dojo.utils.uids import check_root_stake


async def _get_all_miner_hotkeys(metagraph: bt.metagraph) -> List[str]:
    """
    returns a list of all miner hotkeys registered to the input metagraph at time of execution.
    """
    return [
        hotkey
        for uid, hotkey in enumerate(metagraph.hotkeys)
        if check_root_stake(metagraph, uid)
    ]


async def _get_task_data(
    validator_hotkey: str,
    all_miner_hotkeys: List[str],
    expire_from: datetime,
    expire_to: datetime,
) -> AnalyticsPayload | None:
    """
    _get_task_data() is a helper function that:
    1. queries postgres for processed ValidatorTasks in a given time window.
    2. calculates the list of scored hotkeys and absent hotkeys for each task.
    3. converts query results into AnalyticsData schema
    4. returns AnalyticsPayload which contains many AnalyticsData.

    @param validator_hotkey: the hotkey of the validator
    @param all_miner_hotkeys: the hotkeys of all miners registered to metagraph at the time of execution.
    @param expire_from: the start time of the time window to query for processed tasks.
    @param expire_to: the end time of the time window to query for processed tasks.
    @return: a AnalyticsPayload object which contains many AnalyticsData.
    """
    processed_tasks = []
    await connect_db()
    try:
        # get processed tasks in batches from db
        async for task_batch, has_more_batches in ORM.get_processed_tasks(
            expire_from=expire_from, expire_to=expire_to
        ):
            if task_batch is None:
                continue
            for task in task_batch:
                # Convert DB data to AnalyticsData
                _miner_responses = (
                    [
                        miner_response.model_dump(mode="json")
                        for miner_response in task.miner_responses
                    ]
                    if task.miner_responses
                    else []
                )

                # Get list of miner hotkeys that submitted scores
                scored_hotkeys = [
                    miner_response["hotkey"]
                    for miner_response in _miner_responses
                    if miner_response["scores"] != []
                ]

                # Get list of miner hotkeys that did not submit scores.
                # This is checked against all_miner_hotkeys which is a snapshot of metagraph miners at time of execution.
                # @dev: could be inaccurate if a miner deregisters after the task was sent out.
                absent_hotkeys = list(set(all_miner_hotkeys) - set(scored_hotkeys))

                # payload must be convertible to JSON. Hence, serialize any nested objects to JSON and convert datetime to string.
                task_data = AnalyticsData(
                    validator_task_id=task.id,
                    validator_hotkey=validator_hotkey,
                    prompt=task.prompt,
                    completions=[
                        completion.model_dump(mode="json")
                        for completion in task.completions
                    ]
                    if task.completions
                    else [],
                    ground_truths=[
                        ground_truth.model_dump(mode="json")
                        for ground_truth in task.ground_truth
                    ]
                    if task.ground_truth
                    else [],
                    miner_responses=[
                        miner_response.model_dump(mode="json")
                        for miner_response in task.miner_responses
                    ]
                    if task.miner_responses
                    else [],
                    scored_hotkeys=scored_hotkeys,
                    absent_hotkeys=absent_hotkeys,
                    created_at=datetime_to_iso8601_str(task.created_at),
                    updated_at=datetime_to_iso8601_str(task.updated_at),
                    metadata=json.loads(task.metadata) if task.metadata else None,
                )
                processed_tasks.append(task_data)
            # clean up memory after processing all tasks
            if not has_more_batches:
                gc.collect()
                break
        payload = AnalyticsPayload(tasks=processed_tasks)
        return payload

    except NoProcessedTasksYet as e:
        logger.info(f"{e}")
        # raise
    except Exception as e:
        logger.error(f"Error when _get_task_data(): {e}")
        raise


async def _post_task_data(payload, hotkey, signature, message):
    """
    _post_task_data() is a helper function that:
    1. POSTs task data to analytics API
    2. returns response from analytics API

    @param payload: the analytics data to be sent to analytics API
    @param hotkey: the hotkey of the validator
    @param message: a message that is signed by the validator
    @param signature: the signature generated from signing the message with the validator's hotkey.

    @to-do: confirm analytics url and add to config
    """
    # TIMEOUT = 15.0
    _http_client = httpx.AsyncClient()
    ANALYTICS_URL = "http://127.0.0.1:8000"

    try:
        response = await _http_client.post(
            url=f"{ANALYTICS_URL}/api/v1/analytics/validators/{hotkey}/tasks",
            json=payload.model_dump(mode="json"),
            headers={
                "X-Hotkey": hotkey,
                "X-Signature": signature,
                "X-Message": message,
                "Content-Type": "application/json",
            },
            # timeout=TIMEOUT,
            timeout=None,
        )
        if response.status_code == 200:
            logger.info(f"Successfully uploaded analytics data for hotkey: {hotkey}")
            return response
        else:
            logger.error(f"Error when _post_task_data(): {response}")
            return response
    except Exception as e:
        logger.error(f"Error when _post_task_data(): {str(e)}", exc_info=True)
        raise


async def run_analytics_upload(scores_alock: asyncio.Lock, expire_from, expire_to):
    """
    run_analytics_upload()
    triggers the collection and uploading of analytics data to analytics API.
    Is called by the validator after the completion of the scoring process.
    """
    async with scores_alock:
        config = ObjectManager.get_config()
        wallet = bt.wallet(config=config)
        validator_hotkey = wallet.hotkey.ss58_address
        metagraph = bt.subtensor(config=config).metagraph(
            netuid=config.netuid, lite=True
        )
        all_miners = await _get_all_miner_hotkeys(metagraph)
        anal_data = await _get_task_data(
            validator_hotkey, all_miners, expire_from, expire_to
        )
        message = f"Uploading analytics data for validator hotkey: {validator_hotkey}"
        signature = wallet.hotkey.sign(message).hex()

        if not signature.startswith("0x"):
            signature = f"0x{signature}"

        try:
            await _post_task_data(
                payload=anal_data,
                hotkey=validator_hotkey,
                signature=signature,
                message=message,
            )
        except Exception as e:
            logger.error(f"Error when run_analytics_upload(): {e}")
            raise


# # Main function for testing. Remove / Comment in prod.
# if __name__ == "__main__":
#     import asyncio

#     async def main():
#         # for testing
#         from datetime import datetime, timedelta, timezone

#         from commons.utils import datetime_as_utc

#         from_14_days = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(days=14)
#         from_24_hours = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(
#             hours=24
#         )
#         from_1_hours = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(hours=1)
#         to_now = datetime_as_utc(datetime.now(timezone.utc))
#         res = await run_analytics_upload(asyncio.Lock(), from_14_days, to_now)
#         print(f"Response: {res}")

#     asyncio.run(main())
