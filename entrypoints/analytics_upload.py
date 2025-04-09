"""
analytics_upload.py
    contains code to query and upload analytics data to analytics_endpoint.py
"""

import asyncio
import gc
import json
import os
import traceback
from datetime import datetime, timedelta, timezone
from typing import List

import bittensor as bt
import httpx
from bittensor.core.async_subtensor import AsyncSubtensor
from bittensor.core.metagraph import AsyncMetagraph
from bittensor.utils.btlogging import logging as logger

from commons.exceptions import NoProcessedTasksYet
from commons.objects import ObjectManager
from commons.orm import ORM
from commons.utils import aget_effective_stake, datetime_to_iso8601_str
from database.client import connect_db
from dojo import ANALYTICS_UPLOAD, VALIDATOR_MIN_STAKE
from dojo.protocol import AnalyticsData, AnalyticsPayload

VALIDATOR_API_BASE_URL = os.getenv("VALIDATOR_API_BASE_URL")


async def _get_all_miner_hotkeys(
    subnet_metagraph: AsyncMetagraph, root_metagraph: AsyncMetagraph
) -> List[str]:
    """
    returns a list of all metagraph hotkeys with stake less than VALIDATOR_MIN_STAKE
    """
    return [
        hotkey
        for hotkey in subnet_metagraph.hotkeys
        if await aget_effective_stake(
            hotkey, root_metagraph=root_metagraph, subnet_metagraph=subnet_metagraph
        )
        < VALIDATOR_MIN_STAKE
    ]


async def _get_task_data(
    validator_hotkey: str,
    all_miner_hotkeys: List[str],
    expire_from: datetime,
    expire_to: datetime,
) -> AnalyticsPayload:
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
    logger.debug(f"retrieving processed tasks from {expire_from} to {expire_to}")
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
        raise
    except Exception as e:
        logger.error(f"Error when _get_task_data(): {e}")
        raise


async def _post_task_data(payload, hotkey, signature, message) -> httpx.Response | None:
    """
    _post_task_data() is a helper function that:
    1. POSTs task data to analytics API
    2. returns response from analytics API

    @param payload: the analytics data to be sent to analytics API
    @param hotkey: the hotkey of the validator
    @param message: a message that is signed by the validator
    @param signature: the signature generated from signing the message with the validator's hotkey.
    @returns: httpx.Response or None if no response is received.
    """
    _http_client = httpx.AsyncClient(timeout=300)
    try:
        logger.debug("POST-ing analytics data to validator API")
        response = await _http_client.post(
            url=f"{VALIDATOR_API_BASE_URL}/api/v1/analytics/validators/{hotkey}/tasks",
            json=payload.model_dump(mode="json"),
            headers={
                "X-Hotkey": hotkey,
                "X-Signature": signature,
                "X-Message": message,
                "Content-Type": "application/json",
            },
            timeout=300,
        )

        if not response:
            logger.error("_post_task_data() got no response from analytics API")
            return None
        if response.status_code == 200:
            logger.success(f"Successfully uploaded analytics data for hotkey: {hotkey}")
            return response
        else:
            logger.error(f"_post_task_data() response error {response.status_code}")
            logger.error(traceback.format_exc())
            return response
    except Exception as e:
        logger.error(f"Error when _post_task_data(): {str(e)}")
        logger.error(traceback.format_exc())
        raise


async def run_analytics_upload(
    scores_alock: asyncio.Lock, expire_from: datetime | None, expire_to: datetime
) -> datetime | None:
    """
    run_analytics_upload()
    driver function called by validator that triggers:
    1. collection of analytics data from postgres
    2. upload of analytics data to analytics API

    @param scores_alock: The async lock used by scoring. Used to ensure upload occurs only after scoring is complete.
    @param expire_from: start of time window to query for processed tasks. Should be the last successful upload time.
    @param expire_to: end time of time window to query for processed tasks. Should be current time.
    @returns: datetime of last successful upload
    """
    async with scores_alock:
        last_analytics_upload_time = expire_from
        logger.debug(f"Last analytics upload time: {expire_from}")
        # if there is no last analytics upload time, get tasks from 65 minutes ago.
        if expire_from is None:
            expire_from = datetime.now(timezone.utc) - timedelta(
                seconds=ANALYTICS_UPLOAD
            )

        logger.info(
            f"Uploading analytics data for processed tasks between {expire_from} and {expire_to}"
        )

        config = ObjectManager.get_config()
        wallet = bt.wallet(config=config)
        validator_hotkey = wallet.hotkey.ss58_address

        async with AsyncSubtensor(config=config) as subtensor:
            subnet_metagraph = await subtensor.metagraph(config.netuid)  # type: ignore
            root_metagraph = await subtensor.metagraph(0)
            all_miners = await _get_all_miner_hotkeys(subnet_metagraph, root_metagraph)

        # 1. collect processed tasks from db
        anal_data = await _get_task_data(
            validator_hotkey, all_miners, expire_from, expire_to
        )

        # 2. upload data to analytics API
        message = f"Uploading analytics data for validator hotkey: {validator_hotkey}"
        signature = wallet.hotkey.sign(message).hex()

        if not signature.startswith("0x"):
            signature = f"0x{signature}"

        try:
            res = await _post_task_data(
                payload=anal_data,
                hotkey=validator_hotkey,
                signature=signature,
                message=message,
            )

            # if upload was successful, return new upload time
            # else return last successful upload time
            if res and res.status_code == 200:
                return expire_to
            return last_analytics_upload_time
        except NoProcessedTasksYet:
            logger.info("No processed tasks to upload. Skipping analytics upload.")
            return last_analytics_upload_time
        except Exception as e:
            logger.error(f"Error when run_analytics_upload(): {e}")
            raise


# # Main function for testing. Remove / Comment in prod.
# if __name__ == "__main__":
#     import asyncio

# async def main():
# # for testing
# from datetime import datetime, timedelta, timezone

# from commons.utils import datetime_as_utc

# from_14_days = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(days=14)
# # from_24_hours = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(
# #     hours=24
# # )
# # from_1_hours = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(hours=1)
# to_now = datetime_as_utc(datetime.now(timezone.utc))
# res = await run_analytics_upload(asyncio.Lock(), from_14_days, to_now)
# print(f"Response: {res}")

# payload = AnalyticsPayload(tasks=[])
# hotkey = "test_hk"
# signature = "0xtest"
# message = "test_msg"
# res = await _post_task_data(
#     payload=payload, hotkey=hotkey, signature=signature, message=message
# )
# print(f"Response: {res}")

# asyncio.run(main())
