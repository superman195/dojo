"""
analytics_endpoint.py
is the endpoint for receiving analytics data from validators
is hosted by validator_api_service.py
"""

import json
import time
import traceback
from datetime import datetime

import aioboto3
import uvicorn
from bittensor.utils.btlogging import logging as logger
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.datastructures import State

from commons.utils import check_stake, verify_hotkey_in_metagraph, verify_signature
from dojo.protocol import AnalyticsData, AnalyticsPayload

analytics_router = APIRouter()
ONE_DAY_SECONDS = 60 * 60 * 24  # cached tasks to expire after 1 day


def _save_to_athena_format(data: dict):
    """
    converts input analytics JSON data to athena format by un-nesting elements
    """
    try:
        formatted_data = ""
        for task in data["tasks"]:
            # Format each item with proper indentation and save to file
            unnested_obj = json.dumps(task, indent=2)
            formatted_data += unnested_obj + "\n"
        return formatted_data
    except OSError as e:
        logger.error(f"Error writing to athena_pp_output.json: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing data for Athena format: {str(e)}")
        raise


async def _upload_to_s3(data: AnalyticsPayload, hotkey: str, state: State):
    """
    uploads received analytics data to s3.
    caches uploaded task_ids in redis to prevent duplicate uploads to s3

    @param data: the analytics data to be uploaded. Must be formatted according to AnalyticsPayload
    @param hotkey: the hotkey of the sender
    @param state: state var passed from API server, validator_api_service.py
    """
    rc = state.redis
    cfg = state.api_config

    if not data.tasks:
        logger.error("No analytics data to upload")
        return

    logger.info("Connecting to Redis...")
    await rc.connect()
    try:
        # check if any tasks have been uploaded previously
        new_tasks: list[AnalyticsData] = []

        for task in data.tasks:
            val_task_id = task.validator_task_id
            key = rc._build_key(rc._anal_prefix_, rc._upload_key_, val_task_id)

            # if task already exists in redis then do not upload it.
            task_exists = await rc.redis.get(key)
            if task_exists:
                logger.error(f"Task {val_task_id} already exists in Redis")
                continue
            # cache new tasks
            else:
                await rc.redis.set(key, val_task_id, ONE_DAY_SECONDS)
                new_tasks.append(task)

        # convert to athena format
        # @dev: this can be optimized by converting to athena format when we are checking for uploaded tasks.
        data_to_upload = AnalyticsPayload(tasks=new_tasks)
        formatted_data = _save_to_athena_format(data_to_upload.model_dump())

        # attempt upload to s3
        session = aioboto3.Session(region_name=cfg.AWS_REGION)
        async with session.resource("s3") as s3:
            bucket = await s3.Bucket(cfg.BUCKET_NAME)
            filename = f"analytics/{hotkey}_{datetime.now().strftime('%Y-%m-%d_%H-%M')}_analytics.txt"

            await bucket.put_object(
                Key=filename,
                Body=formatted_data,
            )
    except Exception as e:
        # Remove new tasks from redis if AWS upload is unsuccessful
        logger.info(f"Error uploading to s3: {str(e)}")
        for task in new_tasks:
            val_task_id = task.validator_task_id
            key = rc._build_key(rc._anal_prefix_, rc._upload_key_, val_task_id)
            try:
                await rc.delete(key)
            except Exception as redis_err:
                logger.error(
                    f"Error removing task {val_task_id} from Redis: {redis_err}"
                )
        logger.trace("Removed new tasks from analytics redis cache")

        raise


@analytics_router.post("/api/v1/analytics/validators/{path_hotkey}/tasks")
async def create_analytics_data(
    request: Request,
    data: AnalyticsPayload,
    path_hotkey: str,  # Path parameter
    header_hotkey: str = Header(..., alias="X-Hotkey"),
    signature: str = Header(..., alias="X-Signature"),
    message: str = Header(..., alias="X-Message"),
):
    """
    create_analytics_data() is the route that receives analytics data from validators
    1. uses pydantic to validate incoming data against AnalyticsPayload. Rejects non-compliant data.
    2. verifies incoming signature against hotkey and message. Rejects invalid signatures.
    3. verifies incoming hotkey is in metagraph. Rejects invalid hotkeys.
    4. verifies incoming hotkey has sufficient stake to be a validator. Rejects insufficient stake.
    5. converts data to athena string format and uploads to s3.

    @dev incoming requests must contain the Hotkey, Signature and Message headers.
    @param request: the fastAPI request object. Used to access state vars.
    @param data: the analytics data to be uploaded. Must be formatted according to AnalyticsPayload
    @param path_hotkey: the hotkey of the sender from the path
    @param header_hotkey: the hotkey of the sender from the headers
    @param signature: the signature of the sender
    @param message: the message of the sender
    """
    start_time = time.time()

    try:
        logger.info(f"Received request from hotkey: {header_hotkey}")
        metagraph = request.app.state.metagraph

        # Verify that path hotkey matches header hotkey
        if path_hotkey != header_hotkey:
            raise HTTPException(
                status_code=401, detail="Hotkey in path does not match hotkey in header"
            )

        if not verify_signature(header_hotkey, signature, message):
            logger.error(f"Invalid signature for address={header_hotkey}")
            raise HTTPException(status_code=401, detail="Invalid signature")

        if not verify_hotkey_in_metagraph(metagraph, header_hotkey):
            logger.error(f"Hotkey {header_hotkey} not found in metagraph")
            raise HTTPException(
                status_code=401, detail="Hotkey not found in metagraph."
            )

        if not check_stake(request.app.state.subtensor, header_hotkey):
            logger.error(f"Insufficient stake for hotkey {header_hotkey}")
            raise HTTPException(
                status_code=401, detail="Insufficient stake for hotkey."
            )

        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Auth completed in {execution_time:.4f} seconds")

        await _upload_to_s3(data, header_hotkey, request.app.state)

        response = {
            "success": True,
            "message": f"Analytics data received from {header_hotkey}",
        }

        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Request completed in {execution_time:.4f} seconds")

        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        logger.error(traceback.format_exc())
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
    uvicorn.run(
        "analytics_endpoint:analytics_router", host="0.0.0.0", port=8000, reload=True
    )
