"""
analytics_endpoint.py
- is the endpoint that receives analytics data from analytics.py
- validates incoming data against Task schema
- performs verification on sender hotkey
- converts data to athena format and uploads to s3.

@to-do:
    - add proper error and success responses
    - add upload to s3.
"""

import json
import logging
from re import I
import traceback
import os
from datetime import datetime
from typing import List
import aioboto3
from dojo.protocol import Scores
import bittensor as bt
import uvicorn
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from substrateinterface import Keypair
from commons.objects import ObjectManager

from dojo.protocol import AnalyticsData, AnalyticsPayload

# # """
# # Task is a schema that defines the structure of the task data.
# # The payload must match up with the schema in analytics_upload.py for successful uploads.
# # """
# class Task(BaseModel):
#     validator_task_id: str
#     validator_hotkey: str
#     completions: List[dict]
#     ground_truths: List[dict]
#     miner_responses: List[dict]
#     created_at: str
#     metadata: dict | None


# class AnalyticsPayload(BaseModel):
#     tasks: List[Task]

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

config = ObjectManager.get_config()
subtensor = bt.subtensor(config=config)
metagraph = subtensor.metagraph(netuid=config.netuid, lite=True)
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("ANAL_BUCKET_NAME")
AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("analytics_endpoint")


def verify_signature(hotkey: str, signature: str, message: str) -> bool:
    logger.info(f"Verifying signature: {signature}")
    keypair = Keypair(ss58_address=hotkey, ss58_format=42)
    if not keypair.verify(data=message, signature=signature):
        logger.error(f"Invalid signature for address={hotkey}")
        return False
    return True


def verify_hotkey_in_metagraph(hotkey: str) -> bool:
    # logger.info(f"{metagraph.hotkeys=}")
    return hotkey in metagraph.hotkeys


def save_to_athena_format(data):
    try:
        pp = ""
        for item in data["tasks"]:
        # Format each item with proper indentation and save to file
            formatted_data = json.dumps(item, indent=2)
            with open("athena_pp_output.json", "a") as f:
                f.write(formatted_data + "\n")
            pp += formatted_data + "\n"
        logger.info(f"Saved {len(data['tasks'])} tasks to athena_pp_output.json")
        return pp
    except OSError as e:
        logger.error(f"Error writing to athena_pp_output.json: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing data for Athena format: {str(e)}")
        raise


async def upload_to_s3(data, hotkey):
    """
    to be implemented.
    """
    print(f"Uploading to S3: {BUCKET_NAME}")
    try:
        session = aioboto3.Session(region_name=AWS_REGION)
        print("access_key_id", AWS_ACCESS_KEY_ID)
        async with session.resource("s3") as s3:
            bucket = await s3.Bucket(BUCKET_NAME)        
            filename = f"{hotkey}_{datetime.now().strftime('%Y-%m-%d_%H-%M')}_analytics.txt"

            await bucket.put_object(
                Key=filename,
                Body=data,
            )
    except Exception as e:
        logger.error(f"Error uploading to s3: {str(e)}")
        raise


@app.post("/api/v1/analytics/validators/{validator_hotkey}/tasks")
async def create_analytics_data(
    validator_hotkey: str,
    data: AnalyticsPayload,
    hotkey: str = Header(..., alias="X-Hotkey"),
    signature: str = Header(..., alias="X-Signature"),
    message: str = Header(..., alias="X-Message"),
):
    """
    create_analytics_data() is the endpoint that receives analytics data from analytics.py
    1. uses pydantic to validate incoming data against AnalyticsPayload. Rejects non-compliant data.
    2. verifies incoming signature against hotkey and message. Rejects invalid signatures.
    3. verifies incoming hotkey is in metagraph. Rejects invalid hotkeys.
    4. converts data to athena string format and uploads to s3.

    @dev incoming requests must contain the Hotkey, Signature and Message headers.
    @param validator_hotkey: the hotkey of the validator
    @param data: the analytics data to be uploaded. Must be formatted according to AnalyticsPayload
    @param hotkey: the hotkey of the sender
    @param signature: the signature of the sender
    @param message: the message of the sender
    """
    logger.info(f"Received request from hotkey: {hotkey}")
    try:
        if not verify_signature(hotkey, signature, message=message):
            logger.error(f"Invalid signature for address={hotkey}")
            raise HTTPException(status_code=401, detail="Invalid signature")

        # if not verify_hotkey_in_metagraph(hotkey):
        #     logger.error(f"Hotkey {hotkey} not found in metagraph")
        #     raise HTTPException(
        #         status_code=401, detail="Hotkey not found in metagraph."
        #     )

        # convert to athena format
        athena_anal_data = save_to_athena_format(data.model_dump())

        await upload_to_s3(athena_anal_data, validator_hotkey)

        response = {
            "success": True,
            "message": f"Analytics data received from {hotkey}",
        }

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


async def _test_s3_upload():
    # read JSON from sample_anal_payload.json
    with open("sample_anal_payload.json", "r") as f:
        data_str = f.read()
    await upload_to_s3(data_str, "hotkey")

if __name__ == "__main__":
    import asyncio
    import sys
    
    if "--test" in sys.argv:        
        from dotenv import load_dotenv

        load_dotenv()
        asyncio.run(_test_s3_upload())
    else:
        uvicorn.run("analytics_endpoint:app", host="0.0.0.0", port=8000, reload=True)