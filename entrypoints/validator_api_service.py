"""
validator_api_service.py
    API to receive data from validators.
"""

import asyncio
import os
from contextlib import asynccontextmanager
from typing import List
from urllib.parse import urlparse

import aioboto3
import aiofiles
import bittensor as bt
import httpx
import uvicorn
from bittensor.utils.btlogging import logging as logger
from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from commons.api_settings import ValidatorAPISettings, get_settings
from commons.cache import RedisCache
from commons.objects import ObjectManager
from commons.utils import (
    check_stake,
    verify_hotkey_in_metagraph,
    verify_signature,
)
from entrypoints.analytics_endpoint import analytics_router

load_dotenv()
settings: ValidatorAPISettings = get_settings()
cfg: bt.config = ObjectManager.get_config()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.bt_cfg = cfg
    app.state.api_config = settings.aws
    app.state.redis = RedisCache(settings.redis)
    app.state.subtensor = bt.subtensor(config=app.state.bt_cfg)
    yield
    await app.state.redis.close()
    app.state.subtensor.close()


app = FastAPI(title="Validator API Service", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes:
app.include_router(analytics_router)


@app.post("/upload_dataset")
async def upload_dataset(
    hotkey: str = Form(...),
    signature: str = Form(...),
    message: str = Form(...),
    files: List[UploadFile] = File(...),
):
    api_config = app.state.api_config
    metagraph = app.state.subtensor.metagraph(app.state.bt_cfg.netuid)
    metagraph.sync(block=None, lite=True)

    try:
        if not signature.startswith("0x"):
            raise HTTPException(
                status_code=401, detail="Invalid signature format, must be hex."
            )

        if not verify_signature(hotkey, signature, message):
            logger.error(f"Invalid signature for address={hotkey}")
            raise HTTPException(status_code=401, detail="Invalid signature.")

        if not verify_hotkey_in_metagraph(metagraph, hotkey):
            logger.error(f"Hotkey {hotkey} not found in metagraph")
            raise HTTPException(
                status_code=401, detail="Hotkey not found in metagraph."
            )

        if not check_stake(metagraph, hotkey):
            logger.error(f"Insufficient stake for hotkey {hotkey}")
            raise HTTPException(
                status_code=401, detail="Insufficient stake for hotkey."
            )

        session = aioboto3.Session(region_name=api_config.AWS_REGION)
        async with session.resource("s3") as s3:
            bucket = await s3.Bucket(api_config.BUCKET_NAME)
            for file in files:
                content = await file.read()
                file_size = len(content)
                if (
                    file_size > api_config.MAX_CHUNK_SIZE_MB * 1024 * 1024
                ):  # 50MB in bytes
                    raise HTTPException(
                        status_code=413,
                        detail=f"File too large. Maximum size is {api_config.MAX_CHUNK_SIZE_MB}MB",
                    )

                filename = f"hotkey_{hotkey}_{file.filename}"

                await bucket.put_object(
                    Key=filename,
                    Body=content,
                )
    except Exception as e:
        logger.error(f"Error uploading dataset: {e}")
        raise HTTPException(status_code=500, detail=f"Error uploading dataset: {e}")

    return {
        "success": True,
        "message": "Files uploaded successfully",
        "filenames": [file.filename for file in files],
    }


async def server():
    # host endpoint with .env VALIDATOR_API_BASE_URL var; default to localhost:9999
    api_url = os.getenv("VALIDATOR_API_BASE_URL", "http://0.0.0.0:9999")
    parsed_url = urlparse(api_url)
    # Extract host and port
    host = parsed_url.hostname or "0.0.0.0"
    port = parsed_url.port or 9999

    # Configure server
    config = uvicorn.Config(app, host=host, port=port)
    server = uvicorn.Server(config)
    await server.serve()


async def test_endpoint():
    # Create test data
    test_data = {
        "hotkey": "asdfg",
        "signature": "0xasdfg",
        "message": "<Bytes>On 2024-12-02 18:15:23.663947 +08 Tensorplex is awesome</Bytes>",
    }
    # Create a temporary test file
    test_filename = "dataset_20241202.jsonl"

    # Build form data similar to how dojo.py does it
    files = []

    # Add file to form data if it exists
    if os.path.exists(test_filename):
        async with aiofiles.open(test_filename, "rb") as f:
            file_content = await f.read()
            files.append(("files", (test_filename, file_content, "application/json")))
    else:
        raise FileNotFoundError(f"Test file {test_filename} not found")

    # Make request using httpx
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://localhost:8000/upload_dataset",
            data={
                "hotkey": test_data["hotkey"],
                "signature": test_data["signature"],
                "message": test_data["message"],
            },
            files=files,
            timeout=30.0,
        )
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")


if __name__ == "__main__":
    import sys

    if "--test" in sys.argv:
        asyncio.run(test_endpoint())
    else:
        asyncio.run(server())
