import os

import aiofiles
import httpx

from commons.logging import logging as logger


async def test_endpoint():
    # Create test data
    test_data = {
        "hotkey": "asdfg",
        "signature": "0xasdfg",
        "message": "<Bytes>On 2024-12-02 18:15:23.663947 +08 Tensorplex is awesome</Bytes>",
    }

    # Create a temporary test file
    test_filename = "dataset_20241202.jsonl"

    # Build form data
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
        logger.info(f"Status: {response.status_code}")
        logger.info(f"Response: {response.json()}")
