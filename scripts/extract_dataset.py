import asyncio
import os
from datetime import datetime
from typing import AsyncGenerator, List

import aiofiles
import bittensor as bt
import httpx
import numpy as np
from bittensor.btlogging import logging as logger
from pydantic import BaseModel, model_serializer

from commons.exceptions import (
    NoNewExpiredTasksYet,
)
from commons.objects import ObjectManager
from database.client import connect_db, disconnect_db
from database.mappers import (
    map_feedback_request_model_to_feedback_request,
)
from database.prisma.models import (
    Feedback_Request_Model,
)
from database.prisma.types import (
    Feedback_Request_ModelInclude,
    Feedback_Request_ModelWhereInput,
)
from dojo import TASK_DEADLINE
from dojo.protocol import (
    CompletionResponses,
    DendriteQueryResponse,
)
from dojo.utils.config import source_dotenv

source_dotenv()

DATASET_SERVICE_BASE_URL = os.getenv("DATASET_SERVICE_BASE_URL")
MAX_CHUNK_SIZE_MB = int(os.getenv("MAX_CHUNK_SIZE_MB", 50))

if DATASET_SERVICE_BASE_URL is None:
    raise ValueError("DATASET_SERVICE_BASE_URL must be set")
if MAX_CHUNK_SIZE_MB is None:
    raise ValueError("MAX_CHUNK_SIZE_MB must be set")


# represents a row in the jsonl dataset
class Row(BaseModel):
    prompt: str
    completions: list[CompletionResponses]
    # shape (num_miners, num_completions)
    raw_scores: list[list[float]]
    # shape (num_completions)
    mean_scores: list[float]

    class Config:
        arbitrary_types_allowed = True

    @model_serializer
    def serialize_model(self):
        return {
            "prompt": self.prompt,
            "completions": self.completions,
            "raw_scores": self.raw_scores,
            "mean_scores": self.mean_scores,
        }


async def build_jsonl(filename: str):
    with open(filename, "w") as file:
        batch_size = 10
        task_count = 0
        async for task_batch, has_more_batches in get_processed_tasks(batch_size):
            if not has_more_batches and not task_batch:
                break

            for task in task_batch:
                # Extract prompt from validator request
                prompt = task.request.prompt

                # Extract completions from miner responses
                completions = task.request.completion_responses

                raw_scores = []
                for miner_response in task.miner_responses:
                    miner_ratings = [
                        c.score for c in miner_response.completion_responses
                    ]
                    if any(rating is None for rating in miner_ratings):
                        continue
                    raw_scores.append(miner_ratings)

                # shape (num_completions, num_miners)
                raw_scores_vec = np.array(raw_scores)
                logger.info(f"raw_scores_vec shape: {raw_scores_vec.shape}")
                logger.info(f"raw_scores_vec: {raw_scores_vec}")

                if raw_scores_vec.size > 0:
                    mean_scores = raw_scores_vec.mean(axis=1)
                    logger.info(f"mean_scores shape: {mean_scores.shape}")
                    jsonl_row = Row(
                        prompt=prompt,
                        completions=completions,
                        raw_scores=raw_scores,
                        mean_scores=mean_scores.tolist(),
                    )
                else:
                    jsonl_row = Row(
                        prompt=prompt,
                        completions=completions,
                        raw_scores=[],
                        mean_scores=[],
                    )

                # Write the entry as a JSON line
                file.write(jsonl_row.model_dump_json() + "\n")

            task_count += len(task_batch)
            logger.info(f"Scraped task count: {task_count}")


async def get_processed_tasks(
    batch_size: int = 10,
) -> AsyncGenerator[tuple[List[DendriteQueryResponse], bool], None]:
    """Yields batches of processed Feedback_Request_Model records along with a boolean flag indicating the presence of additional batches.

    This function retrieves tasks that have been fully processed. The batch size can be specified to control the number of tasks returned in each batch.

    Args:
        batch_size (int, optional): The number of tasks to include in each batch. Defaults to 10.

    Raises:
        NoNewExpiredTasksYet: Raised if no processed tasks are available for retrieval.

    Yields:
        AsyncGenerator[tuple[List[DendriteQueryResponse], bool], None]: An asynchronous generator yielding a tuple containing a list of DendriteQueryResponse objects and a boolean indicating if more batches are available.
    """

    # find all validator requests first
    include_query = Feedback_Request_ModelInclude(
        {
            "completions": True,
            "criteria_types": True,
            "ground_truths": True,
            "parent_request": True,
        }
    )

    vali_where_query = Feedback_Request_ModelWhereInput(
        {
            "parent_id": None,  # no parent means it's a validator request
            # only check for tasks that are completely done
            "is_processed": {"equals": True},
        }
    )

    # count first total including non
    task_count = await Feedback_Request_Model.prisma().count(
        where=vali_where_query,
    )

    logger.info(f"Count of processed tasks: {task_count}")

    if not task_count:
        raise NoNewExpiredTasksYet(
            f"No expired tasks found for processing, please wait for tasks to pass the task deadline of {TASK_DEADLINE} seconds."
        )

    for i in range(0, task_count, batch_size):
        # find all unprocesed validator requests
        validator_requests = await Feedback_Request_Model.prisma().find_many(
            include=include_query,
            where=vali_where_query,
            order={"created_at": "desc"},
            skip=i,
            take=batch_size,
        )

        # find all miner responses
        processed_vali_request_ids = [r.id for r in validator_requests]
        miner_responses = await Feedback_Request_Model.prisma().find_many(
            include=include_query,
            where={
                "parent_id": {"in": processed_vali_request_ids},
                "is_processed": {"equals": True},
            },
            order={"created_at": "desc"},
        )

        # NOTE: technically a DendriteQueryResponse represents a task
        tasks: list[DendriteQueryResponse] = []
        for validator_request in validator_requests:
            vali_request = map_feedback_request_model_to_feedback_request(
                validator_request
            )

            m_responses = list(
                map(
                    lambda x: map_feedback_request_model_to_feedback_request(
                        x, is_miner=True
                    ),
                    [m for m in miner_responses if m.parent_id == validator_request.id],
                )
            )

            tasks.append(
                DendriteQueryResponse(request=vali_request, miner_responses=m_responses)
            )

        # yield responses, so caller can do something
        has_more_batches = True
        yield tasks, has_more_batches

    yield [], False


async def upload(hotkey: str, signature: str, message: str, filename: str):
    if not signature.startswith("0x"):
        signature = f"0x{signature}"

    # Build form data similar to how dojo.py does it
    form_body = {
        "hotkey": hotkey,
        "signature": signature,
        "message": message,
    }
    # Add file to form data if it exists
    if os.path.exists(filename):
        chunks = await chunk_file(filename, MAX_CHUNK_SIZE_MB)

        # Make request using httpx
        async with httpx.AsyncClient() as client:
            for chunk_filename, chunk_content in chunks:
                # Append to files list with correct format
                files = [("files", (chunk_filename, chunk_content, "application/json"))]
                response = await client.post(
                    f"{DATASET_SERVICE_BASE_URL}/upload_dataset",
                    data=form_body,
                    files=files,
                    timeout=60.0,
                )
                logger.info(f"Status: {response.status_code}")
                response_json = response.json()
                logger.info(f"Response: {response_json}")
                is_success = response.status_code == 200 and response_json.get(
                    "success"
                )
                if not is_success:
                    raise Exception(f"Failed to upload file {chunk_filename}")
                await asyncio.sleep(1)


async def chunk_file(filename: str, chunk_size_mb: int = 50):
    chunk_size = chunk_size_mb * 1024 * 1024  # Convert MB to bytes

    if os.path.exists(filename):
        async with aiofiles.open(filename) as f:  # Open in text mode
            chunks = []
            current_chunk = []
            current_chunk_size = 0

            # ensure that when we chunk, we don't split across lines
            async for line in f:
                line_size = len(line.encode("utf-8"))  # Get size of line in bytes
                if current_chunk_size + line_size > chunk_size:
                    # Use consistent format
                    base, ext = os.path.splitext(filename)
                    chunk_filename = f"{base}_part{len(chunks) + 1}{ext}"
                    chunks.append((chunk_filename, "".join(current_chunk)))
                    current_chunk = []
                    current_chunk_size = 0

                current_chunk.append(line)
                current_chunk_size += line_size

            # Use same format for last chunk
            if current_chunk:
                base, ext = os.path.splitext(filename)
                chunk_filename = f"{base}_part{len(chunks) + 1}{ext}"
                chunks.append((chunk_filename, "".join(current_chunk)))

            return chunks
    else:
        raise FileNotFoundError(f"Test file {filename} not found")


async def main():
    await connect_db()
    config = ObjectManager.get_config()
    wallet = bt.wallet(config=config)
    hotkey = wallet.hotkey.ss58_address
    message = f"Uploading dataset for validator with hotkey: {hotkey}"
    signature = wallet.hotkey.sign(message).hex()  # Convert signature to hex string

    # Create filename with current date
    current_date = datetime.now().strftime("%Y%m%d")
    filename = f"dataset_{current_date}.jsonl"
    # Check if file already exists
    if os.path.exists(filename):
        logger.warning(f"File {filename} already exists, skipping scrape db step")
    else:
        await build_jsonl(filename)

    try:
        upload_success = await upload(hotkey, signature, message, filename)
        if upload_success:
            logger.info("Upload successful! Removing local dataset file.")
            os.remove(filename)
    except Exception as e:
        logger.error(f"Error occurred while trying to upload dataset: {e}")
    finally:
        await disconnect_db()


async def _test_chunking():
    filename = "dummy_dataset.jsonl"
    chunks = await chunk_file(filename, MAX_CHUNK_SIZE_MB)
    logger.info(f"number of chunks: {len(chunks)}")
    for i, (chunk_filename, chunk_content) in enumerate(chunks, 1):
        logger.info(f"\nSaving chunk {i} to {chunk_filename}")
        async with aiofiles.open(chunk_filename, "w") as f:
            await f.write(chunk_content)
        logger.info(f"Saved chunk {i} ({len(chunk_content)} bytes)")


if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(_test_chunking())
