import asyncio
import json
import os
import traceback
from collections import defaultdict
from datetime import datetime
from typing import AsyncGenerator

import aiofiles
import bittensor as bt
import httpx
from bittensor.utils.btlogging import logging as logger
from pydantic import BaseModel, ValidationError

from commons.objects import ObjectManager
from commons.utils import datetime_to_iso8601_str
from database.client import connect_db, disconnect_db, prisma
from database.prisma.models import Completion, MinerScore, ValidatorTask
from database.prisma.types import (
    ValidatorTaskWhereInput,
)
from dojo.protocol import Scores
from dojo.utils.config import source_dotenv

source_dotenv()

DATASET_SERVICE_BASE_URL = os.getenv("DATASET_SERVICE_BASE_URL")
MAX_CHUNK_SIZE_MB = int(os.getenv("MAX_CHUNK_SIZE_MB", 50))

if DATASET_SERVICE_BASE_URL is None:
    raise ValueError("DATASET_SERVICE_BASE_URL must be set")
if MAX_CHUNK_SIZE_MB is None:
    raise ValueError("MAX_CHUNK_SIZE_MB must be set")


class MinerResponseDataset(BaseModel):
    miner_coldkey: str
    miner_hotkey: str
    # scores object, directly taken from database
    completion_id_to_scores: dict[str, MinerScore]


class CompletionWithHeuristics(Completion):
    mean_scores: Scores


# 1 row represents 1 task in the dataset
class Row(BaseModel):
    prompt: str
    completions: list[CompletionWithHeuristics]
    created_at: str
    miner_responses: list[MinerResponseDataset]

    class Config:
        arbitrary_types_allowed = True


"""

{
    "prompt": "Write a function to calculate fibonacci numbers",
    "completions": [
        {
            "completion": {
                "files": [
                    {
                        "filename": "fib.py",
                        "content": "def fib(n):\n    if n <= 1:\n        return n\n    return fib(n-1) + fib(n-2)",
                        "language": "python",
                    }
                ]
            },
            "completion_id": "comp_123",
            "mean_score": {
                "raw_score": 0.85,
                "normalised_score": 0.9,
                "ground_truth_score": 1.0,
                "cosine_similarity_score": 0.95,
                "normalised_cosine_similarity_score": 0.92,
                "cubic_reward_score": 0.88,
            }
            "model": "gpt-4",
        }
    ],
    "created_at": "2024-01-01T00:00:00Z",
    "miner_responses": [
        {
            "miner_coldkey": "asdfg",
            "miner_hotkey": "asdfg",
            "completion_id_to_scores": {
                "comp_123": {
                    "scores": {
                        "raw_score": 0.85,
                        "normalised_score": 0.9,
                        "ground_truth_score": 1.0,
                        "cosine_similarity_score": 0.95,
                        "normalised_cosine_similarity_score": 0.92,
                        "cubic_reward_score": 0.88,
                    }
                }
            },
        }
    ],
}

"""


async def build_jsonl(filename: str):
    with open(filename, "w") as file:
        batch_size = 10
        task_count = 0
        async for task_batch, has_more_batches in get_processed_tasks(batch_size):
            if not has_more_batches and not task_batch:
                break

            mresponses = []
            for task in task_batch:
                prompt = task.prompt
                completions = task.completions
                assert completions is not None, "Completions should not be None"

                row = Row(
                    prompt=prompt,
                    completions=[
                        CompletionWithHeuristics(
                            id=c.id,
                            completion_id=c.completion_id,
                            validator_task_id=c.validator_task_id,
                            model=c.model,
                            # # NOTE: hack because otherwise the mapper.py functgion fails
                            # completion.completion = json.dumps(completion.completion)  # type: ignore
                            completion=json.dumps(c.completion),  # type: ignore
                            created_at=c.created_at,
                            updated_at=c.updated_at,
                            mean_scores=Scores(),
                        )
                        for c in completions
                    ],
                    created_at=datetime_to_iso8601_str(task.created_at),
                    miner_responses=mresponses,
                )

                # ensure ordering of scores based on the validator's completions ordering
                # ensure ordering of scores based on the validator's completions ordering
                # ensure ordering of scores based on the validator's completions ordering
                criterion_ids = []

                criterion_id_to_completion: dict[str, Completion] = {}
                for c in completions:
                    assert c.criterion is not None, "Criterion should not be None"
                    # at the moment it should only be 1
                    assert (
                        len(c.criterion) == 1
                    ), "Only 1 criterion per completion is supported at the moment"

                    for criterion in c.criterion:
                        criterion_ids.append(criterion.id)
                        criterion_id_to_completion[criterion.id] = c

                miner_responses = task.miner_responses
                if miner_responses is None:
                    logger.warning(f"No miner responses for task {task.id}")
                    continue

                assert (
                    task.miner_responses is not None
                ), "Miner responses should not be None"

                # NOTE: calculate heuristics here
                completion_id_to_mean_scores = defaultdict(
                    lambda: Scores(
                        raw_score=0.0,
                        rank_id=None,
                        normalised_score=0.0,
                        ground_truth_score=0.0,
                        cosine_similarity_score=0.0,
                        normalised_cosine_similarity_score=0.0,
                        cubic_reward_score=0.0,
                    )
                )

                for m_response in task.miner_responses:
                    if not m_response.scores:
                        logger.warning(f"No scores for miner response {m_response.id}")
                        continue
                    else:
                        logger.debug(
                            f"Scores for miner response {m_response.id}: {m_response.scores}"
                        )

                    ordered_scores: list[MinerScore] = []
                    for criterion_id in criterion_ids:
                        score = next(
                            (
                                s
                                for s in m_response.scores
                                if s.criterion_id == criterion_id
                            ),
                            None,
                        )
                        if score:
                            ordered_scores.append(score)
                        else:
                            logger.error(
                                f"Criterion id {criterion_id} not found in scores"
                            )
                            continue

                        existing_scores = Scores()
                        try:
                            existing_scores = Scores.model_validate_json(
                                json.dumps(score.scores)
                            )
                            logger.debug("Successfully parsed scores from database")
                        except ValidationError:
                            pass

                        completion_id = criterion_id_to_completion[criterion_id].id
                        completion_id_to_mean_scores[completion_id] = sum_scores(
                            completion_id_to_mean_scores[completion_id], existing_scores
                        )

                        row.miner_responses.append(
                            MinerResponseDataset(
                                miner_coldkey=m_response.coldkey,
                                miner_hotkey=m_response.hotkey,
                                completion_id_to_scores={
                                    completion_id: score  # type: ignore
                                },
                            )
                        )

                for completion in row.completions:
                    completion.mean_scores = completion_id_to_mean_scores[completion.id]

                # Write the entry as a JSON line
                file.write(row.model_dump_json() + "\n")

            task_count += len(task_batch)
            logger.info(f"Scraped task count: {task_count}")


def sum_scores(scores1: Scores, scores2: Scores) -> Scores:
    return Scores(
        raw_score=(scores1.raw_score or 0) + (scores2.raw_score or 0),
        rank_id=(scores1.rank_id or 0) + (scores2.rank_id or 0),
        normalised_score=(scores1.normalised_score or 0)
        + (scores2.normalised_score or 0),
        ground_truth_score=(scores1.ground_truth_score or 0)
        + (scores2.ground_truth_score or 0),
        cosine_similarity_score=(scores1.cosine_similarity_score or 0)
        + (scores2.cosine_similarity_score or 0),
        normalised_cosine_similarity_score=(
            scores1.normalised_cosine_similarity_score or 0
        )
        + (scores2.normalised_cosine_similarity_score or 0),
    )


async def get_processed_tasks(
    batch_size: int = 10,
) -> AsyncGenerator[tuple[list[ValidatorTask], bool], None]:
    vali_where_query = ValidatorTaskWhereInput(
        {
            "is_processed": True,
        }
    )
    num_processed_tasks = await prisma.validatortask.count(where=vali_where_query)

    for skip in range(0, num_processed_tasks, batch_size):
        validator_tasks = await prisma.validatortask.find_many(
            skip=skip,
            take=batch_size,
            where=vali_where_query,
            include={
                "completions": {"include": {"criterion": True}},
                "ground_truth": True,
                "miner_responses": {
                    "include": {
                        "scores": True,
                    }
                },
            },
        )
        yield validator_tasks, skip + batch_size < num_processed_tasks

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
        logger.error(
            f"Error occurred while trying to upload dataset: {e}, traceback: {traceback.format_exc()}"
        )
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
