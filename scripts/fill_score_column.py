import asyncio
import json
import multiprocessing
from typing import AsyncGenerator

from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from commons.orm import ORM
from commons.scoring import Scoring
from database.client import connect_db, disconnect_db, prisma
from database.mappers import (
    map_miner_response_to_task_synapse_object,
    map_validator_task_to_task_synapse_object,
)
from database.prisma import Json
from database.prisma.models import MinerResponse, MinerScore, ValidatorTask
from database.prisma.types import (
    CriterionWhereInput,
    MinerScoreUpdateInput,
    ValidatorTaskWhereInput,
)
from dojo.protocol import Scores
from dojo.utils.config import source_dotenv

source_dotenv()

# Get number of CPU cores
nproc = multiprocessing.cpu_count()

sem = asyncio.Semaphore(nproc * 2 + 1)  # Limit concurrent operations


# 1. for each record from `miner_response` find the corresponding record from `Completion_Response_Model`
# 2. gather all scores from multiple `Completion_Response_Model` records and fill the relevant records inside `miner_response.scores`
# 3. based on all the raw scores provided by miners, apply the `Scoring.score_by_criteria` to calculate the scores for each criterion


def _is_empty_scores(record: MinerScore) -> bool:
    scores = json.loads(record.scores)
    return not scores


def _is_all_empty_scores(records: list[MinerScore]) -> bool:
    return all(_is_empty_scores(record) for record in records)


async def _process_miner_response(miner_response: MinerResponse, task: ValidatorTask):
    scores = miner_response.scores

    if scores is not None and not _is_all_empty_scores(scores):
        return
    else:
        logger.trace("No scores for miner response, attempting to fill from old tables")

    # find the scores from old tables
    # logger.debug(f"where query: task_id:{task.id}, hotkey: {miner_response.hotkey}")

    feedback_request = await prisma.feedback_request_model.find_first(
        where={
            "parent_id": task.id,
            "hotkey": miner_response.hotkey,
        }
    )
    if feedback_request is None:
        logger.warning("Feedback request not found, skipping")
        return
    # assert feedback_request is not None, (
    #     "Feedback request id should not be None"
    # )
    completions = await prisma.completion_response_model.find_many(
        where={"feedback_request_id": feedback_request.id}
    )

    try:
        async with prisma.tx(timeout=120000) as tx:
            for completion in completions:
                # Find or create the criterion record
                criterion = await tx.criterion.find_first(
                    where=CriterionWhereInput(
                        {
                            "completion_relation": {
                                "is": {
                                    "completion_id": completion.completion_id,
                                    "validator_task_id": task.id,
                                }
                            }
                        }
                    )
                )

                if not criterion:
                    logger.warning("Criterion not found, but it should already exist")
                    continue

                # the basics, just create raw scores
                if completion.score is None:
                    continue

                # TODO: figure out why the completion.score is None
                # TODO: figure out completion.rank_id is None, need to reconstruct from ground truth
                scores = Scores(
                    raw_score=completion.score,
                    rank_id=completion.rank_id,
                    # Initialize other scores as None - they'll be computed later
                    normalised_score=None,
                    ground_truth_score=None,
                    cosine_similarity_score=None,
                    normalised_cosine_similarity_score=None,
                    cubic_reward_score=None,
                )

                # Check if all fields in scores are None
                if all(value is None for value in scores.model_dump().values()):
                    logger.warning(
                        f"All scores are None for completion {completion.completion_id}"
                    )
                    continue

                await tx.minerscore.update(
                    where={
                        "criterion_id_miner_response_id": {
                            "criterion_id": criterion.id,
                            "miner_response_id": miner_response.id,
                        }
                    },
                    data=MinerScoreUpdateInput(
                        scores=Json(json.dumps(scores.model_dump()))
                    ),
                )
    except Exception as e:
        logger.error(f"Transaction failed for miner response {miner_response.id}: {e}")
    return


async def _process_task(task: ValidatorTask):
    if not task.miner_responses:
        logger.warning("No miner responses for task, skipping")
        return

    # Create semaphore to limit concurrent DB operations
    async def _process_with_semaphore(miner_response):
        async with sem:
            return await _process_miner_response(miner_response, task)

    tasks = []
    for miner_response in task.miner_responses:
        tasks.append(_process_with_semaphore(miner_response))

    # Wait for all tasks to complete
    if tasks:
        await asyncio.gather(*tasks)

    # Reload miner responses with updated scores
    updated_task = await prisma.validatortask.find_unique(
        where={"id": task.id},
        include={
            "completions": {"include": {"criterion": {"include": {"scores": True}}}},
            "ground_truth": True,
            "miner_responses": {
                "include": {
                    "scores": True,
                }
            },
        },
    )

    if not updated_task:
        logger.error(f"Failed to reload task {task.id} with updated scores")
        return

    task = updated_task

    # ensure completions are all json strings
    assert task.completions is not None, "Completions should not be None"
    # Ensure completions are in string format for the mapper
    for completion in task.completions:
        # logger.info(f"{type(completion.completion)}")
        if isinstance(completion.completion, dict):
            # NOTE: hack because otherwise the mapper.py functgion fails
            completion.completion = json.dumps(completion.completion)  # type: ignore
        elif isinstance(completion.completion, str):
            # Already in the right format
            pass
        else:
            logger.warning(f"Unexpected completion type: {type(completion.completion)}")

    mapped_miner_responses = [
        map_miner_response_to_task_synapse_object(
            miner_response,
            validator_task=task,
        )
        for miner_response in (task.miner_responses or [])
    ]

    updated_miner_responses = Scoring.calculate_score(
        validator_task=map_validator_task_to_task_synapse_object(task),
        miner_responses=mapped_miner_responses,
    )

    max_retries = 3
    retry_delay = 0.5  # seconds
    attempt = 0

    while attempt < max_retries:
        success, failed_hotkeys = await ORM.update_miner_scores(
            task_id=task.id,
            miner_responses=updated_miner_responses,
        )

        if success and not failed_hotkeys:
            break

        attempt += 1
        if attempt < max_retries:
            logger.warning(
                f"Failed to update scores for task: {task.id} on attempt {attempt}. "
                f"Failed hotkeys: {failed_hotkeys}. Retrying in {retry_delay} seconds..."
            )
            await asyncio.sleep(retry_delay)
        else:
            logger.error(
                f"Failed to update scores for task: {task.id} after {max_retries} attempts. "
                f"Failed hotkeys: {failed_hotkeys}"
            )


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
async def connect_with_retry():
    try:
        await connect_db()
        # Test the connection
        await prisma.validatortask.count()
        logger.info("Database connection established successfully")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


async def main():
    try:
        await connect_with_retry()
        all_bg_tasks = []

        async for validator_tasks, has_more_batches in get_processed_tasks(
            batch_size=20
        ):
            bg_tasks = []
            for task in validator_tasks:
                bg_task = asyncio.create_task(_process_task(task))
                bg_tasks.append(bg_task)
                all_bg_tasks.append(bg_task)

            if bg_tasks:
                await asyncio.gather(*bg_tasks)

            if not has_more_batches:
                logger.info("No more task batches to process")
                break

        if all_bg_tasks:
            await asyncio.gather(*all_bg_tasks)

        await disconnect_db()
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        try:
            await disconnect_db()
        except Exception as e:
            pass


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


if __name__ == "__main__":
    asyncio.run(main())
