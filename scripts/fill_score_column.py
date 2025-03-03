import asyncio
import json
import multiprocessing
import os
import random
import time

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

BATCH_SIZE = int(os.getenv("FILL_SCORE_BATCH_SIZE", 10))
MAX_CONCURRENT_TASKS = int(os.getenv("FILL_SCORE_MAX_CONCURRENT_TASKS", 5))
TX_TIMEOUT = int(os.getenv("FILL_SCORE_TX_TIMEOUT", 10000))

# Get number of CPU cores
nproc = multiprocessing.cpu_count()
sem = asyncio.Semaphore(min(MAX_CONCURRENT_TASKS, nproc))  # Limit concurrent operations


class FillScoreStats:
    def __init__(self):
        self.start_time = time.time()
        self.last_count = 0
        self.last_time = self.start_time
        self.tasks_per_minute = 0

        # Processing stats
        self.total_tasks = 0
        self.processed_tasks = 0
        self.failed_tasks = 0
        self.updated_scores = 0

    def update_rate(self):
        """Calculate tasks per minute rate"""
        current_time = time.time()
        current_count = self.processed_tasks

        # Calculate tasks per minute
        time_diff = current_time - self.last_time
        if time_diff >= 1.0:  # Update rate every second
            count_diff = current_count - self.last_count
            self.tasks_per_minute = (count_diff * 60) / time_diff
            self.last_count = current_count
            self.last_time = current_time

    def _get_progress_bar(self, width=50):
        """Generate a progress bar string."""
        if self.total_tasks == 0:
            return "[" + " " * width + "] 0%"

        progress = self.processed_tasks / self.total_tasks
        filled = int(width * progress)
        bar = (
            "["
            + "=" * filled
            + (">" if filled < width else "")
            + " " * (width - filled - 1)
            + "]"
        )
        return f"{bar} {progress * 100:.1f}%"

    def log_progress(self):
        """Show progress bar and stats"""
        current_time = time.time()
        elapsed = current_time - self.start_time
        progress_bar = self._get_progress_bar(width=30)
        self.update_rate()

        # Calculate ETA
        if self.processed_tasks > 0:
            rate = self.processed_tasks / elapsed
            remaining = self.total_tasks - self.processed_tasks
            eta_seconds = remaining / rate if rate > 0 else 0
            hours = int(eta_seconds // 3600)
            minutes = int((eta_seconds % 3600) // 60)
            seconds = int(eta_seconds % 60)
            eta_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        else:
            eta_str = "--:--:--"

        # Format elapsed time
        elapsed_hours = int(elapsed // 3600)
        elapsed_minutes = int((elapsed % 3600) // 60)
        elapsed_seconds = int(elapsed % 60)
        elapsed_str = f"{elapsed_hours:02d}:{elapsed_minutes:02d}:{elapsed_seconds:02d}"

        # Print progress with fixed width format and progress bar
        print(
            f"\r{progress_bar} | {self.processed_tasks}/{self.total_tasks} | Updated: {self.updated_scores} | {self.tasks_per_minute:.0f} t/min | Time: {elapsed_str} | ETA: {eta_str}",
            end="",
            flush=True,
        )

    def print_final_stats(self):
        """Print detailed statistics at the end of processing."""
        elapsed = time.time() - self.start_time
        success_rate = (
            (self.processed_tasks - self.failed_tasks) / max(self.processed_tasks, 1)
        ) * 100

        print("\n\nFill Score Results:")
        print("=" * 50)
        print(f"\nTime Taken: {elapsed:.2f} seconds")
        print("\nProcessed Tasks:")
        print("-" * 20)
        print(f"Total: {self.processed_tasks}/{self.total_tasks}")
        print(f"Failed: {self.failed_tasks}")
        print(f"Success Rate: {success_rate:.1f}%")
        print(f"Updated Scores: {self.updated_scores}")
        print("\n" + "=" * 50)


# Initialize stats
stats = FillScoreStats()


@retry(stop=stop_after_attempt(8), wait=wait_exponential(multiplier=3, min=5, max=120))
async def execute_transaction(miner_response_id, tx_function):
    try:
        # Add a small random delay before starting transaction to reduce contention
        await asyncio.sleep(random.uniform(0.1, 1.0))
        async with prisma.tx(timeout=TX_TIMEOUT) as tx:
            await tx_function(tx)
    except Exception as e:
        logger.error(f"Transaction failed for miner response {miner_response_id}: {e}")
        raise  # Re-raise to trigger retry


# 1. for each record from `miner_response` find the corresponding record from `Completion_Response_Model`
# 2. gather all scores from multiple `Completion_Response_Model` records and fill the relevant records inside `miner_response.scores`
# 3. based on all the raw scores provided by miners, apply the `Scoring.score_by_criteria` to calculate the scores for each criterion


def _is_empty_scores(record: MinerScore) -> bool:
    # Avoid parsing JSON if possible
    if not record.scores:
        return True

    # Only parse if needed
    try:
        scores = json.loads(record.scores)
        return not scores
    except (json.JSONDecodeError, TypeError):
        logger.warning(f"Invalid JSON in scores for record {record.id}")
        return True


def _is_all_empty_scores(records: list[MinerScore]) -> bool:
    return all(_is_empty_scores(record) for record in records)


async def _process_miner_response(miner_response: MinerResponse, task: ValidatorTask):
    scores = miner_response.scores

    if scores is not None and not _is_all_empty_scores(scores):
        return False  # No update needed
    else:
        logger.trace("No scores for miner response, attempting to fill from old tables")

    # find the scores from old tables
    feedback_request = await prisma.feedback_request_model.find_first(
        where={
            "parent_id": task.id,
            "hotkey": miner_response.hotkey,
        }
    )
    if feedback_request is None:
        logger.warning("Feedback request not found, skipping")
        return False

    completions = await prisma.completion_response_model.find_many(
        where={"feedback_request_id": feedback_request.id}
    )

    if not completions:
        logger.warning(
            f"No completions found for feedback request {feedback_request.id}"
        )
        return False

    # Define the transaction function
    async def tx_function(tx):
        updated_count = 0
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

            try:
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
                updated_count += 1
            except Exception as e:
                logger.warning(
                    f"Failed to update score for criterion {criterion.id}, miner response {miner_response.id}: {e}"
                )
                # Continue with other updates instead of failing the whole transaction
                continue

        return updated_count

    try:
        # Use the retry-enabled transaction executor
        updated_count = await execute_transaction(miner_response.id, tx_function)
        if updated_count:
            stats.updated_scores += updated_count
            return True
        return False
    except Exception as e:
        logger.error(
            f"All transaction attempts failed for miner response {miner_response.id}: {e}"
        )
        return False


async def _process_task(task: ValidatorTask):
    async with sem:  # Use semaphore to limit concurrent tasks
        try:
            if not task.miner_responses:
                logger.warning("No miner responses for task, skipping")
                return False

            # Process each miner response
            updated = False
            for miner_response in task.miner_responses:
                result = await _process_miner_response(miner_response, task)
                updated = updated or result
                # Add small delay between processing miner responses to avoid overloading DB
                await asyncio.sleep(0.1)

            logger.info(f"Proceeding with score calculation for task {task.id}")

            # Reload miner responses with updated scores
            updated_task = await prisma.validatortask.find_unique(
                where={"id": task.id},
                include={
                    "completions": {
                        "include": {"criterion": {"include": {"scores": True}}}
                    },
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
                return False

            task = updated_task

            # ensure completions are all json strings
            assert task.completions is not None, "Completions should not be None"
            # Ensure completions are in string format for the mapper
            for completion in task.completions:
                if isinstance(completion.completion, dict):
                    # NOTE: hack because otherwise the mapper.py function fails
                    completion.completion = json.dumps(completion.completion)  # type: ignore
                elif isinstance(completion.completion, str):
                    # Already in the right format
                    pass
                else:
                    logger.warning(
                        f"Unexpected completion type: {type(completion.completion)}"
                    )

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
                    return True

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

            return False
        except Exception as e:
            logger.error(f"Error processing task {task.id}: {e}")
            stats.failed_tasks += 1
            return False


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

        # Count total tasks to process for progress tracking
        vali_where_query = ValidatorTaskWhereInput({"is_processed": True})
        stats.total_tasks = await prisma.validatortask.count(where=vali_where_query)

        logger.info(f"Starting to process {stats.total_tasks} validator tasks")

        skip = 0
        while True:
            # Get batch of tasks
            validator_tasks = await prisma.validatortask.find_many(
                skip=skip,
                take=BATCH_SIZE,
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

            if not validator_tasks:
                break

            # Create tasks for batch processing
            batch_tasks = []
            for task in validator_tasks:
                batch_tasks.append(asyncio.create_task(_process_task(task)))

            # Wait for all tasks in batch to complete
            if batch_tasks:
                await asyncio.gather(*batch_tasks)
                stats.processed_tasks += len(batch_tasks)
                stats.log_progress()

            # Add a small delay between batches to reduce database load
            await asyncio.sleep(1.0)
            skip += BATCH_SIZE

            # Check if we've processed all tasks
            if skip >= stats.total_tasks:
                break

        await disconnect_db()
        stats.print_final_stats()
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        try:
            await disconnect_db()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nProcess interrupted by user")
        stats.print_final_stats()
    except Exception as e:
        logger.error(f"Unhandled exception in main: {e}")
        try:
            # Attempt to disconnect DB on any error
            asyncio.run(disconnect_db())
        except Exception:
            pass
        raise
