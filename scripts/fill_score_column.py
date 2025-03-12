import asyncio
import json
import os
import sys
import time
import traceback

import numpy as np
import torch
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from database.client import connect_db, disconnect_db, prisma
from database.prisma import Json
from database.prisma.models import (
    Criterion,
    GroundTruth,
    MinerResponse,
    MinerScore,
    ValidatorTask,
)
from database.prisma.types import (
    CriterionWhereInput,
    MinerScoreUpdateInput,
    ValidatorTaskWhereInput,
)
from dojo.protocol import Scores
from dojo.utils.config import source_dotenv

logger.remove()  # Remove default handlers
logger.add(
    sink=sys.stderr,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
)


def minmax_scale(tensor: torch.Tensor | np.ndarray) -> torch.Tensor:
    """Move this function outside the class to match scoring.py"""
    if isinstance(tensor, np.ndarray):
        tensor = torch.from_numpy(tensor)
    min = tensor.min()
    max = tensor.max()

    # If max == min, return a tensor of ones
    if max == min:
        return torch.ones_like(tensor)

    return (tensor - min) / (max - min)


def _reward_cubic(
    miner_outputs: np.ndarray,
    ground_truth: np.ndarray,
    scaling: float = 0.006,
    translation: float = 7,
    offset: float = 2,
    visualize: bool = False,
) -> tuple[np.ndarray, np.ndarray, torch.Tensor, torch.Tensor]:
    """Calculate cubic reward based on miner outputs and ground truth.

    Args:
        miner_outputs (np.ndarray): 2D array of miner outputs (shape: num_miners x num_completions).
        ground_truth (np.ndarray): 1D array of ground truth values (shape: num_completions).
        scaling (float): Scaling factor for the cubic function.
        translation (float): Translation factor for the cubic function.
        offset (float): Offset for the cubic function.
        visualize (bool): Whether to visualize the results.

    Returns:
        tuple: (points, cosine_similarity_scores, normalised_cosine_similarity_scores, cubic_reward_scores)
    """
    # ensure ground truth is a column vector for broadcasting
    # shape: (1, num_completions)
    ground_truth = ground_truth.reshape(1, -1)

    # ensure dims for broadcasting
    assert len(ground_truth.shape) == 2
    assert len(miner_outputs.shape) == 2

    # shape: (num_miners,)
    # number range [-1, 1]
    cosine_similarity_scores = torch.nn.functional.cosine_similarity(
        torch.from_numpy(miner_outputs.copy()),
        torch.from_numpy(ground_truth.copy()),
        dim=1,
    ).numpy()

    # Convert nans to -1 to send it to the bottom
    cosine_similarity_scores = np.where(
        np.isnan(cosine_similarity_scores), -1, cosine_similarity_scores
    )

    # transform from range [-1, 1] to [0, 1]
    normalised_cosine_similarity_scores = (cosine_similarity_scores + 1) / 2

    # ensure sum is 1
    normalised_cosine_similarity_scores = torch.nn.functional.normalize(
        torch.from_numpy(normalised_cosine_similarity_scores), p=1, dim=0
    )
    assert normalised_cosine_similarity_scores.shape[0] == miner_outputs.shape[0]

    # apply the cubic transformation
    cubic_reward_scores = (
        scaling * (normalised_cosine_similarity_scores - translation) ** 3 + offset
    )

    # case where a miner provides the same score for all completions
    # convert any nans to zero
    points = np.where(np.isnan(cubic_reward_scores), 0, cubic_reward_scores)

    # ensure all values are in the range [0, 1]
    points = minmax_scale(points)
    points = points.numpy()

    assert isinstance(points, np.ndarray)
    return (
        points,
        cosine_similarity_scores,
        normalised_cosine_similarity_scores,
        cubic_reward_scores,
    )


class Scoring:
    @classmethod
    def minmax_scale(cls, tensor: torch.Tensor | np.ndarray) -> torch.Tensor:
        """Keep for backward compatibility, but use the global function instead"""
        return minmax_scale(tensor)

    @classmethod
    def _convert_ground_truth_ranks_to_scores(
        cls,
        cids_with_ranks: list[tuple[str, int]],
    ) -> np.ndarray:
        # check if the cids with ranks are sorted in ascending order
        ranks = [rank for _, rank in cids_with_ranks]
        # check if the ranks are continuous e.g. [0, 1, 2, 3] and not [0, 1, 3, 2]
        is_sorted_and_continuous = all(
            ranks[i] == ranks[i - 1] + 1 for i in range(1, len(ranks))
        )
        if not is_sorted_and_continuous:
            raise ValueError("Provided ranks must be sorted and must be continuous")

        # use minmax scale to ensure ground truth is in the range [0, 1]
        ground_truth_arr = minmax_scale(np.array(ranks)).numpy()

        # reverse order here, because the lowest rank is the best
        # e.g. ranks: ('cid1', 0), ('cid2', 1), ('cid3', 2), ('cid4', 3)
        # after minmax scale: [0, 0.33, 0.667, 1]
        # but we want the reverse, so: [1, 0.667, 0.33, 0], since cid1 is the best
        ground_truth_arr = ground_truth_arr[::-1]

        return ground_truth_arr

    @classmethod
    def ground_truth_scoring(
        cls,
        ground_truth_dict: dict[str, int],
        miner_outputs_list: list[list[float]],
    ) -> tuple[
        torch.Tensor,
        np.ndarray,
        np.ndarray,
        np.ndarray,
        torch.Tensor,
        torch.Tensor,
    ]:
        """
        Calculate score between all miner outputs and ground truth.
        Updated to handle multiple miners with multiple completions, matching scoring.py.

        Args:
            ground_truth_dict: Dictionary mapping completion_id to rank.
            miner_outputs_list: List of lists, where each inner list contains scores for one miner.

        Returns:
            tuple: (cubic_reward, miner_outputs, miner_outputs_normalised,
                   cosine_similarity_scores, normalised_cosine_similarity_scores, cubic_reward_scores)
        """
        cid_rank_tuples = [
            (completion_id, rank) for completion_id, rank in ground_truth_dict.items()
        ]

        # Sort cids by rank. In the order, 0 is the best, 1 is the second best, etc.
        cid_with_rank_sorted = sorted(
            cid_rank_tuples, key=lambda x: x[1], reverse=False
        )

        if not miner_outputs_list:
            raise ValueError("Miner outputs cannot be empty")

        # Ensure all miners have provided scores for all completions
        if any(None in miner_outputs for miner_outputs in miner_outputs_list):
            raise ValueError("Miner outputs cannot contain None values")

        # Convert to numpy array
        miner_outputs_arr = np.array(miner_outputs_list, dtype=np.float32)

        # Handle the case of a single miner
        if len(miner_outputs_arr.shape) == 1:
            miner_outputs_arr = miner_outputs_arr.reshape(1, -1)

        # convert miner outputs to something ordinal
        miner_outputs_normalised = np.array(
            [cls.minmax_scale(m) for m in miner_outputs_arr]
        )

        # use minmax scale to ensure ground truth is in the range [0, 1]
        ground_truth_arr = cls._convert_ground_truth_ranks_to_scores(
            cid_with_rank_sorted
        )

        (
            cubic_reward,
            cosine_similarity_scores,
            normalised_cosine_similarity_scores,
            cubic_reward_scores,
        ) = _reward_cubic(
            miner_outputs_arr, ground_truth_arr, 0.006, 7, 2, visualize=False
        )

        # normalize to ensure sum is 1
        cubic_reward = cubic_reward / np.sum(cubic_reward)

        return (
            torch.from_numpy(cubic_reward.copy()),
            miner_outputs_arr,
            miner_outputs_normalised,
            cosine_similarity_scores,
            normalised_cosine_similarity_scores,
            cubic_reward_scores,
        )

    @classmethod
    def calculate_scores_for_completion(
        cls,
        raw_score: float,
        rank_id: int | None,
        ground_truth: list[GroundTruth],
        miner_outputs: list[float],
    ) -> Scores | None:
        """Calculate scores for a single miner completion."""
        if not ground_truth:
            logger.warning("No ground truth data provided for score calculation")
            return None

        # Prepare ground truth data
        ground_truth_dict = {
            gt.real_model_id: gt.rank_id
            for gt in ground_truth
            if gt.real_model_id is not None and gt.rank_id is not None
        }

        if not ground_truth_dict:
            logger.warning(
                "No valid ground truth data found, ground truth length: ",
                len(ground_truth),
            )
            return None

        try:
            # Call with a list containing a single miner's outputs
            (
                gt_score,
                miner_outputs_arr,
                miner_outputs_normalised,
                cosine_similarity_scores,
                normalised_cosine_similarity_scores,
                cubic_reward_scores,
            ) = cls.ground_truth_scoring(ground_truth_dict, [miner_outputs])

            return Scores(
                raw_score=raw_score,
                rank_id=rank_id,
                normalised_score=float(miner_outputs_normalised[0, 0]),
                ground_truth_score=float(gt_score[0]),
                cosine_similarity_score=float(cosine_similarity_scores[0]),
                normalised_cosine_similarity_score=float(
                    normalised_cosine_similarity_scores[0]
                ),
                cubic_reward_score=float(cubic_reward_scores[0]),
            )

        except Exception as e:
            logger.warning(f"Failed to calculate scores: {e}")
            return None

    @classmethod
    def calculate_scores_for_all_completions(
        cls,
        task: ValidatorTask,
        miner_responses: list[MinerResponse],
        completion_data: dict[str, list[tuple[str, float]]],
    ) -> dict[str, dict[str, Scores]]:
        """
        Calculate scores for all miners across all completions.

        Args:
            task: The validator task with ground truth
            miner_responses: List of miner response database objects
            completion_data: Dictionary mapping miner hotkeys to lists of (completion_id, score) tuples

        Returns:
            Dictionary mapping miner_response_id to a dictionary of criterion_id -> Scores
        """
        if not task.ground_truth:
            logger.warning("No ground truth data provided for score calculation")
            return {}

        # Prepare ground truth data
        ground_truth_dict = {
            gt.real_model_id: gt.rank_id
            for gt in task.ground_truth
            if gt.real_model_id is not None and gt.rank_id is not None
        }

        if not ground_truth_dict:
            logger.warning("No valid ground truth data found")
            return {}

        # Prepare the miner outputs in the right format for ground_truth_scoring
        sorted_cids = [
            cid
            for cid, _ in sorted(
                [(cid, rank) for cid, rank in ground_truth_dict.items()],
                key=lambda x: x[1],
            )
        ]

        miner_outputs_list = []
        hotkey_to_index = {}

        for i, miner in enumerate(miner_responses):
            if miner.hotkey not in completion_data:
                continue

            # hotkey_to_index[miner.hotkey] = i

            # Sort the completion scores by completion ID order
            completion_scores = completion_data[miner.hotkey]
            scores_by_cid = {cid: score for cid, score in completion_scores}

            # Create a list of scores in the order of sorted_cids
            sorted_scores = [scores_by_cid.get(cid, None) for cid in sorted_cids]

            # Skip miners with missing scores
            if None in sorted_scores:
                continue

            hotkey_to_index[miner.hotkey] = len(miner_outputs_list)
            miner_outputs_list.append(sorted_scores)

        if not miner_outputs_list:
            logger.warning("No valid miner outputs found")
            return {}

        try:
            # Calculate scores for all miners at once
            (
                gt_scores,
                miner_outputs_arr,
                miner_outputs_normalised,
                cosine_similarity_scores,
                normalised_cosine_similarity_scores,
                cubic_reward_scores,
            ) = cls.ground_truth_scoring(ground_truth_dict, miner_outputs_list)

            # Organize results by miner response ID
            results = {}
            miner_index = 0
            # logger.info(f"hotkey_to_index: {hotkey_to_index}")
            # logger.info(f"Number of valid miners: {len(miner_outputs_list)}")
            for miner in miner_responses:
                if miner.hotkey not in hotkey_to_index:
                    continue

                miner_index = hotkey_to_index[miner.hotkey]
                miner_results = {}

                # For each completion by this miner
                # logger.info(f"Processing miner {miner.hotkey} with index {miner_index}")
                for cid_idx, cid in enumerate(sorted_cids):
                    # Find the completion record for this miner and completion
                    completion_scores = [
                        s for c, s in completion_data.get(miner.hotkey, []) if c == cid
                    ]
                    if not completion_scores:
                        continue

                    raw_score = completion_scores[0]

                    # Create a Scores object
                    scores = Scores(
                        raw_score=float(raw_score),
                        normalised_score=float(
                            miner_outputs_normalised[miner_index, cid_idx]
                        ),
                        ground_truth_score=float(gt_scores[miner_index]),
                        cosine_similarity_score=float(
                            cosine_similarity_scores[miner_index]
                        ),
                        normalised_cosine_similarity_score=float(
                            normalised_cosine_similarity_scores[miner_index]
                        ),
                        cubic_reward_score=float(cubic_reward_scores[miner_index]),
                    )

                    # Store by completion ID
                    miner_results[cid] = scores

                results[miner.id] = miner_results
                # logger.info(f"Processed miner {miner.hotkey} with index {miner_index}")

            return results

        except Exception as e:
            traceback.print_exc()
            logger.error(f"Failed to calculate scores for all completions: {e}")
            return {}


source_dotenv()

BATCH_SIZE = int(os.getenv("FILL_SCORE_BATCH_SIZE", 10))
MAX_CONCURRENT_TASKS = int(os.getenv("FILL_SCORE_MAX_CONCURRENT_TASKS", 5))
TX_TIMEOUT = int(os.getenv("FILL_SCORE_TX_TIMEOUT", 30000))
UPDATE_BATCH_SIZE = int(os.getenv("FILL_SCORE_UPDATE_BATCH_SIZE", 10))

# Get number of CPU cores
sem = asyncio.Semaphore(MAX_CONCURRENT_TASKS)  # Limit concurrent operations


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
        async with prisma.tx(timeout=TX_TIMEOUT) as tx:
            result = await tx_function(tx)
            return result
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


async def prefetch_completion_data(task_id, miner_hotkeys):
    """Fetch all completion data for a task in a single query"""
    # Get all feedback requests for this task
    feedback_requests = await prisma.feedback_request_model.find_many(
        where={"parent_id": task_id, "hotkey": {"in": miner_hotkeys}},
    )

    # Get all completions for these feedback requests
    if not feedback_requests:
        return {}

    # we do this because we want to avoid a N+1 query
    request_ids = [fr.id for fr in feedback_requests]
    hotkey_by_request = {fr.hotkey: fr.id for fr in feedback_requests}

    # collect all completions for these child feedback requests (previously miner responses)
    completions = await prisma.completion_response_model.find_many(
        where={"feedback_request_id": {"in": request_ids}},
    )

    # Organize by miner hotkey
    result = {}
    for completion in completions:
        if completion.score is not None and completion.completion_id:
            for hotkey, request_id in hotkey_by_request.items():
                if completion.feedback_request_id == request_id:
                    if hotkey not in result:
                        result[hotkey] = []
                    result[hotkey].append((completion.completion_id, completion.score))
                    break

    return result


async def prefetch_criteria(task_id, completion_ids):
    """Prefetch all criteria for a task to avoid individual queries"""
    criteria: list[Criterion] = await prisma.criterion.find_many(
        where=CriterionWhereInput(
            {
                "completion_relation": {
                    "is": {
                        "validator_task_id": task_id,
                        "completion_id": {"in": completion_ids},
                    }
                }
            }
        ),
        include={"completion_relation": True},
    )

    # Create lookup dictionary
    result = {}
    for c in criteria:
        if c.completion_relation and c.completion_relation.completion_id:
            result[c.completion_relation.completion_id] = c

    return result


async def batch_update_scores(tx, updates) -> int:
    """Process updates in batches to reduce database round trips"""
    successful_updates = 0

    # Process in smaller batches
    batch_size = UPDATE_BATCH_SIZE
    # logger.info(
    #     f"Updating {len(updates)} scores in {len(updates) // batch_size} batches"
    # )
    for i in range(0, len(updates), batch_size):
        batch = updates[i : i + batch_size]
        async with tx.batch_() as batcher:  # type: ignore
            for update in batch:
                try:
                    batcher.minerscore.update(**update)

                    successful_updates += 1
                except Exception as e:
                    logger.warning(f"Failed to update score: {e}")

    return successful_updates


async def _process_task(task: ValidatorTask):
    async with sem:  # Use semaphore to limit concurrent tasks
        try:
            # Check if miner_responses exists and is not empty
            if not task.miner_responses:
                logger.warning("No miner responses for task, skipping")
                return False

            # 1. Collect all miner data for the task
            miner_hotkeys = []
            for miner in task.miner_responses:
                if miner.hotkey:
                    miner_hotkeys.append(miner.hotkey)

            all_completion_data = await prefetch_completion_data(task.id, miner_hotkeys)

            # logger.info(f"All completion data: {all_completion_data}")

            if not all_completion_data:
                logger.debug("No valid completion data found for any miner")
                return False

            # 2. Calculate scores for all miners together
            all_scores = Scoring.calculate_scores_for_all_completions(
                task,
                [
                    m
                    for m in task.miner_responses or []
                    if m.hotkey in all_completion_data
                ],
                all_completion_data,
            )

            if not all_scores:
                logger.warning("No scores calculated for any miner")
                return False

            # 3. Update database in a single transaction
            async def tx_function(tx):
                # Collect all completion IDs
                all_completion_ids = set()
                for miner_id, completions in all_scores.items():
                    all_completion_ids.update(completions.keys())

                # Prefetch all criteria
                criteria_by_completion = await prefetch_criteria(
                    task.id, list(all_completion_ids)
                )

                # Prepare all updates
                all_updates = []

                if not task.miner_responses:
                    logger.warning("No miner responses for task, skipping")
                    return False

                for miner in task.miner_responses:
                    # Skip if no scores calculated for this miner
                    if miner.id not in all_scores:
                        continue

                    # Skip if already has scores
                    if miner.scores is not None and not _is_all_empty_scores(
                        miner.scores
                    ):
                        continue

                    miner_scores = all_scores[miner.id]

                    for completion_id, score_obj in miner_scores.items():
                        if completion_id not in criteria_by_completion:
                            continue

                        criterion = criteria_by_completion[completion_id]
                        all_updates.append(
                            {
                                "where": {
                                    "criterion_id_miner_response_id": {
                                        "criterion_id": criterion.id,
                                        "miner_response_id": miner.id,
                                    }
                                },
                                "data": MinerScoreUpdateInput(
                                    scores=Json(json.dumps(score_obj.model_dump()))
                                ),
                            }
                        )

                # Batch update all scores
                return await batch_update_scores(tx, all_updates)

            try:
                # Use the retry-enabled transaction executor
                updated_count = await execute_transaction(task.id, tx_function)
                if updated_count:
                    stats.updated_scores += updated_count
                    return True
                return False
            except Exception as e:
                logger.error(f"All transaction attempts failed for task {task.id}: {e}")
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
