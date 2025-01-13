import json
import time
from functools import lru_cache

import bittensor as bt
from bittensor.utils.btlogging import logging as logger

from database.client import connect_db, disconnect_db, prisma
from database.prisma import Json
from database.prisma.enums import CriteriaTypeEnum, TaskTypeEnum

BATCH_SIZE = 5  # Number of records to process in each batch


class MigrationStats:
    def __init__(self):
        self.start_time = time.time()
        # Old table stats
        self.total_feedback_requests = 0
        self.parent_requests = 0
        self.child_requests = 0
        self.old_completions = 0
        self.old_ground_truths = 0
        self.old_criteria_types = 0

        # Migration progress stats
        self.total_requests = 0
        self.processed_requests = 0
        self.processed_parent_requests = 0
        self.processed_child_requests = 0
        self.failed_requests = 0

        # New table stats
        self.validator_tasks_count = 0
        self.miner_responses_count = 0
        self.completions_count = 0
        self.ground_truths_count = 0
        self.criteria_count = 0
        self.miner_scores_count = 0

        # Pass tracking
        self.current_pass = 1

    async def collect_old_stats(self):
        """Collect statistics from old tables."""
        # Get counts from old tables
        self.total_feedback_requests = await prisma.feedback_request_model.count()
        self.parent_requests = await prisma.feedback_request_model.count(
            where={"parent_id": None}
        )
        self.child_requests = self.total_feedback_requests - self.parent_requests

        # Set total_requests to match total_feedback_requests
        self.total_requests = self.total_feedback_requests

        self.old_completions = await prisma.completion_response_model.count()
        self.old_ground_truths = await prisma.ground_truth_model.count()
        self.old_criteria_types = await prisma.criteria_type_model.count()

        # Print old table stats
        print("\nOld Table Statistics:")
        print("=" * 50)
        print(f"Feedback Requests Total: {self.total_feedback_requests}")
        print(f"├─ Parent Requests: {self.parent_requests}")
        print(f"└─ Child Requests: {self.child_requests}")
        print(f"\nCompletions: {self.old_completions}")
        print(f"Ground Truths: {self.old_ground_truths}")
        print(f"Criteria Types: {self.old_criteria_types}")
        print("=" * 50)
        print("\nStarting migration...\n")

    def _get_progress_bar(self, width=50):
        """Generate a progress bar string."""
        if self.current_pass == 1:
            total = self.parent_requests
            processed = self.processed_parent_requests
        else:
            total = self.child_requests
            processed = self.processed_child_requests

        if total == 0:
            return "[" + " " * width + "] 0%"

        progress = processed / total
        filled = int(width * progress)
        bar = "[" + "=" * filled + ">" + " " * (width - filled - 1) + "]"
        return f"{bar} {progress * 100:.1f}%"

    def log_progress(self):
        """Show progress bar and stats for current pass."""
        elapsed = time.time() - self.start_time
        progress_bar = self._get_progress_bar()

        if self.current_pass == 1:
            print(
                f"\rStep 1: {progress_bar} ({self.processed_parent_requests}/{self.parent_requests}) "
                f"[Tasks: {self.validator_tasks_count}, Completions: {self.completions_count}, "
                f"Ground Truths: {self.ground_truths_count}, Criteria: {self.criteria_count}] "
                f"[{elapsed:.1f}s]",
                end="",
                flush=True,
            )
        else:
            print(
                f"\rStep 2: {progress_bar} ({self.processed_child_requests}/{self.child_requests}) "
                f"[Miner Responses: {self.miner_responses_count}, Miner Scores: {self.miner_scores_count}] "
                f"[{elapsed:.1f}s]",
                end="",
                flush=True,
            )

    def print_final_stats(self):
        """Print detailed statistics at the end of migration."""
        elapsed = time.time() - self.start_time
        success_rate = (
            (self.processed_requests - self.failed_requests)
            / max(self.processed_requests, 1)
        ) * 100

        print("\n\nMigration Results:")
        print("=" * 50)
        print(f"\nTime Taken: {elapsed:.2f} seconds")
        print("\nProcessed Requests:")
        print("-" * 20)
        print(f"Total: {self.processed_requests}/{self.total_requests}")
        print(
            f"├─ Parent Requests: {self.processed_parent_requests}/{self.parent_requests}"
        )
        print(
            f"└─ Child Requests: {self.processed_child_requests}/{self.child_requests}"
        )
        print(f"Failed: {self.failed_requests}")
        print(f"Success Rate: {success_rate:.1f}%")

        print("\nMigrated Records:")
        print("-" * 20)
        print(f"Validator Tasks: {self.validator_tasks_count}")
        print(f"Miner Responses: {self.miner_responses_count}")
        print(f"Completions: {self.completions_count}")
        print(f"Ground Truths: {self.ground_truths_count}")
        print(f"Criteria: {self.criteria_count}")
        print("\n" + "=" * 50)


# Initialize stats at module level
stats = MigrationStats()


@lru_cache(maxsize=1024)
def get_coldkey_from_hotkey(subtensor: bt.Subtensor, hotkey: str) -> str:
    coldkey_scale_encoded = subtensor.query_subtensor(
        name="Owner",
        params=[hotkey],
    )
    return coldkey_scale_encoded.value  # type: ignore


async def migrate():
    await connect_db()
    subtensor = bt.subtensor(network="finney")

    # Collect and display old table statistics
    await stats.collect_old_stats()

    try:
        # Pass 1: Process parent requests
        print("\nProcessing parent requests...")
        stats.current_pass = 1

        skip = 0
        while True:
            batch = await prisma.feedback_request_model.find_many(
                where={"parent_id": None},
                take=BATCH_SIZE,
                skip=skip,
                include={
                    "completions": True,
                    "criteria_types": True,
                    "ground_truths": True,
                },
            )

            if not batch:
                break

            for old_request in batch:
                try:
                    # Map task type
                    task_type = TaskTypeEnum.CODE_GENERATION  # Default fallback
                    if old_request.task_type.lower().find("image") >= 0:
                        task_type = TaskTypeEnum.TEXT_TO_IMAGE
                    elif old_request.task_type.lower().find("3d") >= 0:
                        task_type = TaskTypeEnum.TEXT_TO_THREE_D

                    # Create parent validator task
                    new_validator_task = await prisma.validatortask.upsert(
                        where={"id": old_request.id},
                        data={
                            "create": {
                                "id": old_request.id,
                                "prompt": old_request.prompt,
                                "task_type": task_type,
                                "is_processed": old_request.is_processed,
                                "expire_at": old_request.expire_at,
                                "created_at": old_request.created_at,
                                "updated_at": old_request.updated_at,
                            },
                            "update": {
                                "prompt": old_request.prompt,
                                "task_type": task_type,
                                "is_processed": old_request.is_processed,
                                "expire_at": old_request.expire_at,
                                "created_at": old_request.created_at,
                                "updated_at": old_request.updated_at,
                            },
                        },
                    )
                    stats.validator_tasks_count += 1

                    # Migrate completions
                    if old_request.completions:
                        for old_completion in old_request.completions:
                            # Create or update completion
                            new_completion = await prisma.completion.upsert(
                                where={"id": old_completion.id},
                                data={
                                    "create": {
                                        "id": old_completion.id,
                                        "completion_id": old_completion.completion_id,
                                        "validator_task_id": new_validator_task.id,
                                        "model": old_completion.model,
                                        "completion": old_completion.completion,
                                        "created_at": old_completion.created_at,
                                        "updated_at": old_completion.updated_at,
                                    },
                                    "update": {
                                        "model": old_completion.model,
                                        "completion": old_completion.completion,
                                        "created_at": old_completion.created_at,
                                        "updated_at": old_completion.updated_at,
                                        "validator_task_relation": {  # This also verifies and connects the relationship
                                            "connect": {"id": new_validator_task.id}
                                        },
                                    },
                                },
                            )
                            stats.completions_count += 1

                            # Migrate criteria for this completion
                            if old_request.criteria_types:
                                for old_criterion in old_request.criteria_types:
                                    # Skip RANKING_CRITERIA as it's no longer supported
                                    if old_criterion.type == "RANKING_CRITERIA":
                                        continue

                                    # Convert MULTI_SCORE to SCORE
                                    criteria_type = old_criterion.type
                                    config = None
                                    if criteria_type == "MULTI_SCORE":
                                        criteria_type = "SCORE"
                                        config = json.dumps(
                                            {
                                                "min": old_criterion.min,
                                                "max": old_criterion.max,
                                            }
                                        )

                                    # First find if criterion exists
                                    existing_criterion = (
                                        await prisma.criterion.find_first(
                                            where={
                                                "completion_id": new_completion.id,
                                                "criteria_type": CriteriaTypeEnum[
                                                    criteria_type
                                                ],
                                            }
                                        )
                                    )

                                    if existing_criterion:
                                        await prisma.criterion.update(
                                            where={"id": existing_criterion.id},
                                            data={
                                                "config": Json(
                                                    config if config else "{}"
                                                ),
                                                "updated_at": old_criterion.updated_at,
                                            },
                                        )
                                    else:
                                        await prisma.criterion.create(
                                            data={
                                                "completion_id": new_completion.id,
                                                "criteria_type": CriteriaTypeEnum[
                                                    criteria_type
                                                ],
                                                "config": Json(
                                                    config if config else "{}"
                                                ),
                                                "created_at": old_criterion.created_at,
                                                "updated_at": old_criterion.updated_at,
                                            }
                                        )
                                    stats.criteria_count += 1

                    # Migrate ground truths
                    if old_request.ground_truths:
                        for old_ground_truth in old_request.ground_truths:
                            # Map rank_id to corresponding score
                            rank_scores = {0: 0.0, 1: 0.33333334, 2: 0.6666667, 3: 1.0}
                            ground_truth_score = rank_scores.get(
                                old_ground_truth.rank_id, 0.0
                            )

                            await prisma.groundtruth.upsert(
                                where={"id": old_ground_truth.id},
                                data={
                                    "create": {
                                        "id": old_ground_truth.id,
                                        "validator_task_id": new_validator_task.id,
                                        "obfuscated_model_id": old_ground_truth.obfuscated_model_id,
                                        "real_model_id": old_ground_truth.real_model_id,
                                        "rank_id": old_ground_truth.rank_id,
                                        "ground_truth_score": ground_truth_score,
                                        "created_at": old_ground_truth.created_at,
                                        "updated_at": old_ground_truth.updated_at,
                                    },
                                    "update": {
                                        "obfuscated_model_id": old_ground_truth.obfuscated_model_id,
                                        "real_model_id": old_ground_truth.real_model_id,
                                        "rank_id": old_ground_truth.rank_id,
                                        "ground_truth_score": ground_truth_score,
                                        "updated_at": old_ground_truth.updated_at,
                                        "validator_task_relation": {
                                            "connect": {"id": new_validator_task.id}
                                        },
                                    },
                                },
                            )
                            stats.ground_truths_count += 1

                    stats.processed_parent_requests += 1
                    stats.processed_requests += 1
                    stats.log_progress()

                except Exception as e:
                    logger.error(
                        f"Failed to migrate parent request {old_request.id}: {str(e)}"
                    )
                    stats.failed_requests += 1
                    continue

            skip += BATCH_SIZE

        # Pass 2: Process child requests
        print("\nProcessing child requests...")
        stats.current_pass = 2

        skip = 0
        # Get all requests
        while True:
            batch = await prisma.feedback_request_model.find_many(
                take=BATCH_SIZE,
                skip=skip,
                include={
                    "completions": True,
                    "criteria_types": True,
                    "parent_request": {
                        "include": {
                            "ground_truths": True,
                            "completions": True,
                            "criteria_types": True,
                        }
                    },
                },
            )

            if not batch:
                break

            # Filter for child requests
            child_requests = [r for r in batch if r.parent_id is not None]

            for old_request in child_requests:
                try:
                    if not old_request.parent_request:
                        logger.error(
                            f"Parent request not found for request {old_request.id}"
                        )
                        stats.failed_requests += 1
                        continue

                    # Get validator task with completions and their criteria
                    validator_task = await prisma.validatortask.find_unique(
                        where={"id": old_request.parent_request.id},
                        include={"completions": {"include": {"Criterion": True}}},
                    )

                    if not validator_task:
                        logger.error(
                            f"Parent validator task not found for request {old_request.id}"
                        )
                        stats.failed_requests += 1
                        continue

                    # Create MinerResponse
                    if old_request.dojo_task_id and old_request.hotkey:
                        task_result = {
                            "type": "score",  # Updated from 'multi-score'
                            "value": {},
                        }

                        if not old_request.parent_request.ground_truths:
                            logger.debug(f"No ground truths for task {old_request.id}")
                            continue

                        if not old_request.completions:
                            logger.debug(f"No completions for task {old_request.id}")
                            continue

                        # Get all ground truths and their corresponding scores
                        for ground_truth in old_request.parent_request.ground_truths:
                            # Find completion response for this ground truth
                            completion_response = next(
                                (
                                    comp
                                    for comp in old_request.completions
                                    if comp.model == ground_truth.real_model_id
                                ),
                                None,
                            )

                            if completion_response:
                                task_result["value"][ground_truth.real_model_id] = (
                                    completion_response.score
                                )

                        # Check if miner_response already exists
                        existing_miner_response = await prisma.minerresponse.find_first(
                            where={
                                "validator_task_id": validator_task.id,
                                "dojo_task_id": old_request.dojo_task_id,
                                "hotkey": old_request.hotkey,
                            }
                        )
                        coldkey = get_coldkey_from_hotkey(subtensor, old_request.hotkey)

                        if existing_miner_response:
                            await prisma.minerresponse.update(
                                where={"id": existing_miner_response.id},
                                data={
                                    "dojo_task_id": old_request.dojo_task_id,
                                    "hotkey": old_request.hotkey,
                                    "coldkey": coldkey,
                                    "task_result": Json(json.dumps(task_result)),
                                    "created_at": old_request.created_at,
                                    "updated_at": old_request.updated_at,
                                    "validator_task_relation": {
                                        "connect": {"id": validator_task.id}
                                    },
                                },
                            )
                            miner_response = existing_miner_response
                        else:
                            miner_response = await prisma.minerresponse.create(
                                data={
                                    "validator_task_id": validator_task.id,
                                    "dojo_task_id": old_request.dojo_task_id,
                                    "hotkey": old_request.hotkey,
                                    "coldkey": coldkey,
                                    "task_result": Json(json.dumps(task_result)),
                                    "created_at": old_request.created_at,
                                    "updated_at": old_request.updated_at,
                                }
                            )
                        stats.miner_responses_count += 1

                        if validator_task and validator_task.completions:
                            # Create miner scores for each criterion
                            for completion in validator_task.completions:
                                if completion.Criterion:
                                    for criterion in completion.Criterion:
                                        # Create or update MinerScore
                                        await prisma.minerscore.upsert(
                                            where={
                                                "criterion_id_miner_response_id": {
                                                    "criterion_id": criterion.id,
                                                    "miner_response_id": miner_response.id,
                                                }
                                            },
                                            data={
                                                "create": {
                                                    "criterion_id": criterion.id,
                                                    "miner_response_id": miner_response.id,
                                                    "scores": Json(json.dumps({})),
                                                    "created_at": old_request.created_at,
                                                    "updated_at": old_request.updated_at,
                                                },
                                                "update": {
                                                    "scores": Json(json.dumps({})),
                                                    "created_at": old_request.created_at,
                                                    "updated_at": old_request.updated_at,
                                                },
                                            },
                                        )
                                        stats.miner_scores_count += 1
                        else:
                            logger.debug(
                                f"No validator task or completions found for task {old_request.id}"
                            )

                    stats.processed_child_requests += 1
                    stats.processed_requests += 1
                    stats.log_progress()

                except Exception as e:
                    logger.error(
                        f"Failed to migrate child request {old_request.id}: {str(e)}"
                    )
                    stats.failed_requests += 1
                    continue

            skip += BATCH_SIZE

    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        raise
    finally:
        stats.print_final_stats()
        await disconnect_db()


if __name__ == "__main__":
    import asyncio

    asyncio.run(migrate())
