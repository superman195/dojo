import asyncio
import json
import logging
import os
import sys
import time
from datetime import timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import bittensor as bt
import redis.asyncio as redis

from database.client import connect_db, disconnect_db, prisma
from database.prisma import Json
from database.prisma.enums import CriteriaTypeEnum
from dojo.protocol import TaskTypeEnum
from dojo.utils.config import source_dotenv

source_dotenv()

# rank 0 is the best so score should be max, output is the result of `minmax_scale`
rank_id_to_score_map = {0: 1.0, 1: 0.6666667, 2: 0.33333334, 3: 0.0}


# Load configuration from environment variables with defaults
BATCH_SIZE = int(os.getenv("MIGRATION_BATCH_SIZE", 5000))
MAX_CONCURRENT_TASKS = int(os.getenv("MIGRATION_MAX_CONCURRENT_TASKS", 15))
LOG_DIR = os.getenv("MIGRATION_LOG_DIR", "logs/migration/")
MINER_TX_TIMEOUT = int(os.getenv("MINER_TX_TIMEOUT", 10))
VALIDATOR_TX_TIMEOUT = int(os.getenv("VALIDATOR_TX_TIMEOUT", 10))

REDIS_USERNAME = os.getenv("REDIS_USERNAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")

# Ensure log directory exists
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)


# Construct Redis URL with auth
REDIS_URL = f"redis://{REDIS_USERNAME}:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}"

if REDIS_USERNAME and REDIS_PASSWORD:
    REDIS_URL = f"redis://{REDIS_USERNAME}:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}"
elif REDIS_USERNAME:
    REDIS_URL = f"redis://{REDIS_USERNAME}@{REDIS_HOST}:{REDIS_PORT}"
else:
    REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}"


REDIS_COLDKEY_PREFIX = "migration:coldkey:"
REDIS_MINER_RESPONSE_PREFIX = (
    "migration:miner_response:"  # New prefix for miner response cache
)
REDIS_COLDKEY_TTL = 60 * 60 * 24 * 1  # 1 day in seconds
REDIS_MINER_RESPONSE_TTL = 60 * 60 * 24 * 1  # 1 day in seconds

# Initialize Redis client
redis_client: redis.Redis | None = None

# Remove the in-memory cache
# hotkey_coldkey_cache = {}  # We'll use Redis instead


def setup_logger():
    """Configure logging with both file and console handlers."""
    logger = logging.getLogger("migration")
    logger.setLevel(logging.INFO)

    # Formatter for detailed log messages
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(funcName)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler for error log
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Rotating file handler for error logs (10MB per file, keep 5 backup files)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    error_log_path = os.path.join(LOG_DIR, f"migration_errors_{timestamp}.log")
    file_handler = RotatingFileHandler(
        error_log_path,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Log the configuration
    print(
        f"Migration Configuration:\n"
        f"- BATCH_SIZE: {BATCH_SIZE}\n"
        f"- MAX_CONCURRENT_TASKS: {MAX_CONCURRENT_TASKS}\n"
        f"- LOG_DIR: {LOG_DIR}"
    )

    return logger


# Use the logger
logger = setup_logger()


class MigrationStats:
    def __init__(self):
        self.start_time = time.time()
        self.last_count = 0
        self.last_time = self.start_time
        self.tasks_per_minute = 0

        # Old table stats
        self.total_feedback_requests = 0
        self.parent_requests = 0
        self.child_requests = 0
        self.old_completions = 0
        self.old_ground_truths = 0
        self.old_criteria_types = 0

        # New table stats
        self.existing_validator_tasks = 0
        self.existing_completions = 0
        self.existing_ground_truths = 0
        self.existing_criteria = 0
        self.existing_miner_responses = 0
        self.existing_miner_scores = 0

        # Migration progress stats
        self.total_requests = 0
        self.processed_requests = 0
        self.processed_parent_requests = 0
        self.processed_child_requests = 0
        self.failed_requests = 0

        # New records stats
        self.validator_tasks_count = 0
        self.miner_responses_count = 0
        self.completions_count = 0
        self.ground_truths_count = 0
        self.criteria_count = 0
        self.miner_scores_count = 0

        # Pass tracking
        self.current_pass = 1

    def update_rate(self):
        """Calculate tasks per minute rate"""
        current_time = time.time()
        current_count = self.processed_requests

        # Calculate tasks per minute
        time_diff = current_time - self.last_time
        if time_diff >= 1.0:  # Update rate every second
            count_diff = current_count - self.last_count
            self.tasks_per_minute = (count_diff * 60) / time_diff
            self.last_count = current_count
            self.last_time = current_time

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

    async def collect_new_stats(self):
        """Collect statistics from new tables."""
        # Get counts from new tables
        self.existing_validator_tasks = await prisma.validatortask.count()
        self.existing_completions = await prisma.completion.count()
        self.existing_ground_truths = await prisma.groundtruth.count()
        self.existing_criteria = await prisma.criterion.count()
        self.existing_miner_responses = await prisma.minerresponse.count()
        self.existing_miner_scores = await prisma.minerscore.count()

        # Print new table stats
        print("\nNew Table Statistics:")
        print("=" * 50)
        print(f"Validator Tasks: {self.existing_validator_tasks}")
        print(f"Completions: {self.existing_completions}")
        print(f"Ground Truths: {self.existing_ground_truths}")
        print(f"Criteria: {self.existing_criteria}")
        print(f"Miner Responses: {self.existing_miner_responses}")
        print(f"Miner Scores: {self.existing_miner_scores}")
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
        bar = (
            "["
            + "=" * filled
            + (">" if filled < width else "")
            + " " * (width - filled - 1)
            + "]"
        )
        return f"{bar} {progress * 100:.1f}%"

    def log_progress(self):
        """Show progress bar and stats for current pass."""
        current_time = time.time()
        elapsed = current_time - self.start_time
        progress_bar = self._get_progress_bar(width=30)  # Shorter width for better fit
        self.update_rate()

        # Calculate progress
        if self.current_pass == 1:
            total = self.parent_requests
            processed = self.processed_parent_requests
            stats_str = f"Tasks: {self.validator_tasks_count} Comp: {self.completions_count} GT: {self.ground_truths_count} Crit: {self.criteria_count}"
        else:
            total = self.child_requests
            processed = self.processed_child_requests
            stats_str = f"MinerResp: {self.miner_responses_count} MinerScore: {self.miner_scores_count}"

        # Calculate ETA
        if processed > 0:
            rate = processed / elapsed
            remaining = total - processed
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
            f"\rStep {self.current_pass} | {progress_bar} | {processed}/{total} | {stats_str} | {self.tasks_per_minute:.0f} t/min | Time: {elapsed_str} | ETA: {eta_str}",
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


async def init_redis():
    """Initialize Redis connection."""
    global redis_client
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        await redis_client.ping()
        print("Redis connected successfully")
    except Exception as e:
        print("\nError: Failed to connect to Redis")
        print(f"Redis URL: redis://{REDIS_USERNAME}:****@{REDIS_HOST}:{REDIS_PORT}/0")
        print(f"Error details: {str(e)}")
        sys.exit(1)


async def get_coldkey(hotkey: str) -> str:
    """Get coldkey from Redis cache."""
    if not redis_client:
        raise Exception("Redis client not initialized")
    try:
        coldkey = await redis_client.get(f"{REDIS_COLDKEY_PREFIX}{hotkey}")
        if coldkey:
            return coldkey
        return "dummy_coldkey"
    except Exception as e:
        logger.warning(f"Failed to get coldkey from Redis for {hotkey}: {str(e)}")
        return "dummy_coldkey"


async def set_coldkey(hotkey: str, coldkey: str):
    """Set coldkey in Redis cache with TTL."""
    if not redis_client:
        raise Exception("Redis client not initialized")
    try:
        await redis_client.set(
            f"{REDIS_COLDKEY_PREFIX}{hotkey}", coldkey, ex=REDIS_COLDKEY_TTL
        )
    except Exception as e:
        logger.warning(f"Failed to set coldkey in Redis for {hotkey}: {str(e)}")


async def preload_coldkeys(subtensor: bt.Subtensor):
    """Preload all unique hotkeys and their coldkeys before processing child requests."""
    print("\nPreloading coldkeys...")

    # Get all unique hotkeys using raw query
    unique_hotkeys = await prisma.query_raw(
        """
        SELECT DISTINCT hotkey
        FROM "Feedback_Request_Model"
        WHERE parent_id IS NOT NULL
        """
    )

    hotkeys = [r["hotkey"] for r in unique_hotkeys]
    total = len(hotkeys)
    print(f"Found {total} unique hotkeys")

    # Process in batches
    batch_size = 50
    processed = 0

    for i in range(0, total, batch_size):
        batch = hotkeys[i : i + batch_size]
        for hotkey in batch:
            # First check Redis cache
            cached_coldkey = await get_coldkey(hotkey)
            if cached_coldkey != "dummy_coldkey":
                processed += 1
                continue

            # If not in cache, query subtensor
            retry_count = 0
            max_retries = 5
            while retry_count < max_retries:
                try:
                    coldkey_scale_encoded = subtensor.query_subtensor(
                        name="Owner",
                        params=[hotkey],
                    )
                    coldkey = (
                        str(coldkey_scale_encoded.value)
                        if coldkey_scale_encoded
                        else "dummy_coldkey"
                    )
                    await set_coldkey(hotkey, coldkey)
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count == max_retries:
                        logger.warning(
                            f"Failed to get coldkey for hotkey {hotkey}: {str(e)}"
                        )
                        await set_coldkey(hotkey, "dummy_coldkey")
                    await asyncio.sleep(1)

            processed += 1
            if processed % 50 == 0:  # Print progress every 10 hotkeys
                print(f"Loaded coldkeys: {processed}/{total}")


async def migrate():
    await connect_db()
    try:
        await init_redis()  # Initialize Redis connection
        subtensor = bt.subtensor(network="finney")

        # Collect and display old and new table statistics
        await stats.collect_old_stats()
        await stats.collect_new_stats()

        # Preload coldkeys before Step 2
        await preload_coldkeys(subtensor)

        # Step 1: Process parent requests
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

            # Create tasks for all requests in the batch
            batch_tasks = []
            for old_request in batch:
                try:
                    # Map task type
                    task_type = TaskTypeEnum.CODE_GENERATION  # Default fallback
                    if old_request.task_type.lower().find("image") >= 0:
                        task_type = TaskTypeEnum.TEXT_TO_IMAGE
                    elif old_request.task_type.lower().find("3d") >= 0:
                        task_type = TaskTypeEnum.TEXT_TO_THREE_D

                    # Add task to batch tasks with semaphore
                    batch_tasks.append(
                        asyncio.create_task(
                            process_parent_request(old_request, task_type)
                        )
                    )

                    # If we hit the concurrent task limit, wait for some to complete
                    if len(batch_tasks) >= MAX_CONCURRENT_TASKS:
                        await asyncio.gather(*batch_tasks[:MAX_CONCURRENT_TASKS])
                        batch_tasks = batch_tasks[MAX_CONCURRENT_TASKS:]

                except Exception as e:
                    logger.error(
                        f"Failed to create task for request {old_request.id}: {str(e)}"
                    )
                    stats.failed_requests += 1
                    continue

            # Process all tasks in the batch concurrently
            if batch_tasks:
                await asyncio.gather(*batch_tasks)

            skip += BATCH_SIZE

        # Step 2: Process child requests
        print("\nProcessing child requests...")
        stats.current_pass = 2

        skip = 0
        while True:
            batch = await prisma.feedback_request_model.find_many(
                take=BATCH_SIZE,
                skip=skip,
                include={
                    "completions": True,
                    "parent_request": {
                        "include": {
                            "ground_truths": True,
                        }
                    },
                },
                # none works too but fails typings
                where={"parent_id": {"not": ""}},
            )

            if not batch:
                break

            # Filter for child requests
            child_requests = [r for r in batch if r.parent_id is not None]

            # Create tasks for all child requests in the batch
            batch_tasks = []
            for old_request in child_requests:
                try:
                    batch_tasks.append(
                        asyncio.create_task(
                            process_child_request(old_request, subtensor)
                        )
                    )
                    # If we hit the concurrent task limit, wait for some to complete
                    if len(batch_tasks) >= MAX_CONCURRENT_TASKS:
                        await asyncio.gather(*batch_tasks[:MAX_CONCURRENT_TASKS])
                        batch_tasks = batch_tasks[MAX_CONCURRENT_TASKS:]

                except Exception as e:
                    logger.error(
                        f"Failed to create task for child request {old_request.id}: {str(e)}"
                    )
                    stats.failed_requests += 1
                    continue

            # Process all tasks in the batch concurrently
            if batch_tasks:
                await asyncio.gather(*batch_tasks)

            skip += BATCH_SIZE

    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        raise
    finally:
        stats.print_final_stats()
        if redis_client:
            await redis_client.aclose()  # Use aclose instead of close
        await disconnect_db()


async def process_child_request(old_request, subtensor):
    """Process a child request."""
    try:
        if not old_request.parent_request:
            logger.error(f"Parent request not found for request {old_request.id}")
            stats.failed_requests += 1
            return

        if not (old_request.dojo_task_id and old_request.hotkey):
            logger.error(f"Missing dojo_task_id or hotkey for request {old_request.id}")
            stats.failed_requests += 1
            return

        if not old_request.parent_request.ground_truths:
            logger.error(f"No ground truths for task {old_request.id}")
            stats.failed_requests += 1
            return

        if not old_request.completions:
            logger.error(f"No completions for task {old_request.id}")
            stats.failed_requests += 1
            return

        # Get validator task with completions and their criteria
        validator_task = await prisma.validatortask.find_unique(
            where={"id": old_request.parent_request.id},
            include={"completions": {"include": {"criterion": True}}},
        )

        if not validator_task:
            logger.error(
                f"Parent validator task not found for request {old_request.id}"
            )
            stats.failed_requests += 1
            return

        # Check if miner_response already exists
        existing_miner_response = await prisma.minerresponse.find_first(
            where={
                "validator_task_id": validator_task.id,
                "dojo_task_id": old_request.dojo_task_id,
                "hotkey": old_request.hotkey,
            }
        )

        if existing_miner_response:
            logger.debug(
                f"Miner response for request {old_request.id} already exists, skipping"
            )
            stats.processed_child_requests += 1
            stats.processed_requests += 1
            stats.log_progress()
            return

        # Prepare task result with scores
        task_result = {
            "type": "score",
            "value": {},
        }

        for ground_truth in old_request.parent_request.ground_truths:
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

        # Get coldkey from Redis
        coldkey = await get_coldkey(old_request.hotkey)

        try:
            async with prisma.tx(
                timeout=timedelta(seconds=MINER_TX_TIMEOUT)
            ) as transaction:
                # Create new miner response
                miner_response = await transaction.minerresponse.create(
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

                # Process miner scores
                if validator_task.completions:
                    for completion in validator_task.completions:
                        if completion.criterion:
                            for criterion in completion.criterion:
                                await transaction.minerscore.create(
                                    data={
                                        "criterion_id": criterion.id,
                                        "miner_response_id": miner_response.id,
                                        "scores": Json(json.dumps({})),
                                        "created_at": old_request.created_at,
                                        "updated_at": old_request.updated_at,
                                    }
                                )
                                stats.miner_scores_count += 1

            stats.processed_child_requests += 1
            stats.processed_requests += 1
            stats.log_progress()

        except Exception as e:
            logger.error(f"Failed to create miner response and scores: {str(e)}")
            stats.failed_requests += 1

    except Exception as e:
        logger.error(f"Failed to migrate child request {old_request.id}: {str(e)}")
        stats.failed_requests += 1


async def process_parent_request(request, task_type):
    """Process a request with connection limiting."""
    try:
        # Check if validator task already exists
        existing_validator_task = await prisma.validatortask.find_unique(
            where={"id": request.id},
            include={"completions": {"include": {"criterion": True}}},
        )

        if existing_validator_task:
            logger.debug(f"Validator task {request.id} already exists, skipping")
            stats.processed_parent_requests += 1
            stats.processed_requests += 1
            stats.log_progress()
            return

        async with prisma.tx(
            timeout=timedelta(seconds=VALIDATOR_TX_TIMEOUT)
        ) as transaction:
            # Create parent validator task
            new_validator_task = await transaction.validatortask.create(
                data={
                    "id": request.id,
                    "prompt": request.prompt,
                    "task_type": task_type,
                    "is_processed": request.is_processed,
                    "expire_at": request.expire_at,
                    "created_at": request.created_at,
                    "updated_at": request.updated_at,
                }
            )
            stats.validator_tasks_count += 1

            # Process completions and their criteria
            if request.completions:
                for old_completion in request.completions:
                    # Create completion
                    new_completion = await transaction.completion.create(
                        data={
                            "id": old_completion.id,
                            "completion_id": old_completion.completion_id,
                            "validator_task_id": new_validator_task.id,
                            "model": old_completion.model,
                            "completion": old_completion.completion,
                            "created_at": old_completion.created_at,
                            "updated_at": old_completion.updated_at,
                        }
                    )
                    stats.completions_count += 1

                    # Process criteria
                    if request.criteria_types:
                        for old_criterion in request.criteria_types:
                            if old_criterion.type == "RANKING_CRITERIA":
                                continue

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

                            await transaction.criterion.create(
                                data={
                                    "completion_id": new_completion.id,
                                    "criteria_type": CriteriaTypeEnum[criteria_type],
                                    "config": Json(config if config else "{}"),
                                    "created_at": old_criterion.created_at,
                                    "updated_at": old_criterion.updated_at,
                                }
                            )
                            stats.criteria_count += 1

            # Process ground truths
            if request.ground_truths:
                for old_ground_truth in request.ground_truths:
                    # Map rank_id to corresponding score
                    try:
                        ground_truth_score = rank_id_to_score_map[
                            old_ground_truth.rank_id
                        ]
                    except KeyError:
                        logger.fatal(
                            f"Rank id of {old_ground_truth.id} is not expected"
                        )
                        raise

                    await transaction.groundtruth.create(
                        data={
                            "id": old_ground_truth.id,
                            "validator_task_id": new_validator_task.id,
                            "obfuscated_model_id": old_ground_truth.obfuscated_model_id,
                            "real_model_id": old_ground_truth.real_model_id,
                            "rank_id": old_ground_truth.rank_id,
                            "ground_truth_score": ground_truth_score,
                            "created_at": old_ground_truth.created_at,
                            "updated_at": old_ground_truth.updated_at,
                        }
                    )
                    stats.ground_truths_count += 1

        stats.processed_parent_requests += 1
        stats.processed_requests += 1
        stats.log_progress()
    except Exception as e:
        logger.error(f"Failed to migrate parent request {request.id}: {str(e)}")
        stats.failed_requests += 1


if __name__ == "__main__":
    import asyncio

    async def main():
        try:
            await migrate()
        except KeyboardInterrupt:
            print("\n\nProcess interrupted by user. Printing stats before exit...")
        except Exception as e:
            print(f"\n\nProcess failed with error: {str(e)}")
        finally:
            stats.print_final_stats()
            if redis_client:
                await redis_client.aclose()  # Use aclose instead of close
            await disconnect_db()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
