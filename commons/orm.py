import asyncio
import json
import math
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, List

from bittensor.utils.btlogging import logging as logger

from commons.exceptions import (
    ExpiredFromMoreThanExpireTo,
    InvalidMinerResponse,
    NoNewExpiredTasksYet,
    NoProcessedTasksYet,
)
from commons.utils import datetime_as_utc
from database.client import prisma, transaction
from database.mappers import (
    map_miner_response_to_task_synapse_object,
    map_task_synapse_object_to_completions,
    map_task_synapse_object_to_miner_response,
    map_task_synapse_object_to_validator_task,
    map_validator_task_to_task_synapse_object,
)
from database.prisma import Json
from database.prisma.errors import PrismaError
from database.prisma.models import GroundTruth, ValidatorTask
from database.prisma.types import (
    CriterionWhereInput,
    FindManyMinerResponseArgsFromValidatorTask,
    MinerResponseCreateWithoutRelationsInput,
    MinerResponseInclude,
    MinerScoreCreateInput,
    MinerScoreUpdateInput,
    ValidatorTaskInclude,
    ValidatorTaskWhereInput,
)
from dojo import TASK_DEADLINE
from dojo.protocol import DendriteQueryResponse, Scores, TaskResult, TaskSynapseObject


class ORM:
    @staticmethod
    async def get_expired_tasks(
        batch_size: int = 10,
        expire_from: datetime | None = None,
        expire_to: datetime | None = None,
        filter_empty_result: bool = False,
    ) -> AsyncGenerator[tuple[List[DendriteQueryResponse], bool], None]:
        """Returns batches of expired ValidatorTask records and a boolean indicating if there are more batches.

        Args:
            batch_size (int, optional): Number of tasks to return in a batch. Defaults to 10.
            expire_from: (datetime | None) If provided, only tasks with expire_at after expire_from will be returned.
            expire_to: (datetime | None) If provided, only tasks with expire_at before expire_to will be returned.
            You must determine the `expire_at` cutoff yourself, otherwise it defaults to current time UTC.
            filter_empty_results: If True, only include miner_responses with empty task_result

        Raises:
            ExpiredFromMoreThanExpireTo: If expire_from is greater than expire_to
            NoNewExpiredTasksYet: If no expired tasks are found for processing.

        Yields:
            tuple[List[DendriteQueryResponse], bool]: Each yield returns:
            - List of DendriteQueryResponse records with their related completions, miner_responses, and GroundTruth
            - Boolean indicating if there are more batches to process
        """

        # Create miner_responses include query
        miner_responses_include: FindManyMinerResponseArgsFromValidatorTask = {
            "include": {"scores": True}
        }

        if filter_empty_result:
            miner_responses_include["where"] = {"task_result": {"equals": Json("{}")}}

        include_query = ValidatorTaskInclude(
            {
                "completions": {
                    "include": {"criterion": {"include": {"scores": True}}}
                },
                "miner_responses": miner_responses_include,
                "ground_truth": True,
            }
        )

        # Set default expiry timeframe of 6 hours before the latest expired tasks
        if not expire_from:
            expire_from = (
                datetime_as_utc(datetime.now(timezone.utc))
                - timedelta(seconds=TASK_DEADLINE)
                - timedelta(hours=6)
            )
        if not expire_to:
            expire_to = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(
                seconds=TASK_DEADLINE
            )

        # Check that expire_from is lesser than expire_to
        if expire_from > expire_to:
            raise ExpiredFromMoreThanExpireTo(
                "expire_from should be less than expire_to."
            )

        vali_where_query_unprocessed = ValidatorTaskWhereInput(
            {
                # only check for expire at since miner may lie
                "expire_at": {
                    "gt": expire_from,
                    "lt": expire_to,
                },
                "is_processed": False,
            }
        )

        # Get total count and first batch of validator tasks in parallel
        task_count_unprocessed, first_batch = await asyncio.gather(
            ValidatorTask.prisma().count(where=vali_where_query_unprocessed),
            ValidatorTask.prisma().find_many(
                include=include_query,
                where=vali_where_query_unprocessed,
                order={"created_at": "desc"},
                take=batch_size,
            ),
        )
        if first_batch and first_batch[0] and first_batch[0].miner_responses:
            logger.debug(
                f"First batch: {[miner_response.task_result for miner_response in first_batch[0].miner_responses]}"
            )
        logger.debug(f"Count of unprocessed validator tasks: {task_count_unprocessed}")

        if not task_count_unprocessed:
            raise NoNewExpiredTasksYet(
                f"No expired validator tasks found for processing, please wait for tasks to pass the task deadline of {TASK_DEADLINE} seconds."
            )

        first_batch_responses = [
            DendriteQueryResponse(
                validator_task=map_validator_task_to_task_synapse_object(task),
                miner_responses=(
                    [
                        map_miner_response_to_task_synapse_object(miner_response, task)
                        for miner_response in task.miner_responses
                    ]
                    if task.miner_responses
                    else []
                ),
            )
            for task in first_batch
        ]
        yield first_batch_responses, task_count_unprocessed > batch_size

        # Process remaining batches
        for skip in range(batch_size, task_count_unprocessed, batch_size):
            validator_tasks = await ValidatorTask.prisma().find_many(
                include=include_query,
                where=vali_where_query_unprocessed,
                order={"created_at": "desc"},
                skip=skip,
                take=batch_size,
            )
            batch_responses = [
                DendriteQueryResponse(
                    validator_task=map_validator_task_to_task_synapse_object(task),
                    miner_responses=(
                        [
                            map_miner_response_to_task_synapse_object(
                                miner_response, task
                            )
                            for miner_response in task.miner_responses
                        ]
                        if task.miner_responses
                        else []
                    ),
                )
                for task in validator_tasks
            ]
            has_more = (skip + batch_size) < task_count_unprocessed
            yield batch_responses, has_more

    @staticmethod
    async def get_real_model_ids(validator_task_id: str) -> dict[str, str]:
        """Fetches a mapping of obfuscated model IDs to real model IDs for a given request ID.

        Args:
            validator_task_id: The ID of the validator task

        Returns:
            A dictionary mapping obfuscated_model_id to real_model_id

        Raises:
            PrismaError: If database query fails
        """
        try:
            ground_truths = await GroundTruth.prisma().find_many(
                where={"validator_task_id": validator_task_id}
            )
            return {gt.obfuscated_model_id: gt.real_model_id for gt in ground_truths}
        except PrismaError as e:
            logger.error(
                f"Database error fetching model IDs for task {validator_task_id}: {e}"
            )
            raise

    @staticmethod
    async def mark_validator_task_as_processed(
        validator_task_ids: list[str],
    ) -> int | None:
        """Mark records associated with validator's tasks as processed.

        Args:
            validator_task_ids (list[str]): List of validator task ids.
        """
        if not validator_task_ids:
            logger.error("No validator task ids provided to mark as processed")
            return

        try:
            async with transaction() as tx:
                num_updated = await tx.validatortask.update_many(
                    data={"is_processed": True},
                    where={"id": {"in": validator_task_ids}},
                )
                if num_updated:
                    logger.success(
                        f"Marked {num_updated} records as processed from {len(validator_task_ids)} task IDs"
                    )
                else:
                    logger.warning("No records were updated")

                return num_updated
        except PrismaError as exc:
            logger.error(f"Prisma error occurred: {exc}")
        except Exception as exc:
            logger.error(f"Unexpected error occurred: {exc}")

    @staticmethod
    async def get_num_processed_tasks() -> int:
        return await ValidatorTask.prisma().count(where={"is_processed": True})

    @staticmethod
    async def update_miner_task_results(
        miner_hotkey: str,
        dojo_task_id: str,
        task_results: List[TaskResult],
        max_retries: int = 3,
    ) -> bool:
        """Updates the task_result field of a MinerResponse with the provided task results.

        Args:
            miner_hotkey (str): The hotkey of the miner
            dojo_task_id (str): The Dojo task ID
            task_results (List[TaskResult]): List of task results to store
            max_retries (int, optional): Maximum number of retry attempts. Defaults to 3.

        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            for attempt in range(max_retries):
                try:
                    # Convert task_results to JSON-compatible format
                    task_results_json = Json(
                        [result.model_dump() for result in task_results]
                    )

                    # Update the miner response
                    updated = await prisma.minerresponse.update_many(
                        where={
                            "hotkey": miner_hotkey,
                            "dojo_task_id": dojo_task_id,
                        },
                        data={
                            "task_result": task_results_json,
                            "updated_at": datetime.now(timezone.utc),
                        },
                    )

                    if updated:
                        logger.success(
                            f"Updated task results for miner {miner_hotkey}, dojo_task_id {dojo_task_id}"
                        )
                        return True
                    else:
                        if attempt == max_retries - 1:
                            logger.error(
                                f"Failed to update task results after {max_retries} attempts"
                            )
                        else:
                            logger.warning(
                                f"Retrying update, attempt {attempt + 2}/{max_retries}"
                            )
                            await asyncio.sleep(2**attempt)

                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Error updating task results: {e}")
                    else:
                        logger.warning(
                            f"Error during attempt {attempt + 1}, retrying: {e}"
                        )
                        await asyncio.sleep(2**attempt)

            return False

        except Exception as e:
            logger.error(f"Unexpected error updating task results: {e}")
            return False

    # TODO: How to store miner scores
    @staticmethod
    async def update_miner_raw_scores(
        miner_responses: List[TaskSynapseObject],
        batch_size: int = 10,
        max_retries: int = 20,
    ) -> tuple[bool, list[int]]:
        """Update the miner's provided raw scores for a list of miner responses.
        NOTE: this is to be used when the task is first saved to validator's database.

        Args:
            miner_responses: List of TaskSynapseObject containing miner responses
            batch_size: Number of responses to process in each batch
            max_retries: Maximum number of retry attempts for failed batches

        Returns:
            Tuple containing:
            - Boolean indicating if all updates were successful
            - List of indices for any failed batches
        """
        if not len(miner_responses):
            logger.warning(
                "Attempting to update miner responses: nothing to update, skipping."
            )
            return True, []

        # Returns ceiling of the division to get number of batches to process
        num_batches = math.ceil(len(miner_responses) / batch_size)
        failed_batch_indices = []

        for batch_id in range(num_batches):
            start_idx = batch_id * batch_size
            end_idx = min((batch_id + 1) * batch_size, len(miner_responses))
            batch_responses = miner_responses[start_idx:end_idx]

            for attempt in range(max_retries):
                try:
                    async with prisma.tx(timeout=timedelta(seconds=30)) as tx:
                        for miner_response in batch_responses:
                            if not miner_response.miner_hotkey:
                                raise InvalidMinerResponse(
                                    f"Miner response {miner_response} must have a hotkey"
                                )

                            # Find the miner response record
                            db_miner_response = await tx.minerresponse.find_first(
                                where={
                                    "hotkey": miner_response.miner_hotkey,
                                    "dojo_task_id": miner_response.dojo_task_id or "",
                                }
                            )

                            if not db_miner_response:
                                raise ValueError(
                                    f"Miner response not found for dojo_task_id: {miner_response.dojo_task_id}, "
                                    f"hotkey: {miner_response.miner_hotkey}"
                                )

                            if not miner_response.completion_responses:
                                continue

                            # Create score records for each completion response
                            for completion in miner_response.completion_responses:
                                # Find or create the criterion record
                                criterion = await tx.criterion.find_first(
                                    where=CriterionWhereInput(
                                        {
                                            "completion_relation": {
                                                "is": {
                                                    "completion_id": completion.completion_id,
                                                    "validator_task_id": miner_response.task_id,
                                                }
                                            }
                                        }
                                    )
                                )

                                if not criterion:
                                    continue

                                # Create scores object
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

                                await tx.minerscore.upsert(
                                    where={
                                        "criterion_id_miner_response_id": {
                                            "criterion_id": criterion.id,
                                            "miner_response_id": db_miner_response.id,
                                        }
                                    },
                                    data={
                                        "create": MinerScoreCreateInput(
                                            criterion_id=criterion.id,
                                            miner_response_id=db_miner_response.id,
                                            scores=Json(
                                                json.dumps(scores.model_dump())
                                            ),
                                        ),
                                        "update": MinerScoreUpdateInput(
                                            scores=Json(json.dumps(scores.model_dump()))
                                        ),
                                    },
                                )

                    logger.debug(
                        f"Updating completion responses: updated batch {batch_id + 1}/{num_batches}"
                    )
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(
                            f"Failed to update batch {batch_id + 1}/{num_batches} after {max_retries} attempts: {e}"
                        )
                        failed_batch_indices.extend(range(start_idx, end_idx))
                    else:
                        logger.warning(
                            f"Retrying batch {batch_id + 1}/{num_batches}, attempt {attempt + 2}/{max_retries}"
                        )
                        await asyncio.sleep(2**attempt)

                await asyncio.sleep(0.1)

        if not failed_batch_indices:
            logger.success(
                f"Successfully updated all {num_batches} batches for {len(miner_responses)} responses"
            )
            return True, []

        return False, failed_batch_indices

    @staticmethod
    async def save_task(
        validator_task: TaskSynapseObject,
        miner_responses: List[TaskSynapseObject],
        ground_truth: dict[str, int],
        metadata: dict | None = None,
    ) -> ValidatorTask | None:
        """Saves a task, which consists of both the validator's request and the miners' responses.

        Args:
            validator_task (ValidatorTask): The task created by the validator.
            miner_responses (List[MinerResponse]): The responses made by the miners.
            ground_truth (dict[str, int]): Mapping of completion_id to rank_id for ground truth.

        Returns:
            ValidatorTask | None: The created validator task, or None if failed.
        """
        try:
            async with prisma.tx(timeout=timedelta(seconds=30)) as tx:
                logger.trace("Starting transaction for saving task.")

                # Map validator task using mapper function
                validator_task_data = map_task_synapse_object_to_validator_task(
                    validator_task, metadata
                )
                if not validator_task_data:
                    logger.error("Failed to map validator task")
                    return None

                created_task = await tx.validatortask.create(data=validator_task_data)

                # Create completions separately, ValidatorTaskCreateInput does not support CompletionCreateInput
                completions = map_task_synapse_object_to_completions(
                    validator_task, created_task.id
                )

                for completion in completions:
                    await tx.completion.create(data=completion)

                # Pre-process all valid miner responses
                valid_miner_data = []
                for miner_response in miner_responses:
                    try:
                        miner_data = map_task_synapse_object_to_miner_response(
                            miner_response,
                            created_task.id,
                        )
                        valid_miner_data.append(miner_data)
                    except InvalidMinerResponse as e:
                        miner_hotkey = miner_response.miner_hotkey
                        logger.debug(
                            f"Miner response from hotkey: {miner_hotkey} is invalid: {e}"
                        )

                if valid_miner_data:
                    # Bulk create all miner responses
                    await tx.minerresponse.create_many(
                        data=[
                            MinerResponseCreateWithoutRelationsInput(**miner_data)
                            for miner_data in valid_miner_data
                        ]
                    )

                return created_task

        except Exception as e:
            logger.error(f"Failed to save task: {e}")
            return None

    @staticmethod
    async def update_miner_scores(
        task_id: str,
        miner_responses: List[TaskSynapseObject],
        batch_size: int = 10,
        max_retries: int = 3,
    ) -> tuple[bool, list[str]]:
        """Update scores for miners in the MinerScore table.

        Args:
            task_id: The validator task ID
            miner_responses: List of miner responses to update
            batch_size: Number of scores to process in each batch
            max_retries: Maximum number of retry attempts for failed updates

        Returns:
            Tuple containing:
            - Boolean indicating if all updates were successful
            - List of hotkeys that failed to update
        """
        failed_hotkeys = []

        try:
            # Get all miner responses for this task
            db_miner_responses = await prisma.minerresponse.find_many(
                where={
                    "validator_task_id": task_id,
                    "hotkey": {
                        "in": [
                            mr.miner_hotkey for mr in miner_responses if mr.miner_hotkey
                        ]
                    },
                },
                include=MinerResponseInclude(
                    {
                        "scores": {
                            "include": {
                                "criterion_relation": {
                                    "include": {"completion_relation": True}
                                }
                            }
                        }
                    }
                ),
            )

            # Process in batches
            for i in range(0, len(db_miner_responses), batch_size):
                batch = db_miner_responses[i : i + batch_size]

                for attempt in range(max_retries):
                    try:
                        async with prisma.tx() as tx:
                            for db_miner_response in batch:
                                # Find matching miner response from input
                                miner_response = next(
                                    (
                                        mr
                                        for mr in miner_responses
                                        if mr.miner_hotkey == db_miner_response.hotkey
                                    ),
                                    None,
                                )
                                if (
                                    not miner_response
                                    or not miner_response.completion_responses
                                ):
                                    continue

                                # Update each MinerScore record for this miner response
                                for score_record in db_miner_response.scores or []:
                                    completion_id = (
                                        score_record.criterion_relation.completion_relation.completion_id
                                        if score_record.criterion_relation
                                        and score_record.criterion_relation.completion_relation
                                        else None
                                    )

                                    matching_completion = next(
                                        (
                                            cr
                                            for cr in miner_response.completion_responses
                                            if cr.completion_id == completion_id
                                        ),
                                        None,
                                    )

                                    if (
                                        not matching_completion
                                        or not matching_completion.criteria_types
                                    ):
                                        continue

                                    # Find matching criteria type
                                    matching_criteria = next(
                                        (
                                            ct
                                            for ct in matching_completion.criteria_types
                                            if hasattr(ct, "scores") and ct.scores
                                        ),
                                        None,
                                    )

                                    if not matching_criteria:
                                        continue

                                    if matching_criteria and matching_criteria.scores:
                                        # Merge existing scores with new scores
                                        existing_scores = json.loads(
                                            score_record.scores
                                        )
                                        new_scores = (
                                            matching_criteria.scores.model_dump()
                                        )
                                        updated_scores = {
                                            k: (
                                                new_scores.get(k)
                                                if new_scores.get(k) is not None
                                                else v
                                            )
                                            for k, v in existing_scores.items()
                                        }
                                        await tx.minerscore.update(
                                            where={
                                                "criterion_id_miner_response_id": {
                                                    "criterion_id": score_record.criterion_id,
                                                    "miner_response_id": db_miner_response.id,
                                                }
                                            },
                                            data={
                                                "scores": Json(
                                                    json.dumps(updated_scores)
                                                )
                                            },
                                        )
                        break

                    except Exception as e:
                        if attempt == max_retries - 1:
                            logger.error(
                                f"Failed to update scores for batch after {max_retries} attempts: {e}"
                            )
                            failed_hotkeys.extend([m.hotkey for m in batch])
                        else:
                            await asyncio.sleep(2**attempt)

            return (
                len(failed_hotkeys) == 0,
                failed_hotkeys,
            )

        except Exception as e:
            logger.error(f"Error updating miner scores: {e}")
            return False, [mr.miner_hotkey for mr in miner_responses if mr.miner_hotkey]

    # TODO: Remove this as this was only used for wandb logging
    @staticmethod
    async def get_scores_and_ground_truth_by_dojo_task_id(
        dojo_task_id: str,
    ) -> dict[str, dict[str, float | int | dict | None]]:
        """
        Fetch the scores, model IDs from Completion_Response_Model for a given Dojo task ID.
        Also fetches rank IDs from Ground_Truth_Model for the given Dojo task ID.

        Args:
            dojo_task_id (str): The Dojo task ID to search for.

        Returns:
            dict[str, dict[str, float | int | dict | None]]: A dictionary mapping model ID to a dict containing:
                - score_data: Complete score data including raw_score, normalised_score, etc.
                - ground_truth_rank_id: The ground truth rank ID for the model
        """
        try:
            # Find the MinerResponse with the given dojo_task_id to get validator_task_id
            miner_response = await prisma.minerresponse.find_first(
                where={"dojo_task_id": dojo_task_id},
                include={
                    "validator_task_relation": {
                        "include": {
                            "completions": True,
                            "ground_truth": True,
                        }
                    }
                },
            )

            if not miner_response or not miner_response.validator_task_relation:
                logger.warning(
                    f"No validator task found for dojo_task_id: {dojo_task_id}"
                )
                return {}

            validator_task = miner_response.validator_task_relation

            # Create mapping of model to ground truth rank_id
            rank_id_map = {
                gt.obfuscated_model_id: gt.rank_id
                for gt in validator_task.ground_truth or []
            }

            # Extract scores from the completions
            scores_and_gts = {
                completion.model: {
                    "score": None,  # Score will come from MinerScore table
                    "ground_truth_rank_id": rank_id_map.get(completion.model),
                }
                for completion in validator_task.completions or []
            }

            # Get scores from MinerScore table
            miner_scores = await prisma.minerscore.find_many(
                where={"miner_response_id": miner_response.id},
                include={
                    "criterion_relation": {"include": {"completion_relation": True}}
                },
            )

            # Update scores in the result
            for score in miner_scores:
                if (
                    score.criterion_relation
                    and score.criterion_relation.completion_relation
                ):
                    model = score.criterion_relation.completion_relation.model
                    if model in scores_and_gts:
                        score_data = json.loads(score.scores)
                        scores_and_gts[model]["score_data"] = score_data

            return scores_and_gts

        except Exception as e:
            logger.error(
                f"Error fetching completion scores and ground truths for dojo_task_id {dojo_task_id}: {e}"
            )
            return {}

    @staticmethod
    async def get_processed_tasks(
        batch_size: int = 10,
        expire_from: datetime | None = None,
        expire_to: datetime | None = None,
    ) -> AsyncGenerator[tuple[List[ValidatorTask], bool], None]:
        """
        Returns batches of processed ValidatorTask records and a boolean indicating if there are more batches.
        Used to collect analytics data.

        Args:
            batch_size (int, optional): Number of tasks to return in a batch. Defaults to 10.
            expire_from: (datetime | None) If provided, only tasks with expire_at after expire_from will be returned.
            expire_to: (datetime | None) If provided, only tasks with expire_at before expire_to will be returned.
            You must determine the `expire_at` cutoff yourself, otherwise it defaults to current time UTC.

        Raises:
            ExpiredFromMoreThanExpireTo: If expire_from is greater than expire_to
            NoProcessedTasksYet: If no processed tasks are found for uploading.

        Yields:
            tuple[validator_task, bool]: Each yield returns:
            - List of ValidatorTask records with their related completions, miner_responses, and GroundTruth
            - Boolean indicating if there are more batches to process

        @to-do: write unit test for this function.
        """
        # find all validator requests first
        include_query = ValidatorTaskInclude(
            {
                "completions": {"include": {"criterion": True}},
                "miner_responses": {"include": {"scores": True}},
                "ground_truth": True,
            }
        )

        # Set default expiry timeframe of 6 hours before the latest expired tasks
        if not expire_from:
            expire_from = (
                datetime_as_utc(datetime.now(timezone.utc))
                - timedelta(seconds=TASK_DEADLINE)
                - timedelta(hours=6)
            )
        if not expire_to:
            expire_to = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(
                seconds=TASK_DEADLINE
            )

        # Check that expire_from is lesser than expire_to
        if expire_from > expire_to:
            raise ExpiredFromMoreThanExpireTo(
                "expire_from should be less than expire_to."
            )

        vali_where_query_processed = ValidatorTaskWhereInput(
            {
                # only check for expire at since miner may lie
                "expire_at": {
                    "gt": expire_from,
                    "lt": expire_to,
                },
                "is_processed": True,
            }
        )

        # Get total count and first batch of validator tasks in parallel
        task_count_processed, first_batch = await asyncio.gather(
            ValidatorTask.prisma().count(where=vali_where_query_processed),
            ValidatorTask.prisma().find_many(
                include=include_query,
                where=vali_where_query_processed,
                order={"created_at": "desc"},
                take=batch_size,
            ),
        )

        logger.debug(f"Count of processed validator tasks: {task_count_processed}")

        if not task_count_processed:
            raise NoProcessedTasksYet(
                "No processed tasks found for uploading, wait for next scoring execution."
            )

        yield first_batch, task_count_processed > batch_size

        # Process remaining batches
        for skip in range(batch_size, task_count_processed, batch_size):
            validator_tasks = await ValidatorTask.prisma().find_many(
                include=include_query,
                where=vali_where_query_processed,
                order={"created_at": "desc"},
                skip=skip,
                take=batch_size,
            )
            has_more = (skip + batch_size) < task_count_processed
            yield validator_tasks, has_more


# ---------------------------------------------------------------------------- #
#                          Test custom ORM functions                           #
# ---------------------------------------------------------------------------- #


async def test_get_expired_tasks():
    """Test function for get_expired_tasks."""
    from database.client import connect_db, disconnect_db

    # Connect to database first
    await connect_db()

    try:
        orm = ORM()
        batch_size = 5
        expire_from = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(days=1)
        expire_to = datetime_as_utc(datetime.now(timezone.utc))

        total_tasks = 0
        async for tasks, has_more in orm.get_expired_tasks(
            batch_size, expire_from, expire_to
        ):
            total_tasks += len(tasks)
            if not has_more:
                break
        logger.info(f"Total number of unprocessed expired tasks: {total_tasks}")
    except NoNewExpiredTasksYet as e:
        print(f"No new expired tasks: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Always disconnect when done
        await disconnect_db()


async def test_get_real_model_ids():
    """Test function for get_real_model_ids."""
    from database.client import connect_db, disconnect_db

    # Connect to database first
    await connect_db()

    try:
        orm = ORM()
        # Use a known validator_task_id from your database
        validator_task_id = "5c91e4b4-a675-47e8-a06b-c2e66ec67239"

        model_id_mapping = await orm.get_real_model_ids(validator_task_id)
        logger.info(f"Model ID mapping: {model_id_mapping}")

        # Print the number of mappings found
        logger.info(f"Number of model ID mappings found: {len(model_id_mapping)}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        # Always disconnect when done
        await disconnect_db()


async def test_mark_validator_task_as_processed():
    """Test function for mark_validator_task_as_processed."""
    from database.client import connect_db, disconnect_db

    # Connect to database first
    await connect_db()

    try:
        orm = ORM()
        # Use a list of known validator_task_ids from your database
        validator_task_ids = [
            "5c91e4b4-a675-47e8-a06b-c2e66ec67239",
        ]

        # Get initial processed state
        initial_tasks = await ValidatorTask.prisma().find_many(
            where={"id": {"in": validator_task_ids}}
        )
        logger.info(
            f"Initial processed state: {[task.is_processed for task in initial_tasks]}"
        )

        # Mark tasks as processed
        num_updated = await orm.mark_validator_task_as_processed(validator_task_ids)
        logger.info(f"Number of tasks marked as processed: {num_updated}")

        # Verify the update
        updated_tasks = await ValidatorTask.prisma().find_many(
            where={"id": {"in": validator_task_ids}}
        )
        logger.info(
            f"Updated processed state: {[task.is_processed for task in updated_tasks]}"
        )

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        # Always disconnect when done
        await disconnect_db()


async def test_get_num_processed_tasks():
    """Test function for get_num_processed_tasks."""
    from database.client import connect_db, disconnect_db

    # Connect to database first
    await connect_db()

    try:
        orm = ORM()
        num_processed = await orm.get_num_processed_tasks()
        logger.info(f"Number of processed tasks: {num_processed}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        # Always disconnect when done
        await disconnect_db()


# TODO: Update this test
# async def test_save_task():
#     """Test function for save_task."""
#     from database.client import connect_db, disconnect_db
#     from datetime import datetime, timezone
#     from dojo.protocol import TaskType, CodeAnswer, CompletionResponses, FileObject

#     # Connect to database first
#     await connect_db()

#     try:
#         orm = ORM()

#         # Create test validator task
#         test_validator_task = ValidatorTask(
#             prompt="Test prompt",
#             task_type=TaskType.CODE_GENERATION.value,
#             expire_at=datetime_as_utc(datetime.now(timezone.utc)),
#             completions=[
#                 Completion(
#                     model="model1",
#                     completion=CodeAnswer(
#                         files=[
#                             FileObject(
#                                 filename="fibonacci.py",
#                                 content="""def fibonacci(n):
#                                         if n <= 1:
#                                             return n
#                                         return fibonacci(n-1) + fibonacci(n-2)""",
#                                 language="python",
#                             )
#                         ]
#                     ),
#                     criterion=[],
#                 ),
#                 Completion(
#                     model="model2",
#                     completion=CodeAnswer(
#                         files=[
#                             FileObject(
#                                 filename="fibonacci.py",
#                                 content="""def fibonacci(n):
#                                         a, b = 0, 1
#                                         for _ in range(n):
#                                             a, b = b, a + b
#                                         return a""",
#                                 language="python",
#                             )
#                         ]
#                     ),
#                     criterion=[],
#                 ),
#             ],
#         )

#         # Create test miner responses
#         test_miner_responses = [
#             MinerResponse(
#                 dojo_task_id="test_task_1",
#                 hotkey="test_hotkey_1",
#                 coldkey="test_coldkey_1",
#                 task_result=CompletionResponses(responses=[{"model1": 1, "model2": 2}]),
#             )
#         ]

#         # Create test ground truth
#         test_ground_truth = {"model1": 1, "model2": 2}

#         # Save the task
#         created_task = await orm.save_task(
#             test_validator_task, test_miner_responses, test_ground_truth
#         )

#         if created_task:
#             logger.success(f"Successfully created task with ID: {created_task.id}")

#             # Verify the saved data
#             saved_task = await ValidatorTask.prisma().find_unique(
#                 where={"id": created_task.id},
#                 include={
#                     "completions": True,
#                     "miner_responses": True,
#                     "GroundTruth": True,
#                 },
#             )

#             logger.info(f"Number of completions: {len(saved_task.completions)}")
#             logger.info(f"Number of miner responses: {len(saved_task.miner_responses)}")
#             logger.info(f"Number of ground truths: {len(saved_task.GroundTruth)}")
#         else:
#             logger.error("Failed to create task")

#     except Exception as e:
#         logger.error(f"An error occurred: {e}")
#     finally:
#         # Always disconnect when done
#         await disconnect_db()


if __name__ == "__main__":

    async def run_tests():
        # await test_get_expired_tasks()
        # await test_get_real_model_ids()
        # await test_mark_validator_task_as_processed()
        await test_get_num_processed_tasks()
        # await test_save_task()

    asyncio.run(run_tests())
