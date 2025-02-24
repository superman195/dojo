import asyncio
import json
import random
import traceback
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import aiohttp
from bittensor.utils.btlogging import logging as logger
from tenacity import RetryError

import dojo
from commons.dataset.synthetic import SyntheticAPI
from commons.hfl_heplers import HFLManager
from commons.orm import ORM
from commons.utils import datetime_as_utc, get_new_uuid, set_expire_time
from database.prisma.enums import HFLStatusEnum
from database.prisma.models import MinerScore
from database.prisma.types import MinerScoreWhereInput
from dojo.protocol import (
    CriteriaType,
    DendriteQueryResponse,
    ScoreCriteria,
    ScoreFeedbackEvent,
    SyntheticQA,
    TaskSynapseObject,
    TaskTypeEnum,
    TextCriteria,
)
from neurons.validator import Validator


class FeedbackLoop:
    async def run(self, validator: Validator):
        """Runs the feedback loop periodically."""
        while True:
            # Wait for the validator update score interval
            await asyncio.sleep(dojo.VALIDATOR_UPDATE_SCORE)
            try:
                await self.start_feedback_loop(validator)
            except Exception as e:
                logger.error(f"Error in feedback loop: {e}")

    async def start_feedback_loop(self, validator: Validator):
        """Starts the feedback loop."""
        result = await self.select_validator_task()
        if result:
            selected_task, selected_completion = result
            text_criteria_task = await self.generate_text_criteria_task(
                selected_task, selected_completion
            )
            if text_criteria_task:
                # Call send_request with the text criteria task
                await validator.send_request(
                    synapse=text_criteria_task,
                    ground_truth=None,
                    obfuscated_model_to_model=None,
                    synthetic_task=False,
                    subset_size=7,
                )

    async def select_validator_task(self) -> Tuple[TaskSynapseObject, str] | None:
        """
        Selects a validator task from the latest expired tasks within a specific time window.
        Time window:
          - expire_from: current time minus 2 hours
          - expire_to: current time minus 1 hour
        Reason for using this time window:We want to select a task that has expired and been scored
        The task is only selected if there exists at least one completion where >50% and <90%
        of the miners scored it the highest.

        Returns:
            Tuple[TaskSynapseObject, str] | None: A tuple of (validator task, completion_id) if criteria are met;
        """
        expire_from = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(hours=48)
        # TODO: Change back to 1 hour
        expire_to = datetime_as_utc(datetime.now(timezone.utc))

        eligible_tasks = []
        try:
            async for tasks_batch, has_more in ORM.get_expired_tasks(
                batch_size=10,
                expire_from=expire_from,
                expire_to=expire_to,
                is_processed=True,
            ):
                for dendrite_response in tasks_batch:
                    eligible_task = await self._evaluate_task(dendrite_response)
                    if eligible_task:
                        eligible_tasks.append(eligible_task)
        except Exception as e:
            logger.error(f"Error retrieving expired tasks: {e}")
            return None

        if not eligible_tasks:
            logger.info("No tasks meeting criteria found.")
            return None

        selected_task, selected_completion = random.choice(eligible_tasks)
        logger.info(
            f"Selected validator task with ID: {selected_task.task_id} and completion ID: {selected_completion}"
        )
        return selected_task, selected_completion

    async def _evaluate_task(
        self, dendrite_response: DendriteQueryResponse
    ) -> Tuple[TaskSynapseObject, str] | None:
        """
        Evaluates a single task based on its miner scores from the MinerScore table.
        For each completion in the task, computes what percentage of miners scored it
        highest based on raw_score. If any completion has >50% and <90% of miners
        scoring it the highest, then the task qualifies.

        Args:
            dendrite_response (DendriteQueryResponse): Contains the validator task and related miner responses.

        Returns:
            Optional[Tuple[TaskSynapseObject, str]]: Tuple of (validator task, completion_id) if criteria are met;
            otherwise None.
        """
        validator_task: TaskSynapseObject = dendrite_response.validator_task

        if not validator_task.task_id:
            logger.debug("Task ID is missing")
            return None

        try:
            # Get all miner scores for this validator task
            miner_scores = await MinerScore.prisma().find_many(
                where=MinerScoreWhereInput(
                    {
                        "miner_response_relation": {
                            "is": {"validator_task_id": validator_task.task_id}
                        }
                    }
                ),
                include={
                    "criterion_relation": {"include": {"completion_relation": True}},
                    "miner_response_relation": True,
                },
            )

            if not miner_scores:
                logger.debug(f"No miner scores found for task {validator_task.task_id}")
                return None

            # Group scores by miner response ID and completion ID
            # map of miner_response_id -> completion_id
            miner_best_completions = {}
            # map of completion_id -> list of miners and their raw scores
            completion_scores = {}

            for score in miner_scores:
                if (
                    not score.scores
                    or not score.criterion_relation
                    or not score.criterion_relation.completion_relation
                ):
                    continue

                # Parse the scores JSON
                scores_dict = json.loads(score.scores)
                miner_raw_score = scores_dict.get("raw_score")
                if miner_raw_score is None:
                    continue

                completion_id = (
                    score.criterion_relation.completion_relation.completion_id
                )
                miner_response_id = score.miner_response_id

                # Store score for this completion
                if completion_id not in completion_scores:
                    completion_scores[completion_id] = []
                completion_scores[completion_id].append(
                    (miner_response_id, miner_raw_score)
                )

            # Find highest scored completion for each miner
            for completion_id, scores in completion_scores.items():
                for miner_response_id, score in scores:
                    current_best = miner_best_completions.get(miner_response_id)
                    if (
                        current_best is None
                        or score > completion_scores[current_best][0][1]
                    ):
                        miner_best_completions[miner_response_id] = completion_id

            total_miners = len(set(miner_best_completions.keys()))
            if total_miners == 0:
                return None

            # Count how many miners scored each completion as best
            completion_counts = {}
            for best_completion in miner_best_completions.values():
                completion_counts[best_completion] = (
                    completion_counts.get(best_completion, 0) + 1
                )

            # Check percentages for each completion
            for completion_id, count in completion_counts.items():
                percentage = (count / total_miners) * 100
                # TODO: Change back to < 90
                if 50 < percentage < 101:
                    logger.info(
                        f"Found eligible completion {completion_id} with {percentage:.1f}% "
                        f"of miners ({count}/{total_miners}) scoring it highest"
                    )
                    return validator_task, completion_id

            return None

        except Exception as e:
            logger.error(f"Error evaluating task {validator_task.task_id}: {e}")
            return None

    async def generate_text_criteria_task(
        self, validator_task: TaskSynapseObject, completion_id: str
    ) -> TaskSynapseObject | None:
        """
        Generates a text criteria task based on a selected validator task and completion.
        This task will be used to evaluate the quality of miners' scoring.

        Args:
            validator_task (TaskSynapseObject): The original validator task
            completion_id (str): ID of the selected completion to be evaluated

        Returns:
            TaskSynapseObject | None: A new task for text-based evaluation, or None if generation fails
        """
        try:
            # Find the selected completion from the original task
            selected_completion = next(
                (
                    c
                    for c in (validator_task.completion_responses or [])
                    if c.completion_id == completion_id
                ),
                None,
            )
            if not selected_completion:
                logger.error(
                    f"Completion {completion_id} not found in task {validator_task.task_id}"
                )
                return None

            # Set text criteria for completion
            text_criteria: List[CriteriaType] = [
                TextCriteria(
                    query="What specific improvements could make this output more accurate, complete, or relevant to the prompt?",
                    text_feedback="",
                ),
            ]
            selected_completion.criteria_types = text_criteria

            prompt = f"""Please analyze this output and suggest specific improvements:
            Prompt: {validator_task.prompt}
            """
            # Create a new task with the same prompt but different criteria type
            new_tf_task = TaskSynapseObject(
                task_id=get_new_uuid(),
                previous_task_id=validator_task.task_id,
                prompt=prompt,
                task_type=TaskTypeEnum.TEXT_TO_COMPLETION,
                expire_at=set_expire_time(
                    int(dojo.TASK_DEADLINE / 2)
                ),  # Half the deadline for TF
                completion_responses=[
                    selected_completion
                ],  # Only include the selected completion
            )

            logger.info(
                f"Generated text criteria task with ID: {new_tf_task.task_id} "
                f"based on task: {validator_task.task_id}"
            )
            return new_tf_task

        except Exception as e:
            logger.error(f"Error generating text criteria task: {e}")
            return None

    async def create_sf_tasks(self, validator: Validator):
        """
        Poll for completed text feedback tasks and process synthetic improvements.
        Runs continuously with SF_TASK_CREATION_INTERVAL delay between iterations.

        Flow:
        1. Query TF_COMPLETED states in batches
        2. For each batch:
            - Check synthetic task status
            - If ready, create improved task and send to miners
            - Update state to SF_PENDING
        """
        while True:
            try:
                # Get tasks with TF_COMPLETED status in batches
                async for tf_tasks_batch, _ in ORM.get_TF_tasks_by_hfl_status(
                    status=HFLStatusEnum.TF_COMPLETED,
                    batch_size=10,
                ):
                    if not tf_tasks_batch:
                        await asyncio.sleep(dojo.SF_TASK_CREATION_INTERVAL)
                        continue

                    for tf_task in tf_tasks_batch:
                        try:
                            if (
                                not tf_task.HFLState
                                or not tf_task.HFLState.current_synthetic_req_id
                            ):
                                logger.debug(
                                    f"No HFLState or current_synthetic_req_id for task-id: {tf_task.id}"
                                )
                                continue

                            # Check if synthetic task is ready
                            improved_task = (
                                await self._generate_improved_synthetic_request(
                                    tf_task.HFLState.current_synthetic_req_id
                                )
                            )
                            if not improved_task:
                                logger.debug(
                                    f"No improved task found for {tf_task.HFLState.current_synthetic_req_id} yet"
                                )
                                continue

                            # Create new task for miners
                            new_task = TaskSynapseObject(
                                task_id=get_new_uuid(),
                                prompt=improved_task.prompt,
                                task_type=TaskTypeEnum.SCORE_FEEDBACK,
                                expire_at=set_expire_time(dojo.TASK_DEADLINE),
                                completion_responses=improved_task.completion_responses,
                            )

                            # Send to miners and get responses
                            sf_task = await validator.send_request(
                                synapse=new_task,
                                synthetic_task=True,
                                prev_task_id=tf_task.id,
                            )

                            if not sf_task:
                                logger.error(
                                    f"Failed to send improved task to miners for {tf_task.id}"
                                )
                                continue

                            # Update HFL state
                            event = ScoreFeedbackEvent(
                                type=HFLStatusEnum.SF_PENDING,
                                task_id=tf_task.id,
                                syn_req_id=tf_task.HFLState.current_synthetic_req_id,
                                iteration=tf_task.HFLState.current_iteration,
                                timestamp=datetime_as_utc(datetime.now(timezone.utc)),
                            )

                            await HFLManager.update_state(
                                tf_task.HFLState.id,
                                {
                                    "status": HFLStatusEnum.SF_PENDING,
                                    "current_task_id": sf_task.id,
                                    "current_synthetic_req_id": None,  # Clear synthetic req ID
                                },
                                event,
                            )

                        except Exception as e:
                            # Log error but continue processing other tasks
                            logger.error(
                                f"Error processing task {tf_task.id}: {str(e)}"
                            )
                            continue

                    await asyncio.sleep(dojo.SF_TASK_CREATION_INTERVAL)

            except Exception as e:
                logger.error(f"Error in synthetic polling loop: {str(e)}")
                await asyncio.sleep(dojo.SF_TASK_CREATION_INTERVAL)

    async def _generate_improved_synthetic_request(
        self, syn_req_id: str
    ) -> TaskSynapseObject | None:
        """Generate an improved synthetic request"""
        try:
            SF_task: SyntheticQA | None = await SyntheticAPI.get_improved_SF(syn_req_id)

            if not SF_task:
                logger.error(f"No improved task found for {syn_req_id}")
                return None

            # Create criteria for each completion response
            criteria: List[CriteriaType] = [
                ScoreCriteria(
                    min=1.0,
                    max=100.0,
                )
            ]

            # Set criteria for each completion response
            for response in SF_task.responses:
                response.criteria_types = criteria

            synapse = TaskSynapseObject(
                task_id=get_new_uuid(),
                prompt=SF_task.prompt,
                task_type=TaskTypeEnum.CODE_GENERATION,
                expire_at=set_expire_time(dojo.TASK_DEADLINE),
                completion_responses=SF_task.responses,
            )

        except (RetryError, ValueError, aiohttp.ClientError) as e:
            logger.error(f"Error getting improved task for {syn_req_id}: {e}")

        except Exception as e:
            logger.error(f"Unexpected error during synthetic data generation: {e}")
            logger.debug(f"Traceback: {traceback.format_exc()}")

        return synapse

    async def update_sf_task_results(self, validator: Validator):
        """
        Update the results of Score Feedback (SF) tasks.

        Flow:
        1. Query SF_PENDING tasks that have expired within a time window
        2. For each task:
            - Get all miner responses
            - Query each miner for task results using their dojo_task_id
            - Update task results in database
            - Update HFL state to SF_COMPLETED
        """
        while True:
            try:
                # Get tasks that expired in the last 2 hours
                expire_from = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(
                    hours=2
                )
                expire_to = datetime_as_utc(datetime.now(timezone.utc))

                logger.debug(
                    f"Processing SF tasks with expire_from: {expire_from} and expire_to: {expire_to}"
                )

                # Get SF_PENDING tasks in batches
                async for sf_tasks_batch, _ in ORM.get_SF_tasks_by_hfl_status(
                    status=HFLStatusEnum.SF_PENDING,
                    expire_from=expire_from,
                    expire_to=expire_to,
                    batch_size=10,
                ):
                    if not sf_tasks_batch:
                        await asyncio.sleep(dojo.SF_TASK_CREATION_INTERVAL)
                        continue

                    for sf_task in sf_tasks_batch:
                        try:
                            if not sf_task.miner_responses:
                                logger.debug(
                                    f"No miner responses for task {sf_task.id}"
                                )
                                continue

                            if not sf_task.HFLState or not sf_task.HFLState.id:
                                logger.debug(
                                    f"No HFLState or HFLState.id for task {sf_task.id}"
                                )
                                continue

                            # Update task results for each miner response
                            for miner_response in sf_task.miner_responses:
                                if (
                                    not miner_response.hotkey
                                    or not miner_response.dojo_task_id
                                ):
                                    logger.debug(
                                        "Missing hotkey or dojo_task_id for miner response"
                                    )
                                    continue

                                # Get task results from miner
                                task_results = (
                                    await validator._get_task_results_from_miner(
                                        miner_hotkey=miner_response.hotkey,
                                        dojo_task_id=miner_response.dojo_task_id,
                                    )
                                )

                                if not task_results:
                                    logger.debug(
                                        f"No task results from miner {miner_response.hotkey}"
                                    )
                                    continue

                                # Update task results in database
                                success = await ORM.update_miner_task_results(
                                    miner_hotkey=miner_response.hotkey,
                                    dojo_task_id=miner_response.dojo_task_id,
                                    task_results=task_results,
                                )

                                if not success:
                                    logger.warning(
                                        f"Failed to update task_result for miner {miner_response.hotkey}"
                                    )

                            # Update HFL state to SF_COMPLETED
                            event = ScoreFeedbackEvent(
                                type=HFLStatusEnum.SF_COMPLETED,
                                task_id=sf_task.id,
                                iteration=sf_task.HFLState.current_iteration,
                                timestamp=datetime_as_utc(datetime.now(timezone.utc)),
                            )

                            await HFLManager.update_state(
                                sf_task.HFLState.id,
                                {
                                    "status": HFLStatusEnum.SF_COMPLETED,
                                },
                                event,
                            )

                            logger.success(
                                f"Successfully processed SF task {sf_task.id}"
                            )

                        except Exception as e:
                            logger.error(
                                f"Error processing SF task {sf_task.id}: {str(e)}"
                            )
                            continue

                # Add delay between batches
                await asyncio.sleep(dojo.SF_TASK_CREATION_INTERVAL)

            except Exception as e:
                logger.error(f"Error in SF task processing loop: {str(e)}")
                await asyncio.sleep(dojo.SF_TASK_CREATION_INTERVAL)
