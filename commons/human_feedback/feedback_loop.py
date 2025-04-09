import asyncio
import json
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from bittensor.utils.btlogging import logging as logger

import dojo
from commons.exceptions import NoNewExpiredTasksYet
from commons.hfl_heplers import HFLManager
from commons.orm import ORM
from commons.utils import datetime_as_utc, get_new_uuid, set_expire_time
from database.prisma.enums import HFLStatusEnum
from database.prisma.models import HFLState, MinerResponse, MinerScore
from database.prisma.types import MinerScoreWhereInput
from dojo.protocol import (
    CriteriaType,
    DendriteQueryResponse,
    TaskSynapseObject,
    TaskTypeEnum,
    TextCriteria,
    TextFeedbackEvent,
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
                await self.update_text_feedback_results(validator)
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
                # Create HFLState for this feedback loop
                hfl_state = await HFLManager.create_state(
                    original_task_id=selected_task.task_id,
                    current_task_id=text_criteria_task.task_id,
                    status=HFLStatusEnum.TF_PENDING,
                )

                # We can't directly set hfl_state_id on the task object since it's not part of the TaskSynapseObject,
                # this is a limitation in the current design, but we can still track the association
                # via the HFLState table which has both IDs
                logger.info(
                    f"Created HFL state with ID: {hfl_state.id} for task: {text_criteria_task.task_id}"
                )

                # Call send_request with the text criteria task
                await validator.send_request(
                    synapse=text_criteria_task,
                    ground_truth=None,
                    obfuscated_model_to_model=None,
                    synthetic_task=False,
                    subset_size=7,
                )

                logger.info(
                    f"Started HFL with state ID: {hfl_state.id}, original task: {selected_task.task_id}, TF task: {text_criteria_task.task_id}"
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
                has_previous_task=False,
                task_type=TaskTypeEnum.CODE_GENERATION,
            ):
                for dendrite_response in tasks_batch:
                    eligible_task = await self._evaluate_task(dendrite_response)
                    if eligible_task:
                        eligible_tasks.append(eligible_task)
        except NoNewExpiredTasksYet as e:
            logger.info(f"No expired CODE_GENERATION tasks found for processing: {e}")
            return None
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

    async def update_text_feedback_results(
        self, validator: Validator
    ) -> Dict[str, List[MinerResponse]]:
        """
        Updates task results for TEXT_TO_COMPLETION tasks that haven't been processed yet.
        Similar to validator.update_task_results but specifically for text feedback tasks.

        Ensures each task has at least 3 responses before marking as processed.
        If fewer than 3 responses are received, the task is sent to additional miners
        until enough responses are collected.
        """
        # Dictionary to store selected responses by task ID
        selected_responses_by_task = {}

        # Track tasks that have been sent for additional responses to avoid excessive retries
        # Reset this tracking every function call so we don't accumulate stale data
        self.resent_task_attempts = getattr(self, "resent_task_attempts", {})

        try:
            logger.info("Updating text feedback task results...")
            batch_size: int = 10

            expire_from = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(
                hours=2
            )
            expire_to = datetime_as_utc(datetime.now(timezone.utc))

            try:
                # Find HFL states associated with TEXT_TO_COMPLETION tasks
                hfl_states = await HFLState.prisma().find_many(
                    where={
                        "status": HFLStatusEnum.TF_PENDING,
                    }
                )

                # Create a map of task IDs to HFL state IDs
                hfl_state_map = {
                    state.current_task_id: state.id for state in hfl_states
                }

                async for task_batch, has_more in ORM.get_expired_tasks(
                    batch_size=batch_size,
                    expire_from=expire_from,
                    expire_to=expire_to,
                    is_processed=False,
                    has_previous_task=True,
                    task_type=TaskTypeEnum.TEXT_TO_COMPLETION,
                ):
                    if not task_batch:
                        continue

                    # Process multiple tasks concurrently
                    tasks = [
                        validator._update_task_results(task) for task in task_batch
                    ]
                    miner_responses_lists = await asyncio.gather(*tasks)

                    sufficient_response_task_ids = []
                    tasks_needing_more_responses = []

                    # Check which tasks have enough responses
                    for i, responses in enumerate(miner_responses_lists):
                        task = task_batch[i].validator_task
                        task_id = task.task_id
                        response_count = len(responses) if responses else 0

                        # Get the count of previous retry attempts, default to 0
                        retry_count = self.resent_task_attempts.get(task_id, 0)

                        if response_count >= 3:
                            logger.info(
                                f"Task {task_id} has {response_count} responses, marking as processed"
                            )
                            sufficient_response_task_ids.append(task_id)

                            # Remove from retry tracking if it was there
                            if task_id in self.resent_task_attempts:
                                del self.resent_task_attempts[task_id]

                            # Select 3 random responses for this task
                            selected_responses = random.sample(responses, 3)
                            selected_responses_by_task[task.task_id] = (
                                selected_responses
                            )

                            # Update HFLState if there's a state associated with this task
                            hfl_state_id = hfl_state_map.get(task_id)
                            if hfl_state_id:
                                # Get the HFL state
                                hfl_state = await HFLState.prisma().find_unique(
                                    where={"id": hfl_state_id}
                                )

                                if hfl_state:
                                    # Update HFLState to TF_COMPLETED
                                    event_data = TextFeedbackEvent(
                                        task_id=task_id,
                                        iteration=hfl_state.current_iteration,
                                        timestamp=datetime_as_utc(
                                            datetime.now(timezone.utc)
                                        ),
                                    )

                                    await HFLManager.update_state(
                                        hfl_state_id=hfl_state.id,
                                        updates={"status": HFLStatusEnum.TF_COMPLETED},
                                        event_data=event_data,
                                    )

                                    logger.info(
                                        f"Updated HFL state {hfl_state.id} to TF_COMPLETED for task {task_id}"
                                    )

                            logger.info(
                                f"Selected {len(selected_responses)} responses for task {task_id}"
                            )
                        else:
                            # Only retry up to a reasonable number of times (e.g., 5 attempts)
                            # This prevents tasks that are consistently failing from being retried indefinitely
                            MAX_RETRY_ATTEMPTS = 5

                            if retry_count < MAX_RETRY_ATTEMPTS:
                                # Increment retry count for this task
                                self.resent_task_attempts[task_id] = retry_count + 1

                                logger.info(
                                    f"Task {task_id} has only {response_count} responses (attempt {retry_count + 1}/{MAX_RETRY_ATTEMPTS}), "
                                    f"sending to more miners"
                                )
                                tasks_needing_more_responses.append(task)
                            else:
                                logger.warning(
                                    f"Task {task_id} failed to get enough responses after {MAX_RETRY_ATTEMPTS} attempts. "
                                    f"Using available {response_count} responses."
                                )

                                # Process the task with whatever responses we have
                                if responses:
                                    # Select all available responses (up to 3)
                                    selected_responses = responses[
                                        : min(3, response_count)
                                    ]
                                    selected_responses_by_task[task_id] = (
                                        selected_responses
                                    )
                                    sufficient_response_task_ids.append(task_id)

                                    # Update HFLState to TF_COMPLETED
                                    hfl_state_id = hfl_state_map.get(task_id)
                                    if hfl_state_id:
                                        hfl_state = await HFLState.prisma().find_unique(
                                            where={"id": hfl_state_id}
                                        )

                                        if hfl_state:
                                            event_data = TextFeedbackEvent(
                                                task_id=task_id,
                                                iteration=hfl_state.current_iteration,
                                                timestamp=datetime_as_utc(
                                                    datetime.now(timezone.utc)
                                                ),
                                            )

                                            await HFLManager.update_state(
                                                hfl_state_id=hfl_state.id,
                                                updates={
                                                    "status": HFLStatusEnum.TF_COMPLETED
                                                },
                                                event_data=event_data,
                                            )

                                            logger.info(
                                                f"Updated HFL state {hfl_state.id} to TF_COMPLETED for task {task_id} "
                                                f"with only {response_count} responses"
                                            )

                                    logger.info(
                                        f"Used all {response_count} available responses for task {task_id}"
                                    )
                                # Remove from tracking since we're done with this task
                                del self.resent_task_attempts[task_id]

                    # Mark tasks with sufficient responses as processed
                    if sufficient_response_task_ids:
                        await ORM.mark_validator_task_as_processed(
                            sufficient_response_task_ids
                        )

                    # Send tasks with insufficient responses to more miners
                    for task in tasks_needing_more_responses:
                        # Update expiry time for the task
                        task.expire_at = set_expire_time(int(dojo.TASK_DEADLINE / 2))

                        # Send to additional miners
                        await validator.send_request(
                            synapse=task,
                            ground_truth=None,
                            obfuscated_model_to_model=None,
                            synthetic_task=False,
                            subset_size=7,  # Send to 7 additional miners
                        )
                        logger.info(f"Sent task {task.task_id} to 7 additional miners")

            except NoNewExpiredTasksYet as e:
                logger.info(
                    f"No expired TEXT_TO_COMPLETION tasks found for processing: {e}"
                )
                return selected_responses_by_task

            return selected_responses_by_task

        except Exception as e:
            logger.error(f"Error during text feedback task monitoring: {str(e)}")
            return selected_responses_by_task
