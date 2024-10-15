import json
from datetime import datetime, timezone
from typing import AsyncGenerator, List

import torch
from bittensor.btlogging import logging as logger

from commons.exceptions import (
    InvalidCompletion,
    InvalidMinerResponse,
    InvalidTask,
    NoNewUnexpiredTasksYet,
    UnexpiredTasksAlreadyProcessed,
)
from database.client import transaction
from database.mappers import (
    map_child_feedback_request_to_model,
    map_completion_response_to_model,
    map_criteria_type_to_model,
    map_feedback_request_model_to_feedback_request,
    map_parent_feedback_request_to_model,
)
from database.prisma import Json
from database.prisma.errors import PrismaError
from database.prisma.models import (
    Feedback_Request_Model,
    Ground_Truth_Model,
    Score_Model,
)
from database.prisma.types import (
    Feedback_Request_ModelInclude,
    Feedback_Request_ModelWhereInput,
    Score_ModelCreateInput,
    Score_ModelUpdateInput,
)
from dojo import TASK_DEADLINE
from dojo.protocol import (
    DendriteQueryResponse,
    FeedbackRequest,
)


class ORM:
    @staticmethod
    async def get_unexpired_tasks(
        validator_hotkeys: list[str],
        batch_size: int = 10,
    ) -> AsyncGenerator[tuple[List[DendriteQueryResponse], bool], None]:
        """Returns a batch of Feedback_Request_Model and a boolean indicating if there are more batches

        Args:
            validator_hotkeys (list[str]): List of validator hotkeys.
            batch_size (int, optional): Number of tasks to return in a batch. Defaults to 10.

            1 task == 1 validator request, N miner responses

        Raises:
            NoNewUnexpiredTasksYet: If no unexpired tasks are found for processing.
            UnexpiredTasksAlreadyProcessed: If all unexpired tasks have already been processed.

        Yields:
            Iterator[AsyncGenerator[tuple[List[DendriteQueryResponse], bool], None]]:
                Returns a batch of DendriteQueryResponse and a boolean indicating if there are more batches

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
        vali_where_query_unprocessed = Feedback_Request_ModelWhereInput(
            {
                "hotkey": {"in": validator_hotkeys, "mode": "insensitive"},
                "child_requests": {"some": {}},
                # only check for expire at since miner may lie
                "expire_at": {
                    "gt": datetime.now(timezone.utc),
                },
                "is_processed": {"equals": False},
            }
        )

        vali_where_query_processed = Feedback_Request_ModelWhereInput(
            {
                "hotkey": {"in": validator_hotkeys, "mode": "insensitive"},
                "child_requests": {"some": {}},
                # only check for expire at since miner may lie
                "expire_at": {
                    "gt": datetime.now(timezone.utc),
                },
                "is_processed": {"equals": True},
            }
        )

        # count first total including non
        task_count_unprocessed = await Feedback_Request_Model.prisma().count(
            where=vali_where_query_unprocessed,
        )

        task_count_processed = await Feedback_Request_Model.prisma().count(
            where=vali_where_query_processed,
        )

        if not task_count_unprocessed:
            if task_count_processed:
                raise UnexpiredTasksAlreadyProcessed(
                    f"No remaining unexpired tasks found for processing, but don't worry as you have processed {task_count_processed} tasks."
                )
            else:
                raise NoNewUnexpiredTasksYet(
                    f"No unexpired tasks found for processing, please wait for tasks to pass the task deadline of {TASK_DEADLINE} seconds."
                )

        for i in range(0, task_count_unprocessed, batch_size):
            # find all validator requests
            validator_requests = await Feedback_Request_Model.prisma().find_many(
                include=include_query,
                where=vali_where_query_unprocessed,
                order={"created_at": "desc"},
                skip=i,
                take=batch_size,
            )

            # find all miner responses
            validator_request_ids = [r.id for r in validator_requests]

            miner_responses = await Feedback_Request_Model.prisma().find_many(
                include=include_query,
                where={
                    "parent_id": {"in": validator_request_ids},
                    "is_processed": {"equals": False},
                },
                order={"created_at": "desc"},
            )

            responses: list[DendriteQueryResponse] = []
            for validator_request in validator_requests:
                vali_request = map_feedback_request_model_to_feedback_request(
                    validator_request
                )

                m_responses = list(
                    map(
                        lambda x: map_feedback_request_model_to_feedback_request(
                            x, is_miner=True
                        ),
                        [
                            m
                            for m in miner_responses
                            if m.parent_id == validator_request.id
                        ],
                    )
                )

                responses.append(
                    DendriteQueryResponse(
                        request=vali_request, miner_responses=m_responses
                    )
                )

            # yield responses, so caller can do something
            has_more_batches = True
            yield responses, has_more_batches

        yield [], False

    @staticmethod
    async def get_real_model_ids(request_id: str) -> dict[str, str]:
        """Fetches a mapping of obfuscated model IDs to real model IDs for a given request ID."""
        ground_truths = await Ground_Truth_Model.prisma().find_many(
            where={"request_id": request_id}
        )
        return {gt.obfuscated_model_id: gt.real_model_id for gt in ground_truths}

    @staticmethod
    async def mark_tasks_processed_by_request_ids(request_ids: list[str]) -> None:
        """Mark records associated with validator's request and miner's responses as processed.

        Args:
            request_ids (list[str]): List of request ids.
        """
        if not request_ids:
            logger.error("No request ids provided to mark as processed")
            return

        try:
            async with transaction() as tx:
                num_updated = await tx.feedback_request_model.update_many(
                    data={"is_processed": True}, where={"id": {"in": request_ids}}
                )
                logger.success(
                    f"Marked {num_updated} records associated to {len(request_ids)} tasks as processed"
                )
        except PrismaError as exc:
            logger.error(f"Prisma error occurred: {exc}")
        except Exception as exc:
            logger.error(f"Unexpected error occurred: {exc}")

    @staticmethod
    async def get_task_by_request_id(request_id: str) -> DendriteQueryResponse | None:
        try:
            # find the parent id first
            include_query = Feedback_Request_ModelInclude(
                {
                    "completions": True,
                    "criteria_types": True,
                    "ground_truths": True,
                    "parent_request": True,
                    "child_requests": True,
                }
            )
            all_requests = await Feedback_Request_Model.prisma().find_many(
                where={
                    "request_id": request_id,
                },
                include=include_query,
            )

            validator_requests = [r for r in all_requests if r.parent_id is None]
            assert len(validator_requests) == 1, "Expected only one validator request"
            validator_request = validator_requests[0]
            if not validator_request.child_requests:
                raise InvalidTask(
                    f"Validator request {validator_request.id} must have child requests"
                )

            miner_responses = [
                map_feedback_request_model_to_feedback_request(r, is_miner=True)
                for r in validator_request.child_requests
            ]
            return DendriteQueryResponse(
                request=map_feedback_request_model_to_feedback_request(
                    model=validator_request, is_miner=False
                ),
                miner_responses=miner_responses,
            )

        except Exception as e:
            logger.error(f"Failed to get feedback request by request_id: {e}")
            return None

    @staticmethod
    async def get_num_processed_tasks() -> int:
        return await Feedback_Request_Model.prisma().count(
            where={"is_processed": True, "parent_id": None}
        )

    @staticmethod
    async def update_miner_completions_by_request_id(
        request_id: str, miner_responses: List[FeedbackRequest]
    ) -> bool:
        """Update the miner's provided rank_id / scores etc. for a given request id that it is responding to validator. This exists because over the course of a task, a miner may recruit multiple workers and we
        need to recalculate the average score / rank_id etc. across all workers.
        """
        try:
            async with transaction() as tx:
                # find the feedback request ids
                miner_hotkeys = []
                for miner_response in miner_responses:
                    if not miner_response.axon or not miner_response.axon.hotkey:
                        raise InvalidMinerResponse(
                            f"Miner response {miner_response} must have a hotkey"
                        )
                    miner_hotkeys.append(miner_response.axon.hotkey)

                found_responses = await tx.feedback_request_model.find_many(
                    where={"request_id": request_id, "hotkey": {"in": miner_hotkeys}}
                )

                # delete the completions for all of these miners
                await tx.completion_response_model.delete_many(
                    where={
                        "feedback_request_id": {"in": [r.id for r in found_responses]}
                    }
                )

                # reconstruct the completion_responses data
                for miner_response in miner_responses:
                    # find the particular request
                    hotkey = miner_response.axon.hotkey  # type: ignore
                    curr_miner_response = await tx.feedback_request_model.find_first(
                        where=Feedback_Request_ModelWhereInput(
                            request_id=request_id,
                            hotkey=hotkey,  # type: ignore
                        )
                    )

                    if not curr_miner_response:
                        raise ValueError("Miner response not found")

                    # recreate completions
                    for completion in miner_response.completion_responses:
                        await tx.completion_response_model.create(
                            data=map_completion_response_to_model(
                                completion, curr_miner_response.id
                            )
                        )

            logger.success(
                f"Successfully updated completion data for miners: {miner_hotkeys}"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to update completion data for miner responses: {e}")
            return False

    @staticmethod
    async def save_task(
        validator_request: FeedbackRequest, miner_responses: List[FeedbackRequest]
    ) -> Feedback_Request_Model | None:
        """Saves a task, which consists of both the validator's request and the miners' responses.

        Args:
            validator_request (FeedbackRequest): The request made by the validator.
            miner_responses (List[FeedbackRequest]): The responses made by the miners.

        Returns:
            Feedback_Request_Model | None: Only validator's feedback request model, or None if failed.
        """
        try:
            feedback_request_model: Feedback_Request_Model | None = None
            async with transaction() as tx:
                logger.trace("Starting transaction for saving task.")

                feedback_request_model = await tx.feedback_request_model.create(
                    data=map_parent_feedback_request_to_model(validator_request)
                )

                # Create related criteria types
                criteria_create_input = [
                    map_criteria_type_to_model(criteria, feedback_request_model.id)
                    for criteria in validator_request.criteria_types
                ]
                await tx.criteria_type_model.create_many(criteria_create_input)

                # Create related miner responses (child) and their completion responses
                created_miner_models: list[Feedback_Request_Model] = []
                for miner_response in miner_responses:
                    try:
                        create_miner_model_input = map_child_feedback_request_to_model(
                            miner_response,
                            feedback_request_model.id,
                            expire_at=feedback_request_model.expire_at,
                        )

                        created_miner_model = await tx.feedback_request_model.create(
                            data=create_miner_model_input
                        )

                        created_miner_models.append(created_miner_model)

                        # Create related completions for miner responses
                        for completion in miner_response.completion_responses:
                            completion_input = map_completion_response_to_model(
                                completion, created_miner_model.id
                            )
                            await tx.completion_response_model.create(
                                data=completion_input
                            )
                            logger.trace(
                                f"Created completion response: {completion_input}"
                            )

                    # we catch exceptions here because whether a miner responds well should not affect other miners
                    except InvalidMinerResponse as e:
                        miner_hotkey = (
                            miner_response.axon.hotkey if miner_response.axon else "??"
                        )
                        logger.debug(
                            f"Miner response from hotkey: {miner_hotkey} is invalid: {e}"
                        )
                    except InvalidCompletion as e:
                        miner_hotkey = (
                            miner_response.axon.hotkey if miner_response.axon else "??"
                        )
                        logger.debug(
                            f"Completion response from hotkey: {miner_hotkey} is invalid: {e}"
                        )

                if len(created_miner_models) == 0:
                    raise InvalidTask(
                        "A task must consist of at least one miner response, along with validator's request"
                    )

                feedback_request_model.child_requests = created_miner_models
            return feedback_request_model
        except Exception as e:
            logger.error(f"Failed to save dendrite query response: {e}")
            return None

    @staticmethod
    async def create_or_update_validator_score(scores: torch.Tensor) -> None:
        # Save scores as a single record
        score_model = await Score_Model.prisma().find_first()
        scores_list = scores.tolist()
        if score_model:
            await Score_Model.prisma().update(
                where={"id": score_model.id},
                data=Score_ModelUpdateInput(score=Json(json.dumps(scores_list))),
            )
        else:
            await Score_Model.prisma().create(
                data=Score_ModelCreateInput(
                    score=Json(json.dumps(scores_list)),
                )
            )

    @staticmethod
    async def get_validator_score() -> torch.Tensor | None:
        score_record = await Score_Model.prisma().find_first(
            order={"created_at": "desc"}
        )
        if not score_record:
            return None

        return torch.tensor(json.loads(score_record.score))
