import json

import bittensor as bt
from pydantic import BaseModel

from commons.utils import datetime_to_iso8601_str, iso8601_str_to_datetime
from database.prisma import Json
from database.prisma.enums import CriteriaTypeEnum, TaskTypeEnum
from database.prisma.models import MinerResponse, ValidatorTask
from database.prisma.types import (
    CompletionCreateInput,
    CriterionCreateWithoutRelationsInput,
    GroundTruthCreateWithoutRelationsInput,
    MinerResponseCreateInput,
    ValidatorTaskCreateInput,
)
from dojo import get_commit_hash, get_latest_git_tag
from dojo.protocol import (
    CompletionResponse,
    CriteriaType,
    ScoreCriteria,
    TaskSynapseObject,
)


class Metadata(BaseModel):
    git_tag: str
    commit_hash: str
    augment_type: str


# ---------------------------------------------------------------------------- #
#                 MAP PROTOCOL OBJECTS TO DATABASE MODEL INPUTS                #
# ---------------------------------------------------------------------------- #
def map_task_synapse_object_to_validator_task(
    synapse: TaskSynapseObject,
    qa_metadata: dict | None = None,
) -> ValidatorTaskCreateInput | None:
    """Maps a TaskSynapseObject to ValidatorTask database model input.

    Args:
        synapse (TaskSynapseObject): The task synapse object to map
        qa_metadata (dict | None): metadata from synthetic-API QA generation.
    Returns:
        ValidatorTaskCreateInput: The database model input
    """
    # Map ground truths if present
    ground_truths = (
        [
            GroundTruthCreateWithoutRelationsInput(
                obfuscated_model_id=model_id,
                real_model_id=model_id,
                rank_id=rank_id,
                ground_truth_score=float(rank_id),  # TODO: Add normalised gt score
            )
            for model_id, rank_id in synapse.ground_truth.items()
        ]
        if synapse.ground_truth
        else []
    )
    augment_type = qa_metadata["augment_type"] if qa_metadata else ""
    metadata = Metadata(
        git_tag=get_latest_git_tag() or "",
        commit_hash=get_commit_hash(),
        augment_type=augment_type,
    )

    return ValidatorTaskCreateInput(
        id=synapse.task_id,
        previous_task_id=synapse.previous_task_id,
        prompt=synapse.prompt,
        task_type=TaskTypeEnum(synapse.task_type),
        expire_at=iso8601_str_to_datetime(synapse.expire_at),
        is_processed=False,
        miner_responses={"create": []},
        ground_truth={"create": ground_truths},
        metadata=Json(json.dumps(metadata.model_dump())),
    )


def map_task_synapse_object_to_completions(
    synapse: TaskSynapseObject, validator_task_id: str
) -> list[CompletionCreateInput]:
    """Maps completion responses to database model inputs"""

    if not synapse.completion_responses:
        raise ValueError("Completion responses are required")

    # Map completions and their associated criteria
    completions: list[CompletionCreateInput] = []
    for resp in synapse.completion_responses:
        completion = CompletionCreateInput(
            completion_id=resp.completion_id,
            validator_task_id=synapse.task_id,
            model=resp.model,
            completion=Json(json.dumps(resp.completion, default=vars)),
        )

        # Add criteria for each completion if they exist
        if hasattr(resp, "criteria_types") and resp.criteria_types:
            criteria = [
                CriterionCreateWithoutRelationsInput(
                    criteria_type=_map_criteria_type_to_enum(criterion),
                    config=Json(json.dumps(_get_criteria_config(criterion))),
                )
                for criterion in resp.criteria_types
            ]
            completion["criterion"] = {"create": criteria}

        completions.append(completion)

    return completions


def _map_criteria_type_to_enum(criteria: CriteriaType) -> CriteriaTypeEnum:
    """Helper function to map CriteriaType to CriteriaTypeEnum."""
    if isinstance(criteria, ScoreCriteria):
        return CriteriaTypeEnum.SCORE


def _get_criteria_config(criteria: CriteriaType) -> dict:
    """Helper function to extract configuration from criteria."""
    config = {}

    if isinstance(criteria, ScoreCriteria):
        config["min"] = criteria.min
        config["max"] = criteria.max

    return config


def map_task_synapse_object_to_miner_response(
    synapse: TaskSynapseObject,
    validator_task_id: str,
) -> MinerResponseCreateInput:
    """Maps a TaskSynapseObject to MinerResponse database model input.

    Args:
        synapse (TaskSynapseObject): The task synapse object to map
        validator_task_id (str): The ID of the parent validator task

    Returns:
        MinerResponseCreateInput: The database model input
    """
    if not synapse.miner_hotkey or not synapse.miner_coldkey:
        raise ValueError("Miner hotkey and coldkey are required")

    if not synapse.dojo_task_id:
        raise ValueError("Dojo task ID is required")

    return MinerResponseCreateInput(
        validator_task_id=validator_task_id,
        dojo_task_id=synapse.dojo_task_id,
        hotkey=synapse.miner_hotkey,
        coldkey=synapse.miner_coldkey,
        task_result=Json(json.dumps({})),
    )


# ---------------------------------------------------------------------------- #
#               MAPPING DATABASE OBJECTS TO OUR PROTOCOL OBJECTS               #
# ---------------------------------------------------------------------------- #
def map_validator_task_to_task_synapse_object(
    model: ValidatorTask,
) -> TaskSynapseObject:
    """Maps a ValidatorTask database model to TaskSynapseObject.

    Args:
        model (ValidatorTask): The database model to map

    Returns:
        TaskSynapseObject: The protocol object
    """
    # Map completion responses
    completion_responses = []
    for completion in model.completions or []:
        # Map criteria types for each completion
        criteria_types = []
        for criterion in completion.criterion or []:
            config = json.loads(criterion.config)
            if criterion.criteria_type == CriteriaTypeEnum.SCORE:
                criteria_types.append(
                    ScoreCriteria(
                        min=config.get("min", 0.0),
                        max=config.get("max", 0.0),
                    )
                )

        completion_responses.append(
            CompletionResponse(
                model=completion.model,
                completion=json.loads(completion.completion),
                completion_id=completion.completion_id,
                criteria_types=criteria_types,
            )
        )

    # Map ground truth if present
    ground_truth = {}
    if model.ground_truth:
        for gt in model.ground_truth:
            ground_truth[gt.obfuscated_model_id] = gt.rank_id

    # # Map miner info if present
    # miner_hotkey = None
    # miner_coldkey = None
    # dojo_task_id = None
    # if model.miner_responses and len(model.miner_responses) > 0:
    #     miner = model.miner_responses[0]  # Take first miner response
    #     miner_hotkey = miner.hotkey
    #     miner_coldkey = miner.coldkey
    #     dojo_task_id = miner.dojo_task_id

    return TaskSynapseObject(
        task_id=model.id,
        previous_task_id=model.previous_task_id,
        prompt=model.prompt,
        task_type=model.task_type,
        expire_at=datetime_to_iso8601_str(model.expire_at),
        completion_responses=completion_responses,
        ground_truth=ground_truth,
        miner_hotkey=None,
        miner_coldkey=None,
        dojo_task_id=None,
    )


def map_miner_response_to_task_synapse_object(
    miner_response: MinerResponse,
    validator_task: ValidatorTask,
) -> TaskSynapseObject:
    """Maps a MinerResponse database model to TaskSynapseObject.

    Args:
        miner_response (MinerResponse): The miner response database model to map
        validator_task (ValidatorTask): The validator task containing completions and criteria

    Returns:
        TaskSynapseObject: The protocol object
    """
    completion_responses = []
    for completion in validator_task.completions or []:
        # Map criteria types for each completion
        criteria_types = []
        score = None

        for criterion in completion.criterion or []:
            config = json.loads(criterion.config)
            # Find the corresponding score for this criterion from miner_response
            for miner_score in criterion.scores or []:
                if miner_score.miner_response_id == miner_response.id:
                    score_data = json.loads(miner_score.scores)
                    score = score_data.get("raw_score")
                    break

            if criterion.criteria_type == CriteriaTypeEnum.SCORE:
                criteria_types.append(
                    ScoreCriteria(
                        min=config.get("min", 0.0),
                        max=config.get("max", 0.0),
                    )
                )

        completion_responses.append(
            CompletionResponse(
                model=completion.model,
                completion=json.loads(completion.completion),
                completion_id=completion.completion_id,
                criteria_types=criteria_types,
                score=score,
            )
        )

    return TaskSynapseObject(
        task_id=miner_response.validator_task_id,
        previous_task_id=validator_task.previous_task_id,
        prompt=validator_task.prompt,
        task_type=validator_task.task_type,
        expire_at=datetime_to_iso8601_str(validator_task.expire_at),
        completion_responses=completion_responses,
        ground_truth=None,
        miner_hotkey=miner_response.hotkey,
        miner_coldkey=miner_response.coldkey,
        dojo_task_id=miner_response.dojo_task_id,
        axon=bt.TerminalInfo(hotkey=miner_response.hotkey),
    )
