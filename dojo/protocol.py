from datetime import datetime
from typing import Dict, List

import bittensor as bt
from pydantic import BaseModel, ConfigDict, Field, model_validator
from strenum import StrEnum

from commons.utils import get_epoch_time, get_new_uuid


class TaskTypeEnum(StrEnum):
    TEXT_TO_THREE_D = "TEXT_TO_THREE_D"
    TEXT_TO_IMAGE = "TEXT_TO_IMAGE"
    CODE_GENERATION = "CODE_GENERATION"


class CriteriaTypeEnum(StrEnum):
    SCORE = "score"


class Scores(BaseModel):
    raw_score: float | None = Field(description="Raw score of the miner", default=None)
    rank_id: int | None = Field(description="Rank of the miner", default=None)
    normalised_score: float | None = Field(
        description="Normalised score of the miner", default=None
    )
    ground_truth_score: float | None = Field(
        description="Ground truth score of the miner", default=None
    )
    cosine_similarity_score: float | None = Field(
        description="Cosine similarity score of the miner", default=None
    )
    normalised_cosine_similarity_score: float | None = Field(
        description="Normalised cosine similarity score of the miner", default=None
    )
    cubic_reward_score: float | None = Field(
        description="Cubic reward score of the miner", default=None
    )


class ScoreCriteria(BaseModel):
    model_config = ConfigDict(frozen=False)

    type: str = Field(default=CriteriaTypeEnum.SCORE.value, frozen=True)
    min: float = Field(description="Minimum score for the task", frozen=True)
    max: float = Field(description="Maximum score for the task", frozen=True)
    scores: Scores | None = Field(description="Scores of the completion", default=None)


CriteriaType = ScoreCriteria


class CodeFileObject(BaseModel):
    filename: str = Field(description="Name of the file")
    content: str = Field(description="Content of the file which can be code or json")
    language: str = Field(description="Programming language of the file")


class CodeAnswer(BaseModel):
    files: List[CodeFileObject] = Field(description="List of FileObjects")


class MultimediaFileObject(BaseModel):
    filename: str = Field(description="Name of the file")
    content: bytes = Field(description="Binary content of the file")
    mime_type: str = Field(
        description="MIME type of the file (e.g., 'image/png', 'model/ply')"
    )


class MultimediaAnswer(BaseModel):
    files: List[MultimediaFileObject] = Field(description="List of multimedia files")


class CompletionResponse(BaseModel):
    model: str = Field(description="Model that generated the completion")
    completion: CodeAnswer | MultimediaAnswer | str | None = Field(
        description="Completion from the model"
    )
    completion_id: str = Field(description="Unique identifier for the completion")
    # TODO: Check if rank_id is needed
    rank_id: int | None = Field(
        description="Rank of the completion", examples=[1, 2, 3, 4], default=None
    )
    score: float | None = Field(description="Score of the completion", default=None)
    criteria_types: List[CriteriaType] = Field(
        description="Types of criteria for the task", default_factory=list
    )


class SyntheticQA(BaseModel):
    prompt: str
    responses: List[CompletionResponse]
    ground_truth: dict[str, int] = Field(
        description="Mapping of unique identifiers to their ground truth values",
        default_factory=dict,
    )
    metadata: dict = Field(description="Metadata of the task", default_factory=dict)

    @model_validator(mode="after")
    def verify_completion_ids(self):
        completion_ids = {resp.completion_id for resp in self.responses}
        ground_truth_keys = set(self.ground_truth.keys())

        if not completion_ids.issubset(ground_truth_keys):
            missing_ids = completion_ids - ground_truth_keys
            raise ValueError(
                f"The following completion_ids are missing from ground_truth: {missing_ids}"
            )

        if not ground_truth_keys.issubset(completion_ids):
            extra_keys = ground_truth_keys - completion_ids
            raise ValueError(
                f"The following keys in ground_truth do not correspond to any completion_id: {extra_keys}"
            )

        return self


class FeedbackRequest(bt.Synapse):
    epoch_timestamp: float = Field(
        default_factory=get_epoch_time,
        description="Epoch timestamp for the request",
    )
    request_id: str = Field(
        default_factory=get_new_uuid,
        description="Unique identifier for the request",
    )
    prompt: str = Field(
        description="Prompt or query from the user sent the LLM",
    )
    completion_responses: List[CompletionResponse] = Field(
        description="List of completions for the prompt",
    )
    task_type: str = Field(description="Type of task")
    criteria_types: List[CriteriaType] = Field(
        description="Types of criteria for the task",
    )
    # task id from miner
    dojo_task_id: str | None = Field(
        description="Dojo task ID for the request", default=None
    )
    expire_at: str = Field(
        description="Expired time for Dojo task which will be used by miner to create task"
    )
    ground_truth: dict[str, int] = Field(
        description="Mapping of unique identifiers to their ground truth values",
        default_factory=dict,
    )


class TaskSynapseObject(bt.Synapse):
    epoch_timestamp: float = Field(
        default_factory=get_epoch_time,
        description="Epoch timestamp for the task",
    )
    task_id: str = Field(
        default_factory=get_new_uuid,
        description="Unique identifier for the task",
    )
    previous_task_id: str | None = Field(
        description="ID of the previous task", default=None
    )
    prompt: str = Field(
        description="Prompt or query from the user sent to the LLM",
    )
    task_type: str = Field(description="Type of task")
    expire_at: str = Field(
        description="Expired time for task which will be used by miner to create dojo task"
    )
    completion_responses: List[CompletionResponse] | None = Field(
        description="List of completions for the task",
        default=None,
    )
    dojo_task_id: str | None = Field(
        description="Dojo task ID returned by miner", default=None
    )
    ground_truth: dict[str, int] | None = Field(
        description="Mapping of unique identifiers to their ground truth values",
        default=None,
    )
    miner_hotkey: str | None = Field(
        description="Hotkey of the miner that created the task", default=None
    )
    miner_coldkey: str | None = Field(
        description="Coldkey of the miner that created the task", default=None
    )


class ScoringResult(bt.Synapse):
    task_id: str = Field(
        description="Unique identifier for the request",
    )
    hotkey_to_completion_responses: Dict[str, List[CompletionResponse]] = Field(
        description="Hotkey to completion responses mapping",
        default_factory=dict,
    )


class Heartbeat(bt.Synapse):
    ack: bool = Field(description="Acknowledgement of the heartbeat", default=False)


# TODO rename this to be a Task or something
class DendriteQueryResponse(BaseModel):
    model_config = ConfigDict(frozen=False)
    validator_task: TaskSynapseObject
    miner_responses: List[TaskSynapseObject]


class Result(BaseModel):
    model: str = Field(description="Model that generated the result")
    criteria: list[dict] = Field(description="List of criteria with scores")


class TaskResult(BaseModel):
    id: str = Field(description="Task ID")
    created_at: datetime = Field(description="Creation timestamp")
    updated_at: datetime = Field(description="Last update timestamp")
    status: str = Field(description="Status of the task result")
    result_data: list[Result] = Field(description="List of Result data for the task")
    dojo_task_id: str = Field(description="ID of the associated dojo task")
    worker_id: str = Field(description="ID of the worker who completed the task")
    # Below not in used at the moment
    stake_amount: float | None = Field(description="Stake amount", default=None)
    potential_reward: float | None = Field(description="Potential reward", default=None)
    potential_loss: float | None = Field(description="Potential loss", default=None)
    finalised_reward: float | None = Field(description="Finalised reward", default=None)
    finalised_loss: float | None = Field(description="Finalised loss", default=None)


class TaskResultRequest(bt.Synapse):
    dojo_task_id: str = Field(description="The ID of the task to retrieve results for")
    task_results: list[TaskResult] = Field(
        description="List of TaskResult objects", default=[]
    )


class AnalyticsData(BaseModel):
    """
    defines the structure for analytics data that will be sent by validators to the analytics endpoint.
    """

    validator_task_id: str
    validator_hotkey: str
    prompt: str
    completions: List[dict]
    ground_truths: List[dict]
    scored_hotkeys: List[str]
    absent_hotkeys: List[str]
    miner_responses: List[dict]  # contains responses from all miners.
    created_at: str
    updated_at: str
    metadata: dict | None


class AnalyticsPayload(BaseModel):
    tasks: List[AnalyticsData]
