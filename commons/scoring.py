from typing import Dict, List

import numpy as np
import torch
from bittensor.utils.btlogging import logging as logger
from torch.nn import functional as F

from commons.exceptions import HFLStateNotContinuous
from commons.orm import ORM
from commons.utils import _terminal_plot
from database.mappers import _parse_hfl_events
from database.prisma.enums import HFLStatusEnum
from database.prisma.models import HFLState, ValidatorTask
from dojo.protocol import (
    CompletionResponse,
    CriteriaType,
    ScoreCriteria,
    Scores,
    TaskSynapseObject,
)


def _reward_cubic(
    miner_outputs: np.ndarray,
    ground_truth: np.ndarray,
    scaling: float,
    translation: float,
    offset: float,
    visualize: bool = False,
) -> tuple[np.ndarray, np.ndarray, torch.Tensor, torch.Tensor]:
    """Calculate cubic reward based on miner outputs and ground truth.

    Args:
        miner_outputs (np.ndarray): 2D array of miner outputs (shape: num_miners x num_completions).
        ground_truth (np.ndarray): 1D array of ground truth values (shape: num_completions).
        scaling (float): Scaling factor for the cubic function.
        translation (float): Translation factor for the cubic function.
        offset (float): Offset for the cubic function.

    Returns:
        np.ndarray: Transformed points based on the cubic function.
    """
    # ensure ground truth is a column vector for broadcasting
    # shape: (1, num_completions)
    ground_truth = ground_truth.reshape(1, -1)
    logger.debug(
        f"scoring: Reshaped ground truth shape: {ground_truth.shape}\n array: {ground_truth}"
    )

    # ensure dims for broadcasting
    assert len(ground_truth.shape) == 2
    assert len(miner_outputs.shape) == 2

    # shape: (num_miners,)
    # number range [-1, 1]
    cosine_similarity_scores = F.cosine_similarity(
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
    logger.debug(
        f"scoring: cosine similarity shape: {normalised_cosine_similarity_scores.shape}\n array: {normalised_cosine_similarity_scores}"
    )
    # ensure sum is 1
    normalised_cosine_similarity_scores = F.normalize(
        torch.from_numpy(normalised_cosine_similarity_scores), p=1, dim=0
    )
    assert normalised_cosine_similarity_scores.shape[0] == miner_outputs.shape[0]

    # apply the cubic transformation
    cubic_reward_scores = (
        scaling * (normalised_cosine_similarity_scores - translation) ** 3 + offset
    )
    logger.debug(
        f"scoring: cubic reward points shape: {cubic_reward_scores.shape}\n array: {cubic_reward_scores}"
    )

    # case where a miner provides the same score for all completions
    # convert any nans to zero
    points = np.where(np.isnan(cubic_reward_scores), 0, cubic_reward_scores)
    logger.debug(
        f"scoring: cubic reward no nans shape: {points.shape}\n array: {points}"
    )
    if visualize:
        _terminal_plot("scoring: cubic reward (raw)", points, sort=True)

    # ensure all values are in the range [0, 1]
    points = minmax_scale(points)
    logger.debug(
        f"scoring: cubic reward minmax scaled shape: {points.shape}\n array: {points}"
    )
    points = points.numpy()
    if visualize:
        _terminal_plot("scoring: cubic reward (minmax scaled)", points, sort=True)

    assert isinstance(points, np.ndarray)
    return (
        points,
        cosine_similarity_scores,
        normalised_cosine_similarity_scores,
        cubic_reward_scores,
    )


def _get_miner_response_by_criteria(criteria, response: CompletionResponse):
    if isinstance(criteria, ScoreCriteria):
        return response.score


def minmax_scale(tensor: torch.Tensor | np.ndarray) -> torch.Tensor:
    if isinstance(tensor, np.ndarray):
        tensor = torch.from_numpy(tensor)
    min = tensor.min()
    max = tensor.max()

    # If max == min, return a tensor of ones
    if max == min:
        return torch.ones_like(tensor)

    return (tensor - min) / (max - min)


class Scoring:
    @staticmethod
    def _convert_ground_truth_ranks_to_scores(
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

    @staticmethod
    def ground_truth_scoring(
        criteria: CriteriaType,
        ground_truth: dict[str, int],
        miner_responses: List[TaskSynapseObject],
    ) -> tuple[
        torch.Tensor,
        np.ndarray,
        np.ndarray,
        np.ndarray,
        torch.Tensor,
        torch.Tensor,
    ]:
        """
        - Calculate score between all miner outputs and ground truth.
        - Ensures that the resulting tensor is normalized to sum to 1.

        Args:
            criteria (CriteriaType): Criteria type
            ground_truth (dict[str, int]): Ground truth, where key is completion id and value is rank.
            miner_responses (List[TaskSynapseObject]): Miner responses

        Raises:
            ValueError: If miner responses are empty or contain None values.

        Returns:
            torch.Tensor: 1D tensor of scores, representing score for each miner based on order in miner_responses.
        """
        cid_rank_tuples = [
            (completion_id, rank) for completion_id, rank in ground_truth.items()
        ]
        logger.debug(f"scoring: cid rank tuples\n{cid_rank_tuples}")

        # Sort cids by rank. In the order, 0 is the best, 1 is the second best, etc.
        cid_with_rank_sorted = sorted(
            cid_rank_tuples, key=lambda x: x[1], reverse=False
        )
        logger.debug(f"scoring: cid with rank sorted\n{cid_with_rank_sorted}")

        # sort miner outputs according to ground truth order
        # we're using this because miners receive a shuffled order of the completions
        cids_sorted = [cid for cid, _ in cid_with_rank_sorted]
        miner_outputs = []
        for response in miner_responses:
            curr_miner_outputs = []
            for completion in sorted(
                response.completion_responses or [],
                key=lambda r: cids_sorted.index(r.completion_id),
            ):
                curr_miner_outputs.append(
                    _get_miner_response_by_criteria(criteria, completion)
                )
            miner_outputs.append(curr_miner_outputs)
        if miner_outputs == []:
            raise ValueError("Miner outputs cannot be empty")

        if None in miner_outputs:
            raise ValueError("Miner outputs cannot contain None values")

        miner_outputs = np.array(miner_outputs)
        logger.debug(f"scoring: raw miner outputs\n{miner_outputs}")
        # convert miner outputs to something ordinal
        miner_outputs_normalised = np.array([minmax_scale(m) for m in miner_outputs])
        logger.debug(
            f"scoring: raw miner outputs with nans\n{miner_outputs_normalised}"
        )

        # use minmax scale to ensure ground truth is in the range [0, 1]
        ground_truth_arr = Scoring._convert_ground_truth_ranks_to_scores(
            cid_with_rank_sorted
        )

        logger.info(f"scoring: Miner outputs normalised\n{miner_outputs_normalised}")
        logger.info(f"scoring: Ground truth\n{ground_truth_arr}")

        # l1_norm = np.linalg.norm(miner_outputs - ground_truth_arr, axis=1)
        # l1_norm = np.linalg.norm(miner_outputs - ground_truth_arr, axis=1)
        (
            cubic_reward,
            cosine_similarity_scores,
            normalised_cosine_similarity_scores,
            cubic_reward_scores,
        ) = _reward_cubic(miner_outputs, ground_truth_arr, 0.006, 7, 2, visualize=True)
        logger.debug(f"scoring: cubic reward\n{cubic_reward}")

        # normalize to ensure sum is 1
        cubic_reward = cubic_reward / np.sum(cubic_reward)

        logger.debug(f"scoring: cubic reward normalized (sum=1)\n{cubic_reward}")

        # calculate sum for each segment of the cubic reward
        try:
            # create a copy of cubic reward
            cubic_reward_copy = np.copy(cubic_reward)
            cubic_reward_copy.sort()
            segment_size = len(cubic_reward_copy) // 5
            segment_sums = [
                np.sum(cubic_reward_copy[i * segment_size : (i + 1) * segment_size])
                for i in range(5)
            ]
            logger.debug(f"scoring: segment sums\n{segment_sums}")
        except Exception as e:
            logger.debug(f"scoring: error calculating segment sums: {e}")
            pass

        return (
            torch.from_numpy(cubic_reward.copy()),
            miner_outputs,
            miner_outputs_normalised,
            cosine_similarity_scores,
            normalised_cosine_similarity_scores,
            cubic_reward_scores,
        )

    # ---------------------------------------------------------------------------- #
    #                           SCORING CORE FUNCTIONS                             #
    # ---------------------------------------------------------------------------- #
    @classmethod
    def calculate_score(
        cls,
        validator_task: TaskSynapseObject,
        miner_responses: List[TaskSynapseObject],
    ) -> List[TaskSynapseObject]:
        """Calculates scores for miners.

        Args:
            validator_task: Task object containing ground truth and completion responses
            miner_responses: List of miner response objects

        Returns:
            Dictionary mapping miner hotkeys to their calculated scores
        """
        updated_miner_responses = []

        # Early validation
        if not validator_task.completion_responses:
            logger.error("No completion responses in validator task")
            return updated_miner_responses

        if not validator_task.ground_truth:
            logger.error("No ground truth found in validator task")
            return updated_miner_responses

        # Use criteria types from the first completion response. This assumes that all completions have the same criteria types
        criteria_types = validator_task.completion_responses[0].criteria_types
        logger.trace(
            f"Calculating scores for miner responses ... {len(miner_responses)}"
        )

        for criteria in criteria_types:
            # valid responses
            valid_miner_responses = [
                response
                for response in miner_responses
                if response.completion_responses
                and all(
                    _get_miner_response_by_criteria(criteria, completion) is not None
                    for completion in response.completion_responses
                )
            ]

            if not valid_miner_responses:
                logger.info(f"📝 No valid responses for {validator_task.task_id}")
                return updated_miner_responses

            logger.info(
                f"📝 Filtered {len(valid_miner_responses)} valid responses for task id {validator_task.task_id}"
            )

            # Check if ground truth is None
            if validator_task.ground_truth is None:
                logger.error("No ground truth found in validator task")
                return updated_miner_responses

            try:
                updated_miner_responses = cls.score_by_criteria(
                    criteria,
                    valid_miner_responses,
                    validator_task.ground_truth,
                )
            except NotImplementedError:
                logger.warning(
                    f"Scoring not implemented for criteria type: {type(criteria)}"
                )
                continue

        return updated_miner_responses

    # ---------------------------------------------------------------------------- #
    #                           SCORING HELPER FUNCTIONS                           #
    # ---------------------------------------------------------------------------- #
    @classmethod
    def score_by_criteria(
        cls,
        criteria: CriteriaType,
        valid_responses: List[TaskSynapseObject],
        ground_truth: Dict[str, int],
    ) -> List[TaskSynapseObject]:
        """Calculates and assigns scores based on criteria type."""
        if not isinstance(criteria, ScoreCriteria):
            raise NotImplementedError("Only score criteria is supported")

        (
            gt_score,
            miner_outputs,
            miner_outputs_normalised,
            cosine_similarity_scores,
            normalised_cosine_similarity_scores,
            cubic_reward_scores,
        ) = cls.ground_truth_scoring(criteria, ground_truth, valid_responses)

        if miner_outputs_normalised.shape[0] == 1:
            miner_outputs_normalised = miner_outputs_normalised.T
            miner_outputs = miner_outputs.T

        for i, response in enumerate(valid_responses):
            if not response.axon or not response.axon.hotkey:
                continue

            for j, completion_response in enumerate(
                response.completion_responses or []
            ):
                scores = Scores(
                    raw_score=float(miner_outputs[i, j]),
                    ground_truth_score=float(gt_score[i]),
                    normalised_score=float(miner_outputs_normalised[i, j]),
                    cosine_similarity_score=float(cosine_similarity_scores[i]),
                    normalised_cosine_similarity_score=float(
                        normalised_cosine_similarity_scores[i]
                    ),
                    cubic_reward_score=float(cubic_reward_scores[i]),
                )

                for criteria in completion_response.criteria_types:
                    if isinstance(criteria, ScoreCriteria):
                        criteria.scores = scores

        return valid_responses


async def score_hfl_tasks():
    sf_tasks = await ORM.get_sf_tasks_by_status(status=HFLStatusEnum.SF_COMPLETED)

    for task in sf_tasks:
        await _score_tf_task(task)


# # TODO: skeleton for actual function
# async def __score_hfl_task(task: ValidatorTask):
#     validate()
#     _prep_for_sf_scores
#     score_sf_task
#     _prep_for_tf_scores
#     score_tf_task
#     return sf_score, tf_score
#
def validate_task(task: ValidatorTask, hfl_state: HFLState):
    if not hfl_state:
        raise Exception("HFL State not found for task")

    if not hfl_state.ValidatorTask or hfl_state.status == HFLStatusEnum.SF_COMPLETED:
        raise Exception("HFL State not ready for scoring")
    if not task.completions:
        raise Exception("Task completions not found")


async def _score_tf_task(task: ValidatorTask):
    """Use a completed SF_TASK to determine the TF_TASK score"""
    hfl_state = await ORM.get_hfl_state_by_current_task_id(task.id)
    try:
        validate_task(task, hfl_state)
    except Exception as e:
        logger.error(f"Error validating task: {e}")
        return

    ### CALC CHANGE IN SCORES
    miner_scores = await ORM.get_scores_for_completed_sf(task)
    logger.info(f"Got {miner_scores}")
    parent_task = await ORM.get_original_or_parent_sf_task(task.id)
    if parent_task is None:
        raise ValueError("Parent task not found")

    # Create a mapping of completion IDs to their order in task.completions
    completion_order: dict[str, int] = {
        comp.id: idx
        for idx, comp in enumerate(task.completions)  # type: ignore
    }
    parent_task_scores = await _calc_avg_score_by_completion_id(
        parent_task, completion_order
    )
    # NOTE: this is not for sf_scoring itself, but for comparing the increments
    sf_task_scores = await _calc_avg_score_by_completion_id(task, completion_order)

    # calculate differences per completion id
    cid_to_scores_dt: dict[str, float] = {}

    # TODO: select only the cid that was selected from original task / SF_1
    completion_id = "dummy"
    for completion_id, sf_score in sf_task_scores.items():
        # positive means improvement, negative means regression
        diff = sf_score - parent_task_scores[completion_id]
        cid_to_scores_dt[completion_id] = float(diff)

    ### CALC CHANGE IN SCORES

    #### FINDING MINER REPSONSES TO TF

    # TODO:figure out and score participants of TF
    # figure out the TF_task that led to SF_task
    tf_task_id = get_tf_task_id_for_sf_task(hfl_state, task)
    tf_task = await ORM.get_validator_task_by_id(tf_task_id)
    # find miners that responded to the tf task
    miner_responses = await ORM.get_miner_responses_by_task_id(tf_task_id)
    #### FINDING MINER REPSONSES TO TF
    #### FINDING MINER REPSONSES TO TF

    #### CALCULATE TF SCORES BASED ON SF
    # TODO: score miner responses for tf_task
    hotkey_to_tf_score: dict[str, float] = {}
    for response in miner_responses:
        hotkey_to_tf_score[response.hotkey] = 0.0

    # NOTE: generate SF score by simply calculating based on existing scoring

    #### CALCULATE TF SCORES BASED ON SF
    return hotkey_to_tf_score


def get_tf_task_id_for_sf_task(hfl_state: HFLState, task: ValidatorTask):
    events = _parse_hfl_events(hfl_state)
    status = HFLStatusEnum.TF_COMPLETED
    for idx, event in enumerate(events):
        if event.task_id != task.id:
            continue

        # found the SF_TASK, now find the tf task
        sublist = events[:idx]

        for event in reversed(sublist):
            if event.type == status:
                return event.task_id
    raise HFLStateNotContinuous(
        f"Expected to find {status} while processing {HFLStatusEnum.SF_COMPLETED}"
    )


async def _calc_avg_score_by_completion_id(
    task: ValidatorTask, completion_order: dict[str, int]
) -> dict[str, float]:
    if not task or not task.completions:
        raise ValueError("TF task must have completions for scoring")
    if not task.miner_responses:
        raise ValueError("TF task must have miner responses for scoring")

    # For each miner response, sort completions in-place based on task.completions order
    for miner_response in task.miner_responses:
        # TODO: ensure proper ordering between validator completions
        if not miner_response.scores:
            raise ValueError("Miner response must have scores for scoring")

        # Sort miner scores based on order from validator's completions
        def get_order(score) -> int:
            completion_id = score.criterion_relation.completion_id
            if completion_id not in completion_order:
                raise ValueError(
                    f"Completion ID {completion_id} not found in task completions"
                )
            return completion_order[completion_id]

        miner_response.scores.sort(key=get_order)

    # calculate the average score per completion
    # Create a dictionary to store the sum of scores and the count of scores for each completion
    completion_scores = {}

    for miner_response in task.miner_responses:
        for completion in miner_response.completion_responses:
            completion_id = completion.completion_id
            score = completion.score
            if completion_id not in completion_scores:
                completion_scores[completion_id] = {"sum": 0, "count": 0}

            completion_scores[completion_id]["sum"] += score
            completion_scores[completion_id]["count"] += 1

    # Calculate the average score for each completion
    prev_task_cid_to_avg_score: dict[str, float] = {}
    for completion_id, scores in completion_scores.items():
        average_score = scores["sum"] / scores["count"]
        prev_task_cid_to_avg_score[completion_id] = average_score
        print(f"Completion {completion_id}: Average score = {average_score}")

    return prev_task_cid_to_avg_score
