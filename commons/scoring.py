from typing import Dict, List

import numpy as np
import pandas as pd
import pingouin as pg
import torch
from bittensor.utils.btlogging import logging as logger
from torch.nn import functional as F

from commons.utils import _terminal_plot
from database.client import prisma
from database.prisma.models import ValidatorTask
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


# NOTE: bruh this doesn't need to be async...
async def score_hfl_task():
    """Score human feedback learning task by aggregating miner scores.

    Steps:
    1. Retrieve the SF (Scoring Function) task from the database
    2. Get all miner responses for the SF task
    3. Get all miner scores associated with those responses
    4. Order criteria based on completion IDs from SF task
    5. Create mapping of criterion IDs to indices and completion IDs
    6. Sort miner scores based on criteria ordering
    7. Parse scores into numpy array
    8. Build mapping of hotkeys to indices
    9. Create data list with target, rater and score information to calculate
    IRR metric, in this case ICC(2,1) - Intraclass Correlation Coefficient

    Returns:
        List[TaskSynapseObject]: List of task synapse objects with updated scores
    """
    # 1. get SF task
    sf_task: ValidatorTask = None  # type: ignore

    # 2. get all miner responses for the sf task
    # TODO: shift over to ORM.py
    miner_responses = await prisma.minerresponse.find_many(
        where={"validator_task_id": sf_task.id},
        include={"scores": True},
    )

    miner_scores = await prisma.minerscore.find_many(
        where={"miner_response_id": {"in": [m.id for m in miner_responses]}},
        include={"miner_response_relation": True},
    )

    # order based on completions in sf_task
    completion_ids = (
        [c.id for c in sf_task.completions] if sf_task and sf_task.completions else []
    )
    criteria = await prisma.criterion.find_many(
        where={"completion_id": {"in": completion_ids}},
    )
    # Create a mapping of criterion id to its index in the criteria list
    criteria_order = {criterion.id: idx for idx, criterion in enumerate(criteria)}
    criteria_completion_map = {
        criterion.id: criterion.completion_id for criterion in criteria
    }
    # Sort miner_scores based on the criteria ordering
    miner_scores.sort(key=lambda x: criteria_order[x.criterion_id])

    # 3. create numpy array of miner scores
    parsed_scores = []

    # maintain a mapping of hotkey to index
    hotkey_to_index = {}
    # Build a list of dictionaries for each rating.
    # Here, we assume that:
    # - Each rating has a 'criterion_id' that acts as the target.
    # - Each miner is identified by their hotkey.
    # - The score is parsed as before.
    data_list = []
    target_key = "target"
    rater_key = "rater"
    score_key = "score"
    for idx, miner_score in enumerate(miner_scores):
        hotkey_to_index[miner_score.miner_response_relation.hotkey] = idx  # type: ignore
        parsed_scores.append(Scores.model_validate(miner_score.scores).raw_score)
        data_list.append(
            {
                target_key: criteria_completion_map[miner_score.criterion_id],
                rater_key: miner_score.miner_response_relation.hotkey,  # type:ignore
                score_key: Scores.model_validate(miner_score.scores).raw_score,
            }
        )

    # Create the DataFrame
    df = pd.DataFrame(data_list)

    # Calculate ICC. The 'ratings' parameter tells Pingouin which column holds the scores.
    icc_results = pg.intraclass_corr(
        data=df, targets=target_key, raters=rater_key, ratings=score_key
    )

    # If you want only ICC(2,1), filter by its type:
    icc_2_1 = icc_results[icc_results["Type"] == "ICC2"]

    return icc_2_1


async def get_original_or_parent_sf_task(sf_task_id: str):
    """
    ┌─────────────┐       ┌──────┐       ┌──────┐      ┌──────┐     ┌──────┐
    │             │       │      │       │      │      │      │     │      │
    │Original Task│──────▶│ TF_1 │──────▶│ SF_1 │─────▶│ TF_2 │────▶│ SF_2 │
    │             │       │      │       │      │      │      │     │      │
    └─────────────┘       └──────┘       └──────┘      └──────┘     └──────┘
    """
    # TODO fetch tf task properly
    validator_task: ValidatorTask = get_original_or_parent_sf_task()  # type: ignore
    return validator_task


async def _calculate_prev_iter_scores(sf_task_id: str):
    tf_task = await get_original_or_parent_sf_task(sf_task_id=sf_task_id)
    if not tf_task or not tf_task.completions:
        raise ValueError("TF task must have completions for scoring")
    if not tf_task.miner_responses:
        raise ValueError("TF task must have miner responses for scoring")

    # Create a mapping of completion IDs to their order in tf_task.completions
    completion_order = {comp_id: idx for idx, comp_id in enumerate(tf_task.completions)}

    # For each miner response, sort completions in-place based on tf_task.completions order
    for miner_response in tf_task.miner_responses:
        # Sort completions based on the completion_order mapping
        miner_response.completion_responses.sort(
            key=lambda x: completion_order[x.completion_id]
        )

    # calculate the average score per completion
    # Create a dictionary to store the sum of scores and the count of scores for each completion
    completion_scores = {}

    for miner_response in tf_task.miner_responses:
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
