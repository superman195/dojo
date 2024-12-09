from collections import defaultdict
from typing import Dict, List

import numpy as np
import pandas as pd
import pingouin as pg
import torch
from attr import define, field
from bittensor.btlogging import logging as logger
from pydantic import BaseModel, Field
from scipy.stats import spearmanr
from torch.nn import functional as F

from commons.utils import _terminal_plot
from dojo.protocol import (
    CodeAnswer,
    CompletionResponses,
    CriteriaType,
    FeedbackRequest,
    MultiScoreCriteria,
    RankingCriteria,
)


@define(kw_only=True, frozen=True, slots=True)
class Result:
    # Each request id has multiple completions, where each miner scores each of these completions.
    request_id: str
    cid_to_hotkey_to_score: Dict[str, Dict[str, float]] = field(factory=dict)


class GroundTruthScore(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    score: torch.Tensor


class ConsensusScore(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    score: torch.Tensor
    mse_by_miner: torch.Tensor
    icc_by_miner: torch.Tensor


def _reward_cubic(
    miner_outputs: np.ndarray,
    ground_truth: np.ndarray,
    scaling: float,
    translation: float,
    offset: float,
    visualize: bool = False,
) -> np.ndarray:
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
    x = F.cosine_similarity(
        torch.from_numpy(miner_outputs.copy()),
        torch.from_numpy(ground_truth.copy()),
        dim=1,
    ).numpy()

    # Convert nans to -1 to send it to the bottom
    x = np.where(np.isnan(x), -1, x)

    # transform from range [-1, 1] to [0, 1]
    x = (x + 1) / 2
    logger.debug(f"scoring: cosine similarity shape: {x.shape}\n array: {x}")
    # ensure sum is 1
    x = F.normalize(torch.from_numpy(x), p=1, dim=0)
    assert x.shape[0] == miner_outputs.shape[0]

    # apply the cubic transformation
    points = scaling * (x - translation) ** 3 + offset
    logger.debug(
        f"scoring: cubic reward points shape: {points.shape}\n array: {points}"
    )

    # case where a miner provides the same score for all completions
    # convert any nans to zero
    points = np.where(np.isnan(points), 0, points)
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
    return points


def _reward_l1_norm(miner_outputs: np.ndarray, ground_truth: np.ndarray):
    return np.linalg.norm(miner_outputs - ground_truth, axis=1)


class Score(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    ground_truth: torch.Tensor = Field(description="Raw score from ground truth")
    consensus: ConsensusScore = Field(description="Raw score from ground truth")
    weighted_consensus: torch.Tensor | None = Field(
        default=None, description="Weighted score from consensus"
    )
    weighted_ground_truth: torch.Tensor | None = Field(
        default=None, description="Weighted score from ground truth"
    )


def _get_miner_response_by_criteria(criteria, response: CompletionResponses):
    if isinstance(criteria, RankingCriteria):
        return response.rank_id
    elif isinstance(criteria, MultiScoreCriteria):
        return response.score


def _get_ground_truth_by_criteria(criteria, model_with_score_sorted):
    gt = []
    if isinstance(criteria, RankingCriteria):
        gt = [i + 1 for i in range(len(model_with_score_sorted))]
    elif isinstance(criteria, MultiScoreCriteria):
        gt = [score for _, score in model_with_score_sorted]
    return np.array(gt)


def minmax_scale(tensor: torch.Tensor | np.ndarray) -> torch.Tensor:
    if isinstance(tensor, np.ndarray):
        tensor = torch.from_numpy(tensor)
    min = tensor.min()
    max = tensor.max()
    return (tensor - min) / (max - min)


class Scoring:
    @staticmethod
    def consensus_score(
        criteria: CriteriaType,
        request: FeedbackRequest,
        miner_responses: List[FeedbackRequest],
    ):
        """Given a list of responses, will only return a dict of hotkey to their normalized scores.
        e.g. if a miner failed to respond, its hotkey won't be a key in the dict.
        """

        # depending on the criteria, this may be average ranks or average scores
        if not len(miner_responses):
            raise ValueError("Responses cannot be empty")

        # shape (num completions)
        avg = None
        # shape (num miners, num completions)
        miner_outputs = None
        icc_arr = []

        # for ordering based on criteria
        model_id_to_avg_rank = defaultdict(list)
        model_id_to_scores = defaultdict(list)

        if isinstance(criteria, RankingCriteria):
            logger.debug("consensus scoring for ranking criteria")
            for response in miner_responses:
                for completion in response.completion_responses:
                    # if completion.model_id not in model_id_to_average_rank:
                    #     model_id_to_average_rank[completion.model_id] = []
                    model_id_to_avg_rank[completion.model].append(completion.rank_id)

            for model_id, ranks in model_id_to_avg_rank.items():
                model_id_to_avg_rank[model_id] = sum(ranks) / len(ranks)

            model_id_to_avg_rank = dict(
                sorted(model_id_to_avg_rank.items(), key=lambda item: item[1])
            )

            # shape (num miners, num completions)
            # order ranks based on their order in the sorted dict
            miner_outputs = [
                [
                    _get_miner_response_by_criteria(criteria, x)
                    for x in sorted(
                        response.completion_responses,
                        key=lambda x: model_id_to_avg_rank[x.model],
                    )
                ]
                for response in miner_responses
            ]
            miner_outputs = np.array(miner_outputs)
            avg = np.array([i + 1 for i in range(len(model_id_to_avg_rank.keys()))])

        elif isinstance(criteria, MultiScoreCriteria):
            logger.debug("consensus scoring for multi-score criteria")
            # calculate average score per model
            for response in miner_responses:
                for completion in response.completion_responses:
                    model_id_to_scores[completion.model].append(completion.score)
            # for each model calculate the average score
            # USE DICT BECAUSE WE NEED TO ENSURE CORRECT ORDERING
            model_id_to_avg_score = {
                model: sum(scores) / len(scores)
                for model, scores in model_id_to_scores.items()
            }

            # shape (num miners, num completions)
            # collect all scores from each miner based on ordering in model_id_avg_score
            miner_outputs = np.array(
                [
                    [
                        completion.score
                        for completion in sorted(
                            response.completion_responses,
                            key=lambda x: model_id_to_avg_score[x.model],
                        )
                    ]
                    for response in miner_responses
                ]
            )

            avg: np.ndarray = np.array([v for k, v in model_id_to_avg_score.items()])

        else:
            raise NotImplementedError(
                f"Consensus score for type {criteria} not implemented yet"
            )

        if avg is None or miner_outputs is None:
            raise ValueError("avg and miner_outputs cannot be None")

        logger.info(f"Average across all miners: {avg}")
        logger.info(f"Miner outputs {miner_outputs}")
        logger.info(f"Model id to avg {model_id_to_avg_score}")

        # create df with the original number of completions
        df = pd.DataFrame(
            {
                "subject": [i for i in range(len(request.completion_responses))],
            }
        )
        # prepare dataframe for calculating ICC
        for response in miner_responses:
            rater_id = response.axon.hotkey
            ordered_scores = [
                x.score
                for x in sorted(
                    response.completion_responses,
                    key=lambda x: (
                        model_id_to_avg_score[x.model]
                        if criteria == MultiScoreCriteria
                        else model_id_to_avg_rank[x.model]
                    ),
                )
            ]
            # order scores based on order in model_id_to_avg_score
            df[rater_id] = ordered_scores
        rater_ids = list(df.columns)
        rater_ids.remove("subject")
        df["avg"] = df[rater_ids].mean(axis=1)

        # this works because we are calculating ICC for each rater VS the avg
        for rater_id in rater_ids:
            try:
                data_by_rater = df[["subject", rater_id, "avg"]]
                # only use the columns for the current rater and avg
                data_by_rater = data_by_rater.melt(
                    id_vars=["subject"], var_name=rater_id, value_name="score"
                )
                icc = pg.intraclass_corr(
                    data=data_by_rater,
                    targets="subject",
                    raters=rater_id,
                    ratings="score",
                )

                # take ICC(2,1)
                icc2_value = icc[icc["Type"] == "ICC2"]["ICC"].iloc[0]
                icc_arr.append(icc2_value)

            except Exception as e:
                logger.error(f"Error calculating ICC for rater {rater_id}: {e}")
                logger.debug(f"Data by rater: {data_by_rater}")
                continue

        # already in the range [0, 1]
        icc_arr: torch.Tensor = torch.tensor(np.array(icc_arr))

        # only use this for ordinal data
        # spearman = np.array(
        #     [
        #         spearmanr(miner_output, avg, nan_policy="propagate").statistic
        #         for miner_output in miner_outputs
        #     ]
        # )
        # num_nans = np.sum(np.isnan(spearman))

        mse = torch.tensor(np.mean(np.abs(miner_outputs - avg) ** 2, axis=1))
        logger.debug(f"MSE raw: {mse}")
        logger.info(f"ICC raw: {icc_arr}")

        mse_reward = F.softmax(-1 * mse, dim=0)

        if not np.isnan(icc_arr).any():
            return ConsensusScore(
                score=torch.tensor(icc_arr),
                mse_by_miner=mse_reward,
                icc_by_miner=icc_arr,
            )

        logger.warning("ICC array contains NaN values, using just MSE instead")

        # use negative sign to penalize higher mse

        # # edge case where all miners provide the same rating
        # if torch.all(mse_reward_norm == 0):
        #     logger.warning("MSE reward normalization resulted in all zeros.")
        #     reward_per_miner = 1 / len(miner_outputs)
        #     mse_reward_norm = torch.full_like(mse_reward_norm, reward_per_miner)

        logger.debug(f"MSE reward: {mse_reward}")
        logger.debug(f"MSE normalized: {mse_reward}")

        return ConsensusScore(
            score=mse_reward,
            mse_by_miner=mse_reward,
            icc_by_miner=icc_arr,
        )

    @staticmethod
    def ground_truth_score_V1(
        criteria: CriteriaType,
        ground_truth: dict[str, int],
        miner_responses: List[FeedbackRequest],
    ):
        """
        - Calculate score between all miner outputs and ground truth.
        - Ensures that the resulting tensor is normalized to sum to 1.

        Args:
            criteria (CriteriaType): Criteria type
            ground_truth (dict[str, int]): Ground truth, where key is completion id and value is rank.
            miner_responses (List[FeedbackRequest]): Miner responses

        Raises:
            ValueError: If miner responses are empty or contain None values.

        Returns:
            torch.Tensor: 1D tensor of scores, representing score for each miner based on order in miner_responses.
        """
        cid_rank_tuples = [
            (completion_id, rank) for completion_id, rank in ground_truth.items()
        ]
        logger.debug(f"scoring: cid rank tuples\n{cid_rank_tuples}")

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
                response.completion_responses,
                key=lambda r: cids_sorted.index(r.model),
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

        miner_outputs = miner_outputs_normalised

        # use minmax scale to ensure ground truth is in the range [0, 1]
        ground_truth_arr = minmax_scale(
            np.array([rank for _, rank in cid_with_rank_sorted])
        ).numpy()

        # reverse order here, because the lowest rank is the best
        # e.g. ranks: ('cid1', 0), ('cid2', 1), ('cid3', 2), ('cid4', 3)
        # after minmax scale: [0, 0.33, 0.667, 1]
        # but we want the reverse, so: [1, 0.667, 0.33, 0], since cid1 is the best
        ground_truth_arr = ground_truth_arr[::-1]

        logger.info(f"scoring: Miner outputs\n{miner_outputs}")
        logger.info(f"scoring: Ground truth\n{ground_truth_arr}")

        # l1_norm = np.linalg.norm(miner_outputs - ground_truth_arr, axis=1)
        # l1_norm = np.linalg.norm(miner_outputs - ground_truth_arr, axis=1)
        cubic_reward: np.ndarray = _reward_cubic(
            miner_outputs, ground_truth_arr, 0.006, 7, 2, visualize=True
        )
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

        return torch.from_numpy(cubic_reward.copy())

    @staticmethod
    def cmp_ground_truth(
        criteria: CriteriaType,
        request: FeedbackRequest,
        miner_responses: List[FeedbackRequest],
    ):
        # determine the ground truth ordering based on request
        # we can assume `model` is the same as the `completion_id`, see validator.obfuscate_model_names function
        model_score_tuples = _map_ground_truth_rank_to_score(
            criteria, request.ground_truth
        )
        model_with_score_sorted = sorted(
            model_score_tuples, key=lambda x: (x[1] is not None, x[1]), reverse=True
        )
        model_ids_sorted = [model[0] for model in model_with_score_sorted]

        # sort miner outputs according to ground truth order
        # this may be scores or ranks
        # log miner models to check
        miner_models = []
        for r in miner_responses:
            for completion in r.completion_responses:
                miner_models.append(completion.model)

        miner_outputs = []
        for response in miner_responses:
            curr_miner_outputs = []
            for completion in sorted(
                response.completion_responses,
                key=lambda r: model_ids_sorted.index(r.model),
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

        # this may be scores or ranks
        ground_truth = _get_ground_truth_by_criteria(criteria, model_with_score_sorted)

        logger.info(f"Miner outputs: {miner_outputs}")
        logger.info(f"Ground truth: {ground_truth}")

        diff_gt = torch.tensor(
            -1 * np.linalg.norm(miner_outputs - ground_truth, ord=2, axis=1)
        )
        logger.debug(f"{diff_gt=}")
        gt_reward = F.softmax(diff_gt, dim=0)
        logger.debug(f"{gt_reward=}")

        return torch.tensor(gt_reward)

    @staticmethod
    def spm_ground_truth(
        criteria: CriteriaType,
        request: FeedbackRequest,
        miner_responses: List[FeedbackRequest],
    ):
        """
        Calculate Spearman Correlation between miner outputs and ground truth using 'cid'.
        """

        gt_keys = list(request.ground_truth.keys())
        gt_values = list(request.ground_truth.values())

        # Gather miner outputs based on their responses
        miner_outputs = []
        for response in miner_responses:
            curr_miner_outputs = []
            for completion in sorted(
                response.completion_responses,
                key=lambda response: gt_keys.index(response.completion_id),
            ):
                curr_miner_outputs.append(
                    _get_miner_response_by_criteria(criteria, completion)
                )
            miner_outputs.append(curr_miner_outputs)

        # Convert miner outputs to numpy array for easier processing
        miner_outputs = np.array(miner_outputs)

        # Calculate Spearman correlation for each miner's output against the ground truth
        spearman_scores = [
            spearmanr(miner_output, gt_values).correlation
            for miner_output in miner_outputs
        ]

        # Convert the Spearman correlation scores into rewards
        spearman_scores = torch.tensor(
            np.nan_to_num(spearman_scores), dtype=torch.float32
        )  # Handle NaN values
        gt_reward = F.softmax(torch.tensor(spearman_scores, dtype=torch.float32), dim=0)

        return gt_reward

    @staticmethod
    def calculate_score(
        criteria_types: List[CriteriaType],
        request: FeedbackRequest,
        miner_responses: List[FeedbackRequest],
    ) -> tuple[dict[CriteriaType, Score], Dict[str, float]]:
        """Combines both consensus score and difference with ground truths scoring to output a final score per miner"""
        criteria_to_miner_scores = defaultdict(Score)
        hotkey_to_final_score: dict[str, float] = defaultdict(float)
        logger.trace(
            f"Calculating scores for miner responses ... {len(miner_responses)}"
        )
        for criteria in criteria_types:
            # valid responses
            valid_miner_responses = []
            for response in miner_responses:
                values = [
                    _get_miner_response_by_criteria(criteria, completion)
                    for completion in response.completion_responses
                ]
                if any(v is None for v in values):
                    continue
                valid_miner_responses.append(response)

            if not len(valid_miner_responses):
                logger.info(f"📝 No valid responses for {request.request_id}")

                for r in miner_responses:
                    hotkey_to_final_score[r.axon.hotkey] = 0.0  # type: ignore
                consensus_score = ConsensusScore(
                    score=torch.zeros(len(miner_responses)),
                    mse_by_miner=torch.zeros(len(miner_responses)),
                    icc_by_miner=torch.zeros(len(miner_responses)),
                )

                criteria_to_miner_scores[criteria.type] = Score(
                    ground_truth=torch.zeros(len(miner_responses)),
                    consensus=consensus_score,
                )
                return criteria_to_miner_scores, hotkey_to_final_score

            # if len(valid_miner_responses) < 2:
            #     logger.warning(
            #         f"Skipping scoring for request id: {request.request_id} as not enough valid responses"
            #     )
            #     for r in valid_miner_responses:
            #         hotkey_to_final_score[r.axon.hotkey] = 0.0

            #     continue

            # # if isinstance(criteria, RankingCriteria):
            # #     gt_score = Scoring.spm_ground_truth(
            # #         criteria, request, valid_miner_responses
            # #     )

            logger.info(
                f"📝 Filtered {len(valid_miner_responses)} valid responses for request id {request.request_id}"
            )

            if not isinstance(criteria, MultiScoreCriteria):
                raise NotImplementedError("Only multi-score criteria is supported atm")
            gt_score = Scoring.ground_truth_score_V1(
                criteria, request.ground_truth, valid_miner_responses
            )

            # TODO @dev add heuristics once scoring is stable
            # consensus_score = Scoring.consensus_score(
            #     criteria, request, valid_miner_responses
            # )

            # dummy for now
            consensus_score = ConsensusScore(
                score=torch.zeros(len(valid_miner_responses)),
                mse_by_miner=torch.zeros(len(valid_miner_responses)),
                icc_by_miner=torch.zeros(len(valid_miner_responses)),
            )

            for i, r in enumerate(valid_miner_responses):
                # consensus = 0.2 * consensus_score.score[i]
                ground_truth = gt_score[i]

                # NOTE: just use ground truth for now
                hotkey_to_final_score[r.axon.hotkey] = float(
                    ground_truth / len(criteria_types)
                )

            criteria_to_miner_scores[criteria.type] = Score(
                ground_truth=gt_score, consensus=consensus_score
            )
        return criteria_to_miner_scores, hotkey_to_final_score


def _map_ground_truth_rank_to_score(
    criteria: CriteriaType, ground_truth: dict[str, int]
) -> list[tuple[str, float]]:
    if not isinstance(criteria, MultiScoreCriteria):
        raise NotImplementedError("Only multi-score criteria is supported")

    completion_ids = list(ground_truth.keys())
    unique_ranks = set(ground_truth.values())
    expected_ranks = set(range(0, len(completion_ids)))

    assert (
        unique_ranks == expected_ranks
    ), f"Ground truth values must be discrete integers from 0 to {len(completion_ids) - 1}"

    def convert_rank_to_score(
        rank: int,
        min_rank: int,
        max_rank: int,
        min_score: int | float,
        max_score: int | float,
    ):
        # invert the rank because rank with lower number
        # i.e. rank 1 is best, rank 3 is worst (0-indexed)
        inverted_rank = max_rank - rank + min_rank
        return (
            inverted_rank / (max_rank - min_rank) * (max_score - min_score) + min_score
        )

    completion_id_score_tuples: list[tuple[str, float]] = []

    min_rank = min(list(unique_ranks))
    max_rank = max(list(unique_ranks))

    for completion_id, rank in list(ground_truth.items()):
        score = convert_rank_to_score(
            rank,
            min_rank,
            max_rank,
            criteria.min,
            criteria.max,
        )
        completion_id_score_tuples.append((completion_id, float(score)))

    return completion_id_score_tuples


def _test_ground_truth_score_v1():
    gt = {
        "a": 0,
        "b": 1,
        "c": 2,
        "d": 3,
    }

    a = CompletionResponses(
        model="a", completion=CodeAnswer(files=[]), completion_id="a"
    )
    b = CompletionResponses(
        model="b", completion=CodeAnswer(files=[]), completion_id="b"
    )
    c = CompletionResponses(
        model="c", completion=CodeAnswer(files=[]), completion_id="c"
    )
    d = CompletionResponses(
        model="d", completion=CodeAnswer(files=[]), completion_id="d"
    )

    criteria = MultiScoreCriteria(options=["a", "b", "c", "d"], min=0, max=100)

    miner_responses = [
        FeedbackRequest(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponses(
                    model="a", completion=a.completion, completion_id="a", score=56
                ),
                CompletionResponses(
                    model="b", completion=b.completion, completion_id="b", score=78
                ),
                CompletionResponses(
                    model="c", completion=c.completion, completion_id="c", score=89
                ),
                CompletionResponses(
                    model="d", completion=d.completion, completion_id="d", score=100
                ),
            ],
        ),
        FeedbackRequest(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponses(
                    model="a", completion=a.completion, completion_id="a", score=12
                ),
                CompletionResponses(
                    model="b", completion=b.completion, completion_id="b", score=12
                ),
                CompletionResponses(
                    model="c", completion=c.completion, completion_id="c", score=12
                ),
                CompletionResponses(
                    model="d", completion=d.completion, completion_id="d", score=12
                ),
            ],
        ),
        FeedbackRequest(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponses(
                    model="a", completion=a.completion, completion_id="a", score=12
                ),
                CompletionResponses(
                    model="b", completion=b.completion, completion_id="b", score=23
                ),
                CompletionResponses(
                    model="c", completion=c.completion, completion_id="c", score=12
                ),
                CompletionResponses(
                    model="d", completion=d.completion, completion_id="d", score=12
                ),
            ],
        ),
        FeedbackRequest(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponses(
                    model="a", completion=a.completion, completion_id="a", score=56
                ),
                CompletionResponses(
                    model="b", completion=b.completion, completion_id="b", score=34
                ),
                CompletionResponses(
                    model="c", completion=c.completion, completion_id="c", score=23
                ),
                CompletionResponses(
                    model="d", completion=d.completion, completion_id="d", score=23
                ),
            ],
        ),
        FeedbackRequest(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponses(
                    model="a", completion=a.completion, completion_id="a", score=23
                ),
                CompletionResponses(
                    model="b", completion=b.completion, completion_id="b", score=23
                ),
                CompletionResponses(
                    model="c", completion=c.completion, completion_id="c", score=34
                ),
                CompletionResponses(
                    model="d", completion=d.completion, completion_id="d", score=12
                ),
            ],
        ),
        FeedbackRequest(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponses(
                    model="a", completion=a.completion, completion_id="a", score=23
                ),
                CompletionResponses(
                    model="b", completion=b.completion, completion_id="b", score=23
                ),
                CompletionResponses(
                    model="c", completion=c.completion, completion_id="c", score=76
                ),
                CompletionResponses(
                    model="d", completion=d.completion, completion_id="d", score=100
                ),
            ],
        ),
        FeedbackRequest(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponses(
                    model="a", completion=a.completion, completion_id="a", score=1
                ),
                CompletionResponses(
                    model="b", completion=b.completion, completion_id="b", score=1
                ),
                CompletionResponses(
                    model="c", completion=c.completion, completion_id="c", score=76
                ),
                CompletionResponses(
                    model="d", completion=d.completion, completion_id="d", score=100
                ),
            ],
        ),
        FeedbackRequest(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponses(
                    model="a", completion=a.completion, completion_id="a", score=1
                ),
                CompletionResponses(
                    model="b", completion=b.completion, completion_id="b", score=12
                ),
                CompletionResponses(
                    model="c", completion=c.completion, completion_id="c", score=23
                ),
                CompletionResponses(
                    model="d", completion=d.completion, completion_id="d", score=34
                ),
            ],
        ),
        FeedbackRequest(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponses(
                    model="a", completion=a.completion, completion_id="a", score=1
                ),
                CompletionResponses(
                    model="b", completion=b.completion, completion_id="b", score=23
                ),
                CompletionResponses(
                    model="c", completion=c.completion, completion_id="c", score=76
                ),
                CompletionResponses(
                    model="d", completion=d.completion, completion_id="d", score=100
                ),
            ],
        ),
    ]

    import matplotlib.pyplot as plt

    scores = Scoring.ground_truth_score_V1(criteria, gt, miner_responses)
    scores, _ = torch.sort(scores, descending=False)
    # Check if the sum of scores is 1
    print(f"{scores=}")

    plt.figure(figsize=(10, 6))
    (line,) = plt.plot(range(len(scores)), scores, marker="o")
    plt.xlabel("Miner Response Index")
    plt.ylabel("Score")
    plt.title("Ground Truth Scores")

    annot = plt.gca().annotate(
        "",
        xy=(0, 0),
        xytext=(20, 20),
        textcoords="offset points",
        bbox=dict(boxstyle="round", fc="w"),
        arrowprops=dict(arrowstyle="->"),
    )
    annot.set_visible(False)

    def update_annot(line, ind):
        x, y = line.get_data()
        annot.xy = (x[ind["ind"][0]], y[ind["ind"][0]])
        text = f"{y[ind['ind'][0]]:.2f}"
        annot.set_text(text)
        annot.get_bbox_patch().set_alpha(0.4)

    def hover(event):
        vis = annot.get_visible()
        if event.inaxes == plt.gca():
            cont, ind = line.contains(event)
            if cont:
                update_annot(line, ind)
                annot.set_visible(True)
                plt.gcf().canvas.draw_idle()
            else:
                if vis:
                    annot.set_visible(False)
                    plt.gcf().canvas.draw_idle()

    plt.gcf().canvas.mpl_connect("motion_notify_event", hover)
    plt.show()


def _test_reward_cubic():
    miner_outputs = np.array(
        [
            [0.1, 0.2, 0.3, 0.4],
            [0.5, 0.6, 0.7, 0.8],
            [0.9, 0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6, 0.7],
            [0.8, 0.9, 0.1, 0.2],
            [0.3, 0.4, 0.5, 0.6],
            [0.7, 0.8, 0.9, 0.1],
            [0.2, 0.3, 0.4, 0.5],
            [0.6, 0.7, 0.8, 0.9],
            [np.nan, np.nan, np.nan, np.nan],
            [0.15, 0.25, 0.35, 0.45],
            [0.55, 0.65, 0.75, 0.85],
            [0.95, 0.05, 0.15, 0.25],
            [0.45, 0.55, 0.65, 0.75],
            [0.85, 0.95, 0.05, 0.15],
            [0.35, 0.45, 0.55, 0.65],
            [0.75, 0.85, 0.95, 0.05],
            [0.25, 0.35, 0.45, 0.55],
            [0.65, 0.75, 0.85, 0.95],
            [0.05, 0.15, 0.25, 0.35],
            [1.0, 0.0, 0.0, 1.0],
            [0.0, 1.0, 1.0, 0.0],
            [0.99, 0.01, 0.01, 0.99],
            [0.01, 0.99, 0.99, 0.01],
            [0.5, 0.5, 0.5, 0.5],
            [0.25, 0.75, 0.75, 0.25],
            [0.75, 0.25, 0.25, 0.75],
            [0.1, 0.9, 0.9, 0.1],
            [1, 0.6666667, 0.33333334, 0],
            [0.0, 0.0, 0.0, 0.0],
        ]
    )
    ground_truth = np.array([0, 0.33333334, 0.6666667, 1])
    scaling = 0.006
    translation = 7
    offset = 2

    expected_shape = (30,)
    result = _reward_cubic(miner_outputs, ground_truth, scaling, translation, offset)

    assert isinstance(result, np.ndarray), "Result should be a numpy array"
    assert (
        result.shape == expected_shape
    ), f"Expected shape {expected_shape}, but got {result.shape}"
    assert np.all(result >= 0) and np.all(
        result <= 1
    ), "All values should be in the range [0, 1]"

    # Visualize the result using _terminal_plot
    _terminal_plot("Cubic Reward Test Result", result, sort=False)

    print("test_reward_cubic passed.")


if __name__ == "__main__":
    # _test_ground_truth_score_v1()
    _test_reward_cubic()
