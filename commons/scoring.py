from typing import Dict, List

import numpy as np
import torch
from bittensor.utils.btlogging import logging as logger
from torch.nn import functional as F

from commons.utils import _terminal_plot
from dojo.protocol import (
    CodeAnswer,
    CompletionResponse,
    CriteriaType,
    MultiScoreCriteria,
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


def _get_miner_response_by_criteria(criteria, response: CompletionResponse):
    if isinstance(criteria, ScoreCriteria):
        return response.score


def minmax_scale(tensor: torch.Tensor | np.ndarray) -> torch.Tensor:
    if isinstance(tensor, np.ndarray):
        tensor = torch.from_numpy(tensor)
    min = tensor.min()
    max = tensor.max()
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
    ):
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
        ground_truth_arr = Scoring._convert_ground_truth_ranks_to_scores(
            cid_with_rank_sorted
        )

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

    # ---------------------------------------------------------------------------- #
    #                           SCORING CORE FUNCTIONS                             #
    # ---------------------------------------------------------------------------- #
    @classmethod
    def calculate_score(
        cls,
        validator_task: TaskSynapseObject,
        miner_responses: List[TaskSynapseObject],
    ) -> Dict[str, Scores]:
        """Calculates scores for miners.

        Args:
            validator_task: Task object containing ground truth and completion responses
            miner_responses: List of miner response objects

        Returns:
            Dictionary mapping miner hotkeys to their calculated scores
        """
        hotkey_to_scores: dict[str, Scores] = {}
        # Initialize empty scores for all miners
        for response in miner_responses:
            hotkey_to_scores[response.axon.hotkey] = Scores()

        # validation
        if (
            not validator_task.completion_responses
            or not validator_task.completion_responses[0].criteria_types
        ):
            logger.error("No criteria types found in completion responses")
            return hotkey_to_scores

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
                if all(
                    _get_miner_response_by_criteria(criteria, completion) is not None
                    for completion in response.completion_responses
                )
            ]

            if not valid_miner_responses:
                logger.info(f"📝 No valid responses for {validator_task.task_id}")
                return hotkey_to_scores

            logger.info(
                f"📝 Filtered {len(valid_miner_responses)} valid responses for task id {validator_task.task_id}"
            )

            try:
                hotkey_to_scores = cls._assign_scores(
                    criteria,
                    valid_miner_responses,
                    validator_task.ground_truth,
                    hotkey_to_scores,
                )
            except NotImplementedError:
                logger.warning(
                    f"Scoring not implemented for criteria type: {type(criteria)}"
                )
                continue

        return hotkey_to_scores

    # ---------------------------------------------------------------------------- #
    #                           SCORING HELPER FUNCTIONS                           #
    # ---------------------------------------------------------------------------- #
    @classmethod
    def _assign_scores(
        cls,
        criteria: CriteriaType,
        valid_responses: List[TaskSynapseObject],
        ground_truth: Dict[str, int],
        score_dict: Dict[str, Scores],
    ) -> Dict[str, Scores]:
        """Calculates and assigns scores based on criteria type."""
        if isinstance(criteria, ScoreCriteria):
            gt_scores = cls.ground_truth_scoring(
                criteria, ground_truth, valid_responses
            )
            for i, response in enumerate(valid_responses):
                scores = score_dict.get(response.axon.hotkey, Scores())
                scores.ground_truth_score = float(gt_scores[i])
                score_dict[response.axon.hotkey] = scores
            return score_dict
        else:
            raise NotImplementedError("Only score criteria is supported")


def _test_ground_truth_score_v1():
    gt = {
        "a": 0,
        "b": 1,
        "c": 2,
        "d": 3,
    }

    a = CompletionResponse(
        model="a", completion=CodeAnswer(files=[]), completion_id="a"
    )
    b = CompletionResponse(
        model="b", completion=CodeAnswer(files=[]), completion_id="b"
    )
    c = CompletionResponse(
        model="c", completion=CodeAnswer(files=[]), completion_id="c"
    )
    d = CompletionResponse(
        model="d", completion=CodeAnswer(files=[]), completion_id="d"
    )

    criteria = MultiScoreCriteria(options=["a", "b", "c", "d"], min=0, max=100)

    miner_responses = [
        TaskSynapseObject(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponse(
                    model="a", completion=a.completion, completion_id="a", score=56
                ),
                CompletionResponse(
                    model="b", completion=b.completion, completion_id="b", score=78
                ),
                CompletionResponse(
                    model="c", completion=c.completion, completion_id="c", score=89
                ),
                CompletionResponse(
                    model="d", completion=d.completion, completion_id="d", score=100
                ),
            ],
        ),
        TaskSynapseObject(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponse(
                    model="a", completion=a.completion, completion_id="a", score=12
                ),
                CompletionResponse(
                    model="b", completion=b.completion, completion_id="b", score=12
                ),
                CompletionResponse(
                    model="c", completion=c.completion, completion_id="c", score=12
                ),
                CompletionResponse(
                    model="d", completion=d.completion, completion_id="d", score=12
                ),
            ],
        ),
        TaskSynapseObject(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponse(
                    model="a", completion=a.completion, completion_id="a", score=12
                ),
                CompletionResponse(
                    model="b", completion=b.completion, completion_id="b", score=23
                ),
                CompletionResponse(
                    model="c", completion=c.completion, completion_id="c", score=12
                ),
                CompletionResponse(
                    model="d", completion=d.completion, completion_id="d", score=12
                ),
            ],
        ),
        TaskSynapseObject(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponse(
                    model="a", completion=a.completion, completion_id="a", score=56
                ),
                CompletionResponse(
                    model="b", completion=b.completion, completion_id="b", score=34
                ),
                CompletionResponse(
                    model="c", completion=c.completion, completion_id="c", score=23
                ),
                CompletionResponse(
                    model="d", completion=d.completion, completion_id="d", score=23
                ),
            ],
        ),
        TaskSynapseObject(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponse(
                    model="a", completion=a.completion, completion_id="a", score=23
                ),
                CompletionResponse(
                    model="b", completion=b.completion, completion_id="b", score=23
                ),
                CompletionResponse(
                    model="c", completion=c.completion, completion_id="c", score=34
                ),
                CompletionResponse(
                    model="d", completion=d.completion, completion_id="d", score=12
                ),
            ],
        ),
        TaskSynapseObject(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponse(
                    model="a", completion=a.completion, completion_id="a", score=23
                ),
                CompletionResponse(
                    model="b", completion=b.completion, completion_id="b", score=23
                ),
                CompletionResponse(
                    model="c", completion=c.completion, completion_id="c", score=76
                ),
                CompletionResponse(
                    model="d", completion=d.completion, completion_id="d", score=100
                ),
            ],
        ),
        TaskSynapseObject(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponse(
                    model="a", completion=a.completion, completion_id="a", score=1
                ),
                CompletionResponse(
                    model="b", completion=b.completion, completion_id="b", score=1
                ),
                CompletionResponse(
                    model="c", completion=c.completion, completion_id="c", score=76
                ),
                CompletionResponse(
                    model="d", completion=d.completion, completion_id="d", score=100
                ),
            ],
        ),
        TaskSynapseObject(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponse(
                    model="a", completion=a.completion, completion_id="a", score=1
                ),
                CompletionResponse(
                    model="b", completion=b.completion, completion_id="b", score=12
                ),
                CompletionResponse(
                    model="c", completion=c.completion, completion_id="c", score=23
                ),
                CompletionResponse(
                    model="d", completion=d.completion, completion_id="d", score=34
                ),
            ],
        ),
        TaskSynapseObject(
            prompt="test_prompt",
            task_type="test_task",
            criteria_types=[criteria],
            expire_at="",
            completion_responses=[
                CompletionResponse(
                    model="a", completion=a.completion, completion_id="a", score=1
                ),
                CompletionResponse(
                    model="b", completion=b.completion, completion_id="b", score=23
                ),
                CompletionResponse(
                    model="c", completion=c.completion, completion_id="c", score=76
                ),
                CompletionResponse(
                    model="d", completion=d.completion, completion_id="d", score=100
                ),
            ],
        ),
    ]

    import matplotlib.pyplot as plt

    scores = Scoring.ground_truth_scoring(criteria, gt, miner_responses)
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
