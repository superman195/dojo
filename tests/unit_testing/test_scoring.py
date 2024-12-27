import numpy as np
import pytest
import torch

from commons.scoring import Scoring
from commons.utils import set_expire_time
from dojo.protocol import (
    CodeAnswer,
    CompletionResponse,
    FeedbackRequest,
    MultiScoreCriteria,
    ScoreCriteria,
    TaskSynapseObject,
    TaskTypeEnum,
)


@pytest.fixture
def scoring_module():
    # ensure we import them depending on mock_env_var so the ValueError doesn't
    # get raised
    from commons.scoring import Scoring
    from dojo.protocol import (
        CodeAnswer,
        TaskTypeEnum,
    )

    return (
        Scoring,
        FeedbackRequest,
        CodeAnswer,
        MultiScoreCriteria,
        CompletionResponse,
        TaskTypeEnum,
    )


def mock_response(
    model: str,
    filename: str,
    content: str,
    language: str,
    score: float | None = 0.0,
    cid: str = "",
    rank_id: int = 0,
):
    from dojo.protocol import CodeFileObject

    return CompletionResponse(
        model=model,
        completion=CodeAnswer(
            files=[
                CodeFileObject(filename=filename, content=content, language=language)
            ]
        ),
        score=score,
        rank_id=rank_id,
        completion_id=cid,
    )


def create_mock_miner_response(
    hotkey: str | None = None,
    coldkey: str | None = None,
    scores: list[float] | None = None,
):
    prompt = "Write a hello world program in python"
    model_names = [
        "cid-1",
        "cid-2",
        "cid-3",
        "cid-4",
    ]

    responses = [
        mock_response(
            model=model,
            score=score,
            filename="hello_world.py",
            content="print('hello, world!')",
            language="python",
        )
        for model, score in zip(
            model_names, scores if scores is not None else [None] * len(model_names)
        )
    ]

    # Include the ground truth in the request object if provided
    return TaskSynapseObject(
        prompt=prompt,
        task_type=TaskTypeEnum.CODE_GENERATION,
        expire_at=set_expire_time(6 * 3600),  # 6 hours
        completion_responses=responses,
        miner_hotkey=hotkey,
        miner_coldkey=coldkey,
    )


def test_single_miner_responded(disable_terminal_plot):
    mock_miner_response = create_mock_miner_response(
        hotkey="hotkeyA", scores=[75, 100, 50, 69]
    )
    mock_ground_truth = {
        "cid-1": 0,
        "cid-2": 1,
        "cid-3": 2,
        "cid-4": 3,
    }

    scores = Scoring.ground_truth_scoring(
        criteria=ScoreCriteria(min=0.0, max=100.0),
        ground_truth=mock_ground_truth,
        miner_responses=[mock_miner_response],
    )

    assert isinstance(scores, torch.Tensor), "Scores should be a torch.Tensor"
    assert len(scores.shape) == 1, "Scores should be a 1D tensor"
    assert scores.shape == torch.Size(
        [1]
    ), "Scores tensor shape should be [1] for a single miner's response"
    assert not torch.isnan(scores).any(), "Scores array contains NaN values"


def test_single_miner_responded_all_same_scores(disable_terminal_plot):
    mock_miner_response = create_mock_miner_response(
        hotkey="hotkeyA", scores=[50, 50, 50, 50]
    )
    mock_ground_truth = {
        "cid-1": 0,
        "cid-2": 1,
        "cid-3": 2,
        "cid-4": 3,
    }

    scores = Scoring.ground_truth_scoring(
        criteria=ScoreCriteria(min=0.0, max=100.0),
        ground_truth=mock_ground_truth,
        miner_responses=[mock_miner_response],
    )

    assert isinstance(scores, torch.Tensor), "Scores should be a torch.Tensor"
    assert len(scores.shape) == 1, "Scores should be a 1D tensor"
    assert scores.shape == torch.Size(
        [1]
    ), "Scores tensor shape should be [1] for a single miner's response"
    assert torch.isnan(scores).all(), "Scores array should contain only NaN values"


def test_miners_provides_all_same_scores(disable_terminal_plot):
    mock_miner_responses = []
    for score in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        mock_response = create_mock_miner_response(
            hotkey=f"hotkey{chr(65 + score//10 - 1)}",
            scores=[score, score, score, score],
        )
        mock_miner_responses.append(mock_response)

    mock_ground_truth = {
        "cid-1": 0,
        "cid-2": 1,
        "cid-3": 2,
        "cid-4": 3,
    }

    scores = Scoring.ground_truth_scoring(
        criteria=ScoreCriteria(min=0.0, max=100.0),
        ground_truth=mock_ground_truth,
        miner_responses=mock_miner_responses,
    )

    assert isinstance(scores, torch.Tensor), "Scores should be a torch.Tensor"
    assert len(scores.shape) == 1, "Scores should be a 1D tensor"
    assert scores.shape == torch.Size(
        [len(mock_miner_responses)]
    ), "Scores tensor shape should match number of miners that responded"
    assert (
        torch.isnan(scores).all()
    ), "Scores array should contain only NaN values as miner responses are not valid"


def test_miners_different_scores():
    pass


def test_minmax_scale():
    pass


def test_convert_ground_truth_ranks_to_scores():
    cids_with_ranks = [("cid-1", 0), ("cid-2", 1), ("cid-3", 2), ("cid-4", 3)]

    ground_truth_scores = Scoring._convert_ground_truth_ranks_to_scores(cids_with_ranks)

    expected = np.array([1.0, 2 / 3, 1 / 3, 0.0])
    assert np.allclose(
        ground_truth_scores, expected, rtol=1e-6, atol=1e-6
    ), f"Ground truth scores should be in descending order, got {ground_truth_scores}"
    assert np.all(ground_truth_scores >= 0) and np.all(
        ground_truth_scores <= 1
    ), "All values should be in the range [0, 1]"


def test_convert_miner_outputs_to_scores_invalid():
    cids_with_ranks = [("cid-1", 0), ("cid-2", 2), ("cid-3", 1), ("cid-4", 3)]
    with pytest.raises(
        ValueError, match="Provided ranks must be sorted and must be continuous"
    ):
        Scoring._convert_ground_truth_ranks_to_scores(cids_with_ranks)
