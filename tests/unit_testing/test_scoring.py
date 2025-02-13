import numpy as np
import pytest
import torch

from commons.scoring import Scoring
from commons.utils import set_expire_time
from dojo.protocol import (
    CodeAnswer,
    CompletionResponse,
    FeedbackRequest,
    ScoreCriteria,
    TaskSynapseObject,
    TaskTypeEnum,
)


@pytest.fixture
def scoring_module():
    # ensure we import them depending on mock_env_var so the ValueError doesn't
    # get raised
    from commons.scoring import Scoring
    from dojo.protocol import CodeAnswer, TaskTypeEnum

    return (
        Scoring,
        FeedbackRequest,
        CodeAnswer,
        ScoreCriteria,
        CompletionResponse,
        TaskTypeEnum,
    )


def mock_response(
    model: str,
    filename: str = "hello_world.py",
    content: str = "print('hello, world!')",
    language: str = "python",
    score: float = 0.0,
    completion_id: str = "",
    rank_id: int = 0,
) -> CompletionResponse:
    """Create a mock completion response for testing."""
    from dojo.protocol import CodeFileObject

    return CompletionResponse(
        model=model,
        completion=CodeAnswer(
            files=[
                CodeFileObject(filename=filename, content=content, language=language)
            ]
        ),
        completion_id=completion_id,
        rank_id=rank_id,
        score=score,
    )


def create_mock_miner_response(
    hotkey: str,
    scores: list[float] | None = None,
    completion_ids: list[str] | None = None,
    coldkey: str | None = None,
) -> TaskSynapseObject:
    """Create a mock miner response for testing."""
    prompt = "Write a hello world program in python"

    # Default completion IDs if none provided
    if completion_ids is None:
        completion_ids = [f"cid-{i+1}" for i in range(4)]

    # Default scores if none provided
    if scores is None:
        scores = [0.0] * len(completion_ids)

    responses = [
        mock_response(
            model=f"model-{i+1}",
            score=score,
            completion_id=cid,
            rank_id=i,
        )
        for i, (cid, score) in enumerate(zip(completion_ids, scores))
    ]

    return TaskSynapseObject(
        prompt=prompt,
        task_type=TaskTypeEnum.CODE_GENERATION,
        expire_at=set_expire_time(6 * 3600),  # 6 hours
        completion_responses=responses,
        miner_hotkey=hotkey,
        miner_coldkey=coldkey or "mock_coldkey",
    )


def test_single_miner_responded(disable_terminal_plot):
    """Test scoring with a single miner response."""
    completion_ids = ["cid-1", "cid-2", "cid-3", "cid-4"]
    scores = [75.0, 100.0, 50.0, 69.0]

    mock_miner_response = create_mock_miner_response(
        hotkey="hotkeyA", scores=scores, completion_ids=completion_ids
    )

    # Ground truth maps completion IDs to their ranks (0 is best)
    mock_ground_truth = {
        "cid-1": 0,  # Best response
        "cid-2": 1,
        "cid-3": 2,
        "cid-4": 3,  # Worst response
    }

    (
        final_scores,
        raw_outputs,
        normalized_outputs,
        cosine_scores,
        norm_cosine_scores,
        cubic_scores,
    ) = Scoring.ground_truth_scoring(
        criteria=ScoreCriteria(min=0.0, max=100.0),
        ground_truth=mock_ground_truth,
        miner_responses=[mock_miner_response],
    )

    # Check final scores shape and values
    assert not torch.isnan(final_scores).any(), "Final scores contain NaN values"
    assert torch.all(final_scores >= 0) and torch.all(
        final_scores <= 1
    ), "Final scores should be in range [0, 1]"

    # Check raw outputs shape and values
    assert raw_outputs.shape == (
        1,
        4,
    ), "Raw outputs shape should be [1, 4] for one miner and four responses"
    assert not np.isnan(raw_outputs).any(), "Raw outputs contain NaN values"
    np.testing.assert_array_equal(
        raw_outputs[0], scores, "Raw outputs should match input scores"
    )

    # Check normalized outputs shape and values
    assert normalized_outputs.shape == (
        1,
        4,
    ), "Normalized outputs shape should be [1, 4]"
    assert not np.isnan(
        normalized_outputs
    ).any(), "Normalized outputs contain NaN values"
    assert np.all(normalized_outputs >= 0) and np.all(
        normalized_outputs <= 1
    ), "Normalized outputs should be in range [0, 1]"

    # Check cosine similarity scores
    assert not np.isnan(cosine_scores).any(), "Cosine scores contain NaN values"
    assert np.all(cosine_scores >= -1) and np.all(
        cosine_scores <= 1
    ), "Cosine scores should be in range [-1, 1]"

    # Check normalized cosine similarity scores
    assert not torch.isnan(
        norm_cosine_scores
    ).any(), "Normalized cosine scores contain NaN values"
    assert torch.all(norm_cosine_scores >= 0) and torch.all(
        norm_cosine_scores <= 1
    ), "Normalized cosine scores should be in range [0, 1]"

    # Check cubic reward scores
    assert not torch.isnan(cubic_scores).any(), "Cubic scores contain NaN values"
    assert torch.all(cubic_scores >= 0) and torch.all(
        cubic_scores <= 1
    ), "Cubic scores should be in range [0, 1]"

    # Cubic scores should be ~0.704 (from _reward_cubic with alpha=0.006, beta=7, gamma=2)
    assert torch.allclose(
        cubic_scores, torch.tensor([0.7040], dtype=cubic_scores.dtype)
    ), "Cubic scores should be ~0.704 based on reward function parameters"


def test_single_miner_responded_all_same_scores(disable_terminal_plot):
    """Test scoring when a miner gives the same score to all responses."""
    completion_ids = ["cid-1", "cid-2", "cid-3", "cid-4"]
    scores = [50.0] * 4  # All same scores

    mock_miner_response = create_mock_miner_response(
        hotkey="hotkeyA", scores=scores, completion_ids=completion_ids
    )

    mock_ground_truth = {
        "cid-1": 0,  # Best response
        "cid-2": 1,
        "cid-3": 2,
        "cid-4": 3,  # Worst response
    }

    (
        final_scores,
        raw_outputs,
        normalized_outputs,
        cosine_scores,
        norm_cosine_scores,
        cubic_scores,
    ) = Scoring.ground_truth_scoring(
        criteria=ScoreCriteria(min=0.0, max=100.0),
        ground_truth=mock_ground_truth,
        miner_responses=[mock_miner_response],
    )

    # Check raw outputs match input
    np.testing.assert_array_equal(raw_outputs[0], scores)

    # When all scores are the same, normalized outputs should be all ones
    assert np.allclose(
        normalized_outputs, 1.0
    ), "Normalized outputs should be all ones when all scores are the same"

    # Cosine similarity should be ~0.8 with ground truth [1.0, 2/3, 1/3, 0.0]
    assert np.allclose(
        cosine_scores, 0.801783
    ), "Cosine scores should be ~0.8 when all scores are the same"

    # Normalized cosine similarity should be 1 when all scores are the same
    assert torch.allclose(
        norm_cosine_scores, torch.ones_like(norm_cosine_scores)
    ), "Normalized cosine scores should be ones"

    # Cubic scores should be ~0.704 (from _reward_cubic with alpha=0.006, beta=7, gamma=2)
    assert torch.allclose(
        cubic_scores, torch.tensor([0.7040], dtype=cubic_scores.dtype)
    ), "Cubic scores should be ~0.704 based on reward function parameters"

    # Final scores should sum to 1
    assert torch.allclose(
        final_scores.sum(), torch.tensor(1.0, dtype=final_scores.dtype)
    ), "Final scores should sum to 1"


def test_miners_provides_all_same_scores(disable_terminal_plot):
    """Test scoring when multiple miners give the same scores."""
    completion_ids = ["cid-1", "cid-2", "cid-3", "cid-4"]
    mock_miner_responses = []

    # Create 10 miners, each giving the same score to all responses
    for score in [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]:
        mock_response = create_mock_miner_response(
            hotkey=f"hotkey{chr(65 + int(score/10) - 1)}",
            scores=[score] * 4,  # Same score for all responses
            completion_ids=completion_ids,
        )
        mock_miner_responses.append(mock_response)

    mock_ground_truth = {
        "cid-1": 0,  # Best response
        "cid-2": 1,
        "cid-3": 2,
        "cid-4": 3,  # Worst response
    }

    (
        final_scores,
        raw_outputs,
        normalized_outputs,
        cosine_scores,
        norm_cosine_scores,
        cubic_scores,
    ) = Scoring.ground_truth_scoring(
        criteria=ScoreCriteria(min=0.0, max=100.0),
        ground_truth=mock_ground_truth,
        miner_responses=mock_miner_responses,
    )

    # When all scores for each miner are the same, check the behavior
    assert isinstance(
        final_scores, torch.Tensor
    ), "Final scores should be a torch.Tensor"
    assert len(final_scores.shape) == 1, "Final scores should be a 1D tensor"

    # Check raw outputs match input scores
    for i, score in enumerate(
        [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
    ):
        np.testing.assert_array_equal(raw_outputs[i], [score] * 4)

    # All miners should have same normalized outputs [1,1,1,1] after min-max scaling
    assert np.allclose(normalized_outputs, 1.0), "All normalized outputs should be 1.0"

    # All miners should have same cosine similarity with ground truth
    expected_cosine = (
        0.8017837  # cosine similarity between [1,1,1,1] and [1, 2/3, 1/3, 0]
    )
    assert np.allclose(
        cosine_scores, expected_cosine
    ), "All miners should have same cosine similarity"

    # All miners should have same normalized cosine similarity and sum to 1
    expected_norm_cosine = 1.0 / len(mock_miner_responses)  # Equal distribution
    assert torch.allclose(
        norm_cosine_scores,
        torch.tensor(
            [expected_norm_cosine] * len(mock_miner_responses),
            dtype=norm_cosine_scores.dtype,
        ),
    ), "Normalized cosine scores should be equal and sum to 1"

    # All miners should have same cubic scores
    # With 10 miners, each gets 0.1 of probability mass, which transforms to ~0.0289 through cubic function
    expected_cubic = 0.0289  # from _reward_cubic with normalized cosine score of 0.1
    assert torch.allclose(
        cubic_scores,
        torch.tensor(
            [expected_cubic] * len(mock_miner_responses), dtype=cubic_scores.dtype
        ),
        rtol=1e-2,  # Increased relative tolerance
        atol=1e-4,  # Added absolute tolerance
    ), "Cubic scores should be ~0.0289 when probability mass is equally distributed"

    # Final scores should be equal for all miners and sum to 1
    expected_score = 1.0 / len(mock_miner_responses)  # Equal distribution
    assert torch.allclose(
        final_scores,
        torch.tensor(
            [expected_score] * len(mock_miner_responses), dtype=final_scores.dtype
        ),
    )


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
