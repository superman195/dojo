import logging
import math
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import bittensor as bt
import numpy as np
import pytest
from loguru import logger

from commons.utils import get_epoch_time
from dojo import VALIDATOR_MIN_STAKE
from dojo.protocol import (
    CompletionResponse,
    ScoreCriteria,
    Scores,
    ScoringResult,
    TaskSynapseObject,
    TaskTypeEnum,
)
from neurons.miner import Miner

valid_task_synapse = TaskSynapseObject(
    prompt="test_prompt",
    dendrite=bt.TerminalInfo(hotkey="mock_hotkey"),
    task_type=TaskTypeEnum.CODE_GENERATION,
    completion_responses=[
        CompletionResponse(
            model="test_model",
            completion="test_completion",
            completion_id="test_uuid1234",
        )
    ],
    expire_at="2024-10-12T09:45:25Z",
)

invalid_task_synapse = TaskSynapseObject(
    prompt="test_prompt",
    task_type=TaskTypeEnum.CODE_GENERATION,
    completion_responses=[],  # Invalid because responses list is empty
    expire_at="2024-10-12T09:45:25Z",
)

miner_hotkey = "mock_miner_hotkey"
validator_hotkey = "mock_validator_hotkey"

MOCK_HOTKEYS: list[str] = [miner_hotkey, validator_hotkey]


# Fixture to propagate loguru logs to standard logging
@pytest.fixture(autouse=True)
def configure_loguru(caplog):
    class PropagateHandler(logging.Handler):
        def emit(self, record):
            logging.getLogger(record.name).handle(record)

    logger.add(PropagateHandler(), format="{message}")


@pytest.fixture
def mock_miner(mock_initialise):
    """Fixture to setup miner with mock components for testing."""
    mock_initialise_func, mock_wallet, mock_subtensor, mock_metagraph, mock_dendrite = (
        mock_initialise
    )

    with patch.object(Miner, "__init__", lambda x: None):
        miner = Miner()

        # Inject the mocked components
        miner.wallet = mock_wallet
        miner.subtensor = mock_subtensor
        miner.metagraph = mock_metagraph
        miner.metagraph.hotkeys = MOCK_HOTKEYS
        miner.metagraph.neurons = [
            MagicMock(
                stake=MagicMock(tao=float(VALIDATOR_MIN_STAKE - 5.0)),
                hotkey=miner_hotkey,
            ),
            MagicMock(
                stake=MagicMock(tao=float(VALIDATOR_MIN_STAKE + 5.0)),
                hotkey=validator_hotkey,
            ),
        ]
        miner.metagraph.total_stake = np.array(
            [
                float(VALIDATOR_MIN_STAKE - 5.0),  # this means miner stake
                float(VALIDATOR_MIN_STAKE + 5.0),  # this means validator stake
            ]
        )
        miner.dendrite = mock_dendrite
        miner.config = mock_wallet.config

        yield miner


@pytest.mark.asyncio
async def test_forward_result_valid(mock_miner: Miner):
    """Test that a valid miner's ScoringResult has expected return"""
    # Mock a valid scoring result
    mock_hotkey = str(mock_miner.wallet.hotkey.ss58_address)  # Convert hotkey to string
    synapse = ScoringResult(
        task_id="test_request_id",
        hotkey_to_completion_responses={
            mock_hotkey: [
                CompletionResponse(
                    model="test_model",
                    completion="test_completion",
                    completion_id="test_uuid1234",
                    criteria_types=[
                        ScoreCriteria(
                            type="score",
                            min=0.0,
                            max=100.0,
                            scores=Scores(
                                ground_truth_score=0.5,
                                cosine_similarity_score=0.5,
                                normalised_cosine_similarity_score=0.5,
                                cubic_reward_score=0.5,
                            ),
                        )
                    ],
                )
            ]
        },
    )

    response: ScoringResult = await mock_miner.forward_score_result(synapse)

    assert response.hotkey_to_completion_responses[mock_hotkey] == [
        CompletionResponse(
            model="test_model",
            completion="test_completion",
            completion_id="test_uuid1234",
            criteria_types=[
                ScoreCriteria(
                    type="score",
                    min=0.0,
                    max=100.0,
                    scores=Scores(
                        ground_truth_score=0.5,
                        cosine_similarity_score=0.5,
                        normalised_cosine_similarity_score=0.5,
                        cubic_reward_score=0.5,
                    ),
                )
            ],
        )
    ]


@pytest.mark.asyncio
async def test_forward_result_missing_hotkey_to_scores(mock_miner: Miner, caplog):
    """Test that a miner's ScoringResult is None or hotkey_to_scores attribute is missing or empty object"""
    synapse = None
    with caplog.at_level(logging.ERROR):
        response = await mock_miner.forward_score_result(synapse)
    assert response is None

    synapse = ScoringResult(
        task_id="test_request_id", hotkey_to_completion_responses={}
    )
    with caplog.at_level(logging.ERROR):
        response = await mock_miner.forward_score_result(synapse)

    assert response.hotkey_to_completion_responses == {}
    assert (
        "Invalid synapse object or missing hotkey_to_completion_responses attribute."
        in caplog.text
    )


@pytest.mark.asyncio
async def test_forward_result_hotkey_not_found(mock_miner: Miner, caplog):
    """Test that a miner's hotkey not found in hotkey_to_scores triggers an error."""
    synapse = ScoringResult(
        task_id="test_request_id",
        hotkey_to_completion_responses={
            "another_hotkey": [
                CompletionResponse(
                    model="test_model",
                    completion="test_completion",
                    completion_id="test_uuid1234",
                )
            ]
        },  # different hotkey
    )
    expected_log = f"Miner hotkey {mock_miner.wallet.hotkey.ss58_address} not found in scoring result but yet was sent the result"

    # Call the forward_result function
    with caplog.at_level(logging.ERROR):
        response = await mock_miner.forward_score_result(synapse)

    # Assert that the response is handled correctly
    assert response == synapse
    assert expected_log in caplog.text


@pytest.mark.asyncio
async def test_forward_feedback_request_invalid_synapse(mock_miner: Miner, caplog):
    """Test the case where the synapse or its properties are None."""
    synapse = invalid_task_synapse
    with caplog.at_level(logging.ERROR):
        _ = await mock_miner.forward_task_request(synapse)
        logger.info(caplog.text)
        assert "Invalid synapse: missing synapse or completion_responses" in caplog.text

    synapse = invalid_task_synapse
    synapse.completion_responses = [
        CompletionResponse(
            model="test_model",
            completion="test_completion",
            completion_id="test_uuid1234",
        )
    ]
    synapse.dendrite = None
    with caplog.at_level(logging.ERROR):
        _ = await mock_miner.forward_task_request(synapse)
        logger.info(caplog.text)
        assert "Invalid synapse: missing dendrite information" in caplog.text


@pytest.mark.asyncio
@patch("neurons.miner.DojoAPI.create_task", new_callable=AsyncMock)
async def test_forward_feedback_request_dojo_method(
    mock_create_task, mock_miner: Miner
):
    """Test the case where the scoring method is DOJO and the task creation is successful."""
    synapse = valid_task_synapse
    mock_miner.hotkey_to_request = {"mock_hotkey": synapse}
    mock_create_task.return_value = ["task_id"]

    response = await mock_miner.forward_task_request(synapse)

    assert response == synapse
    assert response.dojo_task_id == "task_id"
    mock_create_task.assert_called_once_with(synapse)


@pytest.mark.asyncio
async def test_miner_blacklisting_invalid_hotkey(mock_miner: Miner, caplog):
    """Test the case where miner blacklisting the unrecognized hotkey"""
    synapse = valid_task_synapse
    synapse.dendrite = bt.TerminalInfo(hotkey="unrecognized_hotkey")

    response = await mock_miner.blacklist_task_request(synapse)
    is_blacklist, msg = response

    assert is_blacklist is True
    assert msg == "Unrecognized hotkey"


@pytest.mark.asyncio
async def test_miner_blacklisting_miner_hotkey(mock_miner: Miner, caplog):
    """Test the case where the hotkey corresponds to a miner"""

    synapse = valid_task_synapse
    synapse.dendrite.hotkey = miner_hotkey

    response = await mock_miner.blacklist_task_request(synapse)
    is_blacklist, msg = response

    assert is_blacklist is True
    assert msg == "Not a validator"


@pytest.mark.asyncio
async def test_miner_blacklisting_is_not_a_validator(mock_miner: Miner, caplog):
    """Test the case where the validator has insufficient stake"""

    synapse = valid_task_synapse
    # we test with miner hotkey, which has insufficient stake
    synapse.dendrite.hotkey = miner_hotkey

    with caplog.at_level(logging.WARNING):
        response = await mock_miner.blacklist_task_request(synapse)
    is_miner, msg = response

    assert is_miner is True
    assert msg == "Not a validator"


@pytest.mark.asyncio
async def test_miner_blacklisting_insufficient_stake(mock_miner: Miner, caplog):
    """Test the case where the validator has insufficient stake"""

    synapse = valid_task_synapse
    synapse.dendrite.hotkey = validator_hotkey

    # we mock the stake of the validator to be insufficient
    caller_uid = mock_miner.metagraph.hotkeys.index(validator_hotkey)
    mock_miner.metagraph.neurons[caller_uid].stake.tao = float(
        VALIDATOR_MIN_STAKE - 5.0
    )

    with caplog.at_level(logging.WARNING):
        response = await mock_miner.blacklist_task_request(synapse)
    is_blacklisted, msg = response

    assert is_blacklisted is True
    assert msg == "Insufficient validator stake"
    assert (
        f"Blacklisting hotkey: {validator_hotkey} with insufficient stake"
        in caplog.text
    )


@pytest.mark.asyncio
async def test_miner_blacklisting_sufficient_stake(mock_miner: Miner, caplog):
    """Test the case where the validator has sufficient stake"""
    synapse = valid_task_synapse
    synapse.dendrite.hotkey = validator_hotkey

    response = await mock_miner.blacklist_task_request(synapse)
    is_blacklisted, msg = response

    assert is_blacklisted is False
    assert msg == "Valid request received from validator"


@pytest.mark.asyncio
async def test_priority_ranking_basic(mock_miner: Miner):
    """Test the basic functionality of the priority_ranking function."""
    synapse = valid_task_synapse
    current_time = datetime.fromtimestamp(get_epoch_time())
    synapse.epoch_timestamp = (
        current_time - timedelta(seconds=10)
    ).timestamp()  # 10 seconds ago

    priority = await mock_miner.priority_ranking(synapse)

    assert math.floor(priority) == 10.0  # 10 seconds difference


@pytest.mark.asyncio
async def test_priority_ranking_different_timestamps(mock_miner: Miner):
    """Test the priority_ranking function with different epoch timestamps."""
    synapse = valid_task_synapse
    current_time = datetime.fromtimestamp(get_epoch_time())

    # Case 1: 20 seconds ago
    synapse.epoch_timestamp = (current_time - timedelta(seconds=20)).timestamp()
    priority_1 = await mock_miner.priority_ranking(synapse)

    # Case 2: 5 seconds ago
    synapse.epoch_timestamp = (current_time - timedelta(seconds=5)).timestamp()
    priority_2 = await mock_miner.priority_ranking(synapse)

    # Case 3: 0 seconds ago
    synapse.epoch_timestamp = current_time.timestamp()
    priority_3 = await mock_miner.priority_ranking(synapse)

    assert priority_1 > priority_2 > priority_3
