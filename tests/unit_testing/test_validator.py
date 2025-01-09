import random
import string
from unittest.mock import MagicMock, patch

import pytest
import torch
from loguru import logger

from neurons.validator import Validator


def generate_unique_hotkey():
    """Generate a unique hotkey for testing purposes."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=16))


class MockConfig:
    class NeuronConfig:
        sample_size = 5

    neuron = NeuronConfig()


@pytest.fixture
def mock_validator(mock_initialise, tmp_path):
    """Fixture to setup validator with mock components for testing."""
    logger.info("Setting up validator fixture.")
    mock_initialise_func, mock_wallet, mock_subtensor, mock_metagraph, mock_dendrite = (
        mock_initialise
    )

    with patch.object(Validator, "__init__", lambda x: None):
        validator = Validator()

        # Inject the mocked components
        validator.wallet = mock_wallet
        validator.subtensor = mock_subtensor
        validator.metagraph = mock_metagraph
        validator.dendrite = mock_dendrite
        validator.config = mock_wallet.config
        validator.scores = torch.zeros(mock_metagraph.n.item(), dtype=torch.float32)

        # Set up the mock attributes
        validator._active_miner_uids = list(range(5))
        validator.scores[:5] = torch.tensor([0.9, 0.8, 0.7, 0.6, 0.5])
        validator._threshold = 0.4  # Set a mock threshold

        # Override the data manager path to the temporary directory
        base_path = tmp_path / "data_manager"
        base_path.mkdir(parents=True, exist_ok=True)
        config = MagicMock()
        config.data_manager.base_path = base_path
        with patch("commons.objects.ObjectManager.get_config", return_value=config):
            yield validator

    logger.info("Validator fixture setup complete.")


# @pytest.mark.asyncio
# async def test_validator_set_weights(mock_validator: Validator):
#     """Test the validator's set_weights method."""
#     await mock_validator.set_weights()


# TODO Implement with test database envrioment

# @patch.object(SyntheticAPI, "get_qa", new_callable=AsyncMock)
# @pytest.mark.asyncio
# async def test_validator_querying_miners_dojo(mock_get_qa, validator):
#     """
#     Test the validator's handling of miners' responses.

#     Specifically, this test verifies that:
#     1. The DojoTaskTracker's internal state is updated with the new tasks.
#     2. The dendrite response is saved correctly with non-obfuscated model IDs.
#     3. The saved data includes the correct prompt and model IDs.
#     """

#     # Mock get_config to return a valid configuration
#     with patch("neurons.validator.get_config", return_value=MockConfig):
#         # Mock axons and add them to the metagraph
#         mock_axons = [MagicMock(spec=bt.AxonInfo) for _ in range(5)]
#         for i, axon in enumerate(mock_axons):
#             axon.hotkey = f"hotkey_{i}"
#             axon.ip = "127.0.0.1"
#             axon.port = 8091
#             axon.is_serving = True  # Ensure the axons are marked as serving

#         validator.metagraph.axons = mock_axons
#         validator.metagraph.n = torch.tensor([len(mock_axons)])

#         # Mock SyntheticAPI.get_qa to return synthetic data
#         synthetic_qa_mock = SyntheticQA(
#             prompt="synthetic_prompt",
#             responses=[
#                 CompletionResponses(
#                     model="synthetic_model",
#                     completion="This is a synthetic response",
#                     cid=generate_unique_hotkey(),
#                 )
#             ],
#         )

#         mock_get_qa.return_value = synthetic_qa_mock

#         # Verify initial state
#         initial_rid_to_mhotkey_to_task_id = copy.deepcopy(
#             DojoTaskTracker._rid_to_mhotkey_to_task_id
#         )
#         initial_task_to_expiry = copy.deepcopy(DojoTaskTracker._task_to_expiry)
#         initial_rid_to_model_map = copy.deepcopy(DojoTaskTracker._rid_to_model_map)

#         logger.info("Calling send_request method.")
#         # Call send_request method
#         await validator.send_request()

#         assert (
#             DojoTaskTracker._rid_to_mhotkey_to_task_id
#             != initial_rid_to_mhotkey_to_task_id
#         ), "DojoTaskTracker's _rid_to_mhotkey_to_task_id was not updated"
#         assert (
#             DojoTaskTracker._task_to_expiry != initial_task_to_expiry
#         ), "DojoTaskTracker's _task_to_expiry was not updated"
#         assert (
#             DojoTaskTracker._rid_to_model_map != initial_rid_to_model_map
#         ), "DojoTaskTracker's _rid_to_model_map was not updated"

#         # Verify that the data was actually saved
#         data_path = DataManager.get_requests_data_path()
#         assert data_path.exists(), "Data file does not exist"

#         data = await DataManager._load_without_lock(path=data_path)

#         assert data, "Data was not saved"
#         assert len(data) > 0, "No data found in saved file"
#         assert data[0].request.prompt == "synthetic_prompt", "Saved data is incorrect"

#         # Verify that model ids are not obfuscated in saved data
#         for response in data[0].miner_responses:
#             for model_response in response.completion_responses:
#                 assert (
#                     model_response.model == "synthetic_model"
#                 ), "Model ID should not be obfuscated in saved data"

#         logger.info("Completed test_validator_querying_miners_dojo.")
