import json
import os
from unittest.mock import patch

import bittensor as bt
import pytest

from dojo.mock import MockDendrite, MockMetagraph, MockSubtensor

# @pytest.fixture(autouse=True)
# def mock_env_var(monkeypatch):
#     monkeypatch.setenv("DOJO_API_BASE_URL", "http://example.com")

#     with patch("subprocess.check_output") as mock_check_output:
#         mock_check_output.return_value = b"v1.0.0\n"
#         yield monkeypatch

#     # Test that the monkey patch for get_latest_git_tag works
#     from template import __version__

#     assert __version__ == "1.0.0"


@pytest.fixture
def mock_evalplus_leaderboard_results():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "evalplus_leaderboard_results.json"
    )
    with open(fixture_path) as f:
        mock_data = json.load(f)

    expected_keys = [
        "claude-2 (Mar 2024)",
        "claude-3-haiku (Mar 2024)",
        "claude-3-opus (Mar 2024)",
        "claude-3-sonnet (Mar 2024)",
    ]
    assert all(
        key in mock_data for key in expected_keys
    ), f"Missing one or more expected keys in the fixture data. Expected keys: {expected_keys}"

    return mock_data


@pytest.fixture
def mock_initialise() -> (
    tuple[patch, bt.Wallet, MockSubtensor, MockMetagraph, MockDendrite]
):
    """Fixture to initialise mock components for testing."""
    netuid = 1

    bt.MockSubtensor.reset()
    mock_wallet = bt.Wallet()
    mock_subtensor = MockSubtensor(netuid=netuid, wallet=mock_wallet)
    mock_metagraph = MockMetagraph(netuid=netuid, subtensor=mock_subtensor)
    mock_dendrite = MockDendrite(wallet=mock_wallet)

    with patch("commons.utils.initialise") as mock_initialise:
        mock_initialise.return_value = (
            mock_wallet,
            mock_subtensor,
            mock_metagraph,
            mock_dendrite,
        )
        yield (
            mock_initialise,
            mock_wallet,
            mock_subtensor,
            mock_metagraph,
            mock_dendrite,
        )


@pytest.fixture
def disable_terminal_plot():
    # disable terminal plots that usually are enabled for debugging purposes
    # remember to add patches for wherever you're using this fixture
    with (
        patch("commons.scoring._terminal_plot", return_value=None),
        patch("neurons.validator._terminal_plot", return_value=None),
    ):
        yield


@pytest.fixture
def mock_keyfile(tmp_path):
    # create a mock wallet using pytest's tmp_path fixture
    wallet_dir = tmp_path / ".bittensor" / "wallets" / "default" / "hotkeys"
    wallet_dir.mkdir(parents=True)

    # Create a mock keyfile
    keyfile = wallet_dir / "default"
    mock_key_data = {
        "public_key": "0x1234...",
        "private_key": "0x5678...",
    }
    keyfile.write_text(json.dumps(mock_key_data))

    # Patch the home directory for bittensor
    with patch("os.path.expanduser") as mock_expanduser:
        mock_expanduser.return_value = str(tmp_path)
        yield
