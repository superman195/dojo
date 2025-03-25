import sys
from functools import lru_cache
from pathlib import Path

import bittensor as bt
from bittensor.utils.btlogging import logging as logger
from pydantic_settings import CliApp

from dojo.logging import apply_custom_logging_format

from .types import Settings


@lru_cache
def get_config() -> Settings:
    """Returns the configuration object specific to this miner or validator after adding relevant arguments."""
    cli_args = sys.argv[1:]  # grab all args except for the python script name itself
    settings: Settings = CliApp.run(Settings, cli_args=cli_args)
    validate_config(settings)
    configure_logging(settings)
    return settings


def validate_config(settings: Settings) -> bool:
    """Validates the settings object."""
    if settings.simulation.enabled or settings.simulation.bad_miner:
        logger.warning(
            f"You have simulation settings enabled: {settings.simulation.model_dump()}!\nPlease ensure that you are NOT running this in a production environment."
        )

    if settings.test.ignore_min_stake or settings.test.fast_mode:
        logger.warning(
            f"You have simulation settings enabled: {settings.test.model_dump()}!\nPlease ensure that you are NOT running this in a production environment."
        )
    return True


def configure_logging(settings: Settings):
    """
    Configures logging based on the provided configuration.
    """

    log_path = (
        Path.cwd()
        / settings.wallet.coldkey
        / settings.wallet.hotkey
        / settings.neuron_type
    )
    log_path.mkdir(parents=True, exist_ok=True)  # ensure the path exists

    try:
        # Configure global logging state
        # bt.logging.set_config(config)
        bt.logging.on()

        if settings.logging.trace:
            bt.logging.set_trace(True)
        elif settings.logging.debug:
            bt.logging.set_debug(True)
        elif settings.logging.info:
            bt.logging.set_info(True)
        else:
            # Default to INFO level
            bt.logging.set_info(True)

    except Exception as e:
        print(f"Failed to configure logging: {str(e)}")
        # Fallback to INFO level
        bt.logging.set_info(True)

    apply_custom_logging_format()


# def add_args(parser: argparse.ArgumentParser):
#     """
#     Adds relevant arguments to the parser for operation.
#     """
#     # Netuid Arg: The netuid of the subnet to connect to.
#     parser.add_argument("--netuid", type=int, help="Subnet netuid", default=52)
#
#     parser.add_argument(
#         "--neuron.type",
#         choices=["miner", "validator"],
#         type=str,
#         help="Whether running a miner or validator",
#     )
#     args, _ = parser.parse_known_args()
#     neuron_type = None
#     if known_args := vars(args):
#         neuron_type = known_args["neuron.type"]
#
#     # device = get_device()
#
#     # NOTE: unused
#     parser.add_argument(
#         "--neuron.device", type=str, help="Device to run on.", default="cpu"
#     )
#
#     # NOTE: unused ??
#     parser.add_argument(
#         "--api.port",
#         type=int,
#         help="FastAPI port for uvicorn to run on, should be different from axon.port as these will serve external requests.",
#         default=1888,
#     )
#
#     parser.add_argument(
#         "--env_file",
#         type=str,
#         help="Path to the environment file to use.",
#     )
#
#     parser.add_argument(
#         "--ignore_min_stake",
#         action="store_true",
#         help="Whether to always include self in monitoring queries, mainly for testing",
#     )
#
#     # NOTE: unused
#     parser.add_argument(
#         "--service",
#         choices=["miner-decentralised", "miner-centralised", "validator"],
#         help="Specify the service to run (miner or validator) for auto_updater.",
#     )
#
#     parser.add_argument(
#         "--fast_mode",
#         action="store_true",
#         help="Whether to run in fast mode, for developers to test locally.",
#     )
#
#     parser.add_argument(
#         "--simulation",
#         action="store_true",
#         help="Whether to run the validator in simulation mode",
#     )
#
#     parser.add_argument(
#         "--simulation_bad_miner",
#         action="store_true",
#         help="Set miner simluation to a bad one",
#     )
#
#     epoch_length = 100
#     known_args, _ = parser.parse_known_args()
#     if known_args := vars(known_args):
#         if known_args["fast_mode"]:
#             epoch_length = 10
#
#     parser.add_argument(
#         "--neuron.epoch_length",
#         type=int,
#         help="The default epoch length (how often we set weights, measured in 12 second blocks).",
#         default=epoch_length,
#     )
#
#     if neuron_type == "validator":
#         # NOTE: unused
#         parser.add_argument(
#             "--neuron.sample_size",
#             type=int,
#             help="The number of miners to query per dendrite call.",
#             default=8,
#         )
#
#         parser.add_argument(
#             "--neuron.moving_average_alpha",
#             type=float,
#             help="Moving average alpha parameter, how much to add of the new observation.",
#             default=0.3,
#         )
#
#     elif neuron_type == "miner":
#         pass
#
