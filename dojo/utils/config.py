import argparse
import inspect
import logging
import os
import site
from functools import lru_cache
from pathlib import Path

import bittensor as bt
from bittensor.utils.btlogging import logging as logger
from dotenv import find_dotenv, load_dotenv

base_path = Path.cwd()


def get_caller_info() -> str | None:
    """jank ass call stack inspection to get same logging format as loguru"""
    try:
        stack = inspect.stack()
        site_packages_path = site.getsitepackages()[0]
        for i in range(len(stack) - 1, 1, -1):
            filename = stack[i].filename
            if os.path.basename(filename) == "loggingmachine.py":
                # get our actual caller frame
                prev_frame = stack[i + 1]
                full_path = prev_frame.filename
                # ensure `/Users/username/...` stripped
                full_path = full_path.replace(site_packages_path, "")
                module_path = (
                    full_path.replace(os.getcwd() + os.sep, "")
                    .replace(os.sep, ".")
                    .lstrip(".")
                )
                module_name = module_path.rsplit(".", 1)[0]
                function_name = prev_frame.function
                line_no = prev_frame.lineno
                caller_info = f"{module_name}:{function_name}:{line_no}".rjust(40)
                return caller_info
    except Exception:
        return None


class CustomFormatter(logging.Formatter):
    def format(self, record):
        caller_info = get_caller_info()
        if caller_info is None:
            # if we fail to inspect stack, default to log_format
            # log_format = "%(filename)s.%(funcName)s:%(lineno)s - %(message)s"
            caller_info = f"{record.filename}:{record.funcName}:{record.lineno}".rjust(
                40
            )
        module_name, function_name, line_no = caller_info.split(":")
        record.name = module_name
        record.filename = function_name
        record.lineno = int(line_no)

        return super().format(record)


date_format = "%Y-%m-%d %H:%M:%S"
custom_formatter = CustomFormatter(datefmt=date_format)


def apply_custom_logging_format():
    # Retrieve the existing Bittensor logger
    bittensor_logger = logging.getLogger(
        "bittensor"
    )  # Ensure this matches the logger name you are using
    # bittensor_logger.setLevel(logging.INFO)  # Set the logging level to INFO

    # Apply the custom formatter to each handler
    for handler in bittensor_logger.handlers:
        handler.setFormatter(custom_formatter)


def check_config(config: bt.config):
    """Checks/validates the config namespace object."""
    # logger.check_config(config)

    log_dir = str(base_path / "logs")
    full_path = os.path.expanduser(
        f"{log_dir}/{config.wallet.name}/{config.wallet.hotkey}/netuid{config.netuid}/{config.neuron.name}"
    )
    config.neuron.full_path = os.path.expanduser(full_path)
    if not os.path.exists(config.neuron.full_path):
        os.makedirs(config.neuron.full_path, exist_ok=True)

    # bt.logging.enable_third_party_loggers()


def configure_logging(config: bt.config):
    """
    Configures logging based on the provided configuration.
    """
    # Configure the global logging state from the config
    bt.logging.set_config(config)

    # Apply logging configurations based on the config
    bt.logging.on()  # Default state: INFO level
    try:
        if config.logging.trace:  # pyright: ignore[reportOptionalMemberAccess]
            bt.logging.set_trace(True)
        elif config.logging.debug:  # pyright: ignore[reportOptionalMemberAccess]
            bt.logging.set_debug(True)
    except Exception:
        pass

    # Optionally enable file logging if `record_log` and `logging_dir` are provided
    if config.record_log and config.logging_dir:
        logging_dir = os.path.expanduser(config.logging_dir)
        if not os.path.exists(logging_dir):
            os.makedirs(logging_dir, exist_ok=True)

        bt.logging.set_config(config)

    apply_custom_logging_format()


def add_args(parser):
    """
    Adds relevant arguments to the parser for operation.
    """
    # Netuid Arg: The netuid of the subnet to connect to.
    parser.add_argument("--netuid", type=int, help="Subnet netuid", default=52)

    neuron_types = ["miner", "validator"]
    parser.add_argument(
        "--neuron.type",
        choices=neuron_types,
        type=str,
        help="Whether running a miner or validator",
    )
    args, _ = parser.parse_known_args()
    neuron_type = None
    if known_args := vars(args):
        neuron_type = known_args["neuron.type"]

    parser.add_argument(
        "--neuron.name",
        type=str,
        help="Trials for this neuron go in neuron.root / (wallet_cold - wallet_hot) / neuron.name. ",
        default=neuron_type,
    )

    # device = get_device()
    parser.add_argument(
        "--neuron.device", type=str, help="Device to run on.", default="cpu"
    )

    parser.add_argument(
        "--api.port",
        type=int,
        help="FastAPI port for uvicorn to run on, should be different from axon.port as these will serve external requests.",
        default=1888,
    )

    parser.add_argument(
        "--env_file",
        type=str,
        help="Path to the environment file to use.",
    )

    parser.add_argument(
        "--ignore_min_stake",
        action="store_true",
        help="Whether to always include self in monitoring queries, mainly for testing",
    )

    parser.add_argument(
        "--service",
        choices=["miner-decentralised", "miner-centralised", "validator"],
        help="Specify the service to run (miner or validator) for auto_updater.",
    )

    parser.add_argument(
        "--fast_mode",
        action="store_true",
        help="Whether to run in fast mode, for developers to test locally.",
    )

    parser.add_argument(
        "--simulation",
        action="store_true",
        help="Whether to run the validator in simulation mode",
    )

    parser.add_argument(
        "--simulation_bad_miner",
        action="store_true",
        help="Set miner simluation to a bad one",
    )

    epoch_length = 100
    known_args, _ = parser.parse_known_args()
    if known_args := vars(known_args):
        if known_args["fast_mode"]:
            epoch_length = 10

    parser.add_argument(
        "--neuron.epoch_length",
        type=int,
        help="The default epoch length (how often we set weights, measured in 12 second blocks).",
        default=epoch_length,
    )

    if neuron_type == "validator":
        parser.add_argument(
            "--neuron.sample_size",
            type=int,
            help="The number of miners to query per dendrite call.",
            default=8,
        )

        parser.add_argument(
            "--neuron.moving_average_alpha",
            type=float,
            help="Moving average alpha parameter, how much to add of the new observation.",
            default=0.3,
        )

        wandb_project_names = ["dojo-devnet", "dojo-testnet", "dojo-mainnet"]
        parser.add_argument(
            "--wandb.project_name",
            type=str,
            choices=wandb_project_names,
            help="Name of the wandb project to use.",
        )

    elif neuron_type == "miner":
        pass


@lru_cache(maxsize=1)
def get_config():
    """Returns the configuration object specific to this miner or validator after adding relevant arguments."""
    parser = argparse.ArgumentParser()
    bt.wallet.add_args(parser)
    bt.subtensor.add_args(parser)
    bt.axon.add_args(parser)
    add_args(parser)

    # Add logging arguments
    bt.logging.add_args(parser)

    # Check and validate config
    _config = bt.config(parser)
    check_config(_config)
    configure_logging(_config)  # Configure logging using bt.logging

    return _config


def source_dotenv():
    """Source env file if provided"""
    config = get_config()
    if config.env_file:
        load_dotenv(find_dotenv(config.env_file), override=True)
        logger.trace(f"Sourcing env vars from {config.env_file}")
        return

    load_dotenv()
