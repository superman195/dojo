import copy

import bittensor as bt
import wandb
from bittensor.utils.btlogging import logging as logger

from commons.objects import ObjectManager
from commons.utils import hide_sensitive_path


def init_wandb(config: bt.config, my_uid, wallet: bt.wallet):
    # Ensure paths are decoupled
    import dojo

    # Deep copy of the config
    config = copy.deepcopy(config)

    # Manually deepcopy neuron and data_manager, otherwise it is referenced to the same object
    config.neuron = copy.deepcopy(config.neuron)

    project_name = None

    config = ObjectManager.get_config()

    project_name = config.wandb.project_name
    if project_name not in ["dojo-devnet", "dojo-testnet", "dojo-mainnet"]:
        raise ValueError("Invalid wandb project name")

    run_name = f"{config.neuron.type}-{my_uid}-{dojo.__version__}"

    # Hide sensitive paths in the config
    config.neuron.full_path = (
        hide_sensitive_path(config.neuron.full_path)
        if config.neuron.full_path
        else None
    )

    config.uid = my_uid
    config.hotkey = wallet.hotkey.ss58_address
    config.run_name = run_name
    config.version = dojo.__version__
    # NOTE: @dev set to None to avoid exposing
    config.subtensor = None

    # Initialize the wandb run for the single project
    kwargs = {
        "name": run_name,
        "project": project_name,
        "entity": "dojo-subnet",
        "config": config,
        "dir": config.full_path,
        "reinit": True,
    }
    run = wandb.init(**kwargs)

    # Sign the run to ensure it's from the correct hotkey
    signature = wallet.hotkey.sign(run.id.encode()).hex()
    config.signature = signature
    wandb.config.update(config, allow_val_change=True)

    logger.success(f"Started wandb run with {kwargs=}")
