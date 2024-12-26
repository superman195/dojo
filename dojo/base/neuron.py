from abc import ABC, abstractmethod

import bittensor as bt
from bittensor.utils.btlogging import logging as logger

from commons.objects import ObjectManager
from commons.utils import initialise, ttl_get_block
from dojo import __spec_version__ as spec_version


class BaseNeuron(ABC):
    """
    Base class for Bittensor miners. This class is abstract and should be inherited by a subclass. It contains the core logic for all neurons; validators and miners.

    In addition to creating a wallet, subtensor, and metagraph, this class also handles the synchronization of the network state via a basic checkpointing mechanism based on epoch length.
    """

    subtensor: bt.subtensor
    wallet: bt.wallet
    metagraph: bt.metagraph
    spec_version: int = spec_version

    @property
    def block(self):
        return ttl_get_block(self.subtensor)

    def __init__(self):
        self.config = ObjectManager.get_config()

        # If a gpu is required, set the device to cuda:N (e.g. cuda:0)
        self.device = self.config.neuron.device

        # Log the configuration for reference.
        logger.info(self.config)

        self.wallet, self.subtensor, self.metagraph, self.axon = initialise(self.config)

        # Check if the miner is registered on the Bittensor network before proceeding further.
        self.check_registered()

        # Each miner gets a unique identity (UID) in the network for differentiation.
        self.uid = self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
        logger.info(
            f"Running neuron on subnet: {self.config.netuid} with uid {self.uid}"
        )
        self.step = 0

    @abstractmethod
    def run(self): ...

    @abstractmethod
    def resync_metagraph(self): ...

    @abstractmethod
    def set_weights(self): ...

    def sync(self):
        """
        1. check if registered on subnet
        2. check if should sync metagraph
        3. check if should set weights
        """
        self.check_registered()

        if self.should_sync_metagraph():
            self.resync_metagraph()

        if self.should_set_weights():
            self.set_weights()

        # Always save state.
        self.save_state()

    def check_registered(self):
        # --- Check for registration.
        if not self.subtensor.is_hotkey_registered(
            netuid=self.config.netuid,
            hotkey_ss58=self.wallet.hotkey.ss58_address,
        ):
            logger.error(
                f"Wallet: {self.wallet} is not registered on netuid {self.config.netuid}."
                f" Please register the hotkey using `btcli s register` before trying again"
            )
            exit()

    def should_sync_metagraph(self):
        """
        Check if enough epoch blocks have elapsed since the last checkpoint to sync.
        """
        return (
            self.block - self.metagraph.last_update[self.uid]
        ) > self.config.neuron.epoch_length

    def should_set_weights(self) -> bool:
        # Don't set weights on initialization.
        if self.step == 0:
            return False

        # Define appropriate logic for when set weights.
        return (
            self.block - self.metagraph.last_update[self.uid]
        ) > self.config.neuron.epoch_length

    def save_state(self):
        pass

    def load_state(self):
        logger.warning(
            "load_state() not implemented for this neuron. You can implement this function to load model checkpoints or other useful data."
        )
