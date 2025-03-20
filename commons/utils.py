import os
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from functools import lru_cache, update_wrapper
from math import floor
from pathlib import Path
from typing import Any, Tuple

import bittensor as bt
import numpy as np
import plotext
import requests
import torch
from bittensor.core.metagraph import AsyncMetagraph
from bittensor.utils.btlogging import logging as logger
from Crypto.Hash import keccak
from tenacity import RetryError, Retrying, stop_after_attempt, wait_exponential_jitter

from commons.objects import ObjectManager

ROOT_WEIGHT = 0.18
ROOT_NETUID = 0


class aobject:
    """Inheriting this class allows you to define an async __init__.

    So you can create objects by doing something like `await MyClass(params)`
    """

    async def __new__(cls, *a, **kw):
        instance = super().__new__(cls)
        await instance.__init__(*a, **kw)
        return instance

    async def __init__(self):
        pass


def get_effective_stake(hotkey: str, subtensor: bt.subtensor) -> float:
    if isinstance(subtensor, bt.AsyncSubtensor):
        raise NotImplementedError("Async subtensor not supported")

    root_stake = 0
    try:
        root_metagraph = subtensor.metagraph(ROOT_NETUID)
        root_stake = root_metagraph.S[root_metagraph.hotkeys.index(hotkey)].item()
    except (ValueError, IndexError):
        logger.trace(
            f"Hotkey {hotkey} not found in root metagraph, defaulting to 0 root_stake"
        )

    alpha_stake = 0
    try:
        config = ObjectManager.get_config()
        subnet_metagraph = subtensor.metagraph(netuid=config.netuid)  # type:ignore
        alpha_stake = subnet_metagraph.alpha_stake[
            subnet_metagraph.hotkeys.index(hotkey)
        ]
    except (ValueError, IndexError):
        logger.trace(
            f"Hotkey {hotkey} not found in subnet metagraph for netuid: {subnet_metagraph.netuid}, defaulting to 0 alpha_stake"
        )

    effective_stake = (root_stake * ROOT_WEIGHT) + alpha_stake

    return effective_stake


async def aget_effective_stake(
    hotkey: str, root_metagraph: AsyncMetagraph, subnet_metagraph: AsyncMetagraph
) -> float:
    root_stake = 0
    try:
        root_stake = root_metagraph.S[root_metagraph.hotkeys.index(hotkey)].item()
    except (ValueError, IndexError):
        logger.trace(
            f"Hotkey {hotkey} not found in root metagraph, defaulting to 0 root_stake"
        )

    alpha_stake = 0
    try:
        idx = subnet_metagraph.hotkeys.index(hotkey)
        alpha_stake = subnet_metagraph.alpha_stake[idx]
    except (ValueError, IndexError):
        logger.trace(
            f"Hotkey {hotkey} not found in subnet metagraph for netuid: {subnet_metagraph.netuid}, defaulting to 0 alpha_stake"
        )

    effective_stake = (root_stake * ROOT_WEIGHT) + alpha_stake

    return effective_stake


def _terminal_plot(
    title: str, y: np.ndarray, x: np.ndarray | None = None, sort: bool = False
):
    """Plot a scatter plot on the terminal.
    It is crucial that we don't modify the original order of y or x, so we make a copy first.

    Args:
        title (str): Title of the plot.
        y (np.ndarray): Y values to plot.
        x (np.ndarray | None, optional): X values to plot. If None, will use a np.linspace from 0 to len(y).
        sort (bool, optional): Whether to sort the y values. Defaults to False.
    """
    if x is None:
        x = np.linspace(0, len(y), len(y) + 1)

    if sort:
        y_copy = np.copy(y)
        y_copy.sort()
        y = y_copy

    # plot actual points first
    plotext.scatter(x, y, marker="bitcoin", color="orange")  # show the points exactly
    plotext.title(title)
    plotext.ticks_color("red")
    plotext.xfrequency(1)  # Set frequency to 1 for better alignment
    plotext.xticks(x)  # Directly use x for xticks
    plotext.ticks_style("bold")
    plotext.grid(horizontal=True, vertical=True)
    plotext.plotsize(
        width=int(plotext.terminal_width() * 0.95),
        height=int(plotext.terminal_height() * 0.95),
    )
    plotext.canvas_color(color=None)
    plotext.theme("clear")

    plotext.show()
    plotext.clear_figure()


def get_new_uuid():
    return str(uuid.uuid4())


def get_epoch_time():
    return time.time()


def datetime_as_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc)


def datetime_to_iso8601_str(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat()


def iso8601_str_to_datetime(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc)


def loaddotenv(varname: str):
    """Wrapper to get env variables for sanity checking"""
    value = os.getenv(varname)
    if not value:
        raise SystemExit(f"{varname} is not set")
    return value


def keccak256_hash(data):
    k = keccak.new(digest_bits=256)
    k.update(data.encode("utf-8"))
    return k.hexdigest()


def hide_sensitive_path(path):
    """
    Replace the path up to the directory just before '/logs' with '~/dir_before_logs'.
    If 'logs' is not found, return the path starting from the first directory after '~'.
    """

    path = str(path)

    home_directory = os.path.expanduser("~")
    # Replace home directory with '~'
    if path.startswith(home_directory):
        path = path.replace(home_directory, "~")

    return Path(path)


def log_retry_info(retry_state):
    """Meant to be used with tenacity's before_sleep callback"""
    logger.warning(
        f"Retry attempt {retry_state.attempt_number} failed with exception: {retry_state.outcome.exception()}",
    )


async def serve_axon(
    subtensor: bt.AsyncSubtensor,
    axon: bt.axon,
    config: bt.config,
    max_attempts: int = 10,
) -> bool:
    """A wrapper around the underlying self.axon.serve(...) call with retries"""
    try:
        for attempt in Retrying(
            stop=stop_after_attempt(max_attempts),
            before_sleep=log_retry_info,
            wait=wait_exponential_jitter(initial=12, max=60, jitter=1),
        ):
            with attempt:
                serve_success = await subtensor.serve_axon(
                    netuid=config.netuid, axon=axon
                )
                if serve_success:
                    return True

                raise Exception(
                    "Failed to serve axon, probably due to many miners/validators trying to serve in this period."
                )
    except RetryError:
        logger.error(f"Failed to serve axon after {max_attempts} attempts.")
        pass
    return False


def initialise(
    config: bt.config,
) -> Tuple[bt.wallet, bt.subtensor, bt.metagraph, bt.axon]:
    # Build Bittensor objects
    # These are core Bittensor classes to interact with the network.
    logger.info("Setting up bittensor objects....")
    # The wallet holds the cryptographic key pairs for the miner.
    wallet = bt.wallet(config=config)
    logger.info(f"Wallet: {wallet}")
    # The subtensor is our connection to the Bittensor blockchain.
    subtensor = bt.subtensor(config=config)
    logger.info(f"Subtensor: {subtensor}")
    # The metagraph holds the state of the network, letting us know about other validators and miners.
    metagraph = subtensor.metagraph(config.netuid)
    logger.info(f"Metagraph: {metagraph}")
    # The axon handles request processing, allowing validators to send this miner requests.
    axon = bt.axon(wallet=wallet, port=config.axon.port)
    logger.info(f"Axon: {axon}")
    return wallet, subtensor, metagraph, axon


def check_registered(subtensor, wallet, config):
    # --- Check for registration.
    if not subtensor.is_hotkey_registered(
        netuid=config.netuid,
        hotkey_ss58=wallet.hotkey.ss58_address,
    ):
        logger.error(
            f"Wallet: {wallet} is not registered on netuid {config.netuid}."
            f" Please register the hotkey using `btcli s register` before trying again"
        )
        exit()


def get_external_ip() -> str:
    response = requests.get("https://ifconfig.me/ip")
    response.raise_for_status()
    return response.text.strip()


def get_device() -> str:
    if torch.cuda.is_available():
        return "cuda"
    elif torch.backends.mps.is_available():
        return "mps"
    else:
        return "cpu"


class DotDict(OrderedDict):
    """
    Quick and dirty implementation of a dot-able dict, which allows access and
    assignment via object properties rather than dict indexing.
    """

    def __init__(self, *args, **kwargs):
        # we could just call super(DotDict, self).__init__(*args, **kwargs)
        # but that won't get us nested dotdict objects
        od = OrderedDict(*args, **kwargs)
        for key, val in od.items():
            if isinstance(val, Mapping):
                value = DotDict(val)
            else:
                value = val
            self[key] = value

    def __delattr__(self, name):
        try:
            del self[name]
        except KeyError as ex:
            raise AttributeError(f"No attribute called: {name}") from ex

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError as ex:
            raise AttributeError(f"No attribute called: {k}") from ex

    __setattr__ = OrderedDict.__setitem__


def remove_key(input_dict, key, depth=0):
    """Recursively remove a specified key from a nested dictionary, keeping track of depth."""
    for k, v in list(input_dict.items()):
        if k == key:
            del input_dict[k]
        elif isinstance(v, dict):
            remove_key(v, key, depth=depth + 1)
    return input_dict


# LRU Cache with TTL
def ttl_cache(maxsize: int = 128, typed: bool = False, ttl: int = -1):
    """
    Decorator that creates a cache of the most recently used function calls with a time-to-live (TTL) feature.
    The cache evicts the least recently used entries if the cache exceeds the `maxsize` or if an entry has
    been in the cache longer than the `ttl` period.

    Args:
        maxsize (int): Maximum size of the cache. Once the cache grows to this size, subsequent entries
                       replace the least recently used ones. Defaults to 128.
        typed (bool): If set to True, arguments of different types will be cached separately. For example,
                      f(3) and f(3.0) will be treated as distinct calls with distinct results. Defaults to False.
        ttl (int): The time-to-live for each cache entry, measured in seconds. If set to a non-positive value,
                   the TTL is set to a very large number, effectively making the cache entries permanent. Defaults to -1.

    Returns:
        Callable: A decorator that can be applied to functions to cache their return values.

    The decorator is useful for caching results of functions that are expensive to compute and are called
    with the same arguments frequently within short periods of time. The TTL feature helps in ensuring
    that the cached values are not stale.

    Example:
        @ttl_cache(ttl=10)
        def get_data(param):
            # Expensive data retrieval operation
            return data
    """
    if ttl <= 0:
        ttl = 65536
    hash_gen = _ttl_hash_gen(ttl)

    def wrapper(func: Callable) -> Callable:
        @lru_cache(maxsize, typed)
        def ttl_func(ttl_hash, *args, **kwargs):
            return func(*args, **kwargs)

        def wrapped(*args, **kwargs) -> Any:
            th = next(hash_gen)
            return ttl_func(th, *args, **kwargs)

        return update_wrapper(wrapped, func)

    return wrapper


def _ttl_hash_gen(seconds: int):
    """
    Internal generator function used by the `ttl_cache` decorator to generate a new hash value at regular
    time intervals specified by `seconds`.

    Args:
        seconds (int): The number of seconds after which a new hash value will be generated.

    Yields:
        int: A hash value that represents the current time interval.

    This generator is used to create time-based hash values that enable the `ttl_cache` to determine
    whether cached entries are still valid or if they have expired and should be recalculated.
    """
    start_time = time.time()
    while True:
        yield floor((time.time() - start_time) / seconds)


# 12 seconds updating block.
@ttl_cache(maxsize=1, ttl=12)
def ttl_get_block(subtensor) -> int:
    """
    Retrieves the current block number from the blockchain. This method is cached with a time-to-live (TTL)
    of 12 seconds, meaning that it will only refresh the block number from the blockchain at most every 12 seconds,
    reducing the number of calls to the underlying blockchain interface.

    Returns:
        int: The current block number on the blockchain.

    This method is useful for applications that need to access the current block number frequently and can
    tolerate a delay of up to 12 seconds for the latest information. By using a cache with TTL, the method
    efficiently reduces the workload on the blockchain interface.

    Example:
        current_block = ttl_get_block(self)

    Note: self here is the miner or validator instance
    """
    return subtensor.get_current_block()


def set_expire_time(expire_in_seconds: int) -> str:
    """
    Sets the expiration time based on the current UTC time and the given number of seconds.

    Args:
        expire_in_seconds (int): The number of seconds from now when the expiration should occur.

    Returns:
        str: The expiration time in ISO 8601 format with 'Z' as the UTC indicator.
    """
    return (
        (datetime.now(timezone.utc) + timedelta(seconds=expire_in_seconds))
        .replace(tzinfo=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def is_valid_expiry(expire_at: str) -> bool:
    """
    Checks if the given expiry time is not None and falls within a reasonable time period.

    Args:
        expire_at (str): The expiry time in ISO format.

    Returns:
        bool: True if the expiry time is valid, False otherwise.
    """
    if expire_at is None:
        return False

    try:
        expiry_time = datetime.fromisoformat(expire_at)
    except ValueError:
        logger.error(f"Invalid expiry time format: {expire_at}")
        return False

    current_time = datetime.now(timezone.utc)
    max_reasonable_time = current_time + timedelta(days=5)

    if current_time <= expiry_time <= max_reasonable_time:
        return True
    else:
        logger.warning(f"Expiry time {expire_at} is out of the reasonable range.")
        return False


def verify_hotkey_in_metagraph(metagraph: bt.metagraph, hotkey: str) -> bool:
    """
    returns true if input hotkey is in input metagraph, false otherwise.
    """
    return hotkey in metagraph.hotkeys


def verify_signature(hotkey: str, signature: str, message: str) -> bool:
    """
    returns true if input signature was created by input hotkey for input message.
    """
    keypair = bt.Keypair(ss58_address=hotkey, ss58_format=42)
    if not keypair.verify(data=message, signature=signature):
        logger.error(f"Invalid signature for address={hotkey}")
        return False

    logger.success(f"Signature verified, signed by {hotkey}")
    return True


def check_stake(subtensor: bt.subtensor, hotkey: str) -> bool:
    """
    returns true if hotkey has enough stake to be a validator and false otherwise.
    """
    from dojo import VALIDATOR_MIN_STAKE

    stake = get_effective_stake(hotkey, subtensor)

    if stake < VALIDATOR_MIN_STAKE:
        return False
    return True
