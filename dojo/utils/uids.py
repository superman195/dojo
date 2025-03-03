import asyncio
import bisect
import uuid
from collections import defaultdict
from typing import List

import bittensor as bt

from commons.objects import ObjectManager
from commons.utils import aget_effective_stake, get_effective_stake, keccak256_hash


def is_uid_available(metagraph: bt.metagraph, uid: int) -> bool:
    """Check if uid is available."""
    # filter non serving axons.
    if not metagraph.axons[uid].is_serving:
        return False
    return True


def is_miner(metagraph: bt.metagraph, uid: int) -> bool:
    """Check if uid is a validator."""
    from dojo import VALIDATOR_MIN_STAKE

    hotkey = metagraph.hotkeys[uid]
    effective_stake = get_effective_stake(hotkey, metagraph.subtensor)
    return effective_stake < VALIDATOR_MIN_STAKE


async def extract_miner_uids() -> List[int]:
    config = ObjectManager.get_config()
    async with bt.AsyncSubtensor(config=config) as subtensor:
        block = await subtensor.get_current_block()
        subnet_metagraph = await subtensor.metagraph(config.netuid, block=block)
        root_metagraph = await subtensor.metagraph(0, block=block)

    from dojo import VALIDATOR_MIN_STAKE

    semaphore = asyncio.Semaphore(20)  # Allow 20 concurrent calls

    async def _semaphore_get_stake(hotkey):
        async with semaphore:
            return await aget_effective_stake(hotkey, root_metagraph, subnet_metagraph)

    # Create tasks for all hotkeys
    num_neurons = int(subnet_metagraph.n.item())
    tasks = [
        asyncio.create_task(_semaphore_get_stake(subnet_metagraph.hotkeys[i]))
        for i in range(num_neurons)
    ]

    # Process all tasks and collect results in order
    eff_stakes = await asyncio.gather(*tasks)

    # Return miner UIDs based on stakes
    return [
        uid
        for uid in range(num_neurons)
        if subnet_metagraph.axons[uid].is_serving
        and eff_stakes[uid] < VALIDATOR_MIN_STAKE
    ]


class MinerUidSelector:
    _instance = None
    ring = []
    nodes_hash_map = {}
    VIRTUAL_NODES = 160

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls.ring = []
            cls.nodes_hash_map = {}
        return cls._instance

    @classmethod
    def __init__(cls, nodes: List[int] | None = None):
        if not nodes:
            return

        cls.reset()
        for node in nodes:
            cls.add_uid(node)

    @classmethod
    def reset(cls):
        cls.ring = []
        cls.nodes_hash_map = {}

    @classmethod
    def hash_function(cls, key):
        return int(keccak256_hash(key), 16)

    @classmethod
    def add_uid(cls, node: int):
        for vnode in range(cls.VIRTUAL_NODES):
            vnode_key = f"{node}#vnode{vnode}"
            hash_value = cls.hash_function(vnode_key)
            cls.ring.append(hash_value)
            cls.nodes_hash_map[hash_value] = node
        cls.ring.sort()

    @classmethod
    def remove_uid(cls, node: int):
        for vnode in range(cls.VIRTUAL_NODES):
            vnode_key = f"{node}#vnode{vnode}"
            hash_value = cls.hash_function(vnode_key)
            if hash_value in cls.ring:
                cls.ring.remove(hash_value)
                del cls.nodes_hash_map[hash_value]

    @classmethod
    def get_target_uids(cls, key, k: int):
        if not cls.ring or k <= 0:
            return []
        hash_value = cls.hash_function(key)
        index = bisect.bisect_left(cls.ring, hash_value) % len(cls.ring)
        nodes = []
        for i in range(min(k, len(cls.ring))):
            node_index = (index + i) % len(cls.ring)
            node = cls.nodes_hash_map[cls.ring[node_index]]
            if node not in nodes:
                nodes.append(node)
        return nodes


if __name__ == "__main__":
    # example usage... always call __init__ then get_target_uids
    miner_uids = list(range(1, 193))
    # when actual usage occurs...
    # miner_uids = MinerUidSelector.extract_miner_uids(metagraph)
    ch = MinerUidSelector(miner_uids)
    requests_per_node = defaultdict(int)
    for _ in range(100_000):
        request_key = str(uuid.uuid4())
        target_nodes = ch.get_target_uids(request_key, k=4)
        for node in target_nodes:
            requests_per_node[node] += 1

    for node, count in requests_per_node.items():
        print(f"Node {node} received {count} requests.")

    import matplotlib.pyplot as plt

    miner_uids = list(requests_per_node.keys())
    requests = list(requests_per_node.values())

    plt.figure(figsize=(10, 6))
    plt.bar(miner_uids, requests, color="skyblue")
    plt.xlabel("Node")
    plt.ylabel("Number of Requests")
    plt.title("Distribution of Requests per Node")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
