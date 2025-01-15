from enum import Enum
from typing import List

import bittensor
import argparse
# import bittensor_cli.cli.commands
# import bittensor_cli.src.commands

import bittensor_cli.src.commands.wallets as w
import bittensor_cli.src.commands.root as r
import bittensor_cli.src.commands.stake as s
import bittensor_cli.src.commands.subnets as sn
from bittensor_cli import CLIManager


# from bittensor_cli.cli import (
#     RegisterCommand,
#     RegisterSubnetworkCommand,
#     RootRegisterCommand,
#     StakeCommand,
#     TransferCommand,
#     WalletBalanceCommand,
# )
import bittensor.core
import bittensor.core.extrinsics
import bittensor.core.extrinsics.root
import bittensor.core.extrinsics.staking
import bittensor.core.extrinsics.registration
from munch import DefaultMunch
from substrateinterface import Keypair

"""
- Uses special accounts (Alice, Bob, Charlie) as different roles
- Sets up wallets for each of the roles
- Creates a subnet with Alice as the owner
- Registers Bob as validator and Charlie as miner
- Register on root network so Bob can stake
"""

# NOTE @dev change to relevant subtensor network
subtensor_network = "ws://localhost:9944"
chain_endpoint = "wss://entrypoint-finney.opentensor.ai:443"
default_wallet_path = "~/.bittensor/wallets"
wallet_path = default_wallet_path
base_args = [
    "--no_prompt",
    "--wallet.path",
    wallet_path,
    "--subtensor.network",
    subtensor_network,
]
transfer_to_subnet_owner_amt = 500
validator_stake_amt = 100
create_new_subnet = False


def get_coldkey_name(uri: str):
    return "special-{}".format(uri.strip("/"))


dict = {
    # "command": "subnet",
    # "config": None,
    "strict": False,
    "no_version_checking": False,
    "no_prompt": True,
    # "subcommand": "list",
    "subtensor": {
        "network": subtensor_network,
        "chain_endpoint": "wss://entrypoint-finney.opentensor.ai:443",
        "_mock": False
    },
}
cli = CLIManager()
cli.config = dict

cli.get_config()
cli.wallet_list(wallet_path=wallet_path,
                        quiet = False,
                        verbose=True)



cli.wallet_list(wallet_path=wallet_path,
                    quiet = False,
                    verbose=True)
wallets = {}
def setup_wallet(uri: str, coldkey_name: str):
    keypair = Keypair.create_from_uri(uri)
    wallet = bittensor.wallet(path=wallet_path, name=coldkey_name)
    print(keypair)
    print(wallet.name)
    # don't encrypt just so that we can do stuff without prompts
    # wallet.set_coldkey(keypair=keypair, encrypt=False, overwrite=True)
    # wallet.set_coldkeypub(keypair=keypair, encrypt=False, overwrite=True)
    # wallet.set_hotkey(keypair=keypair, encrypt=False, overwrite=True)
    seed_phrase = keypair.generate_mnemonic(words=24)
    print(f"URI: {uri}, seed phrase: {seed_phrase}")

    wallets[coldkey_name] = wallet
    
    cli.wallet_create_wallet(
                    wallet_path=wallet_path,
                    wallet_name=coldkey_name,
                    wallet_hotkey=keypair.ss58_address,
                    n_words=12,
                    quiet=False,
                    verbose=True,
                    use_password=False
                    )
    # parser = bittensor.cli.__create_parser__()

    # def exec_command(command, extra_args: List[str]):
    #     # Convert all arguments to strings to avoid any issues with argparse
    #     extra_args = [str(arg) for arg in extra_args]
    #     cli_instance = bittensor.cli(
    #         bittensor.config(
    #             parser=parser,
    #             args=extra_args + base_args,
    #         )
    #     )
    #     command.run(cli_instance)

    return (keypair, wallet)


class Roles(Enum):
    SUBNET_OWNER = "//Alice"
    SUBNET_VALI = "//Bob"
    SUBNET_MINER = "//Charlie"


if __name__ == "__main__":

    
    # lol = cli.subnets_list(network=[chain_endpoint],
    #                        quiet = False,
    #                        verbose=True)
    
    roles = {}
    for r in Roles:
        wallet_setup = setup_wallet(r.value, get_coldkey_name(r.name))
        roles[r.name] = wallet_setup
    print(f"{roles=}")

    # _, owner_wallet = roles[Roles.SUBNET_OWNER.name]
    # if create_new_subnet:
    #     pass
    #     # for r in Roles:
    #     #     # let alice be our subnet owner
    #     #     if r == Roles.SUBNET_OWNER:
    #     #         continue

    #     #     _, _, exec = roles[r.name]
    #     #     coldkey_name = get_coldkey_name(r.name)

    #     #     exec(
    #     #         TransferCommand,
    #     #         [
    #     #             "wallet",
    #     #             "transfer",
    #     #             "--dest",
    #     #             str(owner_wallet.hotkey.ss58_address),
    #     #             "--amount",
    #     #             transfer_to_subnet_owner_amt,
    #     #             "--wallet.name",
    #     #             coldkey_name,
    #     #         ],
    #     #     )
    #     #     exec(
    #     #         WalletBalanceCommand,
    #     #         ["wallet", "balance", "--wallet.name", coldkey_name],
    #     #     )

    #     # _, _, owner_exec = roles[Roles.SUBNET_OWNER.name]

    #     # owner_exec(
    #     #     RegisterSubnetworkCommand,
    #     #     [
    #     #         "subnet",
    #     #         "create",
    #     #         "--wallet.name",
    #     #         get_coldkey_name(Roles.SUBNET_OWNER.name),
    #     #     ],
    #     # )

    # tmp_args = [
    #     "subnet",
    #     "list",
    #     "--no_prompt",
    #     "--subtensor.network",
    #     subtensor_network,
    # ]
    
    # parser = argparse.ArgumentParser(
    #     description=f"bittensor cli v{bittensor.__version__}",
    #     usage="btcli <command> <command args>",
    # )
    

    # config = DefaultMunch(**dict)
    
    
    
    
    
    
    # # tmp_config2 = 
    # tmp_config = bittensor.config(
    #     parser=parser, args=tmp_args
    # )
    # print(f"{tmp_config=}")
    # subtensor = bittensor.subtensor(network="wss://entrypoint-finney.opentensor.ai:443", config=tmp_config)
    # # subtensor = bittensor.subtensor(config=tmp_config)

    # # register for each subnet
    # print(f"{wallets=}")
    # print("trying to get subnet info")
    # subnet_infos: List[bittensor.SubnetInfo] = subtensor.get_all_subnets_info()
    # for info in subnet_infos:
    #     if info.owner_ss58 == owner_wallet.coldkey.ss58_address:
    #         netuid = info.netuid
    #         print(f"Owner owns subnet uid: {netuid}")
    #         print(f"Sunet info: {info}")

    #         print(f"Registering validator for netuid: {netuid}...")
    #         # _, _, vali_exec = roles[Roles.SUBNET_VALI.name]
    #         success =bittensor.core.extrinsics.registration.register_extrinsic(
    #             subtensor=subtensor,
    #             wallet=wallets[get_coldkey_name(Roles.SUBNET_VALI.name)],
    #             netuid=netuid,
    #         )
            
    #         if not success:
    #             print("Failed to register validator")
    #             exit(1)
            
    #         # vali_exec(
    #         #     RegisterCommand,
    #         #     [
    #         #         "subnet",
    #         #         "register",
    #         #         "--netuid",
    #         #         str(netuid),
    #         #         "--wallet.name",
    #         #         get_coldkey_name(Roles.SUBNET_VALI.name),
    #         #         "--wallet.hotkey",
    #         #         "default",
    #         #     ],
    #         # )

    #         print(f"Registering miner for netuid: {netuid}...")
    #         # _, _, miner_exec = roles[Roles.SUBNET_MINER.name]
    #         success = bittensor.core.extrinsics.registration.register_extrinsic(
    #             subtensor=subtensor,
    #             wallet=wallets[get_coldkey_name(Roles.SUBNET_MINER.name)],
    #             netuid=netuid,
    #         )
    #         if not success:
    #             print("Failed to register miner")
    #             exit(1)
            
    #         # miner_exec(
    #         #     RegisterCommand,
    #         #     [
    #         #         "subnet",
    #         #         "register",
    #         #         "--netuid",
    #         #         str(netuid),
    #         #         "--wallet.name",
    #         #         get_coldkey_name(Roles.SUBNET_MINER.name),
    #         #         "--wallet.hotkey",
    #         #         "default",
    #         #     ],
    #         # )
    # # register on root network
    # print("Registering on root subnet...")
    # # _, _, vali_exec = roles[Roles.SUBNET_VALI.name]
    # success=bittensor.core.extrinsics.root.root_register_extrinsic(
    #     subtensor=subtensor,
    #     wallet=wallets[get_coldkey_name(Roles.SUBNET_VALI.name)],
    # )
    # if not success:
    #     print("Failed to register on root subnet")
    #     exit(1)
    # # # vali_exec(
    # # #     RootRegisterCommand,
    # # #     [
    # #         "root",
    # #         "register",
    # #         "--wallet.name",
    # #         get_coldkey_name(Roles.SUBNET_VALI.name),
    # #         "--wallet.hotkey",
    # #         "default",
    # #     ]
    # #     + base_args,
    # # )


    # print("Adding stake to subnet validator")
    # success = bittensor.core.extrinsics.staking.add_stake_extrinsic(
    #     subtensor=subtensor,
    #     wallet=wallets[get_coldkey_name(Roles.SUBNET_VALI.name)],
    #     amount=validator_stake_amt,
    #     wait_for_finalization=True,
    # )
    # if not success:
    #     print("Failed to add stake to subnet validator")
    #     exit(1)
    
    # # vali_exec(
    # #     StakeCommand,
    # #     [
    # #         "stake",
    # #         "add",
    # #         "--wallet.name",
    # #         get_coldkey_name(Roles.SUBNET_VALI.name),
    # #         "--wallet.hotkey",
    # #         "default",
    # #         "--amount",
    # #         str(validator_stake_amt),
    # #     ]
    # #     + base_args,
    # # )
