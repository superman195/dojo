import argparse
from pathlib import Path
from typing import Callable, Dict

import bittensor
import requests
from prompt_toolkit import PromptSession, prompt
from prompt_toolkit.completion import FuzzyCompleter, WordCompleter
from rich.console import Console

from dojo import get_dojo_api_base_url
from dojo.utils.config import get_config, source_dotenv

get_config()
source_dotenv()

DOJO_API_BASE_URL = get_dojo_api_base_url()

console = Console()


def success(message: str, emoji: str = ":white_check_mark:"):
    console.print(f"{emoji} [green]{message}[/green]")


def info(message: str, emoji: str = ":information_source:"):
    console.print(f"{emoji} [white]{message}[/white]")


def error(message: str, emoji: str = ":x:"):
    console.print(f"{emoji} [red]{message}[/red]")


def warning(message: str, emoji: str = ":warning:"):
    console.print(f"{emoji} [yellow]{message}[/yellow]")


def api_key_list(cookies):
    response = requests.get(
        f"{DOJO_API_BASE_URL}/api/v1/miner/api-key/list", cookies=cookies
    )
    response.raise_for_status()
    keys = response.json().get("body", {}).get("apiKeys")
    if len(keys) == 0:
        warning("No API keys found, please generate one.")
    else:
        success(f"All API keys: {keys}")
    return keys


def api_key_generate(cookies):
    response = requests.post(
        f"{DOJO_API_BASE_URL}/api/v1/miner/api-key/generate", cookies=cookies
    )
    response.raise_for_status()
    keys = response.json().get("body", {}).get("apiKeys")
    success(f"All API keys: {keys}")
    return keys


def api_key_delete(cookies):
    keys = api_key_list(cookies)
    if not keys:
        return

    key_completer = FuzzyCompleter(WordCompleter(keys, ignore_case=True))
    selected_key = prompt(
        "Select an API key to delete: ",
        completer=key_completer,
        swap_light_and_dark_colors=True,
    )
    if selected_key not in keys:
        error("Invalid selection.")
        return

    response = requests.put(
        f"{DOJO_API_BASE_URL}/api/v1/miner/api-key/disable",
        json={"apiKey": selected_key},
        cookies=cookies,
    )
    response.raise_for_status()
    remaining_keys = response.json().get("body", {}).get("apiKeys")
    success(f"Remaining API keys: {remaining_keys}")
    return


def subscription_key_list(cookies):
    response = requests.get(
        f"{DOJO_API_BASE_URL}/api/v1/miner/subscription-key/list", cookies=cookies
    )
    response.raise_for_status()
    keys = response.json().get("body", {}).get("subscriptionKeys")
    if len(keys) == 0:
        warning("No subscription keys found, please generate one.")
    else:
        success(f"All subscription keys: {keys}")
    return keys


def subscription_key_generate(cookies):
    response = requests.post(
        f"{DOJO_API_BASE_URL}/api/v1/miner/subscription-key/generate", cookies=cookies
    )
    response.raise_for_status()
    keys = response.json().get("body", {}).get("subscriptionKeys")
    success(f"All subscription keys: {keys}")
    return keys


def subscription_key_delete(cookies):
    keys = subscription_key_list(cookies)
    if not keys:
        return

    key_completer = FuzzyCompleter(WordCompleter(keys, ignore_case=True))
    selected_key = prompt(
        "Select a subscription key to delete: ",
        completer=key_completer,
        swap_light_and_dark_colors=True,
    )
    if selected_key not in keys:
        error("Invalid selection.")
        return

    response = requests.put(
        f"{DOJO_API_BASE_URL}/api/v1/miner/subscription-key/disable",
        json={"subscriptionKey": selected_key},
        cookies=cookies,
    )
    response.raise_for_status()
    remaining_keys = response.json().get("body", {}).get("subscriptionKeys")
    success(f"Remaining subscription keys: {remaining_keys}")
    return


def _get_session_cookies(hotkey: str, signature: str, message: str):
    url = f"{DOJO_API_BASE_URL}/api/v1/miner/session/auth"
    if not signature.startswith("0x"):
        signature = "0x" + signature

    payload = {
        "hotkey": hotkey,
        "signature": signature,
        "message": message,
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()
    cookies = response.cookies.get_dict()
    return cookies


class State:
    def __init__(self, config):
        self.cookies = None
        self.wallet = bittensor.wallet(config=config)
        # cli = bittensor.cli(config=config)
        # self.subtensor = bittensor.subtensor(config=config)
        # axons = self.subtensor.metagraph(netuid=cli.config.netuid, lite=False).axons


def get_session_cookies(wallet):
    kp = wallet.hotkey
    hotkey = str(wallet.hotkey.ss58_address)

    raw_message = "Sign in to Dojo with Substrate"

    def prepare_message(message: str):
        return f"<Bytes>{message}</Bytes>"

    prepared_message = prepare_message(raw_message)
    signature = kp.sign(prepared_message).hex()
    try:
        cookies = _get_session_cookies(hotkey, signature, raw_message)
        success("Successfully got session cookies :cookie:")
        return cookies
    except Exception as e:
        error(f"Failed to get session cookies due to exception: {e}")
        pass
    return


def clear_session_cookies(state: State):
    state.cookies = None
    success("Successfully :skull: session cookies :cookie:")
    return


def placeholder():
    success("go implement it")


subscription_key_actions = {
    "list": subscription_key_list,
    "generate": subscription_key_generate,
    "delete": subscription_key_delete,
}

api_key_actions = {
    "list": api_key_list,
    "generate": api_key_generate,
    "delete": api_key_delete,
}

nested_actions: Dict[str, Callable] = {
    "authenticate": get_session_cookies,
    "api_key": api_key_actions,
    "subscription_key": subscription_key_actions,
    "clear_cookies": clear_session_cookies,
}


def nested_dict_none(d):
    """replace values which are func calls with None else assertion error"""
    if not isinstance(d, dict):
        return None
    return {k: nested_dict_none(v) for k, v in d.items()}


# nested_completer_data = nested_dict_none(nested_actions)


def flatten_nested_dict(d, parent_key="", sep=" "):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_nested_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def main():
    parser = argparse.ArgumentParser(description="Bittensor Wallet CLI")
    bittensor.wallet.add_args(parser)
    config = bittensor.config(parser)
    info(f"Using bittensor config:\n{config}")

    is_wallet_valid = False
    is_wallet_path_decided = False
    is_coldkey_valid = False
    is_hotkey_valid = False
    default_wallet_path = Path(config.wallet.path).expanduser()
    while not is_wallet_valid:
        while not is_wallet_path_decided:
            use_default_path = (
                input(
                    f"Do you want to use the default wallet path {default_wallet_path}? (y/n): "
                )
                .strip()
                .lower()
            )
            if use_default_path == "y":
                config.wallet.path = str(default_wallet_path)
                is_wallet_path_decided = True
            elif use_default_path == "n":
                config.wallet.path = str(input("Enter the wallet path: ").strip())
                if Path(config.wallet.path).expanduser().exists():
                    is_wallet_path_decided = True
                else:
                    error(
                        f"Invalid wallet path {config.wallet.path}, please try again."
                    )
                    continue

        info(
            "Please specify the wallet coldkey name and hotkey name to perform key management."
        )
        if not is_coldkey_valid:
            # config.wallet.name = input("Enter the wallet coldkey name: ").strip()
            coldkeys = [
                f.name
                for f in Path(config.wallet.path).expanduser().iterdir()
                if f.is_dir()
            ]
            coldkey_completer = WordCompleter(coldkeys, ignore_case=True)
            config.wallet.name = prompt(
                "Enter the wallet coldkey name: ",
                completer=coldkey_completer,
                swap_light_and_dark_colors=False,
            ).strip()
            coldkey_path = Path(config.wallet.path).expanduser() / config.wallet.name
            if not coldkey_path.exists():
                error(f"Coldkey path is invalid {coldkey_path}")
                continue
            else:
                is_coldkey_valid = True

        if not is_hotkey_valid:
            hotkeys_path = coldkey_path / "hotkeys"
            hotkeys = [f.name for f in hotkeys_path.iterdir() if f.is_file()]
            hotkey_completer = WordCompleter(hotkeys, ignore_case=True)
            config.wallet.hotkey = prompt(
                "Enter the wallet hotkey name: ",
                completer=hotkey_completer,
                swap_light_and_dark_colors=False,
            ).strip()
            # config.wallet.hotkey = input("Enter the wallet hotkey name: ").strip()
            hotkey_path = coldkey_path / "hotkeys" / config.wallet.hotkey
            if not hotkey_path.exists():
                error(f"Hotkey path is invalid {hotkey_path}")
                continue
            else:
                is_hotkey_valid = True

        info(f"Coldkey path: {coldkey_path}")
        info(f"Hotkey path: {hotkey_path}")

        if coldkey_path.exists() and hotkey_path.exists():
            is_wallet_valid = True

    success("Wallet coldkey name and hotkey name set successfully.")

    state = State(config)
    # method_completer = NestedCompleter.from_nested_dict(nested_dict_none(actions))
    flattened_actions = flatten_nested_dict(nested_actions)
    method_completer = WordCompleter(words=flattened_actions.keys(), ignore_case=True)
    session = PromptSession(
        completer=method_completer, swap_light_and_dark_colors=False
    )

    while True:
        try:
            text = session.prompt(">>> ", completer=method_completer)
            parsed_text = text.strip().lower()
            if parsed_text == "exit":
                break

            action = flattened_actions.get(parsed_text)
            if action:
                if action == get_session_cookies:
                    state.cookies = action(state.wallet)
                elif action == clear_session_cookies:
                    action(state)
                else:
                    if state.cookies is None:
                        warning("No session found, please authenticate first.")
                        continue
                    action(state.cookies)
            else:
                warning("Invalid action, please try again")
        except (KeyboardInterrupt, EOFError):
            break
        except Exception as e:
            error(f"Please try again, exception occurred: {e}")
            pass


if __name__ == "__main__":
    main()
