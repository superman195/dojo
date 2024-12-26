import copy
import random
from typing import List, Tuple

import numpy as np
import requests
from bittensor.utils.btlogging import logging as logger
from strenum import StrEnum

from commons.utils import keccak256_hash, ttl_cache


class Leaderboard(StrEnum):
    EVALPLUS = "evalplus"


@ttl_cache(maxsize=60, ttl=3600)
def get_leaderboard_data(leaderboard: Leaderboard):
    if leaderboard == Leaderboard.EVALPLUS:
        leaderboard_url = "https://evalplus.github.io/results.json"
        response = requests.get(leaderboard_url)
        data = response.json()
        return data

    raise NotImplementedError(f"Leaderboard: {leaderboard} not implemented.")


# NOTE that all keys here correspond to OpenRouter models
# map our model name used on OpenROuter to leaderboard key
MODEL_MAPPING = {
    "mistralai/mixtral-8x22b-instruct": {
        Leaderboard.EVALPLUS: "Mixtral-8x22B-Instruct-v0.1"
    },
    "openai/gpt-4-turbo-2024-04-09": {Leaderboard.EVALPLUS: "GPT-4-Turbo (April 2024)"},
    "openai/gpt-4-1106-preview": {Leaderboard.EVALPLUS: "GPT-4-Turbo (Nov 2023)"},
    "openai/gpt-3.5-turbo-1106": {Leaderboard.EVALPLUS: "GPT-3.5-Turbo (Nov 2023)"},
    "meta-llama/llama-3-70b-instruct": {Leaderboard.EVALPLUS: "Llama3-70B-instruct"},
    "anthropic/claude-3-opus-20240229": {
        Leaderboard.EVALPLUS: "claude-3-opus (Mar 2024)"
    },
    "anthropic/claude-3-sonnet-20240229": {
        Leaderboard.EVALPLUS: "claude-3-sonnet (Mar 2024)"
    },
    "anthropic/claude-3-haiku-20240307": {
        Leaderboard.EVALPLUS: "claude-3-haiku (Mar 2024)"
    },
    "mistralai/mistral-large": {Leaderboard.EVALPLUS: "Mistral Large (Mar 2024)"},
    "google/gemini-pro-1.5": {Leaderboard.EVALPLUS: "Gemini Pro 1.5"},
    "cognitivecomputations/dolphin-mixtral-8x7b": {
        Leaderboard.EVALPLUS: "dolphin-2.6-mixtral-8x7b"
    },
    "cohere/command-r-plus": {Leaderboard.EVALPLUS: "Command-R+"},
    "google/gemini-pro-1.0": {Leaderboard.EVALPLUS: "Gemini Pro 1.0"},
    "meta-llama/llama-3-8b-instruct": {Leaderboard.EVALPLUS: "Llama3-8B-instruct"},
}


def get_leaderboard_scores(models: List[str], leaderboard=Leaderboard.EVALPLUS):
    """Returns a sorted list of tuples, (model, score), where index 0
    is the best model
    since we are sorting in reverse order."""
    leaderboard_data = get_leaderboard_data(leaderboard)

    scores: list[float | None] = [None] * len(models)
    for i, model_name in enumerate(models):
        mapped_model_name = MODEL_MAPPING.get(model_name, {}).get(leaderboard, None)
        if not mapped_model_name:
            logger.error(
                f"Model mapping on Leaderboard: {leaderboard} for model: {model_name} not found."
            )
            continue
        if mapped_model_name not in leaderboard_data:
            logger.warning(
                f"Model: {mapped_model_name} not found in EvalPlus leaderboard."
            )
            continue

        score = leaderboard_data[mapped_model_name].get("pass@1", {}).get("humaneval+")
        if not score:
            logger.error(f"Scores data for model: {mapped_model_name} not found.")
            continue

        scores[i] = score

    # throw valueerror if any of scores is None
    if any(score is None for score in scores):
        raise ValueError(
            f"Scores: {scores} cannot contain None values, models: {models}"
        )

    return zip(models, scores)


def get_gt_ranks(models_with_scores: Tuple[str, float]):
    """Given a list of tuples of models and scores, return the ground truth ranks."""
    sorted_models_with_scores = sorted(
        models_with_scores, key=lambda x: x[1], reverse=True
    )
    print(f"{sorted_models_with_scores=}")

    gt_ranks = []
    for model, _ in models_with_scores:
        found_idx = next(
            (i for i, (m, _) in enumerate(sorted_models_with_scores) if m == model),
            None,
        )
        if found_idx is None:
            logger.error(f"Model: {model} not found")
            continue
        gt_ranks.append(found_idx + 1)

    return gt_ranks


def diff_gt(all_miner_ranks, ground_truth_ranks):
    """Calculate difference between a miner's ranks and the ground truth"""
    if isinstance(all_miner_ranks, list):
        all_miner_ranks = np.array(all_miner_ranks)
    if isinstance(ground_truth_ranks, list):
        ground_truth_ranks = np.array(ground_truth_ranks)

    return np.linalg.norm(all_miner_ranks - ground_truth_ranks, ord=2, axis=1)


if __name__ == "__main__":
    answer_models = [
        "mistralai/mixtral-8x22b-instruct",
        "openai/gpt-4-turbo-2024-04-09",
        "openai/gpt-4-1106-preview",
        "openai/gpt-3.5-turbo-1106",
        "meta-llama/llama-3-70b-instruct",
        "anthropic/claude-3-opus-20240229",
        "anthropic/claude-3-sonnet-20240229",
        "anthropic/claude-3-haiku-20240307",
        "mistralai/mistral-large",
        "google/gemini-pro-1.5",
        "cognitivecomputations/dolphin-mixtral-8x7b",
        "cohere/command-r-plus",
        "google/gemini-pro-1.0",
        "meta-llama/llama-3-8b-instruct",
    ]

    N = 4
    sampled_models = random.sample(answer_models, N)
    print(f"{sampled_models=}")

    model_with_scores = get_leaderboard_scores(answer_models)
    for model, score in model_with_scores:
        print(f"{model}: {score}")

    model_hashes = [keccak256_hash(model) for model in sampled_models]
    # NOT THE RESPONSIBILITY OF VALIDATOR, MOCKING MINER RESPONSE
    model_hashes_copy = copy.deepcopy(model_hashes)
    miner_ranks = list(range(len(model_hashes)))
    random.shuffle(miner_ranks)
    random.shuffle(model_hashes_copy)
    print(f"{model_hashes_copy=}")

    miner_response = []
    for i, m in enumerate(model_hashes_copy):
        miner_response.append({"hash": m, "rank": miner_ranks[i]})

    # NOT THE RESPONSIBILITY OF VALIDATOR, MOCKING MINER RESPONSE

    # ensure consistent ordering with model_hashes known by validator
    ordered_miner_response = sorted(
        miner_response, key=lambda x: model_hashes.index(x["hash"])
    )
    miner_rank_order = [rank["rank"] for rank in ordered_miner_response]
    gt_ranks = get_gt_ranks(model_with_scores)

    pass
