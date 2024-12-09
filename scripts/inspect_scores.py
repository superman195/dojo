import argparse
from typing import List

import torch
from tabulate import tabulate
from termcolor import colored


def format_score_table(scores: torch.Tensor) -> List[List[str]]:
    """Format scores into a table with 10 columns"""
    table_data = []
    row = []
    for i, score in enumerate(scores):
        score_str = f"{score.item():.4f}"
        # Color non-zero scores
        if score.item() > 0:
            score_str = colored(score_str, "green")
        row.append([i, score_str])

        # Use 10 columns for better screen fit
        if len(row) == 10 or i == len(scores) - 1:
            table_data.append(row)
            row = []
    return table_data


def inspect_scores(
    file_path: str = "scores/validator_scores.pt", show_all: bool = False
):
    try:
        scores = torch.load(file_path)

        # Print Summary
        print(colored("\n=== Scores Summary ===", "blue", attrs=["bold"]))
        print(f"Total UIDs: {len(scores)}")
        print(f"Data type: {scores.dtype}")
        print(f"Device: {scores.device}")

        # Print Statistics
        print(colored("\n=== Statistics ===", "blue", attrs=["bold"]))
        print(f"Mean score: {scores.mean().item():.4f}")
        print(f"Min score: {scores.min().item():.4f}")
        print(f"Max score: {scores.max().item():.4f}")
        print(
            f"Non-zero UIDs: {torch.count_nonzero(scores).item()} "
            f"({(torch.count_nonzero(scores).item()/len(scores)*100):.1f}%)"
        )

        # Print Top Scores
        top_k = 10  # Show top 10
        values, indices = torch.topk(scores, k=min(top_k, len(scores)))
        print(colored("\n=== Top 10 Scores ===", "blue", attrs=["bold"]))
        top_scores = [
            [f"UID {idx}", f"{val.item():.4f}"] for idx, val in zip(indices, values)
        ]
        print(tabulate(top_scores, headers=["UID", "Score"], tablefmt="simple"))

        if show_all:
            print(colored("\n=== All Scores ===", "blue", attrs=["bold"]))
            table_data = format_score_table(scores)
            for row in table_data:
                # headers = [f"UID {i[0]}" for i in row]
                values = [f"UID {i[0]} - {i[1]}" for i in row]
                print(tabulate([values], tablefmt="simple"))

        print("\nNote: Green values indicate non-zero scores")

    except FileNotFoundError:
        print(colored(f"Score file not found at {file_path}", "red"))
    except Exception as e:
        print(colored(f"Error reading scores: {e}", "red"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="Show all scores")
    args = parser.parse_args()
    inspect_scores(show_all=args.all)
