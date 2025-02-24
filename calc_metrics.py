import datetime
import json
import os
from collections import defaultdict

from loguru import logger
from pydantic import BaseModel

from dojo.protocol import Scores
from scripts.extract_dataset import Row

BASE_PATH = "/Users/jarvis/Desktop/dojo-validator-datasets/"


class Metrics(BaseModel):
    num_human_tasks: int
    coldkeys: set[str]
    hotkeys: set[str]


overall_metrics = Metrics(num_human_tasks=0, coldkeys=set(), hotkeys=set())

# Find all folders under the base path
if not os.path.exists(BASE_PATH):
    raise ValueError(f"Base path {BASE_PATH} does not exist")

folders = [
    f for f in os.listdir(BASE_PATH) if os.path.isdir(os.path.join(BASE_PATH, f))
]
logger.info(f"Found folders: {folders}")

DAY_PERIOD = 23
date_periods = [
    (
        datetime.datetime(2024, 9, DAY_PERIOD, tzinfo=datetime.UTC),
        datetime.datetime(2024, 10, DAY_PERIOD, tzinfo=datetime.UTC),
    ),
    (
        datetime.datetime(2024, 10, DAY_PERIOD, tzinfo=datetime.UTC),
        datetime.datetime(2024, 11, DAY_PERIOD, tzinfo=datetime.UTC),
    ),
    (
        datetime.datetime(2024, 11, DAY_PERIOD, tzinfo=datetime.UTC),
        datetime.datetime(2024, 12, DAY_PERIOD, tzinfo=datetime.UTC),
    ),
    (
        datetime.datetime(2024, 12, DAY_PERIOD, tzinfo=datetime.UTC),
        datetime.datetime(2025, 1, DAY_PERIOD, tzinfo=datetime.UTC),
    ),
    (
        datetime.datetime(2025, 1, DAY_PERIOD, tzinfo=datetime.UTC),
        datetime.datetime(2025, 2, DAY_PERIOD, tzinfo=datetime.UTC),
    ),
    (
        datetime.datetime(2025, 2, DAY_PERIOD, tzinfo=datetime.UTC),
        datetime.datetime(2025, 3, DAY_PERIOD, tzinfo=datetime.UTC),
    ),
    (
        datetime.datetime(2025, 3, DAY_PERIOD, tzinfo=datetime.UTC),
        datetime.datetime(2025, 4, DAY_PERIOD, tzinfo=datetime.UTC),
    ),
]


def _format_date(date: datetime.datetime) -> str:
    return f"{date.strftime('%Y-%m-%d')}"


period_metrics = defaultdict(
    lambda: Metrics(num_human_tasks=0, coldkeys=set(), hotkeys=set())
)
for start_date, end_date in date_periods:
    period_metrics[f"{_format_date(start_date)}_{_format_date(end_date)}"] = Metrics(
        num_human_tasks=0,
        coldkeys=set(),
        hotkeys=set(),
    )


def get_key(date: datetime.datetime) -> str:
    for start_date, end_date in date_periods:
        if start_date <= date < end_date:
            return f"{_format_date(start_date)}_{_format_date(end_date)}"
    raise ValueError(f"Date {date} does not fall within any defined period")


for folder in folders:
    folder_path = os.path.join(BASE_PATH, folder)
    logger.info(f"Processing folder: {folder_path}")
    if os.path.exists(folder_path):
        # Get all files in the folder
        files = os.listdir(folder_path)

        file = [file for file in files if file.endswith("combined.jsonl")]
        logger.info(f"Found {file} files in {folder_path}")

        if len(file) != 1:
            raise ValueError(f"Expected 1 file, got {len(file)}")
        file_path = os.path.join(folder_path, file[0])

        with open(file_path) as f:
            lines = f.readlines()
            for line in lines:
                json_data = json.loads(line)
                # Convert completion dictionaries to strings before validation
                if "completions" in json_data:
                    for completion in json_data["completions"]:
                        if isinstance(completion.get("completion"), dict):
                            completion["completion"] = json.dumps(
                                completion["completion"]
                            )
                data = Row.model_validate(json_data)
                completion_date = data.completions[0].created_at.astimezone(
                    datetime.UTC
                )
                for response in data.miner_responses:
                    overall_metrics.coldkeys.add(response.miner_coldkey)
                    overall_metrics.hotkeys.add(response.miner_hotkey)
                    period_metrics[get_key(completion_date)].coldkeys.add(
                        response.miner_coldkey
                    )
                    period_metrics[get_key(completion_date)].hotkeys.add(
                        response.miner_hotkey
                    )
                    for (
                        completion_id,
                        score_record,
                    ) in response.completion_id_to_scores.items():
                        if not score_record.scores:
                            continue
                        scores = Scores.model_validate(score_record.scores)
                        if scores.raw_score is not None:
                            overall_metrics.num_human_tasks += 1
                            period_metrics[
                                get_key(completion_date)
                            ].num_human_tasks += 1

# Save metrics to a file
metrics_object = {
    "overall_metrics": {
        "num_human_tasks": overall_metrics.num_human_tasks,
        "num_unique_coldkeys": len(overall_metrics.coldkeys),
        "num_unique_hotkeys": len(overall_metrics.hotkeys),
        "coldkeys": list(overall_metrics.coldkeys),
        "hotkeys": list(overall_metrics.hotkeys),
    },
    "period_metrics": {
        k: {
            "num_human_tasks": v.num_human_tasks,
            "num_unique_coldkeys": len(v.coldkeys),
            "num_unique_hotkeys": len(v.hotkeys),
        }
        for k, v in period_metrics.items()
    },
}

# Create the base directory if it doesn't exist
os.makedirs(BASE_PATH, exist_ok=True)

metrics_file = os.path.join(BASE_PATH, "metrics.json")
with open(metrics_file, "w") as f:
    json.dump(metrics_object, f, indent=2)


print("\nOverall Metrics:")
print(f"Number of Human Tasks: {overall_metrics.num_human_tasks}")
print(f"Number of Unique Coldkeys: {len(overall_metrics.coldkeys)}")
print(f"Number of Unique Hotkeys: {len(overall_metrics.hotkeys)}")

for period, metrics in period_metrics.items():
    print(f"\nPeriod: {period}")
    print(f"Number of Human Tasks: {metrics.num_human_tasks}")
    print(f"Number of Unique Coldkeys: {len(metrics.coldkeys)}")
    print(f"Number of Unique Hotkeys: {len(metrics.hotkeys)}")
