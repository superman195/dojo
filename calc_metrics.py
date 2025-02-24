import json
import os

from loguru import logger

from dojo.protocol import Scores
from scripts.extract_dataset import Row

BASE_PATH = "/Users/jarvis/Desktop/dojo-validator-datasets/"


coldkeys = set()
hotkeys = set()
num_human_tasks: int = 0

# Find all folders under the base path
if not os.path.exists(BASE_PATH):
    raise ValueError(f"Base path {BASE_PATH} does not exist")

folders = [
    f for f in os.listdir(BASE_PATH) if os.path.isdir(os.path.join(BASE_PATH, f))
]
logger.info(f"Found folders: {folders}")


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
                for response in data.miner_responses:
                    coldkeys.add(response.miner_coldkey)
                    hotkeys.add(response.miner_hotkey)
                    for (
                        completion_id,
                        score_record,
                    ) in response.completion_id_to_scores.items():
                        if not score_record.scores:
                            continue
                        scores = Scores.model_validate(score_record.scores)
                        if scores.raw_score is not None:
                            num_human_tasks += 1

# Save metrics to a file
metrics = {
    "num_human_tasks": num_human_tasks,
    "num_unique_coldkeys": len(coldkeys),
    "num_unique_hotkeys": len(hotkeys),
    "coldkeys": list(coldkeys),
    "hotkeys": list(hotkeys),
}

# Create the base directory if it doesn't exist
os.makedirs(BASE_PATH, exist_ok=True)

metrics_file = os.path.join(BASE_PATH, "metrics.json")
with open(metrics_file, "w") as f:
    json.dump(metrics, f, indent=2)


print(f"Number of human tasks: {num_human_tasks}")
print(f"Number of unique coldkeys: {len(coldkeys)}")
print(f"Number of unique hotkeys: {len(hotkeys)}")
