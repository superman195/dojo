import json

from database.client import connect_db, disconnect_db, prisma
from database.prisma import Json
from database.prisma.enums import CriteriaTypeEnum, TaskTypeEnum


async def migrate():
    await connect_db()

    # Fetch all old feedback requests
    old_feedback_requests = await prisma.feedback_request_model.find_many(
        include={
            "completions": True,
            "criteria_types": True,
            "ground_truths": True,
        }
    )

    for old_request in old_feedback_requests:
        # Map task type
        task_type = TaskTypeEnum.CODE_GENERATION  # Default fallback
        if old_request.task_type.lower().find("image") >= 0:
            task_type = TaskTypeEnum.TEXT_TO_IMAGE
        elif old_request.task_type.lower().find("3d") >= 0:
            task_type = TaskTypeEnum.TEXT_TO_THREE_D

        existing_task = await prisma.validatortask.find_unique(
            where={"id": old_request.id}
        )
        if existing_task:
            print(f"Replacing existing task {old_request.id}")
            new_validator_task = await prisma.validatortask.update(
                where={"id": old_request.id},
                data={
                    "prompt": old_request.prompt,
                    "task_type": task_type,
                    "is_processed": old_request.is_processed,
                    "expire_at": old_request.expire_at,
                    "created_at": old_request.created_at,
                    "updated_at": old_request.updated_at,
                },
            )
        else:
            # Create new ValidatorTask (parent)
            new_validator_task = await prisma.validatortask.create(
                data={
                    "id": old_request.id,
                    "prompt": old_request.prompt,
                    "task_type": task_type,
                    "is_processed": old_request.is_processed,
                    "expire_at": old_request.expire_at,
                    "created_at": old_request.created_at,
                    "updated_at": old_request.updated_at,
                }
            )

        # Create MinerResponse (child) if dojo_task_id and hotkey exist
        if old_request.dojo_task_id and old_request.hotkey:
            task_result = {
                "type": "score",  # Updated from 'multi-score'
                "value": {},
            }

            if not old_request.ground_truths or not old_request.completions:
                continue

            # Get all ground truths and their corresponding scores
            for ground_truth in old_request.ground_truths:
                # Find completion response for this ground truth
                completion_response = next(
                    (
                        comp
                        for comp in old_request.completions
                        if comp.model == ground_truth.real_model_id
                    ),
                    None,
                )

                if completion_response:
                    task_result["value"][ground_truth.real_model_id] = (
                        completion_response.score
                    )

            # Check if miner_response already exists
            existing_miner_response = await prisma.minerresponse.find_first(
                where={
                    "validator_task_id": new_validator_task.id,
                    "dojo_task_id": old_request.dojo_task_id,
                    "hotkey": old_request.hotkey,
                }
            )

            if existing_miner_response:
                miner_response = await prisma.minerresponse.update(
                    where={"id": existing_miner_response.id},
                    data={
                        "coldkey": "",
                        "task_result": Json(json.dumps(task_result)),
                        "created_at": old_request.created_at,
                        "updated_at": old_request.updated_at,
                    },
                )
            else:
                miner_response = await prisma.minerresponse.create(
                    data={
                        "validator_task_id": new_validator_task.id,
                        "dojo_task_id": old_request.dojo_task_id,
                        "hotkey": old_request.hotkey,
                        "coldkey": "",
                        "task_result": Json(json.dumps(task_result)),
                        "created_at": old_request.created_at,
                        "updated_at": old_request.updated_at,
                    }
                )

        if not old_request.completions:
            continue

        # Migrate completions and create criteria
        for old_completion in old_request.completions:
            # Check if completion already exists
            existing_completion = await prisma.completion.find_unique(
                where={"id": old_completion.id}
            )

            if existing_completion:
                print(f"Replacing existing completion {old_completion.completion_id}")
                new_completion = await prisma.completion.update(
                    where={"id": old_completion.id},
                    data={
                        "validator_task_id": new_validator_task.id,
                        "model": old_completion.model,
                        "completion": old_completion.completion,
                        "created_at": old_completion.created_at,
                        "updated_at": old_completion.updated_at,
                    },
                )
            else:
                new_completion = await prisma.completion.create(
                    data={
                        "id": old_completion.id,
                        "completion_id": old_completion.completion_id,
                        "validator_task_id": new_validator_task.id,
                        "model": old_completion.model,
                        "completion": old_completion.completion,
                        "created_at": old_completion.created_at,
                        "updated_at": old_completion.updated_at,
                    }
                )

            if not old_request.criteria_types:
                continue

            # Migrate criteria types - now linked to completion instead of request
            for old_criterion in old_request.criteria_types:
                # Skip RANKING_CRITERIA as it's no longer supported
                if old_criterion.type == "RANKING_CRITERIA":
                    continue

                # Convert MULTI_SCORE to SCORE
                criteria_type = old_criterion.type
                config = None
                if criteria_type == "MULTI_SCORE":
                    criteria_type = "SCORE"
                    config = json.dumps(
                        {"min": old_criterion.min, "max": old_criterion.max}
                    )

                new_criterion = await prisma.criterion.create(
                    data={
                        "completion_id": new_completion.id,
                        "criteria_type": CriteriaTypeEnum[criteria_type],
                        "config": Json(config if config else "{}"),
                        "created_at": old_criterion.created_at,
                        "updated_at": old_criterion.updated_at,
                    }
                )

                # Create MinerScore if we have a miner response
                if old_request.dojo_task_id and old_request.hotkey:
                    # Check if miner score already exists
                    existing_miner_score = await prisma.minerscore.find_unique(
                        where={
                            "criterion_id_miner_response_id": {
                                "criterion_id": new_criterion.id,
                                "miner_response_id": miner_response.id,
                            }
                        }
                    )

                    if existing_miner_score:
                        print(
                            f"Skipping existing miner score for criterion {new_criterion.id}"
                        )
                        continue

                    await prisma.minerscore.create(
                        data={
                            "criterion_id": new_criterion.id,
                            "miner_response_id": miner_response.id,
                            "scores": Json(json.dumps({})),
                            "created_at": old_criterion.created_at,
                            "updated_at": old_criterion.updated_at,
                        }
                    )

        if not old_request.ground_truths:
            continue

        # Migrate ground truths
        for old_ground_truth in old_request.ground_truths:
            # Map rank_id to corresponding score
            rank_scores = {0: 0.0, 1: 0.33333334, 2: 0.6666667, 3: 1.0}
            ground_truth_score = rank_scores.get(old_ground_truth.rank_id, 0.0)

            # Check if ground truth already exists
            existing_ground_truth = await prisma.groundtruth.find_unique(
                where={"id": old_ground_truth.id}
            )

            if existing_ground_truth:
                print(f"Updating existing ground truth {old_ground_truth.id}")
                await prisma.groundtruth.update(
                    where={"id": old_ground_truth.id},
                    data={
                        "validator_task_id": new_validator_task.id,
                        "obfuscated_model_id": old_ground_truth.obfuscated_model_id,
                        "real_model_id": old_ground_truth.real_model_id,
                        "rank_id": old_ground_truth.rank_id,
                        "ground_truth_score": ground_truth_score,
                        "updated_at": old_ground_truth.updated_at,
                    },
                )
            else:
                await prisma.groundtruth.create(
                    data={
                        "id": old_ground_truth.id,
                        "validator_task_id": new_validator_task.id,
                        "obfuscated_model_id": old_ground_truth.obfuscated_model_id,
                        "real_model_id": old_ground_truth.real_model_id,
                        "rank_id": old_ground_truth.rank_id,
                        "ground_truth_score": ground_truth_score,
                        "created_at": old_ground_truth.created_at,
                        "updated_at": old_ground_truth.updated_at,
                    }
                )

    await disconnect_db()


if __name__ == "__main__":
    import asyncio

    asyncio.run(migrate())
