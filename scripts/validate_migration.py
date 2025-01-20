import asyncio
import json

from database.client import connect_db, disconnect_db, prisma
from dojo.utils.config import source_dotenv

source_dotenv()


async def validate_migration(sample_size=100):
    """Validate data integrity of migrated records."""
    print("\nStarting validation...")
    errors_found = False

    try:
        # Get sample of parent requests
        parent_requests = await prisma.feedback_request_model.find_many(
            where={"parent_id": None},
            order={"created_at": "asc"},
            take=sample_size,
            include={
                "completions": True,
                "ground_truths": True,
                "criteria_types": True,
            },
        )

        print(f"\nValidating {len(parent_requests)} parent requests...")

        for old_request in parent_requests:
            # Check validator task exists and matches
            validator_task = await prisma.validatortask.find_unique(
                where={"id": old_request.id},
                include={
                    "completions": {"include": {"Criterion": True}},
                    "ground_truth": True,
                },
            )

            if not validator_task:
                errors_found = True
                print(f"\n❌ Validator task missing for request {old_request.id}")
                continue

            # Check completions count matches
            old_completions = old_request.completions or []
            new_completions = validator_task.completions or []
            if (
                len(old_completions)
                and len(new_completions)
                and len(old_completions) != len(new_completions)
            ):
                errors_found = True
                print(f"\n❌ Completion count mismatch for task {old_request.id}")
                print(
                    f"   Expected: {len(old_completions)}, Found: {len(new_completions)}"
                )

            # Check ground truths count matches
            old_ground_truths = old_request.ground_truths or []
            new_ground_truths = validator_task.ground_truth or []
            if (
                len(old_ground_truths)
                and len(new_ground_truths)
                and len(old_ground_truths) != len(new_ground_truths)
            ):
                errors_found = True
                print(f"\n❌ Ground truth count mismatch for task {old_request.id}")
                print(
                    f"   Expected: {len(old_ground_truths)}, Found: {len(new_ground_truths)}"
                )

            # Get child requests for this parent
            child_requests = await prisma.feedback_request_model.find_many(
                where={"parent_id": old_request.id}, include={"completions": True}
            )

            for child_request in child_requests:
                if not (child_request.dojo_task_id and child_request.hotkey):
                    continue

                # Check miner response exists
                miner_response = await prisma.minerresponse.find_first(
                    where={
                        "validator_task_id": validator_task.id,
                        "dojo_task_id": child_request.dojo_task_id,
                        "hotkey": child_request.hotkey,
                    },
                    include={"scores": True},
                )

                if not miner_response:
                    errors_found = True
                    print(
                        f"\n❌ Miner response missing for child request {child_request.id}"
                    )
                    print(f"   Validator Task: {validator_task.id}")
                    print(f"   Dojo Task: {child_request.dojo_task_id}")
                    print(f"   Hotkey: {child_request.hotkey}")

                    # Try finding any miner response with this dojo_task_id and hotkey
                    any_response = await prisma.minerresponse.find_first(
                        where={
                            "dojo_task_id": child_request.dojo_task_id,
                            "hotkey": child_request.hotkey,
                        }
                    )
                    if any_response:
                        print(
                            f"   Found with different validator_task_id: {any_response.validator_task_id}"
                        )
                    continue

                # Validate scores
                total_criteria = sum(
                    len(completion.Criterion or [])
                    for completion in (validator_task.completions or [])
                )
                miner_scores = miner_response.scores or []

                if (
                    len(miner_scores)
                    and total_criteria
                    and len(miner_scores) != total_criteria
                ):
                    errors_found = True
                    print(
                        f"\n❌ Score count mismatch for miner response {miner_response.id}"
                    )
                    print(f"   Expected: {total_criteria}, Found: {len(miner_scores)}")
                    continue

                # Validate task result values match old completion scores
                try:
                    task_result = json.loads(miner_response.task_result)
                    if task_result.get("type") != "score":
                        errors_found = True
                        print(
                            f"\n❌ Invalid task result type for miner response {miner_response.id}"
                        )
                        print(f"   Task Result Type: {task_result.get('type')}")
                        continue

                    value_dict = task_result.get("value", {})
                    if not isinstance(value_dict, dict):
                        errors_found = True
                        print(
                            f"\n❌ Invalid task result value format for {miner_response.id}"
                        )
                        continue

                    # Get parent request with ground truths
                    old_parent = await prisma.feedback_request_model.find_unique(
                        where={"id": old_request.id}, include={"ground_truths": True}
                    )

                    if not old_parent or not old_parent.ground_truths:
                        errors_found = True
                        print(
                            f"\n❌ Cannot find parent request ground truths for {old_request.id}"
                        )
                        continue

                    # Check scores match for each ground truth
                    for ground_truth in old_parent.ground_truths:
                        # Find old completion response for this ground truth
                        old_completion = next(
                            (
                                comp
                                for comp in child_request.completions or []
                                if comp.model == ground_truth.real_model_id
                            ),
                            None,
                        )

                        if old_completion:
                            new_score = value_dict.get(ground_truth.real_model_id)
                            if new_score != old_completion.score:
                                errors_found = True
                                print(
                                    f"\n❌ Score mismatch for miner response {miner_response.id}"
                                )
                                print(
                                    f"   Ground Truth Model: {ground_truth.real_model_id}"
                                )
                                print(
                                    f"   Expected: {old_completion.score}, Found: {new_score}"
                                )
                except Exception as e:
                    errors_found = True
                    print(f"\n❌ Error validating scores: {str(e)}")

        if not errors_found:
            print("\n✅ All validations passed!")
        else:
            print("\n⚠️  Validation completed with errors")

    except Exception as e:
        print(f"\n❌ Validation failed: {str(e)}")


if __name__ == "__main__":

    async def main():
        await connect_db()
        await validate_migration()
        await disconnect_db()

    asyncio.run(main())
