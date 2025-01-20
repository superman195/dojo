import asyncio
import json
from datetime import datetime

from database.client import connect_db, disconnect_db, prisma
from dojo.utils.config import source_dotenv

source_dotenv()


async def validate_statistics():
    """Validate overall statistics between old and new tables."""
    print("\n=== Validating Overall Statistics ===")

    # Get counts from old tables
    old_parent_count = await prisma.feedback_request_model.count(
        where={"parent_id": None}
    )
    old_completion_count = await prisma.completion_response_model.count()
    old_ground_truth_count = await prisma.ground_truth_model.count()
    old_child_count = await prisma.feedback_request_model.count(
        where={"parent_id": {"not": ""}}
    )

    # Get counts from new tables
    new_task_count = await prisma.validatortask.count()
    new_completion_count = await prisma.completion.count()
    new_ground_truth_count = await prisma.groundtruth.count()
    new_response_count = await prisma.minerresponse.count()

    print("\nOld Database:")
    print(f"Parent Requests: {old_parent_count:,}")
    print(f"Child Requests: {old_child_count:,}")
    print(f"Completions: {old_completion_count:,}")
    print(f"Ground Truths: {old_ground_truth_count:,}")

    print("\nNew Database:")
    print(f"Validator Tasks: {new_task_count:,}")
    print(f"Miner Responses: {new_response_count:,}")
    print(f"Completions: {new_completion_count:,}")
    print(f"Ground Truths: {new_ground_truth_count:,}")

    errors_found = False
    if old_parent_count != new_task_count:
        errors_found = True
        print("\n❌ Parent request count mismatch")
        print(f"   Expected: {old_parent_count:,}, Found: {new_task_count:,}")

    # Verify completion counts follow 4:1 ratio with parent requests
    expected_completion_count = (
        old_parent_count * 4
    )  # Each parent should have 4 completions
    if new_completion_count != expected_completion_count:
        errors_found = True
        print("\n❌ Completion count mismatch")
        print(f"   Expected: {expected_completion_count:,} (4 per parent request)")
        print(f"   Found: {new_completion_count:,}")
        print(f"   Parent Request Count: {old_parent_count:,}")

    if old_ground_truth_count != new_ground_truth_count:
        errors_found = True
        print("\n❌ Ground truth count mismatch")
        print(
            f"   Expected: {old_ground_truth_count:,}, Found: {new_ground_truth_count:,}"
        )

    return errors_found


async def validate_parent_data(batch_size=1000):
    """Validate all parent requests and their direct relationships."""
    print("\n=== Validating All Parent Requests ===")
    errors_found = False
    processed = 0
    start_time = datetime.now()

    # Get total count first
    total_count = await prisma.feedback_request_model.count(where={"parent_id": None})
    print(
        f"\nValidating {total_count:,} parent requests in batches of {batch_size:,}..."
    )

    # Process in batches
    for skip in range(0, total_count, batch_size):
        # Get batch of parent requests
        parent_requests = await prisma.feedback_request_model.find_many(
            where={"parent_id": None},
            skip=skip,
            take=batch_size,
            include={
                "completions": True,
                "ground_truths": True,
                "criteria_types": True,
            },
        )

        for old_request in parent_requests:
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
            new_completions = validator_task.completions or []
            # Verify 4:1 completion ratio
            expected_completion_count = 4
            if len(new_completions) != expected_completion_count:
                errors_found = True
                print(f"\n❌ Invalid completion ratio for task {old_request.id}")
                print(
                    f"   Expected exactly {expected_completion_count} completions, Found: {len(new_completions)}"
                )

            # Check ground truths count matches
            old_ground_truths = old_request.ground_truths or []
            new_ground_truths = validator_task.ground_truth or []
            if len(old_ground_truths) != len(new_ground_truths):
                errors_found = True
                print(f"\n❌ Ground truth count mismatch for task {old_request.id}")
                print(
                    f"   Expected: {len(old_ground_truths)}, Found: {len(new_ground_truths)}"
                )

            processed += 1
            if processed % 10 == 0 or processed == total_count:
                elapsed = datetime.now() - start_time
                rate = processed / elapsed.total_seconds()
                remaining = (total_count - processed) / rate if rate > 0 else 0
                print(
                    f"\rProgress: {processed:,}/{total_count:,} ({processed/total_count*100:.1f}%) - {rate:.1f} tasks/sec - ETA: {remaining:.0f}s",
                    end="",
                )

    print(
        f"\nCompleted parent validation in {(datetime.now() - start_time).total_seconds():.1f}s"
    )
    return errors_found


async def validate_child_data(sample_percentage=10, batch_size=1000):
    """Validate a sample of child requests and their relationships."""
    print(f"\n=== Validating {sample_percentage}% of Child Requests ===")
    errors_found = False
    processed = 0
    start_time = datetime.now()

    # Get total count of child requests
    total_child_count = await prisma.feedback_request_model.count(
        where={"parent_id": {"not": ""}}
    )

    sample_size = int(total_child_count * sample_percentage / 100)
    print(f"Total child requests: {total_child_count:,}")
    print(f"Validating sample of {sample_size:,} requests in batches of {batch_size:,}")

    # Process in batches
    for skip in range(0, sample_size, batch_size):
        take = min(batch_size, sample_size - skip)

        # Get batch of child requests
        child_requests = await prisma.feedback_request_model.find_many(
            where={"parent_id": {"not": ""}},
            skip=skip,
            take=take,
            include={"completions": True, "parent_request": True},
        )

        for child_request in child_requests:
            if not (
                child_request.dojo_task_id
                and child_request.hotkey
                and child_request.parent_id
            ):
                continue

            # Check miner response exists
            miner_response = await prisma.minerresponse.find_first(
                where={
                    "validator_task_id": child_request.parent_id,
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
                print(f"   Parent Request: {child_request.parent_id}")
                print(f"   Dojo Task: {child_request.dojo_task_id}")
                print(f"   Hotkey: {child_request.hotkey}")
                continue

            # Validate task result values match old completion scores
            try:
                task_result = json.loads(miner_response.task_result)
                if task_result.get("type") != "score":
                    errors_found = True
                    print(
                        f"\n❌ Invalid task result type for miner response {miner_response.id}"
                    )
                    continue

                value_dict = task_result.get("value", {})
                if not isinstance(value_dict, dict):
                    errors_found = True
                    print(
                        f"\n❌ Invalid task result value format for {miner_response.id}"
                    )
                    continue

                # Check scores match for each completion
                for completion in child_request.completions or []:
                    new_score = value_dict.get(completion.model)
                    if new_score != completion.score:
                        errors_found = True
                        print(
                            f"\n❌ Score mismatch for miner response {miner_response.id}"
                        )
                        print(f"   Model: {completion.model}")
                        print(f"   Expected: {completion.score}, Found: {new_score}")

            except Exception as e:
                errors_found = True
                print(f"\n❌ Error validating scores: {str(e)}")

            processed += 1
            if processed % 10 == 0 or processed == sample_size:
                elapsed = datetime.now() - start_time
                rate = processed / elapsed.total_seconds()
                remaining = (sample_size - processed) / rate if rate > 0 else 0
                print(
                    f"\rProgress: {processed:,}/{sample_size:,} ({processed/sample_size*100:.1f}%) - {rate:.1f} tasks/sec - ETA: {remaining:.0f}s",
                    end="",
                )

    print(
        f"\nCompleted child validation in {(datetime.now() - start_time).total_seconds():.1f}s"
    )
    return errors_found


async def run_validation():
    """Run all validation checks."""
    start_time = datetime.now()
    print(f"Starting validation at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        await connect_db()

        # First validate overall statistics
        stats_errors = await validate_statistics()

        # Then validate all parent data
        parent_errors = await validate_parent_data()

        # Finally validate sample of child data
        child_errors = await validate_child_data(sample_percentage=10)

        if not any([stats_errors, parent_errors, child_errors]):
            print("\n✅ All validations passed!")
        else:
            print("\n⚠️  Validation completed with errors")

        total_time = datetime.now() - start_time
        print(f"\nTotal validation time: {total_time.total_seconds():.1f}s")

    except Exception as e:
        print(f"\n❌ Validation failed: {str(e)}")
    finally:
        await disconnect_db()


if __name__ == "__main__":
    asyncio.run(run_validation())
