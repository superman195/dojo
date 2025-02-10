import asyncio
import json
from typing import AsyncGenerator

from loguru import logger

from database.client import connect_db, disconnect_db, prisma
from database.prisma import Json
from database.prisma.models import ValidatorTask
from database.prisma.types import (
    CriterionWhereInput,
    MinerScoreUpdateInput,
    ValidatorTaskWhereInput,
)
from dojo.protocol import Scores
from dojo.utils.config import source_dotenv

source_dotenv()


# 1. for each record from `miner_response` find the corresponding record from `Completion_Response_Model`
# 2. gather all scores from multiple `Completion_Response_Model` records and fill the relevant records inside `miner_response.scores`
# 3. based on all the raw scores provided by miners, apply the `Scoring.score_by_criteria` to calculate the scores for each criterion
async def main():
    await connect_db()
    async for validator_tasks, has_more_batches in get_processed_tasks():
        for task in validator_tasks:
            if not task.miner_responses:
                logger.warning("No miner responses for task, skipping")
                continue

            for miner_response in task.miner_responses:
                scores = miner_response.scores
                if scores is not None or scores:
                    logger.info(
                        f"Scores data exists for miner response: {miner_response.id}, skipping"
                    )
                else:
                    logger.warning(
                        "No scores for miner response, attempting to fill from old tables"
                    )

                # find the scores from old tables
                feedback_request = await prisma.feedback_request_model.find_first(
                    where={
                        "id": task.id,
                        "hotkey": miner_response.hotkey,
                    }
                )
                assert (
                    feedback_request is not None
                ), "Feedback request id should not be None"
                completions = await prisma.completion_response_model.find_many(
                    where={"feedback_request_id": feedback_request.id}
                )

                async with prisma.tx() as tx:
                    for completion in completions:
                        # Find or create the criterion record
                        criterion = await tx.criterion.find_first(
                            where=CriterionWhereInput(
                                {
                                    "completion_relation": {
                                        "is": {
                                            "completion_id": completion.completion_id,
                                            "validator_task_id": task.id,
                                        }
                                    }
                                }
                            )
                        )

                    if not criterion:
                        logger.warning(
                            "Criterion not found, but it should already exist"
                        )
                        continue

                    # the basics, just create raw scores
                    scores = Scores(
                        raw_score=completion.score,
                        rank_id=completion.rank_id,
                        # Initialize other scores as None - they'll be computed later
                        normalised_score=None,
                        ground_truth_score=None,
                        cosine_similarity_score=None,
                        normalised_cosine_similarity_score=None,
                        cubic_reward_score=None,
                    )

                    await tx.minerscore.update(
                        where={
                            "criterion_id_miner_response_id": {
                                "criterion_id": criterion.id,
                                "miner_response_id": miner_response.id,
                            }
                        },
                        data=MinerScoreUpdateInput(
                            scores=Json(json.dumps(scores.model_dump()))
                        ),
                    )

        if not has_more_batches:
            logger.info("No more task batches to process")
            break
    await disconnect_db()


async def get_processed_tasks(
    batch_size: int = 10,
) -> AsyncGenerator[tuple[list[ValidatorTask], bool], None]:
    vali_where_query = ValidatorTaskWhereInput(
        {
            "is_processed": True,
        }
    )
    num_processed_tasks = await prisma.validatortask.count(where=vali_where_query)

    for skip in range(0, num_processed_tasks, batch_size):
        validator_tasks = await prisma.validatortask.find_many(
            skip=skip,
            take=batch_size,
            where=vali_where_query,
            include={
                "completions": {"include": {"criterion": True}},
                "ground_truth": True,
                "miner_responses": {
                    "include": {
                        "scores": True,
                    }
                },
            },
        )
        yield validator_tasks, skip + batch_size < num_processed_tasks

    yield [], False


if __name__ == "__main__":
    asyncio.run(main())
