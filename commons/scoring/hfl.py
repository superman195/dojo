from collections import defaultdict

from bittensor.utils.btlogging import logging as logger

from commons.orm import ORM
from database.client import prisma
from database.prisma.enums import HFLStatusEnum
from database.prisma.models import Completion, HFLState, MinerScore, ValidatorTask
from dojo.protocol import Scores


# TODO: type annotation
def calc_sf_score(
    hotkey_to_tf_score: dict[str, float], validator_task: ValidatorTask
) -> dict[str, float]:
    # ensure completion ordering
    validator_task = ensure_miner_response_order(validator_task)
    # TODO: we cannot do this because of lacking ground truth for SF task
    # miner_synapses = Scoring.calculate_score(
    #     validator_task=map_validator_task_to_task_synapse_object(validator_task),
    #     miner_responses=[
    #         map_miner_response_to_task_synapse_object(
    #             miner_response=mr, validator_task=validator_task
    #         )
    #         for mr in validator_task.miner_responses or []
    #     ],
    # )
    # TODO: properly unpack, see `validator._score_task`
    return {}


async def score_hfl_tasks() -> dict[str, float]:
    """MAIN DRIVER FUNCTION TO SCORE HFL TASKS"""
    sf_tasks = await ORM.get_sf_tasks_by_status(status=HFLStatusEnum.SF_COMPLETED)

    # average across a few?
    hotkey_to_score: dict[str, float] = defaultdict(float)
    # TODO: tune this
    TF_WEIGHT = 0.7
    SF_WEIGHT = 0.3
    for task in sf_tasks:
        # NOTE: we can only determine the score for a TF_TASK after SF_TASK is
        # completed by comparing the increment/decrement in the ratings from miners
        hotkey_to_tf_score = await calc_tf_score(task)
        if not hotkey_to_tf_score:
            raise ValueError(f"Failed to calculate TF score for task {task.id}")
        hotkey_to_sf_score = calc_sf_score(hotkey_to_tf_score, task)

        for hotkey, tf_score in hotkey_to_tf_score.items():
            hotkey_to_score[hotkey] = (
                TF_WEIGHT * tf_score + SF_WEIGHT * hotkey_to_sf_score.get(hotkey, 0.0)
            )

    return dict(hotkey_to_score)


def validate_task(task: ValidatorTask, hfl_state: HFLState) -> None:
    """validate_task.

    Args:
        task (ValidatorTask): task
        hfl_state (HFLState): hfl_state
    """
    if not hfl_state.ValidatorTask or hfl_state.status == HFLStatusEnum.SF_COMPLETED:
        raise Exception("HFL State not ready for scoring")
    if not task.completions:
        raise Exception("Task completions not found")

    sf_task_id = task.id
    tf_task_id = task.previous_task_id
    if not tf_task_id:
        raise ValueError(
            f"Previous task id should be filled for SF_TASK, task id: {sf_task_id}"
        )


async def calc_change_in_scores(
    sf_task: ValidatorTask,
) -> tuple[dict[str, float], dict[str, Completion]]:
    """calc_change_in_scores.

    Args:
        sf_task (ValidatorTask): sf_task

    Returns:
        tuple[dict[str, float], dict[str, Completion]]: Returns the change in scores for each completion id of the SF Task.
            Also returns the mapping of SF completion id to TF completion.
    """
    parent_task = await ORM.get_original_or_parent_sf_task(sf_task.id)
    if parent_task is None:
        raise ValueError("Parent task not found")

    # Create a mapping of completion IDs to their order in task.completions
    parent_task_scores = await _calc_avg_score_by_completion_id(parent_task)
    # NOTE: this is not for sf_scoring itself, but for comparing the increments
    sf_task_scores = await _calc_avg_score_by_completion_id(sf_task)

    # NOTE: calculate increase/decrease per completion id
    # we require the mapping of sf_cid to tf_cid here!
    sf_cid_to_scores_delta: dict[str, float] = {}
    tf_task_id = sf_task.previous_task_id
    if not tf_task_id:
        raise ValueError(
            f"Previous task id should be filled for SF_TASK, task id: {sf_task.id}, previous task id: {tf_task_id}"
        )
    sf_cid_to_tf_completion = await get_tf_to_sf_completion_mapping(sf_task)
    for sf_cid, sf_score in sf_task_scores.items():
        tf_completion = sf_cid_to_tf_completion[sf_cid]
        # NOTE: here we allow for negative scores, so that bad feedback will be penalised too
        # positive means improvement, negative means regression
        diff = sf_score - parent_task_scores[tf_completion.completion_id]
        sf_cid_to_scores_delta[sf_cid] = float(diff)

    return sf_cid_to_scores_delta, sf_cid_to_tf_completion


async def calc_tf_score(sf_task: ValidatorTask):
    """Use a completed SF_TASK to determine the TF_TASK score"""
    sf_task_id = sf_task.id
    try:
        if (
            hfl_state := await ORM.get_hfl_state_by_current_task_id(sf_task_id)
        ) is not None:
            validate_task(sf_task, hfl_state)
    except Exception as e:
        logger.error(f"Error validating task: {e}")
        return

    ### CALC CHANGE IN SCORES
    sf_cid_to_scores_delta, sf_cid_to_tf_completion = await calc_change_in_scores(
        sf_task
    )
    ### CALC CHANGE IN SCORES

    #### CALCULATE TF SCORES BASED ON SF
    # TODO: score miner responses for tf_task
    hotkey_to_tf_score: dict[str, float] = defaultdict(float)

    # the previous task's completion
    # 1. for each sf task completion, get the corresponding TF completion
    # 2. based on the tf completion, grab the task
    # 3. for that TF task, grab the miner's hotkey
    # 4. assign score based on increment/decrement
    for completion in sf_task.completions or []:
        # get tf completion
        tf_completion = sf_cid_to_tf_completion.get(completion.completion_id)
        if not tf_completion:
            logger.error(
                f"TF completion not found for SF completion {completion.completion_id}"
            )
            continue

        if not tf_completion.validator_task_relation:
            logger.error(
                f"TF completion {tf_completion.id} has no validator task relation"
            )
            continue

        miner_hotkeys = [
            mr.hotkey
            for mr in tf_completion.validator_task_relation.miner_responses or []
        ]

        for hotkey in miner_hotkeys:
            # NOTE: for each miner hotkey, we take their score to be the sum of all increments/decrements
            hotkey_to_tf_score[hotkey] += sf_cid_to_scores_delta[
                completion.completion_id
            ]
            logger.info(f"Hotkey: {hotkey}, got TF score: {hotkey_to_tf_score[hotkey]}")

    #### CALCULATE TF SCORES BASED ON SF
    return dict(hotkey_to_tf_score)


async def get_tf_to_sf_completion_mapping(
    sf_task: ValidatorTask,
) -> dict[str, Completion]:
    if (sf_completion_ids := [comp.id for comp in sf_task.completions or []]) == []:
        logger.error(
            "No SF completions found, therefore unable to map TF to SF task completions"
        )
        return {}

    # NOTE: this is based on the initial design that each TF_TASK only has 1 output (a.k.a. completion)
    sf_cid_to_tf_completion: dict[str, Completion] = {}
    try:
        completion_relation = await prisma.hflcompletionrelation.find_many(
            where={"sf_completion_id": {"in": sf_completion_ids}}
        )
        tf_completions = await prisma.completion.find_many(
            where={
                "id": {"in": [comp.tf_completion_id for comp in completion_relation]}
            }
        )
        # map sf completion id to tf completion
        for comp in tf_completions:
            sf_cid_to_tf_completion[comp.id] = comp

        return sf_cid_to_tf_completion
    except Exception as e:
        logger.error(f"Error getting TF to SF completion mapping: {e}")
        pass

    return {}


def ensure_miner_response_order(validator_task: ValidatorTask) -> ValidatorTask:
    """Ensure that the order of miner responses matches the order of completions in the task"""
    if not validator_task.miner_responses:
        raise ValueError("Task must have miner responses for scoring")

    completion_order: dict[str, int] = {
        comp.id: idx
        for idx, comp in enumerate(validator_task.completions)  # type: ignore
    }

    # For each miner response, sort completions in-place based on validator_task.completions order
    for miner_response in validator_task.miner_responses:
        if not miner_response.scores:
            raise ValueError("Miner response must have scores for scoring")

        # Sort miner scores based on order from validator's completions
        def get_order(score: MinerScore) -> int:
            if not score.criterion_relation:
                raise ValueError(
                    f"Miner score {score.id} has no criterion relation, you must fetch criterion relation"
                )

            completion_id = score.criterion_relation.completion_id
            if completion_id not in completion_order:
                raise ValueError(
                    f"Completion ID {completion_id} not found in task completions"
                )
            return completion_order[completion_id]

        miner_response.scores.sort(key=get_order)
    return validator_task


async def _calc_avg_score_by_completion_id(task: ValidatorTask) -> dict[str, float]:
    if not task or not task.completions:
        raise ValueError(f"Task with id: {task.id} must have completions for scoring")
    if not task.miner_responses:
        raise ValueError(
            f"Task with id: {task.id} must have miner responses for scoring"
        )

    task = ensure_miner_response_order(task)

    # calculate the average score per completion
    # Create a dictionary to store the sum of scores and the count of scores for each completion
    stats_by_completion_id = {}

    for miner_response in task.miner_responses or []:
        if miner_response.scores is None:
            logger.error(f"Miner response {miner_response.id} has no scores")
            continue
        try:
            for score in miner_response.scores or []:
                if not score.criterion_relation:
                    logger.error(f"Score {score.id} has no criterion relation")
                    continue

                completion_id = score.criterion_relation.completion_id

                scores = Scores.model_validate_json(score.scores)

                if completion_id not in stats_by_completion_id:
                    stats_by_completion_id[completion_id] = {"sum": 0, "count": 0}

                stats_by_completion_id[completion_id]["sum"] += scores.raw_score
                stats_by_completion_id[completion_id]["count"] += 1
        except Exception as e:
            logger.error(f"Error calculating average score: {e}")
            continue

    # Calculate the average score for each completion
    # TODO: fix pyright errors
    avg_score_by_completion_id: dict[str, float] = {}
    for completion_id, scores in stats_by_completion_id.items():
        average_score = scores["sum"] / scores["count"]
        avg_score_by_completion_id[completion_id] = average_score
        logger.info(f"Completion {completion_id}: Average score = {average_score}")

    return avg_score_by_completion_id
