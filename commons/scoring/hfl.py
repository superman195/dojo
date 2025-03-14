from bittensor.utils.btlogging import logging as logger

from commons.exceptions import HFLStateNotContinuous
from commons.orm import ORM
from database.client import prisma
from database.mappers import (
    _parse_hfl_events,
    map_miner_response_to_task_synapse_object,
    map_validator_task_to_task_synapse_object,
)
from database.prisma.enums import HFLStatusEnum
from database.prisma.models import Completion, HFLState, ValidatorTask


def calc_sf_score(validator_task: ValidatorTask):
    from .scoring import Scoring

    # ensure completion ordering
    validator_task = ensure_miner_response_order(validator_task)
    miner_synapses = Scoring.calculate_score(
        validator_task=map_validator_task_to_task_synapse_object(validator_task),
        miner_responses=[
            map_miner_response_to_task_synapse_object(
                miner_response=mr, validator_task=validator_task
            )
            for mr in validator_task.miner_responses or []
        ],
    )
    # TODO: properly unpack, see `validator._score_task`
    hotkey_to_sf_score = {mr.miner_hotkey: mr.score for mr in miner_synapses}
    return hotkey_to_sf_score


async def score_hfl_tasks():
    sf_tasks = await ORM.get_sf_tasks_by_status(status=HFLStatusEnum.SF_COMPLETED)

    for task in sf_tasks:
        # NOTE: we can only determine the score for a TF_TASK after SF_TASK is
        # completed by comparing the increment/decrement in the ratings from miners
        hotkey_to_tf_score = await calc_tf_score(task)
        hotkey_to_sf_score = calc_sf_score(task)


def validate_task(task, hfl_state):
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


async def calc_tf_score(sf_task: ValidatorTask):
    """Use a completed SF_TASK to determine the TF_TASK score"""
    sf_task_id = sf_task.id
    hfl_state = await ORM.get_hfl_state_by_current_task_id(sf_task_id)
    try:
        validate_task(sf_task, hfl_state)
    except Exception as e:
        logger.error(f"Error validating task: {e}")
        return

    ### CALC CHANGE IN SCORES
    miner_scores = await ORM.get_scores_for_completed_sf(sf_task)
    logger.info(f"Got {miner_scores}")
    parent_task = await ORM.get_original_or_parent_sf_task(sf_task_id)
    if parent_task is None:
        raise ValueError("Parent task not found")

    # Create a mapping of completion IDs to their order in task.completions

    # FIXME: currently no way to link the individual SF_TASK's completion to
    # the previous task's completion
    parent_task_scores = await _calc_avg_score_by_completion_id(parent_task)
    # NOTE: this is not for sf_scoring itself, but for comparing the increments
    sf_task_scores = await _calc_avg_score_by_completion_id(sf_task)

    # calculate differences per completion id
    sf_cid_to_scores_dt: dict[str, float] = {}

    # TODO: select only the cid that was selected from original task / SF_1
    sf_cid = "dummy"
    for sf_cid, sf_score in sf_task_scores.items():
        # positive means improvement, negative means regression
        diff = sf_score - parent_task_scores[sf_cid]
        sf_cid_to_scores_dt[sf_cid] = float(diff)

    ### CALC CHANGE IN SCORES

    #### FINDING MINER RESPONSES TO TF

    # figure out the TF_task that led to SF_task
    # TODO: rely on the previous_task_id ? or the events
    # tf_task_id = get_tf_task_id_for_sf_task(hfl_state, task)

    tf_task_id = sf_task.previous_task_id
    tf_task = await ORM.get_validator_task_by_id(tf_task_id)
    # find miners that responded to the tf task
    #### FINDING MINER RESPONSES TO TF

    sf_cid_to_tf_completion = await get_tf_to_sf_completion_mapping(tf_task, sf_task)  # type: ignore
    #### CALCULATE TF SCORES BASED ON SF
    # TODO: score miner responses for tf_task
    hotkey_to_tf_score: dict[str, float] = {}
    miner_responses = await ORM.get_miner_responses_by_task_id(tf_task_id)
    for response in miner_responses:
        hotkey_to_tf_score[response.hotkey] = 0.0

    # the previous task's completion
    # 1. for each sf task completion, get the corresponding TF completion
    # 2. based on the tf completion, grab the task
    # 3. for that TF task, grab the miner's hotkey
    # 4. assign score
    for completion in sf_task.completions:
        # get tf completion
        tf_completion = sf_cid_to_tf_completion.get(completion.id)
        if not tf_completion:
            logger.error(f"TF completion not found for SF completion {completion.id}")
            continue
        miner_hotkeys = [
            mr.hotkey
            for mr in tf_completion.validator_task_relation.miner_responses or []
        ]

        for hotkey in miner_hotkeys:
            hotkey_to_tf_score[hotkey] += sf_cid_to_scores_dt[completion.id]
            logger.debug(
                f"Hotkey: {hotkey}, got TF score: {hotkey_to_tf_score[hotkey]}"
            )

    #### CALCULATE TF SCORES BASED ON SF
    return hotkey_to_tf_score


async def get_tf_to_sf_completion_mapping(
    tf_task: ValidatorTask, sf_task: ValidatorTask
):
    if (sf_completion_ids := [comp.id for comp in sf_task.completions or []]) == []:
        logger.error("No SF completions found")

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


def get_tf_task_id_for_sf_task(hfl_state: HFLState, task: ValidatorTask):
    events = _parse_hfl_events(hfl_state)
    status = HFLStatusEnum.TF_COMPLETED
    for idx, event in enumerate(events):
        if event.task_id != task.id:
            continue

        # found the SF_TASK, now find the tf task
        sublist = events[:idx]

        for event in reversed(sublist):
            if event.type == status:
                return event.task_id
    raise HFLStateNotContinuous(
        f"Expected to find {status} while processing {HFLStatusEnum.SF_COMPLETED}"
    )


def ensure_miner_response_order(validator_task: ValidatorTask) -> ValidatorTask:
    """Ensure that the order of miner responses matches the order of completions in the task"""
    if not validator_task.miner_responses:
        raise ValueError("Task must have miner responses for scoring")

    completion_order: dict[str, int] = {
        comp.id: idx
        for idx, comp in enumerate(validator_task.completions)  # type: ignore
    }

    # For each miner response, sort completions in-place based on validator_taskgc.completions order
    for miner_response in validator_task.miner_responses:
        # TODO: ensure proper ordering between validator completions
        if not miner_response.scores:
            raise ValueError("Miner response must have scores for scoring")

        # Sort miner scores based on order from validator's completions
        def get_order(score) -> int:
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
        raise ValueError("TF task must have completions for scoring")
    if not task.miner_responses:
        raise ValueError("TF task must have miner responses for scoring")

    task = ensure_miner_response_order(task)

    # calculate the average score per completion
    # Create a dictionary to store the sum of scores and the count of scores for each completion
    completion_scores = {}

    for miner_response in task.miner_responses:
        for completion in miner_response.completion_responses:
            completion_id = completion.completion_id
            score = completion.score
            if completion_id not in completion_scores:
                completion_scores[completion_id] = {"sum": 0, "count": 0}

            completion_scores[completion_id]["sum"] += score
            completion_scores[completion_id]["count"] += 1

    # Calculate the average score for each completion
    prev_task_cid_to_avg_score: dict[str, float] = {}
    for completion_id, scores in completion_scores.items():
        average_score = scores["sum"] / scores["count"]
        prev_task_cid_to_avg_score[completion_id] = average_score
        print(f"Completion {completion_id}: Average score = {average_score}")

    return prev_task_cid_to_avg_score
