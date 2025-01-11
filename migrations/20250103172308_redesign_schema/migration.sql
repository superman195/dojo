-- CreateEnum
CREATE TYPE "TaskTypeEnum" AS ENUM ('CODE_GENERATION', 'TEXT_TO_IMAGE', 'TEXT_TO_THREE_D');

-- CreateTable
CREATE TABLE "validator_task" (
    "id" TEXT NOT NULL,
    "previous_task_id" TEXT,
    "prompt" TEXT NOT NULL,
    "task_type" "TaskTypeEnum" NOT NULL,
    "is_processed" BOOLEAN NOT NULL DEFAULT false,
    "expire_at" TIMESTAMP(3) NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "validator_task_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "miner_response" (
    "id" TEXT NOT NULL,
    "validator_task_id" TEXT NOT NULL,
    "dojo_task_id" TEXT NOT NULL,
    "hotkey" TEXT NOT NULL,
    "coldkey" TEXT NOT NULL,
    "task_result" JSONB NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "miner_response_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "completion" (
    "id" TEXT NOT NULL,
    "validator_task_id" TEXT NOT NULL,
    "model" TEXT NOT NULL,
    "completion" JSONB NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "completion_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "criterion" (
    "id" TEXT NOT NULL,
    "completion_id" TEXT NOT NULL,
    "criteria_type" "CriteriaTypeEnum" NOT NULL,
    "config" JSONB NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "criterion_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "score" (
    "id" TEXT NOT NULL,
    "criterion_id" TEXT NOT NULL,
    "miner_response_id" TEXT NOT NULL,
    "scores" JSONB NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "score_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ground_truth" (
    "id" TEXT NOT NULL,
    "validator_task_id" TEXT NOT NULL,
    "obfuscated_model_id" TEXT NOT NULL,
    "real_model_id" TEXT NOT NULL,
    "rank_id" INTEGER NOT NULL,
    "ground_truth_score" DOUBLE PRECISION NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ground_truth_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "validator_task_id_key" ON "validator_task"("id");

-- CreateIndex
CREATE INDEX "validator_task_task_type_is_processed_idx" ON "validator_task"("task_type", "is_processed");

-- CreateIndex
CREATE INDEX "validator_task_expire_at_idx" ON "validator_task"("expire_at");

-- CreateIndex
CREATE UNIQUE INDEX "miner_response_id_key" ON "miner_response"("id");

-- CreateIndex
CREATE UNIQUE INDEX "score_criterion_id_miner_response_id_key" ON "score"("criterion_id", "miner_response_id");

-- CreateIndex
CREATE UNIQUE INDEX "ground_truth_validator_task_id_obfuscated_model_id_rank_id_key" ON "ground_truth"("validator_task_id", "obfuscated_model_id", "rank_id");

-- AddForeignKey
ALTER TABLE "validator_task" ADD CONSTRAINT "validator_task_previous_task_id_fkey" FOREIGN KEY ("previous_task_id") REFERENCES "validator_task"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "miner_response" ADD CONSTRAINT "miner_response_validator_task_id_fkey" FOREIGN KEY ("validator_task_id") REFERENCES "validator_task"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "completion" ADD CONSTRAINT "completion_validator_task_id_fkey" FOREIGN KEY ("validator_task_id") REFERENCES "validator_task"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "criterion" ADD CONSTRAINT "criterion_completion_id_fkey" FOREIGN KEY ("completion_id") REFERENCES "completion"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "score" ADD CONSTRAINT "score_criterion_id_fkey" FOREIGN KEY ("criterion_id") REFERENCES "criterion"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "score" ADD CONSTRAINT "score_miner_response_id_fkey" FOREIGN KEY ("miner_response_id") REFERENCES "miner_response"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ground_truth" ADD CONSTRAINT "ground_truth_validator_task_id_fkey" FOREIGN KEY ("validator_task_id") REFERENCES "validator_task"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- Add migration data transfer statements
-- 1. Migrate ValidatorTasks
INSERT INTO validator_task (
    id, prompt, task_type, is_processed, expire_at, created_at, updated_at
)
SELECT
    id,
    prompt,
    CASE
        WHEN LOWER(task_type) LIKE '%image%' THEN 'TEXT_TO_IMAGE'::TaskTypeEnum
        WHEN LOWER(task_type) LIKE '%3d%' THEN 'TEXT_TO_THREE_D'::TaskTypeEnum
        ELSE 'CODE_GENERATION'::TaskTypeEnum
    END as task_type,
    is_processed,
    expire_at,
    created_at,
    updated_at
FROM "Feedback_Request_Model"
ON CONFLICT (id) DO UPDATE SET
    prompt = EXCLUDED.prompt,
    task_type = EXCLUDED.task_type,
    is_processed = EXCLUDED.is_processed,
    expire_at = EXCLUDED.expire_at,
    updated_at = EXCLUDED.updated_at;

-- 2. Migrate MinerResponses with task_result structure
INSERT INTO miner_response (
    id,
    validator_task_id,
    dojo_task_id,
    hotkey,
    coldkey,
    task_result,
    created_at,
    updated_at
)
SELECT
    gen_random_uuid(),
    frm.id as validator_task_id,
    frm.dojo_task_id,
    frm.hotkey,
    '', -- coldkey will be updated by update_coldkeys.py
    jsonb_build_object(
        'type', 'score',
        'value', (
            SELECT jsonb_object_agg(
                crm.model,
                crm.score
            )
            FROM "Completion_Response_Model" crm
            JOIN "Ground_Truth_Model" gtm ON crm.model = gtm.real_model_id
            WHERE gtm.feedback_request_id = frm.id
        )
    ) as task_result,
    frm.created_at,
    frm.updated_at
FROM "Feedback_Request_Model" frm
WHERE frm.dojo_task_id IS NOT NULL
AND frm.hotkey IS NOT NULL
AND EXISTS (SELECT 1 FROM "Ground_Truth_Model" gt WHERE gt.feedback_request_id = frm.id)
AND EXISTS (SELECT 1 FROM "Completion_Response_Model" cr WHERE cr.feedback_request_id = frm.id)
ON CONFLICT (id) DO UPDATE SET
    task_result = EXCLUDED.task_result,
    updated_at = EXCLUDED.updated_at;

-- 3. Migrate Completions
INSERT INTO completion (
    id,
    completion_id,
    validator_task_id,
    model,
    completion,
    created_at,
    updated_at
)
SELECT
    crm.id,
    crm.completion_id,
    crm.feedback_request_id as validator_task_id,
    crm.model,
    crm.completion,
    crm.created_at,
    crm.updated_at
FROM "Completion_Response_Model" crm
ON CONFLICT (id) DO UPDATE SET
    model = EXCLUDED.model,
    completion = EXCLUDED.completion,
    updated_at = EXCLUDED.updated_at;

-- 4. Migrate Criteria
INSERT INTO criterion (
    id,
    completion_id,
    criteria_type,
    config,
    created_at,
    updated_at
)
SELECT
    gen_random_uuid(),
    c.id as completion_id,
    CASE
        WHEN ctm.type = 'MULTI_SCORE' THEN 'SCORE'::CriteriaTypeEnum
        ELSE ctm.type::CriteriaTypeEnum
    END as criteria_type,
    CASE
        WHEN ctm.type = 'MULTI_SCORE' THEN
            jsonb_build_object('min', ctm.min, 'max', ctm.max)
        ELSE '{}'::jsonb
    END as config,
    ctm.created_at,
    ctm.updated_at
FROM "Criteria_Type_Model" ctm
CROSS JOIN "Completion_Response_Model" c
WHERE ctm.type != 'RANKING_CRITERIA';

-- 5. Migrate GroundTruths with score mapping
INSERT INTO ground_truth (
    id,
    validator_task_id,
    obfuscated_model_id,
    real_model_id,
    rank_id,
    ground_truth_score,
    created_at,
    updated_at
)
SELECT
    gt.id,
    gt.feedback_request_id as validator_task_id,
    gt.obfuscated_model_id,
    gt.real_model_id,
    gt.rank_id,
    CASE gt.rank_id
        WHEN 0 THEN 0.0
        WHEN 1 THEN 0.33333334
        WHEN 2 THEN 0.6666667
        WHEN 3 THEN 1.0
        ELSE 0.0
    END as ground_truth_score,
    gt.created_at,
    gt.updated_at
FROM "Ground_Truth_Model" gt
ON CONFLICT (id) DO UPDATE SET
    obfuscated_model_id = EXCLUDED.obfuscated_model_id,
    real_model_id = EXCLUDED.real_model_id,
    rank_id = EXCLUDED.rank_id,
    ground_truth_score = EXCLUDED.ground_truth_score,
    updated_at = EXCLUDED.updated_at;

-- 6. Create MinerScores
INSERT INTO miner_score (
    id,
    criterion_id,
    miner_response_id,
    scores,
    created_at,
    updated_at
)
SELECT
    gen_random_uuid(),
    c.id as criterion_id,
    mr.id as miner_response_id,
    '{}'::jsonb as scores,
    c.created_at,
    c.updated_at
FROM criterion c
JOIN completion comp ON c.completion_id = comp.id
JOIN miner_response mr ON mr.validator_task_id = comp.validator_task_id
ON CONFLICT (criterion_id, miner_response_id) DO NOTHING;
