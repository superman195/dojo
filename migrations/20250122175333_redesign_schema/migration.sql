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
    "metadata" JSONB,

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
    "completion_id" TEXT NOT NULL,
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
CREATE TABLE "miner_score" (
    "id" TEXT NOT NULL,
    "criterion_id" TEXT NOT NULL,
    "miner_response_id" TEXT NOT NULL,
    "scores" JSONB NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "miner_score_pkey" PRIMARY KEY ("id")
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
CREATE INDEX "miner_response_dojo_task_id_idx" ON "miner_response"("dojo_task_id");

-- CreateIndex
CREATE UNIQUE INDEX "miner_response_validator_task_id_dojo_task_id_hotkey_key" ON "miner_response"("validator_task_id", "dojo_task_id", "hotkey");

-- CreateIndex
CREATE UNIQUE INDEX "miner_score_criterion_id_miner_response_id_key" ON "miner_score"("criterion_id", "miner_response_id");

-- CreateIndex
CREATE UNIQUE INDEX "ground_truth_validator_task_id_obfuscated_model_id_rank_id_key" ON "ground_truth"("validator_task_id", "obfuscated_model_id", "rank_id");

-- CreateIndex
CREATE INDEX "Feedback_Request_Model_parent_id_idx" ON "Feedback_Request_Model"("parent_id");

-- AddForeignKey
ALTER TABLE "validator_task" ADD CONSTRAINT "validator_task_previous_task_id_fkey" FOREIGN KEY ("previous_task_id") REFERENCES "validator_task"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "miner_response" ADD CONSTRAINT "miner_response_validator_task_id_fkey" FOREIGN KEY ("validator_task_id") REFERENCES "validator_task"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "completion" ADD CONSTRAINT "completion_validator_task_id_fkey" FOREIGN KEY ("validator_task_id") REFERENCES "validator_task"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "criterion" ADD CONSTRAINT "criterion_completion_id_fkey" FOREIGN KEY ("completion_id") REFERENCES "completion"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "miner_score" ADD CONSTRAINT "miner_score_criterion_id_fkey" FOREIGN KEY ("criterion_id") REFERENCES "criterion"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "miner_score" ADD CONSTRAINT "miner_score_miner_response_id_fkey" FOREIGN KEY ("miner_response_id") REFERENCES "miner_response"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ground_truth" ADD CONSTRAINT "ground_truth_validator_task_id_fkey" FOREIGN KEY ("validator_task_id") REFERENCES "validator_task"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
