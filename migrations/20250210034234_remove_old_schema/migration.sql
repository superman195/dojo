/*
  Warnings:

  - You are about to drop the `Completion_Response_Model` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `Criteria_Type_Model` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `Feedback_Request_Model` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `Ground_Truth_Model` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `Score_Model` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropForeignKey
ALTER TABLE "Completion_Response_Model" DROP CONSTRAINT "Completion_Response_Model_feedback_request_id_fkey";

-- DropForeignKey
ALTER TABLE "Criteria_Type_Model" DROP CONSTRAINT "Criteria_Type_Model_feedback_request_id_fkey";

-- DropForeignKey
ALTER TABLE "Feedback_Request_Model" DROP CONSTRAINT "Feedback_Request_Model_parent_id_fkey";

-- DropForeignKey
ALTER TABLE "Ground_Truth_Model" DROP CONSTRAINT "Ground_Truth_Model_feedback_request_id_fkey";

-- DropTable
DROP TABLE "Completion_Response_Model";

-- DropTable
DROP TABLE "Criteria_Type_Model";

-- DropTable
DROP TABLE "Feedback_Request_Model";

-- DropTable
DROP TABLE "Ground_Truth_Model";

-- DropTable
DROP TABLE "Score_Model";
