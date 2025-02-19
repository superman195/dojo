from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Double,
    Enum,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP

metadata = MetaData()


t_Feedback_Request_Model = Table(
    "Feedback_Request_Model",
    metadata,
    Column("id", Text, primary_key=True),
    Column("request_id", Text, nullable=False),
    Column("prompt", Text, nullable=False),
    Column("task_type", Text, nullable=False),
    Column("is_processed", Boolean, nullable=False, server_default=text("false")),
    Column("dojo_task_id", Text),
    Column("hotkey", Text, nullable=False),
    Column("expire_at", TIMESTAMP(precision=3), nullable=False),
    Column(
        "created_at",
        TIMESTAMP(precision=3),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column("updated_at", TIMESTAMP(precision=3), nullable=False),
    Column("parent_id", Text),
    ForeignKeyConstraint(
        ["parent_id"],
        ["Feedback_Request_Model.id"],
        ondelete="SET NULL",
        onupdate="CASCADE",
        name="Feedback_Request_Model_parent_id_fkey",
    ),
    PrimaryKeyConstraint("id", name="Feedback_Request_Model_pkey"),
    Index("Feedback_Request_Model_id_key", "id", unique=True),
    Index("Feedback_Request_Model_parent_id_idx", "parent_id"),
    Index(
        "Feedback_Request_Model_request_id_hotkey_key",
        "request_id",
        "hotkey",
        unique=True,
    ),
)

t_Score_Model = Table(
    "Score_Model",
    metadata,
    Column("id", Text, primary_key=True),
    Column("score", JSONB, nullable=False),
    Column(
        "created_at",
        TIMESTAMP(precision=3),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column("updated_at", TIMESTAMP(precision=3), nullable=False),
    PrimaryKeyConstraint("id", name="Score_Model_pkey"),
)

t__prisma_migrations = Table(
    "_prisma_migrations",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("checksum", String(64), nullable=False),
    Column("finished_at", DateTime(True)),
    Column("migration_name", String(255), nullable=False),
    Column("logs", Text),
    Column("rolled_back_at", DateTime(True)),
    Column("started_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("applied_steps_count", Integer, nullable=False, server_default=text("0")),
    PrimaryKeyConstraint("id", name="_prisma_migrations_pkey"),
)

t_validator_task = Table(
    "validator_task",
    metadata,
    Column("id", Text, primary_key=True),
    Column("previous_task_id", Text),
    Column("prompt", Text, nullable=False),
    Column(
        "task_type",
        Enum(
            "CODE_GENERATION", "TEXT_TO_IMAGE", "TEXT_TO_THREE_D", name="TaskTypeEnum"
        ),
        nullable=False,
    ),
    Column("is_processed", Boolean, nullable=False, server_default=text("false")),
    Column("expire_at", TIMESTAMP(precision=3), nullable=False),
    Column(
        "created_at",
        TIMESTAMP(precision=3),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column("updated_at", TIMESTAMP(precision=3), nullable=False),
    Column("metadata", JSONB),
    ForeignKeyConstraint(
        ["previous_task_id"],
        ["validator_task.id"],
        ondelete="SET NULL",
        onupdate="CASCADE",
        name="validator_task_previous_task_id_fkey",
    ),
    PrimaryKeyConstraint("id", name="validator_task_pkey"),
    Index("validator_task_expire_at_idx", "expire_at"),
    Index("validator_task_id_key", "id", unique=True),
    Index("validator_task_task_type_is_processed_idx", "task_type", "is_processed"),
)

t_Completion_Response_Model = Table(
    "Completion_Response_Model",
    metadata,
    Column("id", Text, primary_key=True),
    Column("completion_id", Text, nullable=False),
    Column("model", Text, nullable=False),
    Column("completion", JSONB, nullable=False),
    Column("rank_id", Integer),
    Column("score", Double(53)),
    Column(
        "created_at",
        TIMESTAMP(precision=3),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column("updated_at", TIMESTAMP(precision=3), nullable=False),
    Column("feedback_request_id", Text, nullable=False),
    ForeignKeyConstraint(
        ["feedback_request_id"],
        ["Feedback_Request_Model.id"],
        ondelete="RESTRICT",
        onupdate="CASCADE",
        name="Completion_Response_Model_feedback_request_id_fkey",
    ),
    PrimaryKeyConstraint("id", name="Completion_Response_Model_pkey"),
)

t_Criteria_Type_Model = Table(
    "Criteria_Type_Model",
    metadata,
    Column("id", Text, primary_key=True),
    Column(
        "type",
        Enum(
            "RANKING_CRITERIA",
            "MULTI_SCORE",
            "SCORE",
            "MULTI_SELECT",
            name="CriteriaTypeEnum",
        ),
        nullable=False,
    ),
    Column("options", JSONB, nullable=False),
    Column("min", Double(53)),
    Column("max", Double(53)),
    Column("feedback_request_id", Text),
    Column(
        "created_at",
        TIMESTAMP(precision=3),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column("updated_at", TIMESTAMP(precision=3), nullable=False),
    ForeignKeyConstraint(
        ["feedback_request_id"],
        ["Feedback_Request_Model.id"],
        ondelete="SET NULL",
        onupdate="CASCADE",
        name="Criteria_Type_Model_feedback_request_id_fkey",
    ),
    PrimaryKeyConstraint("id", name="Criteria_Type_Model_pkey"),
)

t_Ground_Truth_Model = Table(
    "Ground_Truth_Model",
    metadata,
    Column("id", Text, primary_key=True),
    Column("request_id", Text, nullable=False),
    Column("obfuscated_model_id", Text, nullable=False),
    Column("real_model_id", Text, nullable=False),
    Column("rank_id", Integer, nullable=False),
    Column("feedback_request_id", Text, nullable=False),
    Column(
        "created_at",
        TIMESTAMP(precision=3),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column("updated_at", TIMESTAMP(precision=3), nullable=False),
    ForeignKeyConstraint(
        ["feedback_request_id"],
        ["Feedback_Request_Model.id"],
        ondelete="RESTRICT",
        onupdate="CASCADE",
        name="Ground_Truth_Model_feedback_request_id_fkey",
    ),
    PrimaryKeyConstraint("id", name="Ground_Truth_Model_pkey"),
    Index(
        "Ground_Truth_Model_request_id_obfuscated_model_id_rank_id_key",
        "request_id",
        "obfuscated_model_id",
        "rank_id",
        unique=True,
    ),
)

t_completion = Table(
    "completion",
    metadata,
    Column("id", Text, primary_key=True),
    Column("completion_id", Text, nullable=False),
    Column("validator_task_id", Text, nullable=False),
    Column("model", Text, nullable=False),
    Column("completion", JSONB, nullable=False),
    Column(
        "created_at",
        TIMESTAMP(precision=3),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column("updated_at", TIMESTAMP(precision=3), nullable=False),
    ForeignKeyConstraint(
        ["validator_task_id"],
        ["validator_task.id"],
        ondelete="RESTRICT",
        onupdate="CASCADE",
        name="completion_validator_task_id_fkey",
    ),
    PrimaryKeyConstraint("id", name="completion_pkey"),
)

t_ground_truth = Table(
    "ground_truth",
    metadata,
    Column("id", Text, primary_key=True),
    Column("validator_task_id", Text, nullable=False),
    Column("obfuscated_model_id", Text, nullable=False),
    Column("real_model_id", Text, nullable=False),
    Column("rank_id", Integer, nullable=False),
    Column("ground_truth_score", Double(53), nullable=False),
    Column(
        "created_at",
        TIMESTAMP(precision=3),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column("updated_at", TIMESTAMP(precision=3), nullable=False),
    ForeignKeyConstraint(
        ["validator_task_id"],
        ["validator_task.id"],
        ondelete="RESTRICT",
        onupdate="CASCADE",
        name="ground_truth_validator_task_id_fkey",
    ),
    PrimaryKeyConstraint("id", name="ground_truth_pkey"),
    Index(
        "ground_truth_validator_task_id_obfuscated_model_id_rank_id_key",
        "validator_task_id",
        "obfuscated_model_id",
        "rank_id",
        unique=True,
    ),
)

t_miner_response = Table(
    "miner_response",
    metadata,
    Column("id", Text, primary_key=True),
    Column("validator_task_id", Text, nullable=False),
    Column("dojo_task_id", Text, nullable=False),
    Column("hotkey", Text, nullable=False),
    Column("coldkey", Text, nullable=False),
    Column("task_result", JSONB, nullable=False),
    Column(
        "created_at",
        TIMESTAMP(precision=3),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column("updated_at", TIMESTAMP(precision=3), nullable=False),
    ForeignKeyConstraint(
        ["validator_task_id"],
        ["validator_task.id"],
        ondelete="RESTRICT",
        onupdate="CASCADE",
        name="miner_response_validator_task_id_fkey",
    ),
    PrimaryKeyConstraint("id", name="miner_response_pkey"),
    Index("miner_response_dojo_task_id_idx", "dojo_task_id"),
    Index("miner_response_id_key", "id", unique=True),
    Index(
        "miner_response_validator_task_id_dojo_task_id_hotkey_key",
        "validator_task_id",
        "dojo_task_id",
        "hotkey",
        unique=True,
    ),
)

t_criterion = Table(
    "criterion",
    metadata,
    Column("id", Text, primary_key=True),
    Column("completion_id", Text, nullable=False),
    Column(
        "criteria_type",
        Enum(
            "RANKING_CRITERIA",
            "MULTI_SCORE",
            "SCORE",
            "MULTI_SELECT",
            name="CriteriaTypeEnum",
        ),
        nullable=False,
    ),
    Column("config", JSONB, nullable=False),
    Column(
        "created_at",
        TIMESTAMP(precision=3),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column("updated_at", TIMESTAMP(precision=3), nullable=False),
    ForeignKeyConstraint(
        ["completion_id"],
        ["completion.id"],
        ondelete="CASCADE",
        onupdate="CASCADE",
        name="criterion_completion_id_fkey",
    ),
    PrimaryKeyConstraint("id", name="criterion_pkey"),
)

t_miner_score = Table(
    "miner_score",
    metadata,
    Column("id", Text, primary_key=True),
    Column("criterion_id", Text, nullable=False),
    Column("miner_response_id", Text, nullable=False),
    Column("scores", JSONB, nullable=False),
    Column(
        "created_at",
        TIMESTAMP(precision=3),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column("updated_at", TIMESTAMP(precision=3), nullable=False),
    ForeignKeyConstraint(
        ["criterion_id"],
        ["criterion.id"],
        ondelete="CASCADE",
        onupdate="CASCADE",
        name="miner_score_criterion_id_fkey",
    ),
    ForeignKeyConstraint(
        ["miner_response_id"],
        ["miner_response.id"],
        ondelete="CASCADE",
        onupdate="CASCADE",
        name="miner_score_miner_response_id_fkey",
    ),
    PrimaryKeyConstraint("id", name="miner_score_pkey"),
    Index(
        "miner_score_criterion_id_miner_response_id_key",
        "criterion_id",
        "miner_response_id",
        unique=True,
    ),
)
