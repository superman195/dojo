import asyncio
from datetime import datetime

import asyncpg
from sqlalchemy import not_, select

from database.client import connect_db, disconnect_db
from database.prisma.models import ValidatorTask
from database.prisma.types import (
    ValidatorTaskInclude,
    ValidatorTaskWhereInput,
)
from database.sqlalchemy.tables import (
    t_completion,
    t_validator_task,
)


def get_validator_tasks_query():
    # query = (
    #     select(t_validator_task)
    #     .join(t_completion, t_validator_task.c.id == t_completion.c.validator_task_id)
    #     .join(t_criterion, t_completion.c.id == t_criterion.c.completion_id)
    #     .join(t_miner_score, t_criterion.c.id == t_miner_score.c.criterion_id)
    #     .join(
    #         t_miner_response,
    #         t_validator_task.c.id == t_miner_response.c.validator_task_id,
    #     )
    #     .join(
    #         t_ground_truth, t_validator_task.c.id == t_ground_truth.c.validator_task_id
    #     )
    #     .where(
    #         and_(
    #             # t_validator_task.c.expire_at > expire_from,
    #             # t_validator_task.c.expire_at < expire_to,
    #             not_(t_validator_task.c.is_processed),
    #         )
    #     )
    #     .order_by(t_validator_task.c.created_at.desc())
    #     .limit(5)
    # )

    # query = (
    #     select(t_validator_task)
    #     .select_from(t_validator_task)  # Explicitly start from validator_task table
    #     .distinct()  # Add distinct to remove duplicates
    #     .where(
    #         not_(t_validator_task.c.is_processed),
    #     )
    #     .order_by(t_validator_task.c.created_at.desc())
    #     .limit(5)
    # )
    query = (
        select(
            # t_validator_task.c.id, t_validator_task.c.created_at,
            t_validator_task,
        )  # Explicitly select columns
        .distinct()  # Add distinct to remove duplicates
        .join(
            t_completion,
            t_validator_task.c.id == t_completion.c.validator_task_id,
            isouter=True,
        )
        .where(
            not_(t_validator_task.c.is_processed),
        )
        .order_by(t_validator_task.c.created_at.desc())
        .limit(5)
    )

    # Compile the query to SQL
    from sqlalchemy.dialects import postgresql

    query_str = query.compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
    )

    return query, query_str


async def get_validator_tasks(conn, expire_from=None, expire_to=None):
    """Execute the validator tasks query and return results.

    Args:
        conn: Database connection
        expire_from: Optional start of expiry timeframe
        expire_to: Optional end of expiry timeframe

    Returns:
        List of validator task records with related data
    """
    query = get_validator_tasks_query()
    result = await conn.execute(query)
    fetched_res = result.fetchall()
    print(type(fetched_res))
    return fetched_res


async def get_session():
    """Create and return a new SQLAlchemy async session.

    Returns:
        AsyncSession: A new SQLAlchemy async session
    """
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(
        "postgresql://postgres:mysecretpasswordlocalhost/db",
        echo=True,
    )

    return engine


# async def main():
# engine = await get_session()
# async with engine.begin() as conn:
#     result = await get_validator_tasks(conn)
# pass


async def test_sqlalchemy():
    start_time = datetime.now()
    # Establish a connection to an existing database named "test"
    # as a "postgres" user.
    # Calculate start and end time for expiry window
    conn = await asyncpg.connect("postgresql://postgres:mysecretpassword@localhost/db")
    # Execute a statement to create a new table.
    query, query_str = get_validator_tasks_query()
    query_str = str(query_str)
    # print(query_str)
    # print(await conn.execute(query_str))

    # Select a row from the table.
    # row = await conn.fetchrow("select * from validator_task limit 5")
    values = await conn.fetch(query_str)
    print(type(values))

    for idx, v in enumerate(values):
        print(f"SQLALchemy got {idx}: {v}")

    ids = [v["id"] for v in values]
    # *row* now contains
    # asyncpg.Record(id=1, name='Bob', dob=datetime.date(1984, 3, 1))
    # print(f"{values=}")
    print(f"SQLAlchemy got records: {ids}")

    # Close the connection.
    await conn.close()
    print("Time taken for SQLAlchemy: ", datetime.now() - start_time)


async def test_prisma():
    # find all validator requests first
    start_time = datetime.now()
    await connect_db()
    include_query = ValidatorTaskInclude(
        {
            "completions": {"include": {"criterion": {"include": {"scores": True}}}},
            "miner_responses": {"include": {"scores": True}},
            "ground_truth": True,
        }
    )

    vali_where_query_unprocessed = ValidatorTaskWhereInput(
        {
            "is_processed": False,
        }
    )

    res = await ValidatorTask.prisma().find_many(
        include=include_query,
        where=vali_where_query_unprocessed,
        order={"created_at": "desc"},
        take=5,
    )

    print(f"Prisma query got {[r.id for r in res]}")
    await disconnect_db()
    print("Time taken for Prisma: ", datetime.now() - start_time)


async def main():
    await test_sqlalchemy()
    await test_prisma()


asyncio.run(main())
# cold start
# Time taken for SQLAlchemy:  0:00:00.077851
# Time taken for Prisma:  0:00:01.991388


# after warmup
# Time taken for SQLAlchemy:  0:00:00.075100
# Time taken for Prisma:  0:00:00.295863
