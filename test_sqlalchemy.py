import asyncio
from datetime import datetime, timedelta, timezone

import asyncpg
from sqlalchemy import and_, not_, select

from commons.exceptions import (
    ExpiredFromMoreThanExpireTo,
)
from commons.utils import datetime_as_utc
from database.client import connect_db, disconnect_db
from database.prisma.models import ValidatorTask
from database.prisma.types import (
    ValidatorTaskInclude,
    ValidatorTaskWhereInput,
)
from database.sqlalchemy.tables import (
    t_completion,
    t_criterion,
    t_ground_truth,
    t_miner_response,
    t_miner_score,
    t_validator_task,
)
from dojo import TASK_DEADLINE


def get_validator_tasks_query(expire_from=None, expire_to=None):
    # Set default expiry timeframe
    if not expire_from:
        expire_from = (
            datetime_as_utc(datetime.now(timezone.utc))
            - timedelta(seconds=TASK_DEADLINE)
            - timedelta(hours=6)
        )
    if not expire_to:
        expire_to = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(
            seconds=TASK_DEADLINE
        )

    # Check expiry timeframe validity
    if expire_from > expire_to:
        raise ExpiredFromMoreThanExpireTo("expire_from should be less than expire_to.")

    query = (
        select(
            t_validator_task,
            t_completion,
            t_criterion,
            t_miner_score,
            t_miner_response,
            t_ground_truth,
        )
        .outerjoin(
            t_completion, t_validator_task.c.id == t_completion.c.validator_task_id
        )
        .outerjoin(t_criterion, t_completion.c.id == t_criterion.c.completion_id)
        .outerjoin(t_miner_score, t_criterion.c.id == t_miner_score.c.criterion_id)
        .outerjoin(
            t_miner_response,
            t_validator_task.c.id == t_miner_response.c.validator_task_id,
        )
        .outerjoin(
            t_ground_truth, t_validator_task.c.id == t_ground_truth.c.validator_task_id
        )
        .where(
            and_(
                # t_validator_task.c.expire_at > expire_from,
                # t_validator_task.c.expire_at < expire_to,
                not_(t_validator_task.c.is_processed),
            )
        )
        .order_by(t_validator_task.c.created_at.desc())
        .limit(5)
    )

    # Compile the query to SQL
    from sqlalchemy.dialects import postgresql

    query_str = query.compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
    )

    query = select(t_validator_task).limit(5)

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
    query = get_validator_tasks_query(expire_from, expire_to)
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
        "postgresql://postgres:<password>@localhost/db",
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
    conn = await asyncpg.connect("postgresql://postgres:<password>@localhost/db")
    # Execute a statement to create a new table.
    query, query_str = get_validator_tasks_query()
    query_str = str(query_str)
    print(query_str)
    print(await conn.execute(query_str))

    # Select a row from the table.
    # row = await conn.fetchrow("select * from validator_task limit 5")
    row = await conn.fetchrow(query_str)
    # *row* now contains
    # asyncpg.Record(id=1, name='Bob', dob=datetime.date(1984, 3, 1))
    print(row)

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

    await ValidatorTask.prisma().find_many(
        include=include_query,
        where=vali_where_query_unprocessed,
        order={"created_at": "desc"},
        take=5,
    )
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
