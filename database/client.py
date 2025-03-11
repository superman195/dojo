import asyncio
from contextlib import asynccontextmanager

from bittensor.utils.btlogging import logging as logger

from database.prisma import Prisma

db = None

prisma = Prisma(auto_register=True)


async def connect_db(retries: int = 5, delay: int = 2) -> None:
    global db
    attempt = 1

    if db is not None:
        logger.info("Already connected to the database.")
        return

    while attempt <= retries:
        try:
            await prisma.connect()
            if prisma.is_connected():
                db = prisma
                logger.success("Successfully connected to the database.")
                break

        except Exception as e:
            logger.error(
                f"Failed to connect to the database (Attempt {attempt}/{retries}): {e}"
            )
            await asyncio.sleep(delay**attempt)
            attempt += 1

    if db is None:
        logger.critical("Exceeded maximum retry attempts to connect to the database.")
        raise ConnectionError(
            f"Failed to connect to the database after {retries} attempts."
        )


async def disconnect_db():
    if prisma.is_connected():
        logger.info("Releasing connection......")
        await prisma.disconnect()
        logger.info("Connection released......")


@asynccontextmanager
async def transaction():
    async with prisma.tx() as tx:
        yield tx
