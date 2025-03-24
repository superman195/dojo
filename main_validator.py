import asyncio
from contextlib import asynccontextmanager

import uvicorn
from bittensor.utils.btlogging import logging as logger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from commons.api.middleware import LimitContentLengthMiddleware
from commons.block_subscriber import start_block_subscriber
from commons.dataset.synthetic import SyntheticAPI
from commons.exceptions import FatalSyntheticGenerationError
from commons.objects import ObjectManager
from database.client import connect_db, disconnect_db
from dojo.utils.config import source_dotenv

source_dotenv()

validator = ObjectManager.get_validator()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Performing startup tasks...")
    await connect_db()
    yield
    await _shutdown_validator()


def _check_fatal_errors(task: asyncio.Task):
    """if a fatal error is detected, shut down the validator."""
    try:
        task.result()
    except FatalSyntheticGenerationError as e:
        logger.error(f"Fatal Error - shutting down validator: {e}")
        asyncio.create_task(_shutdown_validator())


async def _shutdown_validator():
    logger.info("Performing shutdown tasks...")
    validator._should_exit = True
    validator.executor.shutdown(wait=True)
    validator.subtensor.substrate.close()
    await validator.save_state()
    await SyntheticAPI.close_session()
    await disconnect_db()

    logger.info("Cancelling remaining tasks ...")
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    for task in tasks:
        task.cancel()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
)
app.add_middleware(LimitContentLengthMiddleware)


async def main():
    config = uvicorn.Config(
        app=app,
        host="0.0.0.0",
        port=ObjectManager.get_config().api.port,
        workers=1,
        log_level="info",
        reload=False,
    )
    server = uvicorn.Server(config)
    running_tasks = [
        asyncio.create_task(validator.log_validator_status()),
        asyncio.create_task(validator.run()),
        asyncio.create_task(validator.update_score_and_send_feedback()),
        asyncio.create_task(validator.send_heartbeats()),
        asyncio.create_task(
            start_block_subscriber(callbacks=[validator.block_headers_callback])
        ),
    ]
    # set a callback on validator.run() to check for fatal errors.
    running_tasks[1].add_done_callback(_check_fatal_errors)

    await server.serve()

    for task in running_tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            logger.info(f"Cancelled task {task.get_name()}")
        except Exception as e:
            logger.error(f"Task {task.get_name()} raised an exception: {e}")
            pass

    logger.info("Exiting main function.")


if __name__ == "__main__":
    asyncio.run(main())
