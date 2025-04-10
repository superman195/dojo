import asyncio
import gc
import logging as python_logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from commons.api.middleware import LimitContentLengthMiddleware
from commons.block_subscriber import start_block_subscriber
from commons.dataset.synthetic import SyntheticAPI
from commons.exceptions import FatalSyntheticGenerationError
from commons.logging import (
    ValidatorAPILogHandler,
    forwarded_log_filter,
    setup_python_logging_to_loguru,
)
from commons.logging import logging as logger
from commons.objects import ObjectManager
from database.client import connect_db, disconnect_db
from dojo import get_dojo_api_base_url
from dojo.chain import get_async_subtensor
from dojo.utils.config import source_dotenv

source_dotenv()

validator = ObjectManager.get_validator()

api_log_handler = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Performing startup tasks...")
    await connect_db()

    # Configure Python's standard logging to forward to Loguru
    setup_python_logging_to_loguru(level=python_logging.INFO)

    # Setup validator API logging
    global api_log_handler

    api_log_handler = ValidatorAPILogHandler(
        api_url=get_dojo_api_base_url(),
        wallet=validator.wallet,
        console_output=False,  # Disable console output since logs are already handled by Loguru
    )
    api_log_handler.start()

    # Add the handler directly to Loguru with the filter
    handler_id = logger.add(
        api_log_handler,
        level="DEBUG",
        format="{message}",
        filter=forwarded_log_filter,
        colorize=True,  # Enable colorization for API logs too
    )

    yield

    # Cleanup logging handlers
    if api_log_handler:
        try:
            # Remove the Loguru handler we added
            if handler_id:
                logger.remove(handler_id)
        except Exception as e:
            print(f"Error removing Loguru handlers: {e}")

        # Stop the handler and flush logs
        await api_log_handler.stop()

    await _shutdown_validator()


def _check_fatal_errors(task: asyncio.Task):
    """if a fatal error is detected, shut down the validator."""
    try:
        task.result()
    except FatalSyntheticGenerationError as e:
        logger.error(f"Fatal Error - shutting down validator: {e}")
        asyncio.create_task(_shutdown_validator())
    finally:
        gc.collect()


async def _shutdown_validator():
    logger.info("Performing shutdown tasks...")
    validator._should_exit = True
    validator.executor.shutdown(wait=True)
    validator.subtensor.substrate.close()
    await validator.save_state()
    await SyntheticAPI.close_session()
    await disconnect_db()
    try:
        subtensor = await get_async_subtensor()
        if subtensor:
            await subtensor.close()
    except Exception as e:
        logger.trace(f"Attempted to close subtensor connection but failed due to {e}")
        pass

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
        asyncio.create_task(validator.update_tasks_polling()),
        asyncio.create_task(validator.score_and_send_feedback()),
        asyncio.create_task(validator.send_heartbeats()),
        asyncio.create_task(
            start_block_subscriber(
                callbacks=[validator.block_headers_callback], max_interval_sec=60
            )
        ),
        asyncio.create_task(validator.cleanup_resources()),
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
