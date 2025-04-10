import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime
from urllib.parse import urlparse

import bittensor as bt
import uvicorn
from analytics.endpoints.routes import analytics_router
from dataset_extraction.endpoints.routes import dataset_router
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from validator_logging.endpoints.routes import logging_router

from commons.api_settings import ValidatorAPISettings, get_settings
from commons.cache import RedisCache
from commons.logging import logging as logger
from commons.objects import ObjectManager
from dojo.utils.config import source_dotenv

source_dotenv()
settings: ValidatorAPISettings = get_settings()
cfg: bt.config = ObjectManager.get_config()
bt.logging.set_debug(True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle manager"""
    app.state.bt_cfg = cfg
    app.state.api_config = settings.aws
    app.state.redis = RedisCache(settings.redis)
    app.state.subtensor = bt.subtensor(config=app.state.bt_cfg)

    # Initialize metagraph
    logger.info("Initializing metagraph...")
    app.state.metagraph = app.state.subtensor.metagraph(app.state.bt_cfg.netuid)
    app.state.metagraph.sync(block=None, lite=True)
    logger.info("Metagraph initialized successfully")

    # Create task for periodic metagraph updates
    app.state.metagraph_update_task = asyncio.create_task(
        periodic_metagraph_update(app)
    )
    yield

    # Cleanup
    if hasattr(app.state, "metagraph_update_task"):
        app.state.metagraph_update_task.cancel()
        try:
            await app.state.metagraph_update_task
        except asyncio.CancelledError:
            pass

    await app.state.redis.close()
    app.state.subtensor.close()


async def periodic_metagraph_update(app):
    """Periodically updates the metagraph in the background"""
    update_interval = 20 * 60  # Update every 20 minutes
    while True:
        try:
            await asyncio.sleep(update_interval)
            app.state.last_metagraph_update = datetime.now()
            logger.info("Updating metagraph...")
            app.state.metagraph = app.state.subtensor.metagraph(app.state.bt_cfg.netuid)
            app.state.metagraph.sync(block=None, lite=True)
            app.state.last_metagraph_update = datetime.now()
            logger.info("Metagraph updated successfully")
        except asyncio.CancelledError:
            logger.info("Metagraph update task cancelled")
            break
        except Exception as e:
            logger.error(f"Error updating metagraph: {e}")


# Initialize FastAPI app
app = FastAPI(title="Validator API Service", lifespan=lifespan)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(analytics_router)
app.include_router(dataset_router)
app.include_router(logging_router)


async def server():
    """Start the API server"""
    api_url = os.getenv("VALIDATOR_API_BASE_URL", "http://0.0.0.0:9999")
    parsed_url = urlparse(api_url)
    host = parsed_url.hostname or "0.0.0.0"
    port = parsed_url.port or 9999

    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level="debug",
        timeout_notify=300,
        timeout_keep_alive=240,
    )
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    import sys

    if "--test" in sys.argv:
        # Import and run test endpoint
        from validator.endpoints.test import test_endpoint

        asyncio.run(test_endpoint())
    else:
        asyncio.run(server())
