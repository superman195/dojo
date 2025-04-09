import asyncio

from bittensor.utils.btlogging import logging as logger

from commons.block_subscriber import start_block_subscriber
from commons.objects import ObjectManager
from dojo.chain import get_async_subtensor
from dojo.utils.config import source_dotenv

source_dotenv()


async def shutdown_miner():
    try:
        subtensor = await get_async_subtensor()
        if subtensor:
            await subtensor.close()
    except Exception as e:
        logger.trace(f"Attempted to close subtensor connection but failed due to {e}")
        pass


async def main():
    miner = await ObjectManager.get_miner()
    tasks = [
        asyncio.create_task(miner.log_miner_status()),
        asyncio.create_task(miner.run()),
        asyncio.create_task(
            start_block_subscriber(
                callbacks=[miner.block_headers_callback], max_interval_sec=60
            )
        ),
    ]

    await asyncio.gather(*tasks)

    logger.info("Performing shutdown tasks...")
    await shutdown_miner()
    logger.info("Done, exiting main function.")


if __name__ == "__main__":
    asyncio.run(main())
