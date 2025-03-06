import asyncio

from bittensor.utils.btlogging import logging as logger

from commons.block_subscriber import start_block_subscriber
from commons.objects import ObjectManager
from dojo.utils.config import source_dotenv

source_dotenv()


async def main():
    miner = await ObjectManager.get_miner()
    tasks = [
        asyncio.create_task(miner.log_miner_status()),
        asyncio.create_task(miner.run()),
        asyncio.create_task(
            start_block_subscriber(callbacks=[miner.block_headers_callback])
        ),
    ]

    await asyncio.gather(*tasks)
    logger.info("Exiting main function.")


if __name__ == "__main__":
    asyncio.run(main())
