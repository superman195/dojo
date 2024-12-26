import asyncio

from bittensor.utils.btlogging import logging as logger

from commons.objects import ObjectManager
from dojo.utils.config import source_dotenv

source_dotenv()


async def main():
    miner = ObjectManager.get_miner()
    log_task = asyncio.create_task(miner.log_miner_status())
    run_task = asyncio.create_task(miner.run())

    await asyncio.gather(log_task, run_task)
    logger.info("Exiting main function.")


if __name__ == "__main__":
    asyncio.run(main())
