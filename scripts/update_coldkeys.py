import asyncio

import bittensor as bt

from database.client import connect_db, disconnect_db, prisma


async def update_coldkeys():
    await connect_db()
    subtensor = bt.subtensor(network="finney")

    # Get all miner responses without coldkeys
    responses = await prisma.minerresponse.find_many(where={"coldkey": ""})

    for response in responses:
        coldkey = subtensor.query_subtensor(
            name="Owner",
            params=[response.hotkey],
        ).value

        await prisma.minerresponse.update(
            where={"id": response.id}, data={"coldkey": coldkey}
        )

    await disconnect_db()


if __name__ == "__main__":
    asyncio.run(update_coldkeys())
