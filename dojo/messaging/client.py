import asyncio
import functools

import aiohttp
from aiohttp.client import ClientSession
from pydantic import BaseModel

from dojo.messaging.types import PayloadA
from dojo.messaging.utils import encode_body


@functools.lru_cache
def get_validator_client() -> ClientSession:
    return aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False, limit=256))


def _build_url(url: str, model: BaseModel):
    return f"http://{url}/{model.__class__.__name__}"


class Client:
    def __init__(self, session: ClientSession):
        self._session: ClientSession = session
        self._headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Content-Encoding": "zstd",
        }

    async def send(self, url: str, model: BaseModel):
        compressed = encode_body(model)
        async with self._session.post(
            _build_url(url, model),
            data=compressed,
            headers=self._headers,
        ) as response:
            return await response.json()

    async def cleanup(self):
        try:
            await self._session.close()
        except Exception:
            pass


# NOTE: example usage below
async def main():
    payload_a = PayloadA(field1="Hello", field2="World" * 50)
    compressed = encode_body(payload_a)
    json_data = payload_a.model_dump_json()
    async with aiohttp.ClientSession() as session:
        # Test with normal JSON
        async with session.post(
            "http://localhost:8001/PayloadA",
            data=compressed,
            headers={"Content-Type": "application/json", "Content-Encoding": "zstd"},
        ) as response:
            print(f"Status: {response.status}")
            print(f"Response: {await response.text()}")
            print(f"Original size: {len(json_data)} bytes")
            print(f"Compressed size: {len(compressed)} bytes")
            print(f"Compression ratio: {len(compressed) / len(json_data):.2f}")


if __name__ == "__main__":
    asyncio.run(main())
