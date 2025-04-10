import aiohttp
from aiohttp.client import ClientSession


def get_client() -> ClientSession:
    return aiohttp.ClientSession()
