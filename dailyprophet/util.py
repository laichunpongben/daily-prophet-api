import random
from typing import List
from datetime import datetime, timedelta
import logging

from aiohttp import ClientSession, ClientResponseError

from .configs import WORKER_URL


logger = logging.getLogger(__name__)

last_wake_up_worker_time = None


def expo_decay_weighted_sample(data: List, k: int, decaying_factor: float = 0.98):
    if k >= len(data):
        return data
    else:
        weights = [decaying_factor**i for i in range(len(data))]
        return random.choices(data, weights=weights, k=k)


def flatten_dict(d, parent_key="", sep="_"):
    items = []
    counter = {}

    if d is None:
        return {}

    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if new_key in counter:
            counter[new_key] += 1
            new_key = f"{new_key}{sep}{counter[new_key]}"
        else:
            counter[new_key] = 0

        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                items.extend(flatten_dict(item, f"{new_key}{sep}{i}", sep=sep).items())
        else:
            items.append((new_key, v))

    return dict(items)


async def async_wake_up_worker():
    global last_wake_up_worker_time
    if last_wake_up_worker_time is None or (
        datetime.utcnow() - last_wake_up_worker_time
    ) > timedelta(minutes=9):
        try:
            url = f"{WORKER_URL}/"
            _ = await async_fetch_url(url)
            last_wake_up_worker_time = datetime.utcnow()
        except Exception as e:
            logger.error(e)


async def async_worker_fetch(source: str, subject: str, n: int):
    url = f"{WORKER_URL}/{source}/{subject}/{n}"
    _ = await async_fetch_url(url)


async def async_fetch_url(url: str, headers: dict = {}):
    try:
        async with ClientSession() as session:
            logger.debug(f"Fetching: {url}")
            async with session.get(url, headers=headers) as response:
                if response.ok:
                    return await response.json()
                else:
                    response.raise_for_status()
    except ClientResponseError as e:
        raise
    except Exception as e:
        logger.error(e)
        return {}


if __name__ == "__main__":
    data = [1, 2, 3]
    k = 5
    out = expo_decay_weighted_sample(data, k=k)
    print(out)
