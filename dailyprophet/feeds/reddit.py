# reddit.py
import logging
import asyncio
from datetime import datetime, timedelta
from math import ceil

import aiohttp
from aiohttp import ClientSession, ClientResponseError

from .feed import Feed
from .util import expo_decay_weighted_sample
from ..mongodb_service import MongoDBService
from ..configs import WORKER_URL

logger = logging.getLogger(__name__)

db = MongoDBService("feeds")


class RedditFeed(Feed):
    def __init__(self, subject: str):
        super().__init__()
        self.subject = subject
        self.fetch_lock = asyncio.Lock()  # Lock to control concurrent fetches

    async def _check_cache(self):
        """
        Check if the cache is still valid and return the cached result if possible.
        If the cache is not enough, initiate background fetching to fill the gap.
        """
        logger.debug("Checking cache")
        criteria = {"source": "reddit", "subject": self.subject, "expire_time": {"$gte": int(datetime.utcnow().timestamp())}}
        valid_cache = db.query(criteria)
        return valid_cache
    
    async def async_worker_fetch(self, n: int):
        url = f"{WORKER_URL}/reddit/{self.subject}/{n}"
        _ = await self.async_fetch_url(url)

    async def async_fetch_url(self, url: str, headers: dict = {}):
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
   
    async def async_fetch(self, n: int):
        try:
            valid_cache = await self._check_cache()

            # attempt to fill the cache if no other coroutine already does
            is_locked = self.fetch_lock.locked()
            if n > len(valid_cache) and not is_locked:
                # Acquire the lock before scheduling the background task
                logger.debug("Attempt to fill cache")
                async with self.fetch_lock:
                    logger.debug("LOCK acquired")

                    target_cache_size = (
                        ceil(n / 30) * 30
                    )  # keep a cache with a size of multiple of 30
                    asyncio.ensure_future(self.async_worker_fetch(target_cache_size))
                    await asyncio.sleep(1)  # keep the lock longer
                
            return expo_decay_weighted_sample(valid_cache, k=n)
        except Exception as e:
            logger.error(f"Error fetching Reddit posts asynchronously: {e}")
            return []


async def test_async_fetch():
    import json

    reddit = RedditFeed("programming")
    fetch_size = 1

    # Run the async_fetch method and capture the background task
    fetch_task1 = asyncio.create_task(reddit.async_fetch(fetch_size))
    fetch_task2 = asyncio.create_task(reddit.async_fetch(fetch_size))
    sleep_task = asyncio.create_task(asyncio.sleep(10))

    # Explicitly wait for the background task and the main task to complete
    out, _, _ = await asyncio.gather(fetch_task1, fetch_task2, sleep_task)

    print("Returned from async_fetch:")
    print(json.dumps(out, indent=2))

    fetch_task3 = asyncio.create_task(reddit.async_fetch(fetch_size))
    out = await fetch_task3

    print("Returned from async_fetch:")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    import time

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)

    start = time.time()
    asyncio.run(test_async_fetch())
    end = time.time()
    lapsed = end - start
    print("Lapsed: ", lapsed)
