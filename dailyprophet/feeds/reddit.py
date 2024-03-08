# reddit.py
import logging
import asyncio
from datetime import datetime
from math import ceil
import re

from .feed import Feed
from ..util import expo_decay_weighted_sample, async_worker_fetch
from ..mongodb_service import MongoDBService
from ..configs import WORKER_URL

logger = logging.getLogger(__name__)

db = MongoDBService("feeds")


class RedditFeed(Feed):
    def __init__(self, subject: str):
        super().__init__()
        self.source = "reddit"
        self.subject = subject
        self.fetch_lock = asyncio.Lock()  # Lock to control concurrent fetches

    async def _check_cache(self):
        """
        Check if the cache is still valid and return the cached result if possible.
        If the cache is not enough, initiate background fetching to fill the gap.
        """
        logger.debug("Checking cache")
        subject_re = re.compile(self.subject.lower().strip(), re.I)
        criteria = {
            "source": "reddit",
            "subject": subject_re,
            "expire_time": {"$gte": int(datetime.utcnow().timestamp())},
        }
        valid_cache = db.query(criteria)
        return valid_cache

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
                    asyncio.ensure_future(
                        async_worker_fetch(self.source, self.subject, target_cache_size)
                    )
                    await asyncio.sleep(1)  # keep the lock longer

            sort_key = lambda x: x["ups"] + x["downs"] + x["num_comments"] * 2
            return expo_decay_weighted_sample(
                sorted(valid_cache, key=sort_key, reverse=True), k=n
            )
        except Exception as e:
            logger.error(f"Error fetching Reddit posts asynchronously: {e}")
            return []


async def test_async_fetch():
    import json

    # reddit = RedditFeed("programming")
    reddit = RedditFeed("dating@singapore")
    fetch_size = 1

    # Run the async_fetch method and capture the background task
    fetch_task1 = asyncio.create_task(reddit.async_fetch(fetch_size))
    fetch_task2 = asyncio.create_task(reddit.async_fetch(fetch_size))
    sleep_task = asyncio.create_task(asyncio.sleep(10))

    # Explicitly wait for the background task and the main task to complete
    out, _, _ = await asyncio.gather(fetch_task1, fetch_task2, sleep_task)

    print("Returned from async_fetch:")
    for item in out:
        if "_id" in item:
            item.pop("_id")
    print(json.dumps(out, indent=2))

    fetch_task3 = asyncio.create_task(reddit.async_fetch(fetch_size))
    out = await fetch_task3

    print("Returned from async_fetch:")
    for item in out:
        if "_id" in item:
            item.pop("_id")
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
