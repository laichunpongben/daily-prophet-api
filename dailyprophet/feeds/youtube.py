import asyncio
from datetime import datetime, timedelta
from math import ceil
import logging

import aiohttp
from aiohttp import ClientResponseError

from dailyprophet.feeds.feed import Feed
from dailyprophet.feeds.util import expo_decay_weighted_sample
from dailyprophet.configs import YOUTUBE_API_KEY_0, YOUTUBE_API_KEY_1, YOUTUBE_API_KEY_2

logger = logging.getLogger(__name__)


class YoutubeFeed(Feed):
    BASE_URL = "https://www.googleapis.com/youtube/v3"

    def __init__(self, q: str):
        super().__init__()
        self.api_keys = [
            YOUTUBE_API_KEY_0,
            YOUTUBE_API_KEY_1,
            YOUTUBE_API_KEY_2,
        ]
        self.active_api_key_index = 0
        self.q = q
        self.cache = []
        self.cache_expiration = None
        self.cache_duration = timedelta(hours=1)
        self.fetch_lock = asyncio.Lock()

    def switch_active_api_key(self):
        self.active_api_key_index = (self.active_api_key_index + 1) % len(self.api_keys)
        logger.warning(
            f"Youtube API key switched to {self.api_keys[self.active_api_key_index][-5:]}"
        )

    def format_url(self, endpoint, params):
        params_str = "&".join([f"{key}={value}" for key, value in params.items()])
        return f"{YoutubeFeed.BASE_URL}/{endpoint}?{params_str}&key={self.api_keys[self.active_api_key_index]}"

    async def async_fetch_url(self, url: str):
        try:
            async with aiohttp.ClientSession() as session:
                logger.debug(f"Fetching: {url}")
                async with session.get(url) as response:
                    data = await response.json()
                    if response.ok:
                        return data.get("items", [])
                    else:
                        response.raise_for_status()
        except ClientResponseError as e:
            raise
        except Exception as e:
            logger.error(e)
            return []

    async def async_verify_channel(self, _):
        if self.q is None or len(self.q) < 23:  # empirical
            return False
        else:
            params = {"part": "snippet", "id": self.q, "maxResults": 1}
            url = self.format_url("channels", params)
            result = await self.async_fetch_url(url)
            return bool(result)

    async def async_retry_operation(
        self, operation, max_try: int = 3, fetch_size: int = 50
    ):
        result = None
        for _ in range(max_try):
            try:
                result = await operation(fetch_size)
                if result is not None:
                    return result
            except ClientResponseError as e:
                if e.status == 403:
                    logger.warning("Youtube quota exceeded")
                    logger.error(e)
                    self.switch_active_api_key()
                else:
                    return []
            except Exception as e:
                logger.error(e)
                return []
        return []

    async def async_fetch_channel(self, n: int):
        params = {
            "part": "snippet",
            "channelId": self.q,
            "order": "date",
            "type": "video",
            "maxResults": n,
        }
        url = self.format_url("search", params)
        return await self.async_fetch_url(url)

    async def async_fetch_q(self, n: int):
        params = {
            "part": "snippet",
            "q": self.q,
            "order": "date",
            "type": "video",
            "maxResults": n,
        }
        url = self.format_url("search", params)
        return await self.async_fetch_url(url)

    async def async_fetch(self, n: int):
        valid_cache = await self._check_cache()
        try:
            # attempt to fill the cache if no other coroutine already does
            is_locked = self.fetch_lock.locked()
            if n > len(valid_cache) and not is_locked:
                # Acquire the lock before scheduling the background task
                logger.info("Attempt to fill cache")
                async with self.fetch_lock:
                    logger.info("LOCK acquired")
                    is_channel = await self.async_retry_operation(
                        self.async_verify_channel
                    )
                    fetch_operation = (
                        self.async_fetch_channel if is_channel else self.async_fetch_q
                    )
                    items = await self.async_retry_operation(
                        fetch_operation,
                        fetch_size=ceil(n / 50) * 50,  # same quota cost for <= 50
                    )
                    parsed_items = [self.parse(item) for item in items]

                    self.update_cache(parsed_items)
                    await asyncio.sleep(0.1)  # keep the lock longer

                    return expo_decay_weighted_sample(self.cache, k=n)
            else:
                return expo_decay_weighted_sample(valid_cache, k=n)
        except Exception as e:
            logger.error(f"Error fetching YouTube feed asynchronously: {e}")
            return []

    def parse(self, video: dict):
        id = video["id"]["videoId"]
        return {
            "type": "youtube",
            "channel": video["snippet"]["channelTitle"],
            "id": id,
            "title": video["snippet"]["title"],
            "description": video["snippet"]["description"],
            "publishTime": video["snippet"]["publishTime"],
            "url": f"https://www.youtube.com/watch?v={id}",
        }

    async def _check_cache(self):
        logger.debug("Checking cache")
        if self.cache_expiration is not None and datetime.now() < self.cache_expiration:
            return self.cache
        else:
            return []

    def update_cache(self, parsed_items):
        self.cache = parsed_items
        logger.debug(f"Cache size: {len(self.cache)}")
        self.cache_expiration = datetime.now() + self.cache_duration
        logger.debug(f"Cache expiration: {self.cache_expiration}")

    def fetch(self, n: int):
        return asyncio.run(self.async_fetch(n))


async def test_async_fetch():
    import json

    # youtube = YoutubeFeed("UCQHX6ViZmPsWiYSFAyS0a3Q")
    youtube = YoutubeFeed("UCu_YquoQYKR3GpP82TO-zRw")
    # youtube = YoutubeFeed("Taylor Swift")

    fetch_size = 50

    fetch_task1 = asyncio.create_task(youtube.async_fetch(fetch_size))
    fetch_task2 = asyncio.create_task(youtube.async_fetch(fetch_size))
    sleep_task = asyncio.create_task(asyncio.sleep(2))

    # Explicitly wait for the background task and the main task to complete
    out, _, _ = await asyncio.gather(fetch_task1, fetch_task2, sleep_task)

    print("Returned from async_fetch:")
    print("size: ", len(out))

    fetch_task3 = asyncio.create_task(youtube.async_fetch(fetch_size))
    out = await fetch_task3

    print("Returned from async_fetch:")
    print(json.dumps(out, indent=2))
    print("size: ", len(out))


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
