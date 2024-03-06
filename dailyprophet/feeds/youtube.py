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
        self.cache = None
        self.cache_expiration = None
        self.cache_duration = timedelta(hours=1)

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
        try:
            if self.cache_valid(n):
                logger.debug("Fetching from cache")
                return expo_decay_weighted_sample(self.cache, k=n)

            is_channel = await self.async_retry_operation(self.async_verify_channel)
            fetch_operation = (
                self.async_fetch_channel if is_channel else self.async_fetch_q
            )
            items = await self.async_retry_operation(
                fetch_operation, fetch_size=ceil(n / 50) * 50  # same quota cost for <= 50
            )

            parsed_items = [self.parse(item) for item in items]

            self.update_cache(parsed_items)
            return expo_decay_weighted_sample(self.cache, k=n)
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

    def cache_valid(self, n: int):
        return (
            self.cache is not None
            and datetime.now() < self.cache_expiration
            and n <= len(self.cache)
        )

    def update_cache(self, parsed_items):
        self.cache = parsed_items
        logger.debug(f"Cache size: {len(self.cache)}")
        self.cache_expiration = datetime.now() + self.cache_duration
        logger.debug(f"Cache expiration: {self.cache_expiration}")

    def fetch(self, n: int):
        return asyncio.run(self.async_fetch(n))


if __name__ == "__main__":
    youtube = YoutubeFeed("UCQHX6ViZmPsWiYSFAyS0a3Q")
    print(youtube.fetch(50))

    # youtube = YoutubeFeed("Taylor Swift")
    # print(youtube.retry_fetch_q(50))
    # print(youtube.fetch(50))

    # youtube = YoutubeFeed("UCQHX6ViZmPsWiYSFAyS0a3Q")
    # youtube2 = YoutubeFeed("UCu_YquoQYKR3GpP82TO-zRw")
    # out = youtube.fetch(2)
    # print(out)
    # out = youtube.fetch(2)
    # print(out)

    # youtube = YoutubeFeed("UCQHX6ViZmPsWiYSFAyS0a3Q")
    # print(youtube.retry_verify_channel())
    # print(youtube.retry_fetch_channel(50))
