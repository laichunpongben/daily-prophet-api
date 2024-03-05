"""
https://developers.google.com/youtube/v3/determine_quota_cost
https://developers.google.com/youtube/v3/docs/search/list

{'kind': 'youtube#searchListResponse', 'etag': 'jc-rdyHBmHEXSiliJpkZgKi1V90', 'nextPageToken': 'CAoQAA', 'regionCode': 'JP', 'pageInfo': {'totalResults': 785686, 'resultsPerPage': 10},
'items': [{'kind': 'youtube#searchResult', 'etag': 'p4F3OjEyVeLn4j3aJuAd6y-PiO4', 'id': {'kind': 'youtube#video', 'videoId': 'KjqpLdO3_CU'}, 'snippet': {'publishedAt': '2024-02-25T17:45:00Z',
'channelId': 'UCQHX6ViZmPsWiYSFAyS0a3Q', 'title': 'HOW TO WIN AT CHESS!!!!!!!', 'description': 'Get My Chess Courses: https://www.chessly.com/ ➡️ Get my best-selling chess book: https://geni.us/gothamchess ➡️ My book ...',
'thumbnails': {'default': {'url': 'https://i.ytimg.com/vi/KjqpLdO3_CU/default.jpg', 'width': 120, 'height': 90}, 'medium': {'url': 'https://i.ytimg.com/vi/KjqpLdO3_CU/mqdefault.jpg', 'width': 320, 'height': 180},
'high': {'url': 'https://i.ytimg.com/vi/KjqpLdO3_CU/hqdefault.jpg', 'width': 480, 'height': 360}}, 'channelTitle': 'GothamChess', 'liveBroadcastContent': 'none', 'publishTime': '2024-02-25T17:45:00Z'}}
"""

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

    def __init__(self, q):
        super().__init__()
        self.api_keys = [
            YOUTUBE_API_KEY_0,
            YOUTUBE_API_KEY_1,
            YOUTUBE_API_KEY_2,
        ]
        self.active_api_key = self.api_keys[0]
        self.q = q
        self.cache = None
        self.cache_expiration = None
        self.cache_duration = timedelta(hours=1)

    def switch_active_api_key(self):
        key = self.active_api_key
        index = self.api_keys.index(key)
        if index == len(self.api_keys) - 1:
            next_index = 0
        else:
            next_index = index + 1
        self.active_api_key = self.api_keys[next_index]
        logger.warning(f"Youtube API key switched to {self.active_api_key[-5:]}")

    def format_search_q_url(self, n: int = 50):
        return f"{YoutubeFeed.BASE_URL}/search?part=snippet&q={self.q}&order=date&type=video&maxResults={n}&key={self.active_api_key}"

    def format_search_channel_url(self, n: int = 50):
        return f"{YoutubeFeed.BASE_URL}/search?part=snippet&channelId={self.q}&order=date&type=video&maxResults={n}&key={self.active_api_key}"

    def format_channel_url(self, n: int = 50):
        return f"{YoutubeFeed.BASE_URL}/channels?part=snippet&id={self.q}&maxResults={n}&key={self.active_api_key}"

    def parse(self, video):
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

    async def async_fetch_url(self, url: str):
        try:
            async with aiohttp.ClientSession() as session:
                logger.debug(f"Fetching: {url}")
                async with session.get(url) as response:
                    data = await response.json()
                    if response.ok:
                        items = data.get("items", [])
                        return items
                    else:
                        response.raise_for_status()
        except ClientResponseError as e:
            raise
        except Exception as e:
            logger.error(e)
            return []

    async def async_verify_channel(self):
        if self.q is None or len(self.q) < 23:  # empirical
            return False
        else:
            url = self.format_channel_url()
            result = await self.async_fetch_url(url)
            if len(result) > 0:
                return True
            else:
                return False

    async def async_retry_verify_channel(self, max_try: int = 3):
        is_channel = None
        for i in range(max_try):
            try:
                is_channel = await self.async_verify_channel()
                if is_channel is not None:
                    return is_channel
            except ClientResponseError as e:
                if e.status == 403:
                    logger.warning("Youtube quota exceeded")
                    logger.error(e)
                    self.switch_active_api_key()
                else:
                    return False
            except Exception as e:
                logger.error(e)
                return False
        return False

    def retry_verify_channel(self, max_try: int = 3):
        return asyncio.run(self.async_retry_verify_channel(max_try=max_try))

    async def async_fetch_channel(self, n: int):
        fetch_size = ceil(n / 50) * 50  # keep a cache with size of multiple of 50
        url = self.format_search_channel_url(fetch_size)
        result = await self.async_fetch_url(url)
        return result

    async def async_retry_fetch_channel(self, n: int, max_try: int = 3):
        result = None
        for i in range(max_try):
            try:
                result = await self.async_fetch_channel(n)
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

    def retry_fetch_channel(self, n: int, max_try: int = 3):
        return asyncio.run(self.async_retry_fetch_channel(n, max_try=max_try))

    async def async_fetch_q(self, n: int):
        fetch_size = ceil(n / 50) * 50  # keep a cache with size of multiple of 50
        url = self.format_search_q_url(fetch_size)
        result = await self.async_fetch_url(url)
        return result

    async def async_retry_fetch_q(self, n: int, max_try: int = 3):
        result = None
        for i in range(max_try):
            try:
                result = await self.async_fetch_q(n)
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

    def retry_fetch_q(self, n: int, max_try: int = 3):
        return asyncio.run(self.async_retry_fetch_q(n, max_try=max_try))

    async def async_fetch(self, n: int):
        try:
            # Check if the cache is still valid
            if (
                self.cache is not None
                and datetime.now() < self.cache_expiration
                and n <= len(self.cache)
            ):
                logger.debug("Fetching from cache")
                return expo_decay_weighted_sample(self.cache, k=n)

            # verify if q is channel id
            is_channel = await self.async_retry_verify_channel()
            if is_channel:
                items = await self.async_retry_fetch_channel(n)
            else:
                items = await self.async_retry_fetch_q(n)

            parsed_items = [self.parse(item) for item in items]

            # Update the cache
            self.cache = parsed_items
            logger.debug(f"Cache size: {len(self.cache)}")
            self.cache_expiration = datetime.now() + self.cache_duration
            logger.debug(f"Cache expiration: {self.cache_expiration}")

            return expo_decay_weighted_sample(self.cache, k=n)
        except Exception as e:
            logger.error(f"Error fetching YouTube feed asynchronously: {e}")
            return []

    def fetch(self, n: int):
        # For backward compatibility, call the asynchronous version synchronously
        return asyncio.run(self.async_fetch(n))


if __name__ == "__main__":
    # youtube = YoutubeFeed("UCQHX6ViZmPsWiYSFAyS0a3Q")
    # youtube2 = YoutubeFeed("UCu_YquoQYKR3GpP82TO-zRw")
    # out = youtube.fetch(2)
    # print(out)
    # out = youtube.fetch(2)
    # print(out)

    # youtube = YoutubeFeed("UCQHX6ViZmPsWiYSFAyS0a3Q")
    # print(youtube.retry_verify_channel())
    # print(youtube.retry_fetch_channel(50))

    # youtube = YoutubeFeed("Taylor Swift")
    # print(youtube.retry_fetch_q(50))
    # print(youtube.fetch(50))

    youtube = YoutubeFeed("UCQHX6ViZmPsWiYSFAyS0a3Q")
    print(youtube.fetch(50))
