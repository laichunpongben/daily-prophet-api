"""
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

from dailyprophet.feeds.feed import Feed
from dailyprophet.feeds.util import expo_decay_weighted_sample
from dailyprophet.configs import YOUTUBE_API_KEY

logger = logging.getLogger(__name__)


class YoutubeFeed(Feed):
    def __init__(self, channel_id):
        super().__init__()
        self.api_key = YOUTUBE_API_KEY
        self.channel_id = channel_id
        self.cache = None
        self.cache_expiration = None
        self.cache_duration = timedelta(hours=1)

    def format_search_url(self, n: int):
        return f"https://www.googleapis.com/youtube/v3/search?part=snippet&channelId={self.channel_id}&order=date&type=video&maxResults={n}&key={self.api_key}"

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

            fetch_size = ceil(n / 50) * 50  # keep a cache with size of multiple of 50

            async with aiohttp.ClientSession() as session:
                url = self.format_search_url(fetch_size)
                logger.debug(url)
                async with session.get(url) as response:
                    data = await response.json()
                    items = data.get("items", [])
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
    youtube = YoutubeFeed("UCQHX6ViZmPsWiYSFAyS0a3Q")
    out = youtube.fetch(2)
    print(out)
    out = youtube.fetch(2)
    print(out)
