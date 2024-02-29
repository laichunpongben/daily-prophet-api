import asyncio
from datetime import datetime, timedelta
from math import ceil
import logging

import aiohttp
import feedparser

from dailyprophet.feeds.feed import Feed
from dailyprophet.feeds.util import expo_decay_weighted_sample

logger = logging.getLogger(__name__)


class ArxivFeed(Feed):
    def __init__(self, category):
        super().__init__()
        self.category = category
        self.feed_url = f"http://export.arxiv.org/rss/{category}"
        self.cache = None
        self.cache_expiration = None
        self.cache_duration = timedelta(hours=1)

    def parse(self, post):
        return {
            "type": "arxiv",
            "category": self.category,
            "id": post.get("id", ""),
            "title": post.get("title", ""),
            "summary": post.get("summary", ""),
            "author": post.get("author", ""),
            "published": post.get("published", ""),
            "updated": post.get("updated", ""),
            "url": post.get("link", ""),
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
                async with session.get(self.feed_url) as response:
                    data = await response.read()
                    feed = feedparser.parse(data)
                    entries = feed.entries[:fetch_size]

                    updated_time = feed.feed.updated
                    published_time = feed.feed.published
                    parsed_entries = [
                        self.parse(
                            {
                                **entry,
                                "published": published_time,
                                "updated": updated_time,
                            }
                        )
                        for entry in entries
                    ]

                    # Update the cache
                    self.cache = parsed_entries
                    logger.debug(f"Cache size: {len(self.cache)}")
                    self.cache_expiration = datetime.now() + self.cache_duration
                    logger.debug(f"Cache expiration: {self.cache_expiration}")

                    return expo_decay_weighted_sample(self.cache, k=n)
        except Exception as e:
            logger.error(f"Error fetching ArXiv feed asynchronously: {e}")
            return []

    def fetch(self, n: int):
        # For backward compatibility, call the asynchronous version synchronously
        return asyncio.run(self.async_fetch(n))


if __name__ == "__main__":
    arxiv = ArxivFeed("cs.LG")
    out = arxiv.fetch(2)
    print(out)
    out = arxiv.fetch(2)
    print(out)
