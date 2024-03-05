# readers/reader.py

from random import choices, shuffle
from collections import Counter
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional
import logging

from dailyprophet.feeds.portfolio import FeedPortfolio
from dailyprophet.feeds.feed_queue import FeedQueue
from dailyprophet.feeds.feed_factory import FeedFactory

logger = logging.getLogger(__name__)


class Reader:
    def __init__(self, name: str, record: Optional[dict] = None) -> None:
        self.name = name

        if record is not None:
            setting = record.get("portfolio")
        else:
            setting = None
        self.portfolio = FeedPortfolio(name, setting)

        self.queue = FeedQueue()
        self.factory = FeedFactory()

    async def async_fetch_feed(self, key, count):
        feed = self.factory[key]
        return await feed.async_fetch(count)

    async def async_sample(self, n: int):
        if not self.portfolio:
            return []

        keys, weights = zip(
            *[(key, weight) for key, weight in self.portfolio.generate_key_weight()]
        )
        sampled_keys = choices(keys, weights=weights, k=n)
        sampled_feed_counts = dict(Counter(sampled_keys))
        logger.debug(sampled_feed_counts)

        async def fetch_sampled_feed(key, count):
            return await self.async_fetch_feed(key, count)

        sampled_feeds = []

        def run(corofn, *args):
            loop = asyncio.new_event_loop()
            try:
                coro = corofn(*args)
                asyncio.set_event_loop(loop)
                return loop.run_until_complete(coro)
            finally:
                loop.close()

        with ThreadPoolExecutor() as executor:
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(executor, run, fetch_sampled_feed, key, count)
                for key, count in sampled_feed_counts.items()
            ]
            sampled_feeds = await asyncio.gather(*tasks)

        sampled_feeds = [
            feed for sublist in sampled_feeds for feed in sublist
        ]  # flattening
        shuffle(sampled_feeds)  # can be sorted by priority instead if available

        return sampled_feeds

    def push_queue(self, feeds: List):
        self.queue.push(feeds)

    async def async_new(self, n: int):
        feeds = await self.async_sample(n)
        self.push_queue(feeds)

    async def async_pop(self):
        return self.queue.pop()
