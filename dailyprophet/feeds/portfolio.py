import os
import json
from dataclasses import dataclass
from random import choices, shuffle
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import asyncio
from typing import List
import logging

from dailyprophet.feeds.feed import Feed
from dailyprophet.feeds.reddit import RedditFeed
from dailyprophet.feeds.arxiv import ArxivFeed
from dailyprophet.feeds.youtube import YoutubeFeed
from dailyprophet.feeds.openweathermap import OpenWeatherMapFeed


logger = logging.getLogger(__name__)


@dataclass
class FeedPortfolioItem:
    feed: Feed
    weight: float


class FeedPortfolio:
    FEED_CLASS_MAPS = {
        "reddit": RedditFeed,
        "arxiv": ArxivFeed,
        "youtube": YoutubeFeed,
        "openweathermap": OpenWeatherMapFeed,
    }

    PORTFOLIO_FILE_PATH = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../data/portfolio.json",
    )

    PORTFOLIO_BACKUP_FILE_PATH = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../data/portfolio.backup.json",
    )

    def __init__(self) -> None:
        self.portfolio = {}
        self.load_setting_from_file()  # eager

    def add(self, feed_type: str, name: str, weight: float):
        feed_class = FeedPortfolio.FEED_CLASS_MAPS[feed_type]
        feed_instance = self.create_feed_instance(feed_class, name)
        item = FeedPortfolioItem(feed_instance, weight)
        key = f"{feed_type}/{name}"
        self.portfolio[key] = item

    def load_setting_from_file(self):
        self.portfolio = {}
        with open(FeedPortfolio.PORTFOLIO_FILE_PATH, "r") as f:
            setting = json.load(f)
            for feed_type, name, weight in setting:
                self.add(feed_type, name, weight)
    
    def load_setting_from_backup_file(self):
        self.portfolio = {}
        with open(FeedPortfolio.PORTFOLIO_BACKUP_FILE_PATH, "r") as f:
            setting = json.load(f)
            for feed_type, name, weight in setting:
                self.add(feed_type, name, weight)

    def load_setting(self, setting: List):
        self.portfolio = {}
        for feed_type, name, weight in setting:
            self.add(feed_type, name, weight)

    def get_setting(self):
        setting = []
        for key, item in self.portfolio.items():
            feed_type, name = key.split("/")
            weight = item.weight
            line = [feed_type, name, weight]
            setting.append(line)
        return setting

    def save_setting_to_file(self):
        setting = self.get_setting()
        with open(FeedPortfolio.PORTFOLIO_FILE_PATH, "w") as f:
            json.dump(setting, f)

    def create_feed_instance(self, feed_class: Feed, name: str):
        if name is None or name == "":
            return feed_class()
        else:
            return feed_class(name)
        
    async def async_fetch_feed(self, key, count):
        feed = self.portfolio[key].feed
        return await feed.async_fetch(count)

    async def async_sample(self, n: int):
        if not self.portfolio:
            return []

        keys, weights = zip(
            *[(key, item.weight) for key, item in self.portfolio.items()]
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

        sampled_feeds = [feed for sublist in sampled_feeds for feed in sublist]  # flattening
        shuffle(sampled_feeds)  # can be sorted by priority instead if available

        return sampled_feeds


async def main():
    portfolio = FeedPortfolio()
    out = await portfolio.async_sample(50)
    return out

if __name__ == "__main__":
    import asyncio
    import time

    start = time.time()
    out = asyncio.run(main())
    print(out)
    end = time.time()
    print(end - start)