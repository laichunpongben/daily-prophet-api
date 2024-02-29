import os
import json
from dataclasses import dataclass
from random import choices, shuffle
from collections import Counter
import concurrent.futures
from typing import List
import logging

from dailyprophet.feeds.feed import Feed
from dailyprophet.feeds.reddit import RedditFeed
from dailyprophet.feeds.arxiv import ArxivFeed
from dailyprophet.feeds.youtube import YoutubeFeed
from dailyprophet.feeds.openweathermap import OpenWeatherMapFeed
from dailyprophet.feeds.foursquare import FoursquareFeed


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
        "foursquare": FoursquareFeed,
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

    def sample(self, n: int):
        if not self.portfolio:
            return []

        keys, weights = zip(
            *[(key, item.weight) for key, item in self.portfolio.items()]
        )
        sampled_keys = choices(keys, weights=weights, k=n)
        sampled_feed_counts = dict(Counter(sampled_keys))
        logger.debug(sampled_feed_counts)

        sampled_feeds = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self._fetch_feed, key, count)
                for key, count in sampled_feed_counts.items()
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    sampled_feeds.extend(result)
                except Exception as e:
                    logger.error(f"Error fetching feeds: {e}")

        shuffle(sampled_feeds)  # can be sorted by priority instead if available

        return sampled_feeds

    def _fetch_feed(self, key, count):
        feed = self.portfolio[key].feed
        return feed.fetch(count)
