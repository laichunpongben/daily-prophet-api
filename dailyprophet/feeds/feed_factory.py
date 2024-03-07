"""
Single factory across the app
"""

from dailyprophet.feeds.feed import Feed
from dailyprophet.feeds.reddit import RedditFeed
from dailyprophet.feeds.arxiv import ArxivFeed
from dailyprophet.feeds.youtube import YoutubeFeed
from dailyprophet.feeds.openweathermap import OpenWeatherMapFeed
from dailyprophet.feeds.lihkg import LihkgFeed


class FeedFactory:
    _instance = None

    FEED_CLASS_MAPS = {
        "reddit": RedditFeed,
        "arxiv": ArxivFeed,
        "youtube": YoutubeFeed,
        "openweathermap": OpenWeatherMapFeed,
        "lihkg": LihkgFeed,
    }

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._feeds = {}
        return cls._instance

    def __getitem__(self, key):
        if key in self._feeds:
            return self._feeds[key]
        else:
            feed = self.create_feed(key)
            self._feeds[key] = feed
            return feed

    def create_feed(self, key: str):
        map = FeedFactory.FEED_CLASS_MAPS
        source, name = key.split("/")
        if source in map:
            feed_class = map[source]
            feed_instance = self._create_feed_instance(feed_class, name)
            return feed_instance
        else:
            raise ValueError(f"Invalid feed source: {source}")

    def _create_feed_instance(self, feed_class: Feed, name: str):
        if name is None or name == "":
            return feed_class()
        else:
            return feed_class(name)


if __name__ == "__main__":
    factory = FeedFactory()
    feed = factory["arxiv/cs.LG"]
    out = feed.fetch(1)
    print(out)
