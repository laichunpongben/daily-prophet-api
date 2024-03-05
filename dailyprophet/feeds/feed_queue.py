# feed_queue.py

from collections import deque
from datetime import datetime
from typing import List
import logging

logger = logging.getLogger(__name__)


class FeedQueue:
    timestamp_key = "timestamp"

    def __init__(self):
        self.q = deque()
        self.set = set()

    def size(self):
        return len(self.q)

    def push(self, feeds: List[dict]):
        current_timestamp = datetime.now().timestamp()
        for feed in feeds:
            feed_with_timestamp = {FeedQueue.timestamp_key: current_timestamp, **feed}

            # Ignore timestamp when checking for uniqueness
            hashable_feed = tuple(feed.items())
            if hashable_feed not in self.set:
                self.q.append(feed_with_timestamp)
                self.set.add(hashable_feed)
            else:
                logger.info("Duplicate. Skip adding to the queue.")

    def pop(self):
        if self.q:
            feed = self.q.popleft()
            hashable_feed = self.create_hashable_feed(feed)
            self.set.remove(hashable_feed)
            return feed
        else:
            return None

    def clear(self):
        self.q.clear()
        self.set.clear()
        logger.info("Queue cleared.")

    def trim_last(self, n: int):
        count = 0
        for _ in range(n):
            try:
                feed = self.q.pop()
                hashable_feed = self.create_hashable_feed(feed)
                self.set.remove(hashable_feed)
                count += 1
            except IndexError:
                break
        logger.info(f"Trimmed {count} items in queue.")
        return count

    def trim_last_until(self, n: int):
        count = 0
        while self.size() > n:
            feed = self.q.pop()
            hashable_feed = self.create_hashable_feed(feed)
            self.set.remove(hashable_feed)
            count += 1
        remaining = self.size()
        logger.info(f"Trimmed {count} items in queue. Remaining {remaining} items.")
        return count

    def create_hashable_feed(self, feed):
        """
        Ignore timestamp when removing from the set
        """
        return tuple(
            (key, value)
            for key, value in feed.items()
            if key != FeedQueue.timestamp_key
        )
