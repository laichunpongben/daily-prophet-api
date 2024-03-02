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
            # Ignore timestamp when removing from the set
            hashable_feed = tuple(
                (key, value)
                for key, value in feed.items()
                if key != FeedQueue.timestamp_key
            )
            self.set.remove(hashable_feed)
            return feed
        else:
            return None

    def clear(self):
        self.q.clear()
        self.set.clear()
        logger.info("Queue cleared.")
