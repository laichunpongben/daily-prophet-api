# reddit.py
import logging
import asyncio
from datetime import datetime, timedelta
from math import ceil
from dataclasses import dataclass

import asyncpraw
from aiohttp import ClientSession

from dailyprophet.feeds.feed import Feed
from dailyprophet.feeds.util import expo_decay_weighted_sample
from dailyprophet.gemini_service import async_summarize_discussion
from dailyprophet.configs import REDDIT_CLIENT_ID, REDDIT_CLEINT_SECRET

logger = logging.getLogger(__name__)


@dataclass
class CachedItem:
    data: dict
    expiration: datetime


class RedditFeed(Feed):
    MIN_UPS = 100
    COMMENT_LIMIT = 10

    def __init__(self, community: str):
        super().__init__()
        self.community = community
        self.cache = []
        self.cache_duration = timedelta(hours=1)
        self.fetch_lock = asyncio.Lock()  # Lock to control concurrent fetches

    def create_client(self, session: ClientSession):
        return asyncpraw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLEINT_SECRET,
            user_agent="daily-prophet/0.4.3",
            requestor_kwargs={"session": session},
        )

    def parse(self, submission, text, summary):
        try:
            return {
                "type": "reddit",
                "community": self.community,
                "id": submission.name,
                "title": submission.title,
                "author": (
                    submission.author.name if submission.author is not None else None
                ),
                "ups": submission.ups,
                "downs": submission.downs,
                "num_comments": submission.num_comments,
                "over_18": submission.over_18,
                "created_utc": submission.created_utc,
                "url": submission.url,
                "text": text,
                "summary": summary,
            }
        except Exception as e:
            logger.error(e)
            return {
                "type": "reddit",
                "community": self.community,
                "id": submission.name,
            }

    async def async_fetch_submission(self, submission):
        logger.debug("REDDIT: async_fetch_submission")
        await submission.load()  # Fetch the submission before accessing comments
        logger.debug("REDDIT: submission loaded")
        post_text = submission.selftext
        comments = []

        async def enumerate_async(iterable, start=0):
            count = start
            async for item in iterable:
                yield count, item
                count += 1

        async def fetch_comment(comment):
            logger.debug("REDDIT: fetch_comment")
            await comment.load()  # Fetch each comment before accessing its properties
            logger.debug("REDDIT: comment loaded")
            return {
                "id": comment.id,
                "author": comment.author.name if comment.author is not None else None,
                "ups": comment.ups,
                "downs": comment.downs,
                "body": comment.body,
                "created_utc": comment.created_utc,
            }

        comment_tasks = []
        async for index, comment in enumerate_async(submission.comments):
            if index >= RedditFeed.COMMENT_LIMIT:
                break
            comment_tasks.append(fetch_comment(comment))

        comments = await asyncio.gather(*comment_tasks)

        return post_text, comments

    async def _check_cache(self):
        """
        Check if the cache is still valid and return the cached result if possible.
        If the cache is not enough, initiate background fetching to fill the gap.
        """
        logger.debug("Checking cache")
        now = datetime.now()
        valid_cache = [item.data for item in self.cache if now < item.expiration]
        return valid_cache

    async def _update_cache_background(self, target_size: int):
        """
        Update the cache in the background until it reaches the target size.
        """
        try:
            logger.info(f"REDDIT: Background task _update_cache_background started")

            now = datetime.now()
            fetch_size = max(
                0,
                target_size
                - len([item for item in self.cache if now < item.expiration]),
            )
            min_ups = RedditFeed.MIN_UPS
            scaling = 1
            parsed_submissions = []
            last_seen_id = None

            async with ClientSession() as session:
                client = self.create_client(session)
                subreddit = await client.subreddit(self.community)
                logger.debug("REDDIT: session started")
                logger.debug(f"REDDIT: subreddit {subreddit}")

                async with asyncio.timeout(300):
                    while len(parsed_submissions) < fetch_size:
                        logger.debug("REDDIT: while loop")
                        logger.debug(f"REDDIT: len parsed {len(parsed_submissions)}")
                        params = {"after": last_seen_id} if last_seen_id else {}
                        async for submission in subreddit.hot(
                            limit=fetch_size * scaling, params=params
                        ):
                            logger.debug("REDDIT: Browsing submission")
                            if submission.ups >= min_ups:
                                post_text, comments = await self.async_fetch_submission(
                                    submission
                                )

                                bodies = [submission.title, post_text] + [
                                    comment["body"] for comment in comments
                                ]
                                text = " ".join(bodies)

                                summary = await async_summarize_discussion(text)
                                logger.debug("REDDIT: summarize")
                                logger.debug(summary)

                                parsed_submission = self.parse(submission, text, summary)
                                expiration_time = now + self.cache_duration
                                self.cache.append(
                                    CachedItem(
                                        data=parsed_submission, expiration=expiration_time
                                    )
                                )
                                parsed_submissions.append(parsed_submission)

                            last_seen_id = submission.name

                        scaling *= 2

            # Cleanup: Remove expired CachedItems from the cache
            self.cache = [
                item for item in self.cache if item.expiration > datetime.now()
            ]

            logger.info(f"REDDIT: Background task _update_cache_background completed")
        except asyncio.TimeoutError:
            logger.error("REDDIT: Timeout occurred in the while loop") 
        except Exception as e:
            logger.error(f"REDDIT: Error in background task _update_cache_background: {e}")
        finally:
            # Remove the attribute after the background task is done
            if hasattr(self, "_update_cache_task"):
                del self._update_cache_task

    async def async_fetch(self, n: int):
        try:
            valid_cache = await self._check_cache()

            # attempt to fill the cache if no other coroutine already does
            is_locked = self.fetch_lock.locked()
            if n > len(valid_cache) and not is_locked:
                # Acquire the lock before scheduling the background task
                logger.info("Attempt to fill cache")
                async with self.fetch_lock:
                    logger.info("LOCK acquired")

                    target_cache_size = (
                        ceil(n / 30) * 30
                    )  # keep a cache with a size of multiple of 30

                    # Check if the background task has already been scheduled
                    if not hasattr(self, "_update_cache_task"):
                        # Log that the background task is being scheduled
                        logger.info(
                            "Scheduling background task _update_cache_background"
                        )

                        # Schedule the background task only if it hasn't been scheduled yet
                        self._update_cache_task = asyncio.ensure_future(
                            self._update_cache_background(target_cache_size)
                        )
                    await asyncio.sleep(1)  # keep the lock longer

            return expo_decay_weighted_sample(valid_cache, k=n)
        except Exception as e:
            logger.error(f"Error fetching Reddit posts asynchronously: {e}")
            return []


async def test_async_fetch():
    import json

    reddit = RedditFeed("programming")
    fetch_size = 1

    # Run the async_fetch method and capture the background task
    fetch_task1 = asyncio.create_task(reddit.async_fetch(fetch_size))
    fetch_task2 = asyncio.create_task(reddit.async_fetch(fetch_size))
    sleep_task = asyncio.create_task(asyncio.sleep(300))

    # Explicitly wait for the background task and the main task to complete
    out, _, _ = await asyncio.gather(fetch_task1, fetch_task2, sleep_task)

    print("Returned from async_fetch:")
    print(json.dumps(out, indent=2))

    fetch_task3 = asyncio.create_task(reddit.async_fetch(fetch_size))
    out = await fetch_task3

    print("Returned from async_fetch:")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    import time

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)

    start = time.time()
    asyncio.run(test_async_fetch())
    end = time.time()
    lapsed = end - start
    print("Lapsed: ", lapsed)
