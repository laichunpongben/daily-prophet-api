import logging
import asyncio
from datetime import datetime, timedelta
from math import ceil

import asyncpraw
from aiohttp import ClientSession

from dailyprophet.feeds.feed import Feed
from dailyprophet.feeds.util import expo_decay_weighted_sample
from dailyprophet.gemini_service import async_summarize_discussion
from dailyprophet.configs import REDDIT_CLIENT_ID, REDDIT_CLEINT_SECRET

logger = logging.getLogger(__name__)

class RedditFeed(Feed):
    MIN_UPS = 100

    def __init__(self, community: str):
        super().__init__()
        self.community = community
        self.cache = None
        self.cache_expiration = None
        self.cache_duration = timedelta(hours=1)

    def create_client(self, session: ClientSession):
        return asyncpraw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLEINT_SECRET,
            user_agent="daily-prophet/0.0.1",
            requestor_kwargs={"session": session},
        )

    def parse(self, submission, text, summary):
        return {
            "type": "reddit",
            "community": self.community,
            "id": submission.name,
            "title": submission.title,
            "author": submission.author.name,
            "ups": submission.ups,
            "downs": submission.downs,
            "num_comments": submission.num_comments,
            "over_18": submission.over_18,
            "created_utc": submission.created_utc,
            "url": submission.url,
            "text": text,
            "summary": summary,
        }

    async def async_fetch_submission(self, submission):
        await submission.load()  # Fetch the submission before accessing comments
        post_text = submission.selftext
        comments = []

        async for comment in submission.comments:
            await comment.load()  # Fetch each comment before accessing its properties
            comments.append({
                "id": comment.id,
                "author": comment.author.name if comment.author is not None else None,
                "ups": comment.ups,
                "downs": comment.downs,
                "body": comment.body,
                "created_utc": comment.created_utc,
            })

        return post_text, comments


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

            min_ups = RedditFeed.MIN_UPS
            scaling = 1
            parsed_submissions = []
            last_seen_id = None

            async with ClientSession() as session:
                client = self.create_client(session)
                subreddit = await client.subreddit(self.community)

                fetch_size = ceil(n / 30) * 30  # keep a cache with size of multiple of 30
                while len(parsed_submissions) < fetch_size:
                    params = {"after": last_seen_id} if last_seen_id else {}
                    async for submission in subreddit.hot(
                        limit=fetch_size * scaling, params=params
                    ):
                        if submission.ups >= min_ups:
                            post_text, comments = await self.async_fetch_submission(submission)

                            bodies = [submission.title, post_text] + [comment["body"] for comment in comments]
                            text = " ".join(bodies)

                            summary = await async_summarize_discussion(text)

                            parsed_submission = self.parse(submission, text, summary)
                            parsed_submissions.append(parsed_submission)

                        last_seen_id = submission.name

                    scaling *= 2

                    if len(parsed_submissions) >= n:
                        break  # Fetched enough posts, exit the loop

                # Update the cache
                self.cache = parsed_submissions
                logger.debug(f"Cache size: {len(self.cache)}")
                self.cache_expiration = datetime.now() + self.cache_duration
                logger.debug(f"Cache expiration: {self.cache_expiration}")

            return expo_decay_weighted_sample(self.cache, k=n)
        except Exception as e:
            logger.error(f"Error fetching Reddit posts asynchronously: {e}")
            return []

    def fetch(self, n: int):
        # For backward compatibility, call the asynchronous version synchronously
        return asyncio.run(self.async_fetch(n))

if __name__ == "__main__":
    import time
    import json

    start = time.time()
    reddit = RedditFeed("singularity")
    out = reddit.fetch(1)
    print(json.dumps(out, indent=2))
    end = time.time()
    lapsed = end - start
    print("Lapsed: ", lapsed)
