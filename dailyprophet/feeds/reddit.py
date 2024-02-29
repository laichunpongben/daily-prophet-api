"""
{'comment_limit': 2048, 'comment_sort': 'confidence', '_reddit': <praw.reddit.Reddit object at 0x7f7b22a8bc10>, 'approved_at_utc': None, 'subreddit': Subreddit(display_name='singularity'),
'selftext': '', 'author_fullname': 't2_65reu', 'saved': False, 'mod_reason_title': None, 'gilded': 0, 'clicked': False, 'title': 'The future of Software Development', 'link_flair_richtext': [],
'subreddit_name_prefixed': 'r/singularity', 'hidden': False, 'pwls': 6, 'link_flair_css_class': '', 'downs': 0, 'thumbnail_height': 140, 'top_awarded_type': None, 'hide_score': False, 'name': 't3_1b012a1',
'quarantine': False, 'link_flair_text_color': 'dark', 'upvote_ratio': 0.9, 'author_flair_background_color': None, 'ups': 417, 'total_awards_received': 0, 'media_embed': {}, 'thumbnail_width': 140,
'author_flair_template_id': None, 'is_original_content': False, 'user_reports': [], 'secure_media': None, 'is_reddit_media_domain': False, 'is_meta': False, 'category': None, 'secure_media_embed': {},
'link_flair_text': 'memes', 'can_mod_post': False, 'score': 417, 'approved_by': None, 'is_created_from_ads_ui': False, 'author_premium': False,
'thumbnail': 'https://b.thumbs.redditmedia.com/DcKNldpl_0bW_UvgsdImQFeunap061d_wM93CW-J-Ss.jpg', 'edited': False, 'author_flair_css_class': None, 'author_flair_richtext': [], 'gildings': {},
'post_hint': 'image', 'content_categories': None, 'is_self': False, 'subreddit_type': 'public', 'created': 1708900213.0, 'link_flair_type': 'text', 'wls': 6, 'removed_by_category': None, 'banned_by': None,
'author_flair_type': 'text', 'domain': 'i.imgflip.com', 'allow_live_comments': False, 'selftext_html': None, 'likes': None, 'suggested_sort': None, 'banned_at_utc': None,
'url_overridden_by_dest': 'https://i.imgflip.com/8h16tq.jpg', 'view_count': None, 'archived': False, 'no_follow': False, 'is_crosspostable': False, 'pinned': False, 'over_18': False,
'preview': {'images': [{'source': {'url': 'https://external-preview.redd.it/2uCOgheGQ8cFy8eNiMp2aMkhrnImC9boJwnd1BtXONs.jpg?auto=webp&s=c27357523ea4d78455a2a4e213d09dafa80756f1', 'width': 500, 'height': 756},
'resolutions': [{'url': 'https://external-preview.redd.it/2uCOgheGQ8cFy8eNiMp2aMkhrnImC9boJwnd1BtXONs.jpg?width=108&crop=smart&auto=webp&s=963317d04a4dc8fa4560de41cb6c9799c6fff70a', 'width': 108, 'height': 163},
{'url': 'https://external-preview.redd.it/2uCOgheGQ8cFy8eNiMp2aMkhrnImC9boJwnd1BtXONs.jpg?width=216&crop=smart&auto=webp&s=54f9c8c2b143ea4e575bd5fa1d562926bbbf4e2d', 'width': 216, 'height': 326},
{'url': 'https://external-preview.redd.it/2uCOgheGQ8cFy8eNiMp2aMkhrnImC9boJwnd1BtXONs.jpg?width=320&crop=smart&auto=webp&s=efb06f111d6d3f47c1af6953ccbc61b3381e85c7', 'width': 320, 'height': 483}], 'variants': {},
'id': 'W0yyETV6qzbizY-5CJf6g6qpF-vyG3fKoansZC407FM'}], 'enabled': True}, 'all_awardings': [], 'awarders': [], 'media_only': False, 'link_flair_template_id': 'ca784836-8d59-11ec-aebe-82c8ece84e86',
'can_gild': False, 'spoiler': False, 'locked': False, 'author_flair_text': None, 'treatment_tags': [], 'visited': False, 'removed_by': None, 'mod_note': None, 'distinguished': None, 'subreddit_id': 't5_2qh8m',
'author_is_blocked': False, 'mod_reason_by': None, 'num_reports': None, 'removal_reason': None, 'link_flair_background_color': '#0079d3', 'id': '1b012a1', 'is_robot_indexable': True, 'report_reasons': None,
'author': Redditor(name='spookmann'), 'discussion_type': None, 'num_comments': 116, 'send_replies': False, 'whitelist_status': 'all_ads', 'contest_mode': False, 'mod_reports': [], 'author_patreon_flair': False,
'author_flair_text_color': None, 'permalink': '/r/singularity/comments/1b012a1/the_future_of_software_development/', 'parent_whitelist_status': 'all_ads', 'stickied': False, 'url': 'https://i.imgflip.com/8h16tq.jpg',
'subreddit_subscribers': 1953563, 'created_utc': 1708900213.0, 'num_crossposts': 1, 'media': None, 'is_video': False, '_fetched': False, '_additional_fetch_params': {}, '_comments_by_id': {}}
"""

import logging
import asyncio
from datetime import datetime, timedelta
from math import ceil

import asyncpraw
from aiohttp import ClientSession

from dailyprophet.feeds.feed import Feed
from dailyprophet.feeds.util import expo_decay_weighted_sample
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

    def parse(self, post):
        return {
            "type": "reddit",
            "community": self.community,
            "id": post.name,
            "title": post.title,
            "author": post.author.name,
            "ups": post.ups,
            "downs": post.downs,
            "url": post.url,
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

            min_ups = RedditFeed.MIN_UPS
            scaling = 1
            parsed_posts = []
            last_seen_id = None

            async with ClientSession() as session:
                client = self.create_client(session)
                subreddit = await client.subreddit(self.community)
                while len(parsed_posts) < n:
                    params = {"after": last_seen_id} if last_seen_id else {}
                    async for post in subreddit.hot(
                        limit=fetch_size * scaling, params=params
                    ):
                        if post.ups >= min_ups:
                            parsed_post = self.parse(post)
                            parsed_posts.append(parsed_post)

                        last_seen_id = post.name

                    scaling *= 2

                    if len(parsed_posts) >= n:
                        break  # Fetched enough posts, exit the loop

                # Update the cache
                self.cache = parsed_posts
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

    reddit = RedditFeed("singularity")
    start = time.time()
    out = reddit.fetch(2)
    end = time.time()
    print(out)
    print(end - start)
    out = reddit.fetch(2)
    print(out)
