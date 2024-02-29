import logging

import tweepy

from dailyprophet.feeds.feed import Feed
from dailyprophet.configs import (
    TWITTER_BEARER_TOKEN,
    TWITTER_ACCESS_TOKEN,
    TWITTER_ACCESS_TOKEN_SECRET,
)

logger = logging.getLogger(__name__)


class TwitterFeed(Feed):
    BASE_URL = "https://api.twitter.com/2/tweets/search/recent"

    def __init__(self, query):
        super().__init__()
        self.query = query
        self.bearer_token = TWITTER_BEARER_TOKEN
        self.access_token = TWITTER_ACCESS_TOKEN
        self.access_token_secret = TWITTER_ACCESS_TOKEN_SECRET

    def parse(self, tweet):
        """
        Example tweet structure from Tweepy:
        {
            'created_at': 'Sun Feb 27 12:34:56 +0000 2022',
            'id': 1234567890123456789,
            'text': 'This is a tweet.',
            'user': {
                'screen_name': 'example_user',
                'name': 'Example User'
            },
            'entities': {
                'urls': [
                    {
                        'url': 'https://example.com',
                        'expanded_url': 'https://example.com',
                        'display_url': 'example.com'
                    }
                ]
            }
        }
        """
        return {
            "type": "twitter",
            "id": tweet["id"],
            "text": tweet["text"],
            "user": tweet["user"]["screen_name"],
            "user_name": tweet["user"]["name"],
            "created_at": tweet["created_at"],
            "url": (
                tweet["entities"]["urls"][0]["expanded_url"]
                if tweet["entities"]["urls"]
                else None
            ),
        }

    def fetch(self, n: int):
        try:
            # Set up your Twitter API credentials here
            # auth = tweepy.OAuthHandler(self.api_key, self.api_key_secret)
            # auth.set_access_token(access_token, access_token_secret)
            # client = tweepy.Client(bearer_token=self.auth_token)
            client = tweepy.Client(
                bearer_token=self.bearer_token,
                access_token=self.access_token,
                access_token_secret=self.access_token_secret,
            )
            print("client created")

            tweets = client.search_recent_tweets(
                self.query, max_results=n, user_auth=False
            )
            parsed_tweets = [self.parse(tweet) for tweet in tweets]
            return parsed_tweets
        except Exception as e:
            logger.error(f"Error fetching Twitter feed: {e}")
            return []


if __name__ == "__main__":
    twitter = TwitterFeed("Haaland")
    out = twitter.fetch(1)
    print(out)
