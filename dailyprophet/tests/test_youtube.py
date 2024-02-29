import unittest

from aioresponses import aioresponses

from dailyprophet.feeds.youtube import YoutubeFeed
from dailyprophet.configs import YOUTUBE_API_KEY


class TestYoutubeFeed(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.channel_id = "UCQHX6ViZmPsWiYSFAyS0a3Q"
        self.api_key = YOUTUBE_API_KEY
        self.n = 5
        self.expected_result = [
            {
                "type": "youtube",
                "channel": "GothamChess",
                "id": "KjqpLdO3_CU",
                "title": "HOW TO WIN AT CHESS!!!!!!!",
                "description": "Get My Chess Courses: https://www.chessly.com/ ➡️ Get my best-selling chess book: https://geni.us/gothamchess ➡️ My book ...",
                "publishTime": "2024-02-25T17:45:00Z",
                "url": "https://www.youtube.com/watch?v=KjqpLdO3_CU",
            },
            # Add more expected results as needed
        ]

    async def test_async_fetch(self):
        with aioresponses() as mock_responses:
            mock_responses.get(
                f"https://www.googleapis.com/youtube/v3/search?part=snippet&channelId={self.channel_id}&order=date&type=video&maxResults={self.n}&key={self.api_key}",
                payload={
                    "items": [
                        {
                            "id": {"videoId": "KjqpLdO3_CU"},
                            "snippet": {
                                "channelTitle": "GothamChess",
                                "title": "HOW TO WIN AT CHESS!!!!!!!",
                                "description": "Get My Chess Courses: https://www.chessly.com/ ➡️ Get my best-selling chess book: https://geni.us/gothamchess ➡️ My book ...",
                                "publishTime": "2024-02-25T17:45:00Z",
                            },
                        }
                    ]
                },
                repeat=self.n,
            )

            youtube_feed = YoutubeFeed(self.channel_id)
            result = await youtube_feed.async_fetch(self.n)
            print(result)
            self.assertEqual(result, self.expected_result)


if __name__ == "__main__":
    unittest.main()
