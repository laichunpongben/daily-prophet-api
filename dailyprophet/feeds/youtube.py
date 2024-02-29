from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from dailyprophet.feeds.feed import Feed
from dailyprophet.configs import YOUTUBE_API_KEY


class YoutubeFeed(Feed):
    def __init__(self, channel_id):
        super().__init__()
        self.api_key = YOUTUBE_API_KEY
        self.channel_id = channel_id
        self.youtube = build("youtube", "v3", developerKey=self.api_key)

    def parse(self, video):
        id = video["id"]["videoId"]
        return {
            "type": "youtube",
            "channel": video["snippet"]["channelTitle"],
            "id": id,
            "title": video["snippet"]["title"],
            "description": video["snippet"]["description"],
            "publishTime": video["snippet"]["publishTime"],
            "url": f"https://www.youtube.com/watch?v={id}",
        }

    def fetch(self, n: int):
        """
        {'kind': 'youtube#searchListResponse', 'etag': 'jc-rdyHBmHEXSiliJpkZgKi1V90', 'nextPageToken': 'CAoQAA', 'regionCode': 'JP', 'pageInfo': {'totalResults': 785686, 'resultsPerPage': 10},
        'items': [{'kind': 'youtube#searchResult', 'etag': 'p4F3OjEyVeLn4j3aJuAd6y-PiO4', 'id': {'kind': 'youtube#video', 'videoId': 'KjqpLdO3_CU'}, 'snippet': {'publishedAt': '2024-02-25T17:45:00Z',
        'channelId': 'UCQHX6ViZmPsWiYSFAyS0a3Q', 'title': 'HOW TO WIN AT CHESS!!!!!!!', 'description': 'Get My Chess Courses: https://www.chessly.com/ ➡️ Get my best-selling chess book: https://geni.us/gothamchess ➡️ My book ...',
        'thumbnails': {'default': {'url': 'https://i.ytimg.com/vi/KjqpLdO3_CU/default.jpg', 'width': 120, 'height': 90}, 'medium': {'url': 'https://i.ytimg.com/vi/KjqpLdO3_CU/mqdefault.jpg', 'width': 320, 'height': 180},
        'high': {'url': 'https://i.ytimg.com/vi/KjqpLdO3_CU/hqdefault.jpg', 'width': 480, 'height': 360}}, 'channelTitle': 'GothamChess', 'liveBroadcastContent': 'none', 'publishTime': '2024-02-25T17:45:00Z'}}
        """
        try:
            response = (
                self.youtube.search()
                .list(
                    part="snippet",
                    channelId=self.channel_id,
                    order="date",
                    type="video",
                    maxResults=n,
                )
                .execute()
            )

            items = response.get("items", [])
            result = [self.parse(item) for item in items]
            return result

        except HttpError as e:
            error_content = e.content.decode("utf-8") if e.content else ""
            raise Exception(
                f"Error fetching YouTube feed: {str(e)}\nContent: {error_content}"
            )

        except Exception as e:
            raise Exception(f"Unexpected error fetching YouTube feed: {str(e)}")
