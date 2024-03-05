import logging
import asyncio
import aiohttp
from datetime import datetime, timedelta

from dailyprophet.feeds.feed import Feed

logger = logging.getLogger(__name__)


class LihkgFeed(Feed):
    API_BASE_URL = "https://lihkg.com/api_v2/thread/category"
    THREAD_BASE_URL = "https://lihkg.com/thread"

    def __init__(self, cat_id):
        self.cat_id = int(cat_id)
        self.page = 1
        self.count = 60
        self.url = f"{LihkgFeed.API_BASE_URL}?cat_id={self.cat_id}&page={self.page}&count={self.count}"

        self.cache = None
        self.cache_expiration = None
        self.cache_duration = timedelta(hours=1)

    def parse(self, item):
        id = item["thread_id"]
        return {
            "type": "lihkg",
            "category": self.cat_id,
            "id": id,
            "title": item["title"],
            "user_id": item["user"]["user_id"],
            "user_nickname": item["user"]["nickname"],
            "create_time": item["create_time"],
            "last_reply_time": item["last_reply_time"],
            "no_of_reply": item["no_of_reply"],
            "like_count": item["like_count"],
            "dislike_count": item["dislike_count"],
            "url": f"{LihkgFeed.THREAD_BASE_URL}/{id}",
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
                return self.cache[:n]

            headers = {
                "Cookie": "_pbjs_userid_consent_data=3524755945110770; _ga=GA1.1.2061876890.1706507029; __eoi=ID=1ff786c190b397fe:T=1706857251:RT=1708878428:S=AA-AfjZcMaVCMGMfF4KUXtk1nzr7; PHPSESSID=f9svlorb56hehdkio1sh6e0htr; __cfruid=f318e6d5da3a474c15b78b6db068fb3bb7147144-1709037896; _cfuvid=3MHH9kZc9Ohy4y1qPr0NlZQQlUyfDVVsifkQZX8Xlvw-1709037896366-0.0-604800000; cf_clearance=VbEj_OW17Vv5zIbUgfF15QdNzVNYV9DFZ6pvyr4zjI8-1709037898-1.0-ASPQ7tL4ZO7TcMhjWzKbzV1ssz+raCzQXTahUQ3dAnDZ8z0xzpEE7//w2+EhCFocyHv/9aU//flSZmfJ6gy0p1I=; __gads=ID=526aadf7783a840e:T=1706507034:RT=1709037947:S=ALNI_MZP0hDngTEUSEOcIh-3EA-_rdH9MA; __gpi=UID=00000cf3240b385c:T=1706507034:RT=1709037947:S=ALNI_MaN0wHjSnGPOFR9UyQxokvAVFZauA; _ga_XXT0Y3PW8P=GS1.1.1709037898.3.1.1709038008.0.0.0; __cf_bm=mCvV3hQCXdLByZEhKqYPEPYpuCxJcxXmIyIS4RkwAy0-1709042009-1.0-AfYgv4x5YRfpq3B+tVzWnhmiwUHXDHFdp/KT8kgD64A1RBg21dRuLSxJsgo9YdmrTbBlTlIp+Iz0KbOnqIWWJUs=; FCNEC=%5B%5B%22AKsRol9caZyvv_aszYi2pFxSaFd5z-0-2Nq99eAG36Fty9X3OivPWZNR40E8wm1lTSqy7kVO0YNBsHkXB3BIsF3S30GJp-2j4gpdKR_qoS2ScF5fosPU7z-V2PXepiyePtGOkdHQdUVZXhntMaYVsaaF0NFiFJa3dA%3D%3D%22%5D%5D; _ga_PPY9Z37CCJ=GS1.1.1709042009.12.1.1709042021.48.0.0; _ga_L8WS4GS6YR=GS1.1.1709042009.11.1.1709042021.0.0.0",
                "Referer": "https://lihkg.com/category/1",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(self.url, headers=headers) as response:
                    data = await response.json()
                    logger.debug(response.status)
                    items = data["response"]["items"]

                    parsed_items = [self.parse(item) for item in items[:n]]

                    # Update the cache
                    self.cache = parsed_items
                    logger.debug(f"Cache size: {len(self.cache)}")
                    self.cache_expiration = datetime.now() + self.cache_duration
                    logger.debug(f"Cache expiration: {self.cache_expiration}")

                    return parsed_items

        except Exception as e:
            logger.error(f"Error fetching LIHKG threads asynchronously: {e}")
            return []

    def fetch(self, n: int):
        # For backward compatibility, call the asynchronous version synchronously
        return asyncio.run(self.async_fetch(n))


if __name__ == "__main__":
    lihkg = LihkgFeed(5)
    out = lihkg.fetch(50)
    print(out)
