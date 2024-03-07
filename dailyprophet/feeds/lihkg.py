# lihkg.py
import logging
from datetime import datetime, timedelta
import asyncio
from math import ceil

import aiohttp
from aiohttp import ClientResponseError

from dailyprophet.feeds.feed import Feed
from dailyprophet.feeds.util import flatten_dict, expo_decay_weighted_sample

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class LihkgFeed(Feed):
    BASE_URL = "https://lihkg.com"
    API_BASE_URL = "https://lihkg.com/api_v2/thread"
    THREAD_BASE_URL = "https://lihkg.com/thread"
    MIN_THUMBS = 100

    def __init__(self, q: str):
        self.q = q
        self.cache = []
        self.cache_expiration = None
        self.cache_duration = timedelta(hours=1)
        self.fetch_lock = asyncio.Lock()

    def format_url(self, endpoint, params):
        params_str = "&".join([f"{key}={value}" for key, value in params.items()])
        return f"{LihkgFeed.API_BASE_URL}/{endpoint}?{params_str}"

    def format_headers(self, endpoint, params):
        new_params = params.copy()
        new_params.pop("count")
        new_params.pop("page")
        params_str = "&".join([f"{key}={value}" for key, value in new_params.items()])
        headers = {
            # "Cookie": "_pbjs_userid_consent_data=3524755945110770; _ga=GA1.1.2061876890.1706507029; _ga_XXT0Y3PW8P=GS1.1.1709043596.4.1.1709043596.0.0.0; PHPSESSID=nidj2fu7hve5v18q2mvsluveb9; __cfruid=951dac58a97ccbb273a4d619a9df1af46ce06c98-1709618426; _cfuvid=sPctx55P0TzJH9RRmjwazl11hC1ZF4tYTG2LvkLmjs8-1709618426251-0.0.1.1-604800000; cf_clearance=hlgoYh0sX.7LLw3UDUANynC.lme2DQbfc96tv5Ll9XY-1709618428-1.0.1.1-5k8eSeL2_EssbwsrTOe66It3gRlEeK02NstNJkunJ.MxzAFSr00R6e3Z407QzxDBs.NfL1NHuZ8xW6AhjbuotQ; __cf_bm=BgTvXaXwTaYD57GtCkRgi.pj1aTjZBwL1aZbhxjZ8jw-1709621432-1.0.1.1-Z_s0Je4w97xsDGTx4nQBLMwGMF0QY4Opejb1l1gLntDSTGyONZXG6ru3zr9TrMTerjhJ5klRFLolcduB4lIHXw; _ga_L8WS4GS6YR=GS1.1.1709618428.12.1.1709621433.0.0.0; __gads=ID=526aadf7783a840e:T=1706507034:RT=1709621433:S=ALNI_MZP0hDngTEUSEOcIh-3EA-_rdH9MA; __gpi=UID=00000cf3240b385c:T=1706507034:RT=1709621433:S=ALNI_MaN0wHjSnGPOFR9UyQxokvAVFZauA; __eoi=ID=1ff786c190b397fe:T=1706857251:RT=1709621433:S=AA-AfjZcMaVCMGMfF4KUXtk1nzr7; FCNEC=%5B%5B%22AKsRol_mo3zXolqtv3s_a_RQVgvuQesYYOrmszFWSCzqFxb6nO9oWT-BF0YKRpn7gQysTkYEizrdqXCrY5-SCJT_iizJ9Kbkbi9tvLw9ALaV3qHvBoLX6RAu5msqHimgYjbn25hMi_t-r6pMJKL0uCJ6ttepqE22xA%3D%3D%22%5D%5D; _ga_PPY9Z37CCJ=GS1.1.1709618428.13.1.1709622026.30.0.0",
            "Cookie": "_pbjs_userid_consent_data=3524755945110770; _ga=GA1.1.2061876890.1706507029; _ga_XXT0Y3PW8P=GS1.1.1709043596.4.1.1709043596.0.0.0; PHPSESSID=nidj2fu7hve5v18q2mvsluveb9; __cfruid=951dac58a97ccbb273a4d619a9df1af46ce06c98-1709618426; _cfuvid=sPctx55P0TzJH9RRmjwazl11hC1ZF4tYTG2LvkLmjs8-1709618426251-0.0.1.1-604800000; __gads=ID=526aadf7783a840e:T=1706507034:RT=1709624794:S=ALNI_MZP0hDngTEUSEOcIh-3EA-_rdH9MA; __gpi=UID=00000cf3240b385c:T=1706507034:RT=1709624794:S=ALNI_MaN0wHjSnGPOFR9UyQxokvAVFZauA; __eoi=ID=1ff786c190b397fe:T=1706857251:RT=1709741090:S=AA-AfjZcMaVCMGMfF4KUXtk1nzr7; cf_clearance=4KClBQk1uuz1JCZAOisnGflydtMKH731HuX61WC0k1Y-1709768545-1.0.1.1-.auA1IrtjS01AhsHjpsZnMupSQTAAxVrpg_JoDhXSYQzu3IbJb.BOvcdOfi0oiI.p1VaC5Jzp7yPfIynh_aiRQ; FCNEC=%5B%5B%22AKsRol92HO9xm8kOq2_7gG0jwJCzRj9808IZIOykC3hjKswhr9O_a0chdVqHdtPctA7CGDKTriHzPjQ3BBY_WPYSF_qtLwI-yvWT-RxHx08Y_vjlXw_k8MQ8CmS8bTh0hWpPvQc4LPTR9_Ee6ZLXfdIuSnvpRK8Lpw%3D%3D%22%5D%5D; __cf_bm=0WTOq.10Fyn52DjrRGHewO1eGC8G1I1CDp6kDVd40Ic-1709770399-1.0.1.1-UXgZKIGUyDw3i1Z5tA4mZa0npi6N2Kmfrumc5V2pJ.VmVCoItChzVpYKFT42XQec_qlZ8dsEMBYZkxy_qvImfA; _ga_PPY9Z37CCJ=GS1.1.1709770399.18.0.1709770399.60.0.0; _ga_L8WS4GS6YR=GS1.1.1709770399.16.0.1709770399.0.0.0",
            "Referer": f"{LihkgFeed.BASE_URL}/{endpoint}?{params_str}",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }
        return headers

    async def async_fetch_url(self, url: str, headers: dict = {}):
        try:
            async with aiohttp.ClientSession() as session:
                logger.debug(f"Fetching: {url}")
                async with session.get(url, headers=headers) as response:
                    if response.ok:
                        data = await response.json()
                        return data["response"]["items"]
                    else:
                        response.raise_for_status()
        except ClientResponseError as e:
            raise
        except Exception as e:
            logger.error(e)
            return []

    def _is_expired(self):
        return self.cache_expiration is None or datetime.now() >= self.cache_expiration

    def _check_cache(self):
        logger.debug("Checking cache")
        return self.cache if not self._is_expired() else []

    def update_cache(self, parsed_items):
        self.cache = parsed_items
        logger.debug(f"Cache size: {len(self.cache)}")
        self.cache_expiration = datetime.now() + self.cache_duration
        logger.debug(f"Cache expiration: {self.cache_expiration}")

    async def async_fetch_category(self, n: int):
        """
        Not used
        """
        params = {
            "cat_id": self.q,
            "page": 1,
            "count": n,
        }
        url = self.format_url("category", params)
        return await self.async_fetch_url(url)

    async def async_fetch_search(self, n: int):
        try:
            params = {
                "q": self.q,
                "page": 1,
                "count": n,
                "sort": "desc_reply_time",  # "desc_create_time" or "score"
                "type": "thread",
            }
            url = self.format_url("search", params)
            headers = self.format_headers("search", params)
            return await self.async_fetch_url(url, headers=headers)
        except Exception as e:
            logger.error(f"Error fetching LIHKG threads asynchronously: {e}")
            return []

    async def async_fetch(self, n: int):
        valid_cache = self._check_cache()
        try:
            # attempt to fill the cache if no other coroutine already does
            is_locked = self.fetch_lock.locked()
            if n > len(valid_cache) and self._is_expired() and not is_locked:
                # Acquire the lock before scheduling the background task
                logger.info("Attempt to fill cache")
                async with self.fetch_lock:
                    logger.info("LOCK acquired")

                    buffer_factor = 1
                    fetch_size = (
                        ceil(n / 50 * (1 + buffer_factor)) * 50
                    )  # keep a cache with size of multiple of 50
                    items = await self.async_fetch_search(fetch_size)
                    parsed_items = []
                    for item in items:
                        thumb_count = item["like_count"] + item["dislike_count"] + item["reply_like_count"] + item["reply_like_count"]
                        if thumb_count >= LihkgFeed.MIN_THUMBS:
                            parsed_items.append(self.parse(item))

                    self.update_cache(parsed_items)
                    await asyncio.sleep(0.1)  # keep the lock longer

                    return expo_decay_weighted_sample(self.cache, k=n)
            else:
                return expo_decay_weighted_sample(valid_cache, k=n)
        except Exception as e:
            logger.error(f"Error fetching LIHKG threads asynchronously: {e}")
            return []

    def parse(self, item):
        thread_id = item["thread_id"]
        url = f"{LihkgFeed.THREAD_BASE_URL}/{thread_id}"
        flattened_item = flatten_dict(item)
        return {
            "source": "lihkg",
            "q": self.q,
            "url": url,
            **flattened_item,
        }

    def fetch(self, n: int):
        # For backward compatibility, call the asynchronous version synchronously
        return asyncio.run(self.async_fetch(n))


async def test_async_fetch():
    import json

    lihkg = LihkgFeed("23%E6%A2%9D")
    # lihkg = LihkgFeed("sex")
    fetch_size = 50

    fetch_task1 = asyncio.create_task(lihkg.async_fetch(fetch_size))
    fetch_task2 = asyncio.create_task(lihkg.async_fetch(fetch_size))
    sleep_task = asyncio.create_task(asyncio.sleep(2))

    # Explicitly wait for the background task and the main task to complete
    out, _, _ = await asyncio.gather(fetch_task1, fetch_task2, sleep_task)

    print("Returned from async_fetch:")
    print("size: ", len(out))

    fetch_task3 = asyncio.create_task(lihkg.async_fetch(fetch_size))
    out = await fetch_task3

    print("Returned from async_fetch:")
    print(json.dumps(out, indent=2))
    print("size: ", len(out))


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
