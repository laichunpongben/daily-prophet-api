import logging

import requests

from dailyprophet.feeds.feed import Feed
from dailyprophet.configs import FOURSQUARE_API_KEY

logger = logging.getLogger(__name__)


class FoursquareFeed(Feed):
    BASE_URL = "https://api.foursquare.com"
    PLACE_SEARCH_ROUTE = "/v3/places/search"

    def __init__(self, param: str):
        super().__init__()

        query, categories, near, radius, sort = param.split(";")

        self.query = query
        self.categories = categories
        self.near = near
        self.radius = int(radius)
        self.sort = sort.strip().upper()

        self.auth_token = FOURSQUARE_API_KEY

    def parse(self, place):
        """
        {'results': [{'fsq_id': '565071c3498e84bcd5ea4e34', 'categories': [{'id': 13034, 'name': 'Café', 'short_name': 'Café', 'plural_name': 'Cafés',
        'icon': {'prefix': 'https://ss3.4sqi.net/img/categories_v2/food/cafe_', 'suffix': '.png'}}, {'id': 13035, 'name': 'Coffee Shop', 'short_name': 'Coffee Shop', 'plural_name': 'Coffee Shops',
        'icon': {'prefix': 'https://ss3.4sqi.net/img/categories_v2/food/coffeeshop_', 'suffix': '.png'}}], 'chains': [], 'closed_bucket': 'VeryLikelyOpen', 'distance': 470,
        'geocodes': {'drop_off': {'latitude': 48.849135, 'longitude': 2.347579}, 'main': {'latitude': 48.84917, 'longitude': 2.347508}, 'roof': {'latitude': 48.84917, 'longitude': 2.347508}},
        'link': '/v3/places/565071c3498e84bcd5ea4e34', 'location': {'address': '14 rue des Carmes', 'admin_region': 'Île-de-France', 'country': 'FR', 'cross_street': 'Rue des Écoles',
        'formatted_address': '14 rue des Carmes (Rue des Écoles), 75005 Paris', 'locality': 'Paris', 'postcode': '75005', 'region': 'Île-de-France'}, 'name': 'Nuage Café', 'related_places': {}, 'timezone': 'Europe/Paris'}]}
        """
        return {
            "type": "foursquare",
            "name": place.get("name", "No Name"),
            "id": place.get("fsq_id", ""),
            "address": place.get("location", {}).get("formatted_address", "No Address"),
            "category": place.get("categories", [{}])[0].get("name", "No Category"),
            "distance": place.get("distance", 0),
            "latitude": place.get("geocodes", {}).get("main", {}).get("latitude", 0.0),
            "longitude": place.get("geocodes", {})
            .get("main", {})
            .get("longitude", 0.0),
            "open": place.get("closed_bucket", "No info on open"),
            "url": f"{FoursquareFeed.BASE_URL}{place.get('link', '')}",
        }

    def fetch(self, n: int):
        try:
            params = {
                "query": self.query,
                "categories": self.categories,
                "near": self.near,
                "sort": self.sort,
                "limit": 50,  # max from Foursquare
            }

            headers = {
                "Authorization": self.auth_token,
                "accept": "application/json",
            }

            url = f"{FoursquareFeed.BASE_URL}{FoursquareFeed.PLACE_SEARCH_ROUTE}"
            response = requests.get(url, params=params, headers=headers)
            data = response.json()
            logger.debug(data)

            places = data.get("results", [])

            parsed_places = []
            for place in places[:n]:
                parsed_place = self.parse(place)
                parsed_places.append(parsed_place)

            return parsed_places

        except Exception as e:
            logger.error(f"Error fetching Foursquare places: {e}")
            return []


if __name__ == "__main__":
    import time

    foursquare = FoursquareFeed("fine dining;13049;Paris,France;1000;POPULARITY")
    start = time.time()
    result = foursquare.fetch(5)
    logger.info(result)
    end = time.time()
    logger.info(end - start)
