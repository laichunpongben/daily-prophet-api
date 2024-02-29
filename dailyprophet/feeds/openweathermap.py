import requests

from dailyprophet.feeds.feed import Feed
from dailyprophet.configs import OPENWEATHERMAP_API_KEY


class OpenWeatherMapFeed(Feed):
    def __init__(self, city: str):
        super().__init__()
        self.base_url = "http://api.openweathermap.org/data/2.5/weather"
        self.geo_base_url = "http://api.openweathermap.org/geo/1.0/direct"
        self.onecall_base_url = "http://api.openweathermap.org/data/3.0/onecall"
        self.api_key = OPENWEATHERMAP_API_KEY
        self.city = city

    def get_current_weather(self, city):
        params = {"q": city, "appid": self.api_key}
        return self._make_request(self.base_url, params)

    def geocode_city(self, city):
        params = {"q": city, "appid": self.api_key}
        return self._make_request(self.geo_base_url, params)

    def get_weather_by_coordinates(self, lat, lon):
        params = {
            "lat": lat,
            "lon": lon,
            "exclude": "minutely,hourly",
            "appid": self.api_key,
        }
        return self._make_request(self.onecall_base_url, params)

    def _make_request(self, url, params):
        try:
            response = requests.get(url, params=params)
            data = response.json()

            if response.status_code == 200:
                return data
            else:
                return {"error": f"Error: {data['message']}"}

        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"}

    def parse(self, weather_data):
        city_name = weather_data.get("name", "")
        temperature = self.correct_temperature(weather_data.get("main", {}).get("temp"))
        feels_like = self.correct_temperature(
            weather_data.get("main", {}).get("feels_like")
        )
        weather_description = weather_data.get("weather", [{}])[0].get("description")

        return {
            "type": "openweathermap",
            "city": city_name,
            "temperature": temperature,
            "feels_like": feels_like,
            "description": weather_description,
        }

    def correct_temperature(self, temp_kelvin):
        # Helper function to correct temperature from Kelvin to Celsius
        if temp_kelvin is not None:
            return round(temp_kelvin - 273.15, 2)
        return None

    def fetch(self, n: int = 1):
        current_weather_data = self.get_current_weather(self.city)
        parsed_data = self.parse(current_weather_data)
        return [parsed_data]
