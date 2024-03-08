import aiohttp

from .feed import Feed
from ..util import flatten_dict
from ..configs import OPENWEATHERMAP_API_KEY


class OpenWeatherMapFeed(Feed):
    CURRENT_WEATHER_BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
    DAILY_FORECAST_BASE_URL = "http://api.openweathermap.org/data/2.5/forecast/daily"
    GEO_BASE_URL = "http://api.openweathermap.org/geo/1.0/direct"
    ONECALL_BASE_URL = "http://api.openweathermap.org/data/3.0/onecall"

    def __init__(self, city: str):
        super().__init__()
        self.api_key = OPENWEATHERMAP_API_KEY
        self.city = city

    async def async_fetch(self, n: int = 1):
        try:
            # current_weather_data = await self.get_current_weather(self.city)
            daily_forcast_data = await self.get_daily_forecast(self.city)
            parsed_data = self.parse(daily_forcast_data)
            return [parsed_data]
        except Exception as e:
            return [{"error": f"An error occurred: {str(e)}"}]

    async def get_current_weather(self, city):
        params = {"q": city, "appid": self.api_key}
        return await self._make_async_request(
            OpenWeatherMapFeed.CURRENT_WEATHER_BASE_URL, params
        )

    async def get_daily_forecast(self, city):
        params = {"q": city, "appid": self.api_key}
        return await self._make_async_request(
            OpenWeatherMapFeed.DAILY_FORECAST_BASE_URL, params
        )

    async def _make_async_request(self, url, params):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                data = await response.json()

                if response.status == 200:
                    return data
                else:
                    return {"error": f"Error: {data['message']}"}

    def parse(self, weather_data):
        return self.parse_daily_forecast(weather_data)

    def parse_current_weather(self, weather_data):
        city_name = weather_data.get("name", "")
        temperature = self.correct_temperature(weather_data.get("main", {}).get("temp"))
        feels_like = self.correct_temperature(
            weather_data.get("main", {}).get("feels_like")
        )
        weather_description = weather_data.get("weather", [{}])[0].get("description")

        return {
            "source": "openweathermap",
            "city": city_name,
            "temperature": temperature,
            "feels_like": feels_like,
            "description": weather_description,
        }

    def parse_daily_forecast(self, weather_data):
        flattened_dict = flatten_dict(weather_data)

        for key, value in flattened_dict.items():
            if "temp" in key and isinstance(value, (int, float)):
                flattened_dict[key] = self.correct_temperature(value)

        return {"source": "openweathermap", **flattened_dict}

    def correct_temperature(self, temp_kelvin):
        # Helper function to correct temperature from Kelvin to Celsius
        if temp_kelvin is not None:
            return round(temp_kelvin - 273.15, 2)
        return None

    def fetch(self, n: int = 1):
        current_weather_data = self.get_current_weather(self.city)
        parsed_data = self.parse(current_weather_data)
        return [parsed_data]


if __name__ == "__main__":
    import asyncio

    async def test_async_fetch():
        weather_feed = OpenWeatherMapFeed("Hong Kong")
        result = await weather_feed.async_fetch()
        print(result)

    asyncio.run(test_async_fetch())
