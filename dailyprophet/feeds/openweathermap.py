import aiohttp

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

    async def async_fetch(self, n: int = 1):
        try:
            current_weather_data = await self.get_current_weather(self.city)
            parsed_data = self.parse(current_weather_data)
            return [parsed_data]
        except Exception as e:
            return [{"error": f"An error occurred: {str(e)}"}]

    async def get_current_weather(self, city):
        params = {"q": city, "appid": self.api_key}
        return await self._make_async_request(self.base_url, params)

    async def _make_async_request(self, url, params):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                data = await response.json()

                if response.status == 200:
                    return data
                else:
                    return {"error": f"Error: {data['message']}"}

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


if __name__ == "__main__":
    import asyncio

    async def test_async_fetch():
        weather_feed = OpenWeatherMapFeed("Hong Kong")
        result = await weather_feed.async_fetch()
        print(result)

    asyncio.run(test_async_fetch())
