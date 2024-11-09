import requests
from typing import Optional, Dict
from datetime import datetime
import pandas as pd
from pydantic import BaseModel, Field
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherData(BaseModel):
    """Pydantic model for validating weather data"""
    city: str
    timestamp: datetime
    temperature: float = Field(..., gt = -100, lt = 100)
    humidity: float = Field(..., ge=0, le=100)
    pressure: float = Field(..., gt=800, lt=1200)
    wind_speed: float = Field(..., ge=0)
    weather_condition: str

class WeatherDataExtractor:
    """Class to handle weather data extraction from OpenWeatherMap API"""

    def __init__(self, api_key, cities):
        """
        Initialize the extractor with API key and list of cities

        Args:
            api_key (str): OpenWeatherMap API Key
            cities (List[str]): List of city names to fetch weather data for
        """
        self.api_key = api_key
        self.cities = cities
        self.base_url = "http://api.openweathermap.org/data/2.5/weather"

    def _make_api_request(self, city:str) -> Optional[Dict]:
        """
        Make a single API request for a city

        Args:
            city (str): City name to fetch weather data for

        Returns:
            Optional[Dict]: JSON response from API or None if request fails
        """

        params = {
            'q': city,
            'appid': self.api_key,
            'units': 'metric'
        }

        try:
            response = requests.get(self.base_url, params = params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"Timeout while fetching data for {city}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {city}: {str(e)}")
            return None

    def _parse_weather_data(self, city: str, raw_data: Dict) -> Optional[WeatherData]:
        """
        Parse raw API response into WeatherData model

        Args:
            city (str): City Name
            raw_data (Dict): Raw API Response

        Returns:
            Optional[WeatherData]: Parsed and Validated Weather Data
        """
        
        try:
            weather_data = WeatherData(
                city=city,
                timestamp=datetime.utcnow,
                temperature=raw_data['main']['temp'],
                humidity=raw_data['main']['humidity'],
                pressure=raw_data['main']['pressure'],
                wind_speed=raw_data['wind']['speed'],
                weather_condition=raw_data['weather'][0]['main']
            )
            return weather_data
        except Exception as e:
            logger.error(f"Error parsing data for {city}: {str(e)}")
            return None
        
    def fetch_weather_data(self) -> pd.DataFrame:
        """
        Fetch weather data for all cities

        Returns:
            pd.DataFrame: DataFrame containing weather data for all cities
        """

        weather_data = []

        for city in self.cities:
            raw_data = self._make_api_request(city)
            if raw_data:
                parsed_data = self._parse_weather_data(city, raw_data)
                if parsed_data:
                    weather_data.append(parsed_data.dict())

        if not weather_data:
            logger.warning("No weather data was collected for any city")
            return pd.DataFrame()
        
        return pd.DataFrame(weather_data)
    
    def validate_api_key(self) -> bool:
        """
        Validate if the API key is working

        Returns:
            bool: True if API key is valid, False otherwise
        """

        if not self.cities:
            return False
        
        test_city = self.cities[0]
        response = self._make_api_request(test_city)

        return response is not None