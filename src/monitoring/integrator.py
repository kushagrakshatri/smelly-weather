import logging
import asyncio
import aiohttp
from datetime import datetime, timezone
from typing import Dict, Optional
import pandas as pd
from validators.quality_checks import WeatherDataValidator
from extractors.weather_api import WeatherDataExtractor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherMonitoringIntegrator:
    """Integrates weather data extraction, validation, and monitoring"""

    def __init__(self, api_key: str, cities: list, monitoring_api_url: str = "http://localhost:8000"):
        """
        Initialize the integrator

        Args:
            api_key (str): OpenWeatherMap API Key
            cities (list): List of cities to monitor
            monitoring_api_url (str): URL of the monitoring service
        """

        self.extractor = WeatherDataExtractor(api_key=api_key, cities=cities)
        self.validator = WeatherDataValidator()
        self.cities = cities
        self.monitoring_api_url = monitoring_api_url

    def _calculate_metrics(self, df: pd.DataFrame, city: str) -> Dict:
        """
        Calculate metrics for a specific city

        Args:
            df (pd.DataFrame): Weather Data
            city (str): City name

        Returns:
            Dict: Calculated Metrics
        """

        city_data = df[df['city'] == city]
        if city_data.empty:
            return {}
        
        return {
            'temperature': float(city_data['temperature'].mean()),
            'humidity': float(city_data['humidity'].mean()),
            'pressure': float(city_data['pressure'].mean()),
            'wind_speed': float(city_data['wind_speed'].mean()),
            'data_completeness': float(
                (1 - city_data.isnull().sum().sum() / (len(city_data) * len(city_data.columns))) * 100
            )
        }
    
    async def _send_to_monitoring_service(self, city: str, validation_result: Dict, metrics: Dict) -> None:
        """
        Send validation results to monitoring service

        Args:
            city (str): City Name
            validation_result (Dict): Validation Results
            metrics (Dict): Calculated Metrics
        """

        payload = {
            'status': validation_result['status'],
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'city': city,
            'issues': validation_result['issues'],
            'metrics': metrics
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(f"{self.monitoring_api_url}/record-validation", json=payload) as response:
                    if response.status != 200:
                        logger.error(f"Failed to send monitoring data for {city}"
                        f"Status: {response.status}")
                    else:
                        logger.info(f"Successfully recorded monitoring data for {city}")
            except Exception as e:
                logger.error(f"Error sending monitoring data for {city}: {str(e)}")

    async def process_single_city(self, df: pd.DataFrame, city: str) -> None:
        """
        Process validation and monitoring for a single city

        Args:
            df (pd.DataFrame): Weather Data
            city (str): City Name
        """

        try:
            city_data = df[df['city'] == city]
            if city_data.empty:
                logger.warning(f"No data available for {city}")
                return
            
            validation_result = self.validator.validate_data(city_data)

            metrics = self._calculate_metrics(df, city)

            await self._send_to_monitoring_service(city=city, validation_result=validation_result, metrics=metrics)

        except Exception as e:
            logger.error(f"Error processing {city}: {str(e)}")

    async def run_monitoring_cycle(self) -> None:
        """Run a complete monitoring cycle"""

        try:
            df = self.extractor.fetch_weather_data()
            if df.empty:
                logger.error("No weather data fetched")
                return
            
            await asyncio.gather(*(self.process_single_city(df, city) for city in self.cities))

        except Exception as e:
            logger.error(f"Error in monitoring cycle: {str(e)}")

    async def start_monitoring_cycle(self, interval_seconds: int = 300) -> None:
        """
        Start continous monitoring

        Args:
            interval_seconds (int): Interval between monitoring cycles in seconds
        """

        logger.info("Starting weather monitoring...")

        while True:
            await self.run_monitoring_cycle()
            await asyncio.sleep(interval_seconds)
