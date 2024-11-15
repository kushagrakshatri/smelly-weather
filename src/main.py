import asyncio
import logging
from dotenv import load_dotenv
import os
from monitoring.integrator import WeatherMonitoringIntegrator

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    api_key = os.getenv('OPENWEATHER_API_KEY')
    cities = os.getenv('MONITORED_CITIES', 'London,New York,Tokyo').split(',')
    monitoring_url = os.getenv('MONITORING_SERVICE_URL', 'http://localhost:8000')
    interval = int(os.getenv('MONITORING_INTERVAL_SECONDS', '300'))

    if not api_key:
        raise ValueError("OpenWeather API key not found in environment variables")
    
    try:
        integrator = WeatherMonitoringIntegrator(api_key=api_key, cities=cities, monitoring_api_url=monitoring_url)

        await integrator.start_monitoring_cycle(interval_seconds=interval)

    except KeyboardInterrupt:
        logger.info("Shutting down monitoring service")
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    print("App started")
    asyncio.run(main())