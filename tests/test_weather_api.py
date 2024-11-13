import requests
import pytest
from unittest.mock import patch, Mock
from datetime import datetime, timezone
import pandas as pd
from src.extractors.weather_api import WeatherDataExtractor, WeatherData

@pytest.fixture
def sample_weather_response():
    return {
        "main": {
            "temp": 20.5,
            "humidity": 65,
            "pressure": 1013
        },
        "wind": {
            "speed": 5.5
        },
        "weather": [
            {"main": "Clouds"}
        ]
    }

@pytest.fixture
def weather_extractor():
    return WeatherDataExtractor(
        api_key="dummy",
        cities=["London", "New York"]
    )

def test_weather_data_model_valid():
    """Tests that WeatherDataModel validates correct data"""

    data = WeatherData(
        city = "London",
        timestamp=datetime.now(timezone.utc),
        temperature=20.5,
        humidity=65,
        pressure=1013,
        wind_speed=5.5,
        weather_condition="Clouds"
    )

    assert data.city == "London"
    assert isinstance(data.timestamp, datetime)

def test_weather_data_model_invalid():
    """Tests that WeatherDataModel rejects invalid data"""

    with pytest.raises(ValueError):
        WeatherData(
            city = "London",
            timestamp=datetime.now(timezone.utc),
            temperature=150,
            humidity=65,
            pressure=1013,
            wind_speed=5.5,
            weather_condition="Clouds"
        )

@patch('requests.get')
def test_make_api_request_success(mock_get, weather_extractor, sample_weather_response):
    """Test Successful API Request"""

    mock_response = Mock()
    mock_response.json.return_value = sample_weather_response
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = weather_extractor._make_api_request("London")
    assert result == sample_weather_response
    mock_get.assert_called_once()

@patch('requests.get')
def test_make_api_request_timeout(mock_get, weather_extractor):
    """Test API request timeout"""

    mock_get.side_effect = requests.exceptions.Timeout
    result = weather_extractor._make_api_request("London")
    assert result is None

def test_parse_weather_data(weather_extractor, sample_weather_response):
    """Test parsing of weather data"""

    result = weather_extractor._parse_weather_data("London", sample_weather_response)
    assert isinstance(result, WeatherData)
    assert result.city == "London"
    assert result.temperature == 20.5
    assert result.humidity == 65

@patch('requests.get')
def test_fetch_data_weather_data_integration(mock_get, weather_extractor, sample_weather_response):
    """Test the complete data fetching process"""

    mock_response = Mock()
    mock_response.json.return_value = sample_weather_response
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = weather_extractor.fetch_weather_data()

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert list(result.columns) == ['city', 'timestamp', 'temperature', 'humidity', 'pressure', 'wind_speed', 'weather_condition']
    assert len(result) == len(weather_extractor.cities)

def test_empty_cities_list():
    """Test behavior with empty cities list"""

    extractor = WeatherDataExtractor(api_key="dummy", cities=[])
    result = extractor.fetch_weather_data()

    assert isinstance(result, pd.DataFrame)
    assert result.empty

@patch('requests.get')
def test_validate_api_key(mock_get, weather_extractor, sample_weather_response):
    """Test API key validation"""

    mock_response = Mock()
    mock_response.json.return_value = sample_weather_response
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    assert weather_extractor.validate_api_key() is True

    mock_get.side_effect = requests.exceptions.RequestException
    assert weather_extractor.validate_api_key() is False