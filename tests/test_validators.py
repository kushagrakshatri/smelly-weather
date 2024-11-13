import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from src.validators.quality_checks import WeatherDataValidator



@pytest.fixture
def sample_weather_data():
    """Create sample weather data for testing"""
    return pd.DataFrame({
        'city': ['London', 'New York', 'Tokyo'],
        'timestamp': [datetime.now(timezone.utc) for _ in range(3)],
        'temperature': [20.5, 22.0, 25.0],
        'humidity': [65, 70, 75],
        'pressure': [1013, 1014, 1012],
        'wind_speed': [5.5, 6.0, 6.5],
        'weather_condition': ['Clouds', 'Clear', 'Rain']
    })

@pytest.fixture
def validator():
    """Create WeatherDataValidator instance"""
    return WeatherDataValidator()

def test_create_expectation_suite(validator):
    """Test creation of Great Expectations suite"""

    suite = validator.create_expectation_suite()

    assert isinstance(suite, dict)
    assert 'expectations' in suite
    assert len(suite['expectations']) > 0

def test_check_data_freshness(validator, sample_weather_data):
    """Test data freshness validation"""

    issues = validator.check_data_freshness(sample_weather_data)
    assert len(issues) == 0

    stale_data = sample_weather_data.copy()
    stale_data['timestamp'] = datetime.now(timezone.utc) - timedelta(hours=2)
    issues = validator.check_data_freshness(stale_data)

    assert len(issues) > 0
    assert all('minutes old' in issue for issue in issues)

def test_detect_anomalies(validator, sample_weather_data):
    """Test anomaly detection"""

    issues = validator.detect_anomalies(sample_weather_data)
    assert len(issues) == 0

    anomalous_data = sample_weather_data.copy()
    anomalous_data.loc[0, 'temperature'] = 100
    issues = validator.detect_anomalies(anomalous_data)
    
    assert len(issues) > 0
    assert 'Anomaly detected' in issues[0]

def test_validate_weather_patterns(validator, sample_weather_data):
    """Test weather data validation"""

    issues = validator.validate_weather_patterns(sample_weather_data)
    assert len(issues) == 0

    suspicious_data = sample_weather_data.copy()
    suspicious_data.loc[0, 'temperature'] = 40
    suspicious_data.loc[0, 'humidity'] = 5

    issues = validator.validate_weather_patterns(suspicious_data)
    
    assert len(issues) > 0
    assert 'Suspicious temperature-humidity' in issues[0]

def test_validate_data_integration(validator, sample_weather_data):
    """Test complete validation process"""

    results = validator.validate_data(sample_weather_data)
    assert results['status'] == 'passed'
    assert len(results['issues']) == 0
    assert results['total_records'] == len(sample_weather_data)

    invalid_data = sample_weather_data.copy()
    invalid_data.loc[0, 'temperature'] = None
    invalid_data.loc[1, 'humidity'] = 150

    results = validator.validate_data(invalid_data)

    assert results['status'] == 'failed'
    assert len(results['issues']) > 0

def test_edge_cases(validator):
    """Test edge cases"""

    empty_df = pd.DataFrame(columns = ['city', 'timestamp', 'temperature', 'humidity', 'pressure', 'wind_speed', 'weather_condition'])
    results = validator.validate_data(empty_df)
    assert results['status'] == 'passed'
    assert results['total_records'] == 0

    incomplete_df = pd.DataFrame({
        'city': ['London'],
        'temperature': [20.5],
        'timestamp': datetime.now(timezone.utc)
    })

    results = validator.validate_data(incomplete_df)
    assert results['status'] == 'failed'