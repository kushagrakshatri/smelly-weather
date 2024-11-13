from typing import Dict, Optional, List
import pandas as pd
import numpy as np
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
from great_expectations import get_context
import logging
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherDataValidator:
    """Class to validate weather data quality"""

    def __init__(self):
        self.validation_rules = {
            'temperature':{
                'min': -50,
                'max': 50,
                'std_dev_threshold': 3
            },
            'humidity':{
                'min': 0,
                'max': 100,
                'std_dev_threshold': 3
            },
            'pressure':{
                'min': 870,
                'max': 1090,
                'std_dev_threshold': 3
            }
        }

    def create_expectation_suite(self) -> Dict:
        """
        Create a GreatExpectations suite for weather data

        Returns:
            Dict: Expectation suite configuration
        """

        null_check_expectations = []

        columns_to_check = ['city', 'timestamp', 'temperature', 'humidity', 'pressure', 'wind_speed', 'weather_condition']

        for col in columns_to_check:
            null_check_expectations.append({
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": col}
            })

        range_check_expectations = [
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "temperature",
                    "min_value": self.validation_rules['temperature']['min'],
                    "max_value": self.validation_rules['temperature']['max']
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "humidity",
                    "min_value": self.validation_rules['humidity']['min'],
                    "max_value": self.validation_rules['humidity']['max']
                }
            }
        ]

        return {
            "expectations": null_check_expectations + range_check_expectations
        }
    
    def check_data_freshness(self, df: pd.DataFrame, max_age_minutes: int = 60) -> List[str]:
        """
        Check if data is fresh enough

        Args:
            df (pd.DataFrame): Weather Data
            max_age_minutes (int): Maximum acceptable age of data in minutes

        Returns:
            List[str]: List of freshness validation issues
        """

        issues = []
        current_time = datetime.now(timezone.utc)

        for city in df['city'].unique():
            city_data = df[df['city'] == city]
            largest_timestamp = city_data['timestamp'].max()
            age = (current_time - largest_timestamp).total_seconds() / 60

            if age > max_age_minutes:
                issues.append(f"Data for {city} is {int(age)} is minutes old (max allowed: {max_age_minutes})")

        return issues
    
    def detect_anomalies(self, df: pd.DataFrame) -> List[str]:
        """
        Detect anomalies in weather data using statistical methods

        Args:
            df (pd.DataFrame): Weather Data

        Returns:
            List[str]: List of detected anomalies
        """

        issues = []

        for column, rules in self.validation_rules.items():
            if column in df.columns and df[column].dtype in ['int64', 'float64']:
                median = df[column].median()
                mad = np.median(np.abs(df[column] - median))
                modified_zscores = np.abs(0.6745 * (df[column] - median) / mad)
                anomalies = df[modified_zscores > rules['std_dev_threshold']]
                if not anomalies.empty:
                    for _, row in anomalies.iterrows():
                        issues.append(
                            f"Anomaly detected in {column} for {row['city']}: value {row[column]:.2f} is {modified_zscores[row.name]:.2f} standard deviations away from mean"
                        )

        return issues
    
    def validate_weather_patterns(self, df: pd.DataFrame) -> List[str]:
        """
        Validate weather patterns for physical consistency

        Args:
            df (pd.DataFrame): Weather data

        Returns:
            List[str]: List of pattern validation issues
        """

        issues = []

        for city in df['city'].unique():
            city_data = df[df['city'] == city]

            if (city_data['temperature'] > 35).any() and (city_data['humidity'] < 10).any():
                issues.append(f"Suspicious temperature-humidity combination in {city}")

            if len(city_data) > 1:
                pressure_change = city_data['pressure'].diff().abs().max()
                if pressure_change > 20:
                    issues.append(f"Suspicious rapid pressure change in {city}: {pressure_change:.2f} hPa")

        return issues
    
    def validate_data(self, df: pd.DataFrame) -> Dict:
        """
        Main validation method that runs all checks

        Args:
            df (pd.DataFrame): Weather data to validate

        Returns:
            Dict: Validation results including all issues
        """

        all_issues = []

        context = get_context()
        
        ge_dataset = context.data_sources.pandas_default.read_dataframe(df)

        suite_dict = self.create_expectation_suite()

        expectations = []
        for exp in suite_dict['expectations']:
            config = ExpectationConfiguration(
                type=exp['expectation_type'],
                kwargs=exp['kwargs']
            )
            expectations.append(config)

        suite = ExpectationSuite(name="weather-validation-suite", expectations=expectations)

        validation_results = ge_dataset.validate(suite)

        for result in validation_results.results:
            if not result.success:
                all_issues.append(f"Data quality check failed: {result.expectation_config.kwargs}")

        all_issues.extend(self.check_data_freshness(df))
        all_issues.extend(self.detect_anomalies(df))
        all_issues.extend(self.validate_weather_patterns(df))

        return {
            'status': 'failed' if all_issues else 'passed',
            'issues': all_issues,
            'timestamp': datetime.now(timezone.utc),
            'total_records': len(df),
            'cities': df['city'].unique().tolist()
        }