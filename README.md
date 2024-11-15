# Weather Data Quality Pipeline
A robust data pipeline that fetches weather data from OpenWeatherMap API and implements comprehensive data quality monitoring.

## Features
- **Real-time Weather Data Collection**
  - Fetches data from OpenWeatherMap API
  - Supports multiple cities
  - Handles API rate limiting and errors

- **Comprehensive Data Validation**
  - Range checks for temperature, humidity, and pressure
  - Missing value detection
  - Anomaly detection
  - Weather pattern consistency checks

- **Advanced Quality Metrics**
  - Completeness scoring
  - Accuracy assessment
  - Data freshness monitoring
  - Cross-field consistency validation

- **Efficient Data Storage**
  - Parquet file format for metrics storage
  - City-based partitioning
  - Timestamp-based organization


## Technical Stack
- Python 3.9+
- pandas & numpy for data processing
- requests for API interaction
- great-expectations for data validation
- pydantic for data modeling
- python-dotenv for configuration
- pytest for testing

## Getting Started
1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/weather-quality-pipeline.git
   cd weather-quality-pipeline
   ```
   
2. **Set up virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   cp .env.template .env
   # Edit .env file with your OpenWeatherMap API key and other settings
   ```

4. **Start monitoring service**
   ```bash
   uvicorn src.monitoring.service:app --reload --port 8000
   ```

5. **Run the pipeline (in another terminal)**
   ```bash
   python src/main.py
   ```

## Data Quality Metrics
The pipeline calculates the following quality metrics:

1. **Completeness Score**
   - Measures the presence of required data points
   - Tracks missing value ratios
  
2. **Accuracy Score**
   - Validates data ranges for weather parameters
   - Detects anomalous values
   - Monitors measurement precision

3. **Freshness Score**
   - Tracks data update frequency
   - Monitors data delays
   - Validates timestamp consistency

4. **Consistency Score**
   - Cross-validates related weather parameters
   - Checks for physically impossible combinations
   - Monitors rapid changes in measurements
  
## Testing
The project includes comprehensive tests for all components:

- Unit tests for API interaction
- Validation logic tests
- Metrics calculation tests
- Integration tests for the complete pipeline

Run tests with coverage reporting:
```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

## Future Improvements
- Implement real-time anomaly detection
- Add support for additional weather data sources
- Enhance metrics with machine learning-based validation
- Add data quality SLA monitoring
- Implement automated alerting for quality issues
