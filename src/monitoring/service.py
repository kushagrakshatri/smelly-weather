from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, List
import pandas as pd
from datetime import datetime, timedelta
import sqlite3
import json
from pydantic import BaseModel

class ValidationResult(BaseModel):
    status: str
    timestamp: datetime
    city: str
    issues: List[str]
    metrics: Dict[str, float]

class HealthStatus(BaseModel):
    status: str
    last_update: datetime
    total_records: int
    error_rate: float

def get_db_connection():
    """Create a database connection"""

    conn = sqlite3.connect('weather_monitoring.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()

    conn.execute('''
        CREATE TABLE IF NOT EXISTS validation_results (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 timestamp TIMESTAMP,
                 city TEXT,
                 status TEXT,
                 issues TEXT,
                 metrics TEXT)
                ''')
    
    conn.commit()
    conn.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield()

app = FastAPI(title="Weather Data Quality Dashboard API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware
)

@app.get("/")
async def root() -> Dict[str, str]:
    """Root endpoint - shows API information"""
    return {
        "message": "Weather Data Quality Dashboard API",
        "version": "1.0.0",
        "documentation": "/docs",
        "health_check": "/health"
    }

@app.get("/health", response_model=HealthStatus)
async def get_health_status():
    """Get overall system health status"""

    conn = get_db_connection()
    try:
        cur = conn.execute('''
            SELECT COUNT(*) as total,
                AVG(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as error_rate,
                MAX(timestamp) as last_update
            FROM validation_results
            WHERE timestamp > datetime('now', '-1 day') 
            ''')
        
        row = cur.fetchone()

        return HealthStatus(
            status = "healthy" if row['error_rate'] < 0.1 else "degraded",
            last_update = datetime.fromisoformat(row['last_update']),
            total_records = row['total'],
            error_rate = row['error_rate']
        )
    finally:
        conn.close()

@app.get("/validation-history/{city}")
async def get_validation_history(city: str, hours: int = 24):
    """Get validation history for a specific city"""

    conn = get_db_connection()
 
    try:
        cur = conn.execute('''
            SELECT * FROM validation_results
            WHERE city = ? AND timestamp > datetime('now', ?)
            ORDER BY timestamp DESC
        ''', (city, f'-{hours} hours'))

        results = []
        for row in cur.fetchall():
            result = dict(row)
            result['issues'] = json.loads(result['issues'])
            result['metrics'] = json.loads(result['metrics'])
            results.append(result)

        return results
    finally:
        conn.close()

@app.get("/cities")
async def get_monitored_cities():
    """Get list of all monitored cities"""

    conn = get_db_connection()

    try:
        cur = conn.execute('SELECT DISTINCT city FROM validation_results')
        cities = [row['city'] for row in cur.fetchall()]
        return cities
    finally:
        conn.close()

@app.get('/summary')
async def get_summary_stats():
    """Get summary statistics for the dashboard"""

    conn = get_db_connection()

    try:
        cur = conn.execute('''
                    SELECT
                           city,
                           COUNT(*) as total_checks,
                           SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_checks,
                           MAX(timestamp) as last_check
                    FROM validation_results
                    WHERE timestamp > datetime('now', '24 hours')
                    GROUP BY city
                ''')
        
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

@app.post("/record-validation")
async def record_validation(result: ValidationResult):
    """Record a new validation result"""

    conn = get_db_connection()

    try:
        conn.execute('''
            INSERT INTO validation_results (timestamp, city, status, issues, metrics)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            result.timestamp,
            result.city,
            result.status,
            json.dumps(result.issues),
            json.dumps(result.metrics)
        ))
        conn.commit()
    finally:
        conn.close()
    
    return {"status": "recorded"}