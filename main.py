from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from sqlalchemy import create_engine, text
import pandas as pd
from urllib.parse import quote
import json
from typing import List
from enum import Enum

username="kd"
password=quote("Nb!R5EvEW9GzXB@")
database="meteomatics"
host="tempraturestestdb.database.windows.net"

# Define database connection string (replace with your connection details)
DATABASE_URL = f"mssql+pyodbc://{username}:{password}@{host}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
# Create database engine
engine = create_engine(DATABASE_URL)

app = FastAPI()

class Metrics(str, Enum):
    temperature = "temperature"
    windspeed = "windspeed"
    precipitation = "precipitation"

@app.get("/")
def read_root():
    return RedirectResponse(url="/docs")

@app.get("/locations")
def list_locations():
    """List all available locations."""
    query = "SELECT distinct city FROM [dbo].[weatherMetrics]"
    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()

    res = [row[0] for row in rows]
    return {"cities": res}

@app.get("/latest_temperatures")
def latest_temperatures():
    """Get the latest temperature for each location for every day."""
    query = """
            WITH LatestTemperatures AS (
            SELECT 
                city, 
                datetime,
                temperature,
                ROW_NUMBER() OVER (PARTITION BY city, CAST(datetime AS DATE) ORDER BY datetime DESC) AS rn
            FROM [dbo].[weatherMetrics]
            )
            SELECT city, datetime, temperature
            FROM LatestTemperatures
            where rn=1
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()
    
    res = {}
    for city, datetime, temperature in rows:
        if city not in res:
            res[city] = []
        res[city].append({
            "datetime": datetime,  # Convert datetime object to string
            "temperature": temperature
        })

    return res

@app.get("/average_temp")
def average_tempratures():
    """Get the average temprature of the last 3 forecasts for each location for every day."""
    query = """
            WITH LastThreeTemperatures AS (
            SELECT 
                city,
                CAST(datetime AS DATE) AS date,
                datetime,
                temperature,
                ROW_NUMBER() OVER (PARTITION BY city, CAST(datetime AS DATE) ORDER BY datetime DESC) AS rn
            FROM [dbo].[weatherMetrics]
        )
            SELECT 
                city,
                date,
                round(AVG(temperature),1) AS avg_temperature
            FROM LastThreeTemperatures
            WHERE rn <= 3 
            GROUP BY city, date

    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()
    
    res = {}
    for city, date, temperature in rows:
        if city not in res:
            res[city] = []  
    
        
        res[city].append({
            "date": date,
            "avg_temperature": temperature
        })

    return res  


@app.get("/top_cities_per_day/")
def get_top_cities_per_day(
    n: int = Query(3, alias="n"),
    metrics: List[Metrics] = Query([Metrics.temperature], description="Select metrics. Pres CTRL for multiple selection")
    ):
    """
    Get the top N cities per day and return the max values for each selected metric.
    """
    selected_metrics = [m for m in metrics ]

    res = {}
    for metric in selected_metrics:
        query = f"""
        WITH RankedCities AS (
            SELECT 
                CAST(datetime AS DATE) AS date,
                city,
                MAX({metric.value}) AS max_{metric.value},
                ROW_NUMBER() OVER (PARTITION BY CAST(datetime AS DATE) ORDER BY MAX({metric.value}) DESC) AS rn
            FROM [dbo].[weatherMetrics]
            GROUP BY CAST(datetime AS DATE), city
        )
        SELECT date, city, max_{metric.value}
        FROM RankedCities
        WHERE rn <= {n}
        """

        with engine.connect() as conn:
            result = conn.execute(text(query), {"n": n})
            rows = result.fetchall()

        metric_results = {}
        for row in rows:
            date, city, max_value = row
            str_date = str(date)

            if str_date not in metric_results:
                metric_results[str_date] = []

            metric_results[str_date].append({
                "city": city,
                metric.value: max_value
            })

        res[metric.value] = metric_results

    return res
