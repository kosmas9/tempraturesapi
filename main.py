from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
import pandas as pd

username="kd"
password="Nb!R5EvEW9GzXB@"
database="meteomatics"
host="tempraturestestdb.database.windows.net"

# Define database connection string (replace with your connection details)
DATABASE_URL = f"mssql+pyodbc://{username}:{password}@{host}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello, Kosmas!"}

# Create database engine
engine = create_engine(DATABASE_URL)

app = FastAPI()

@app.get("/locations")
def list_locations():
    """List all available locations."""
    query = "SELECT * FROM [dbo].[tempratures]"
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return {"locations": [row[0] for row in result]}
