from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
import pandas as pd
from urllib.parse import quote

username="kd"
password=quote("Nb!R5EvEW9GzXB@")
database="meteomatics"
host="tempraturestestdb.database.windows.net"

# Define database connection string (replace with your connection details)
DATABASE_URL = f"mssql+pyodbc://{username}:{password}@{host}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
# Create database engine
engine = create_engine(DATABASE_URL)

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello, Kosmas!"}

@app.get("/locations")
def list_locations():
    """List all available locations."""
    query = "SELECT top 1 * FROM [dbo].[tempratures]"
    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()  # Fetch all rows

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=result.keys())  

    # Convert DataFrame to JSON
    json_data = df.to_json(orient="records", indent=4)
    return json_data
