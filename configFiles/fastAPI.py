from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import joblib
import pandas as pd
from configFiles.config import DB_CONFIG
import psycopg2
import psycopg2.extras
from datetime import datetime

# Load the trained model
model = joblib.load("mlModel/flight_price_predictor.pkl")

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class FlightInput(BaseModel):
    airline: str
    source_city: str
    destination_city: str
    departure_time: str
    arrival_time: str
    travel_class: str
    stops: str
    duration: float
    days_left: int

@app.post("/predict")
def predict_price(flight_data: FlightInput):
    input_df = pd.DataFrame([flight_data.dict()])
    prediction = model.predict(input_df)
    return {"predicted_price": round(prediction[0], 2)}

@app.post("/predict_batch")
def predict_batch(flight_list: List[FlightInput]):
    input_data = [fd.dict() for fd in flight_list]
    df = pd.DataFrame(input_data)
    predictions = model.predict(df)
    return [{"predicted_price": round(p, 2)} for p in predictions]

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

@app.get("/past-predictions")
def past_predictions(start_date: str, end_date: str, source: str = "all"):
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        query = "SELECT * FROM predictions WHERE prediction_time::DATE BETWEEN %s AND %s"
        params = [start_date, end_date]
        if source.lower() != "all":
            query += " AND LOWER(prediction_source) = LOWER(%s)"
            params.append(source)

        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    except Exception as e:
        return {"error": str(e)}
