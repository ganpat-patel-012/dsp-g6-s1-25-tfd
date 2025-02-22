from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import joblib
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime
from configFiles.config import DB_CONFIG

# Load the trained model
#this is updated. bellow line
model = joblib.load(r"C:\Users\hp\dsp-g6-s1-25-tfd\mlModel\flight_price_predictor.pkl")


# Initialize FastAPI app
app = FastAPI()

# Add CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins; change to specific domains for security
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

# Define input schema using Pydantic
class FlightData(BaseModel):
    airline: str
    source_city: str
    destination_city: str
    departure_time: str
    arrival_time: str
    travel_class: str
    stops: str
    duration: float
    days_left: int

# Define the prediction endpoint
@app.post("/predict")
def predict_price(flight_data: FlightData):
    # Convert input data to a format suitable for the model
    input_df = pd.DataFrame([flight_data.dict()])

    # Predict using the loaded model
    prediction = model.predict(input_df)

    # Return the predicted price
    return {"predicted_price": round(prediction[0], 2)}


def get_connection():
    """Establish a PostgreSQL connection."""
    return psycopg2.connect(**DB_CONFIG)


@app.get("/past-predictions")
def past_predictions(start_date: str, end_date: str, source: str = "all"):
    """
    Fetch past predictions filtered by date range and source.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Convert input strings to datetime objects (to ensure proper formatting)
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        # Base query (Extract DATE from timestamp)
        query = """
            SELECT * FROM predictions
            WHERE prediction_time::DATE BETWEEN %s AND %s
        """
        params = [start_date, end_date]

        # Apply source filter if not "All"
        if source.lower() != "all":
            query += " AND LOWER(prediction_source) = LOWER(%s)"
            params.append(source)

        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()

        conn.close()

        # Convert results into JSON-friendly format
        predictions = [dict(row) for row in rows]

        return predictions  # ✅ Always return a list

    except Exception as e:
        return {"error": str(e)}


        # Convert results into JSON-friendly format
        predictions = [dict(row) for row in rows]

        return predictions if predictions else {"message": "No data found in the given range"}

    except Exception as e:
        return {"error": str(e)}


# Run this FastAPI: uvicorn configFiles.fastAPI:app --reload
# http://127.0.0.1:8000/docs#/
