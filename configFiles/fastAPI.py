from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import joblib
import numpy as np
import pandas as pd

# Load the trained model
model = joblib.load("mlModel/flight_price_predictor.pkl")

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
    pass


# Run this FastAPI: uvicorn configFiles.fastAPI:app --reload
# http://127.0.0.1:8000/docs#/
