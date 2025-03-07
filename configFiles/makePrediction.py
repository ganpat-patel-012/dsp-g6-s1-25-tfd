# api.py
import requests
from configFiles.config import API_URL

def get_prediction(payload):
    """Calls FastAPI endpoint and returns the predicted price."""
    try:
        response = requests.post(f"{API_URL}/predict", json=payload)
        if response.status_code == 200:
            return response.json().get("predicted_price", "N/A")
        else:
            return f"Error {response.status_code}"
    except requests.RequestException as e:
        return f"❌ API Request Failed: {e}"
