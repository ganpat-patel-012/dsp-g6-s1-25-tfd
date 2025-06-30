import requests
from configFiles.config import API_URL

def get_prediction(payload):
    try:
        response = requests.post(f"{API_URL}/predict", json=payload)
        if response.status_code == 200:
            return response.json().get("predicted_price", "N/A")
        else:
            return f"Error {response.status_code}"
    except requests.RequestException as e:
        return f"❌ API Request Failed: {e}"

def get_batch_prediction(payload_list):
    try:
        response = requests.post(f"{API_URL}/predict_batch", json=payload_list)
        if response.status_code == 200:
            return response.json()
        else:
            return [{"predicted_price": f"Error {response.status_code}"}] * len(payload_list)
    except requests.RequestException as e:
        return [{"predicted_price": f"❌ API Request Failed: {e}"}] * len(payload_list)
