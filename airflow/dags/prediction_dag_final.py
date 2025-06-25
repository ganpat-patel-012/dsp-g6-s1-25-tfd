import logging
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from datetime import timedelta
from pendulum import today
import pandas as pd
import os
import requests
import psycopg2
from psycopg2 import sql
from datetime import datetime

# Configuration
API_URL = "http://tfd_fastapi:8000"

DB_CONFIG = {
    "dbname": "tfd_db",
    "user": "tfd_user",
    "password": "tfd_pass",
    "host": "tfd_postgres",
    "port": "5432"
}

# Paths
GOOD_DATA_FOLDER = "/opt/output_data/good_data"
PROCESSED_FILES = "/opt/configFiles"
PROCESSED_FILES_TRACKER = os.path.join(PROCESSED_FILES, "processed_files.txt")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("airflow.task")

# Ensure good_data folder exists
if not os.path.exists(GOOD_DATA_FOLDER):
    logger.error(f"Good data folder does not exist: {GOOD_DATA_FOLDER}")
    raise FileNotFoundError(f"Good data folder does not exist: {GOOD_DATA_FOLDER}")

# Database functions
def get_connection():
    """Establishes a PostgreSQL connection."""
    return psycopg2.connect(**DB_CONFIG)

def insert_prediction(data):
    """Inserts prediction data into PostgreSQL."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                insert_query = sql.SQL("""
                    INSERT INTO predictions 
                    (airline, source_city, destination_city, departure_time, arrival_time, 
                    travel_class, stops, duration, days_left, predicted_price, 
                    prediction_source, prediction_time, prediction_type) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """)
                cursor.execute(insert_query, (
                    data["airline"], data["source_city"], data["destination_city"],
                    data["departure_time"], data["arrival_time"], data["travel_class"],
                    data["stops"], data["duration"], data["days_left"],
                    data["predicted_price"], data["prediction_source"], datetime.now(), data["prediction_type"]
                ))
            conn.commit()
        return "✅ Prediction saved to database!"
    except Exception as e:
        return f"❌ Database Error: {e}"

# API function
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

# DAG configuration
default_args = {
    "owner": "airflow_proooo",
    "retries": 1,
    "start_date": today('UTC').add(days=-1),
    "execution_timeout": timedelta(seconds=300)
}

@dag(
    dag_id="prediction_job_flight_data_proo",
    default_args=default_args,
    schedule_interval="*/2 * * * *",  # Run every 2 minutes
    start_date=today("UTC").add(days=-1),
    catchup=False,
    tags=["data_prediction"],
    description="Scheduled prediction job for new ingested data",
)
def prediction_dag():
    @task
    def check_for_new_data() -> list:
        """Check for new ingested files in the good_data folder. Skip DAG if none found."""
        try:
            # List CSV files in good_data folder
            files = [f for f in os.listdir(GOOD_DATA_FOLDER) if f.endswith(".csv")]
            # Load processed files from tracker
            processed_files = set()
            if os.path.exists(PROCESSED_FILES_TRACKER):
                with open(PROCESSED_FILES_TRACKER, "r") as pf:
                    processed_files = set(line.strip() for line in pf if line.strip())
            
            # Identify new files
            new_files = [f for f in files if f not in processed_files]
            if not new_files:
                logger.info("No new files to predict on. Skipping DAG run.")
                raise AirflowSkipException("No new files to predict on.")
            
            logger.info(f"New files found: {new_files}")
            return [os.path.join(GOOD_DATA_FOLDER, f) for f in new_files]
        except Exception as e:
            logger.error(f"Error checking for new data: {str(e)}")
            raise

    @task
    def make_predictions(files: list):
        """Make predictions on the new files by calling the prediction API."""
        if not files:
            logger.info("No files provided for predictions.")
            return
        
        processed = []
        required_columns = [
            "airline", "source_city", "destination_city", "departure_time", "arrival_time",
            "travel_class", "stops", "duration", "days_left"
        ]

        for file in files:
            logger.info(f"Making predictions for {file}")
            try:
                df = pd.read_csv(file)
                missing_cols = [col for col in required_columns if col not in df.columns]
                if missing_cols:
                    logger.error(f"Missing columns in {file}: {', '.join(missing_cols)}")
                    continue

                df_filtered = df[df['source_city'] != df['destination_city']]
                if df_filtered.shape[0] != df.shape[0]:
                    logger.warning(f"Some rows removed in {file} due to same source & destination.")

                results = []
                for _, row in df_filtered.iterrows():
                    payload = row.to_dict()
                    try:
                        predicted_price = get_prediction(payload)
                        result_data = {
                            **payload,
                            "predicted_price": predicted_price,
                            "prediction_source": "Scheduled Predictions",
                            "prediction_type": "Scheduled Job"
                        }
                        msg = insert_prediction(result_data)
                        print(msg)  # Log the message
                        logger.info(f"Prediction result for row in {file}: {msg}")
                        results.append(result_data)
                    except Exception as e:
                        logger.error(f"Error predicting for row in {file}: {str(e)}")
                        continue

                # Mark file as processed only if at least one row was successfully predicted
                if results:
                    processed.append(os.path.basename(file))
                    logger.info(f"File {file} marked as processed.")
                else:
                    logger.warning(f"No successful predictions for {file}, not marking as processed.")

            except Exception as e:
                logger.error(f"Error processing {file}: {str(e)}")
                continue

        # Update processed files tracker
        if processed:
            try:
                with open(PROCESSED_FILES_TRACKER, "a") as pf:
                    for fname in processed:
                        pf.write(f"{fname}\n")
                logger.info(f"Updated processed files tracker with: {processed}")
            except Exception as e:
                logger.error(f"Error updating processed files tracker: {str(e)}")
                # Optionally, don't raise to avoid failing the task
                # raise  # Uncomment if you want to fail the task on tracker write error

    # Define workflow
    new_files = check_for_new_data()
    make_predictions(new_files)

# Instantiate the DAG
try:
    predict_dag = prediction_dag()
except Exception as e:
    logger.error(f"Failed to instantiate DAG: {str(e)}")
    raise