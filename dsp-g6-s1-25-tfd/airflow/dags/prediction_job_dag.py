from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
import os
import requests
import pandas as pd
from datetime import datetime

# Path to the folder containing ingested files
GOOD_DATA_FOLDER = '/opt/airflow/data/good_data/'

# Using existing project database connection
POSTGRES_CONN_ID = "tfd_db"

# Function to get processed files from the processed_files table
def get_processed_files():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT file_name FROM processed_files;")
    processed_files = {row[0] for row in cursor.fetchall()}
    cursor.close()
    conn.close()
    return processed_files

# Function to check for newly ingested files
def check_for_new_data():
    if not os.path.exists(GOOD_DATA_FOLDER):
        print(f"Folder '{GOOD_DATA_FOLDER}' does not exist.")
        raise AirflowSkipException(f"Folder '{GOOD_DATA_FOLDER}' does not exist.")
    
    processed_files = get_processed_files()
    all_files = set(os.listdir(GOOD_DATA_FOLDER))
    if not all_files:
        print("No files found in the 'good_data' folder.")
        raise AirflowSkipException("No files found in the 'good_data' folder.")
    
    new_files = list(all_files - processed_files)
    if not new_files:
        print("No new files to process.")
        raise AirflowSkipException("No new files to process.")
    
    print(f"Found new files: {new_files}")
    return new_files

# Function to insert processed files into the processed_files table
def save_processed_files(file_names):
    if not file_names:
        return
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    for file_name in file_names:
        cursor.execute("INSERT INTO processed_files (file_name) VALUES (%s) ON CONFLICT (file_name) DO NOTHING;", (file_name,))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Saved processed files: {file_names}")

# Function to process new files and make predictions
def make_predictions(**kwargs):
    task_instance = kwargs['task_instance']
    new_files = task_instance.xcom_pull(task_ids='check_for_new_data')
    if not new_files:
        print("No new files to process.")
        return "No new data for predictions"
    
    predictions_results = []
    PREDICTION_API_URL = "http://tfd_fastapi:8000/predict"
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    for file_name in new_files:
        file_path = os.path.join(GOOD_DATA_FOLDER, file_name)
        try:
            print(f"Processing file: {file_path}")
            df = pd.read_csv(file_path)
            required_columns = ["airline", "source_city", "destination_city", "departure_time", "arrival_time", "travel_class", "stops", "duration", "days_left"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print(f"File {file_path} is missing required columns: {missing_columns}")
                continue
            
            data_payload = df.to_dict(orient="records")
            response = requests.post(
                PREDICTION_API_URL,
                json={"data": data_payload},
                headers={"X-Request-Source": "Scheduled Predictions"}
            )
            
            if response.status_code == 200:
                predictions = response.json()
                print(f"Predictions for {file_path} completed successfully.")
                predictions_results.append(predictions)
                
                # Store the predictions in the existing predictions table
                for record, prediction in zip(data_payload, predictions):
                    cursor.execute(
                        """
                        INSERT INTO predictions (airline, source_city, destination_city, departure_time, arrival_time, travel_class, stops, duration, days_left, predicted_price, prediction_source, prediction_type, prediction_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                        (
                            record['airline'], record['source_city'], record['destination_city'],
                            record['departure_time'], record['arrival_time'], record['travel_class'],
                            record['stops'], record['duration'], record['days_left'],
                            prediction['predicted_price'], "Scheduled Job", "Batch", datetime.now()
                        )
                    )
                conn.commit()
            else:
                print(f"Failed to predict for {file_path}. Status code: {response.status_code}, Response: {response.text}")
        except pd.errors.EmptyDataError:
            print(f"File {file_path} is empty or malformed. Skipping.")
        except Exception as e:
            print(f"Unexpected error processing file {file_path}: {e}")
    
    cursor.close()
    conn.close()
    save_processed_files(new_files)
    if predictions_results:
        print(f"Total Predictions made: {len(predictions_results)}")
    return f"Predictions made for {len(new_files)} files"

# Define the DAG
dag = DAG(
    'prediction_job',
    description='Prediction Job for Ingested Files',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2025, 3, 5),
    catchup=False,
)

# Task to check for new data files
check_task = PythonOperator(
    task_id='check_for_new_data',
    python_callable=check_for_new_data,
    dag=dag,
)

# Task to make predictions using FastAPI
prediction_task = PythonOperator(
    task_id='make_predictions',
    python_callable=make_predictions,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
check_task >> prediction_task
