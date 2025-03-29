import os
import random
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define paths for raw-data and good-data folders
RAW_DATA_PATH = "/opt/airflow/data/raw-data"
GOOD_DATA_PATH = "/opt/airflow/data/good-data"

# Define the DAG with a schedule_interval of every 5 minutes
dag = DAG(
    'simple_ingestion_pipeline',
    description='Ingest data from raw-data to good-data',
    schedule_interval='*/1 * * * *',  # Trigger every 1 minute
    start_date=datetime(2025, 3, 5),
    catchup=False,
)

# Task 1: Read one file randomly from the raw-data folder
def read_data():
    files = [f for f in os.listdir(RAW_DATA_PATH) if os.path.isfile(os.path.join(RAW_DATA_PATH, f))]

    if not files:
        print("No files found in raw-data folder. Skipping execution.")
        return None  # Returning None instead of raising an error

    selected_file = random.choice(files)
    file_path = os.path.join(RAW_DATA_PATH, selected_file)
    print(f"Selected file: {file_path}")
    return file_path

# Task 2: Save the file by moving it to the good-data folder
def save_file(**kwargs):
    file_path = kwargs['ti'].xcom_pull(task_ids='read-data')

    if not file_path:
        print("No file selected. Skipping save-file task.")
        return  # Exit gracefully

    destination_path = os.path.join(GOOD_DATA_PATH, os.path.basename(file_path))
    shutil.move(file_path, destination_path)
    print(f"File moved: {file_path} → {destination_path}")

# Create PythonOperator tasks for each step
read_task = PythonOperator(
    task_id='read-data',
    python_callable=read_data,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save-file',
    python_callable=save_file,
    provide_context=True,  # This ensures the context (including `ti`) is passed to the function
    dag=dag,
)
# Set the task sequence
read_task >> save_task