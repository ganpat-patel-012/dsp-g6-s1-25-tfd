import os
import random
import pendulum  # type: ignore
from datetime import timedelta
from airflow.decorators import dag, task  # type: ignore
import shutil
import logging

# Define folders with absolute paths
RAW_DATA_FOLDER = os.path.abspath("raw_data")
GOOD_DATA_FOLDER = os.path.abspath("good_data")

# Ensure folders exist
os.makedirs(RAW_DATA_FOLDER, exist_ok=True)
os.makedirs(GOOD_DATA_FOLDER, exist_ok=True)

# Log folder paths
logging.info(f"Raw data folder: {RAW_DATA_FOLDER}")
logging.info(f"Good data folder: {GOOD_DATA_FOLDER}")

@dag(
    dag_id='first_data_ingestion',
    description='Ingest one file at a time from raw_data to good_data',
    schedule='*/1 * * * *',  # Runs every 2 minutes
    start_date=pendulum.today('UTC').add(days=-1),
    max_active_runs=1,  # Prevents overlapping runs
    catchup=False,
    tags=['dsp_project', 'data_ingestion']
)
def first_data_ingestion():

    @task
    def read_data() -> str:
        """Reads one random file from raw_data folder."""
        files = [f for f in os.listdir(RAW_DATA_FOLDER) if f.endswith(".csv")]
        
        if not files:
            logging.info("No new files to process.")
            return None  # Return None to handle empty folder safely
        
        selected_file = random.choice(files)
        file_path = os.path.join(RAW_DATA_FOLDER, selected_file)
        logging.info(f"Selected file: {file_path}")
        
        return file_path  # Return full file path
    
    @task
    def save_file(file_path: str):
        """Moves the selected file from raw_data to good_data."""
        if file_path is None:
            logging.info("No file to move. Skipping task.")
            return

        destination = os.path.join(GOOD_DATA_FOLDER, os.path.basename(file_path))
        shutil.move(file_path, destination)
        logging.info(f"File moved to {destination}")

    # Define DAG flow
    file_path = read_data()
    save_file(file_path)

first_data_ingestion_dag = first_data_ingestion()
