import logging
import os
from airflow.decorators import dag, task  # type: ignore
from airflow.exceptions import AirflowSkipException  # Import for skipping DAG
from datetime import datetime, timedelta

GOOD_DATA_FOLDER = os.path.abspath("good_data")  # Use absolute path

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='first_data_prediction',
    default_args=default_args,
    schedule='*/2 * * * *',  # Runs every 2 minutes
    start_date=datetime(2024, 5, 19),
    catchup=False
)
def first_data_prediction():

    @task
    def check_for_new_data() -> list:
        """Check if new files exist in good_data folder."""
        files = [f for f in os.listdir(GOOD_DATA_FOLDER) if f.endswith(".csv")]
        
        if not files:
            logging.info("No new files. Skipping DAG run.")
            raise AirflowSkipException("No new files in good_data.")
        
        logging.info(f"New files found: {files}")
        return [os.path.join(GOOD_DATA_FOLDER, f) for f in files]

    @task
    def make_predictions(files: list):
        """Make predictions on the new files."""
        if not files:
            logging.info("No files to make predictions on.")
            return
        
        for file in files:
            logging.info(f"Making predictions for {file}")
            # Placeholder for actual prediction API call
            print(f"Making predictions for {file}")
    
    # Define DAG flow
    new_files = check_for_new_data()
    make_predictions(new_files)

first_data_prediction_dag = first_data_prediction()
