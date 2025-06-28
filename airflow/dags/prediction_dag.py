import logging
import os
from airflow.decorators import dag, task # type: ignore
from datetime import datetime, timedelta

GOOD_DATA_FOLDER = "/opt/airflow/input_data/good_data"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='prediction_dag',
    default_args=default_args,
    schedule='@daily',  # Run daily
    start_date=datetime(2024, 5, 19),
    catchup=False,
    tags=['data_prediction_defence_1']
)
def prediction_dag():

    @task
    def check_for_new_data() -> list:
        """Check if there are new ingested files in the good_data folder."""
        files = [f for f in os.listdir(GOOD_DATA_FOLDER) if f.endswith(".csv")]
        if not files:
            logging.info("No new files to predict on.")
            return []
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
            # Here you would make the API call to your model service
            # For now, we'll just simulate the process
            print(f"Making predictions for {file}")
    
    # Workflow execution
    new_files = check_for_new_data()
    make_predictions(new_files)

predict_dag = prediction_dag()