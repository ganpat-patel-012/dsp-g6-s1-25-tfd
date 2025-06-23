import sys
import os
import random
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, time, timedelta, timezone
from pprint import pformat
import shutil
import json
import logging
from pendulum import today
import requests
import time
from great_expectations.data_context import DataContext
from great_expectations.core.batch import RuntimeBatchRequest
import psycopg2
from psycopg2 import sql
 
# Database Configuration
DB_CONFIG = {
    "dbname": "tfd_db",
    "user": "tfd_user",
    "password": "tfd_pass",
    "host": "tfd_postgres"
    "port": "5432"
}
 
# Database Connection Function
def get_connection():
    """Establishes a PostgreSQL connection."""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        logging.error(f"Failed to establish database connection: {str(e)}")
        raise
 
# Data Quality Schema (for reference, not used directly with psycopg2)
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
 
Base = declarative_base()
 
class DataQualityStat(Base):
    __tablename__ = "data_quality_stats"
    id = Column(Integer, primary_key=True)
    filename = Column(String, nullable=False)
    total_rows = Column(Integer, nullable=False)
    valid_rows = Column(Integer, nullable=False)
    invalid_rows = Column(Integer, nullable=False)
    error_counts = Column(Text, nullable=True)
    is_critical = Column(Boolean, default=False, nullable=False)
    error_type = Column(String, nullable=True)
    error_count = Column(Integer, nullable=True)
    severity = Column(String, nullable=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
 
# Paths
project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir))
RAW_DATA_FOLDER = os.path.join(project_root, "input_data", "raw_data")
GOOD_DATA_FOLDER = os.path.join(project_root, "output_data", "good_data")
BAD_DATA_FOLDER = os.path.join(project_root, "output_data", "bad_data")
 
# Ensure output directories exist
os.makedirs(GOOD_DATA_FOLDER, exist_ok=True)
os.makedirs(BAD_DATA_FOLDER, exist_ok=True)
 
# Teams webhook URL
TEAMS_WEBHOOK_URL = "https://epitafr.webhook.office.com/webhookb2/463ace8d-55de-416c-af6f-0d944ddcd0c6@3534b3d7-316c-4bc9-9ede-605c860f49d2/IncomingWebhook/20883631084a48d5b172851613d551d5/2ee1ac5b-a7da-4763-a97e-b0cee3001618/V2ES_q8c8S2QMVddhkpxeM26E-W7v-O-tJPxr8dZP096w1"
 
# Great Expectations Context
context = DataContext(context_root_dir="/opt/gx")
 
# Helper Function (unchanged)
def get_unexpected_indices(df, file_name):
    logger = logging.getLogger("airflow.task")
    error_indices = set()
    error_stats = {
        "spicejet_blank_price": 0,
        "negative_days_left": 0,
        "same_cities": 0,
        "invalid_duration": 0,
        "premium_class": 0,
        "air_india_vistara": 0,
        "zero_stops_long_duration": 0
    }
    try:
        spicejet_blank_price = df[(df['airline'] == 'SpiceJet') & (df['price'].isna())].index
        if len(spicejet_blank_price) > 0:
            logger.info(f"[ERROR] {file_name}: Blank price values in SpiceJet at indices: {list(spicejet_blank_price)}")
            error_indices.update(spicejet_blank_price)
            error_stats["spicejet_blank_price"] = len(spicejet_blank_price)
 
        negative_days_left = df[df['days_left'] < 0].index
        if len(negative_days_left) > 0:
            logger.info(f"[ERROR] {file_name}: Negative 'days_left' values at indices: {list(negative_days_left)}")
            error_indices.update(negative_days_left)
            error_stats["negative_days_left"] = len(negative_days_left)
 
        same_cities = df[df['source_city'] == df['destination_city']].index
        if len(same_cities) > 0:
            logger.info(f"[ERROR] {file_name}: Same source and destination cities at indices: {list(same_cities)}")
            error_indices.update(same_cities)
            error_stats["same_cities"] = len(same_cities)
 
        invalid_duration = df[df['duration'].astype(str).isin(['Yes', 'No'])].index
        if len(invalid_duration) > 0:
            logger.info(f"[ERROR] {file_name}: Invalid 'duration' values (Yes/No) at indices: {list(invalid_duration)}")
            error_indices.update(invalid_duration)
            error_stats["invalid_duration"] = len(invalid_duration)
 
        premium_class = df[df['travel_class'] == 'Premium'].index
        if len(premium_class) > 0:
            logger.info(f"[ERROR] {file_name}: Premium travel_class found at indices: {list(premium_class)}")
            error_indices.update(premium_class)
            error_stats["premium_class"] = len(premium_class)
 
        df['flight'] = df['flight'].astype(str)
        air_india_vistara = df[(df['airline'] == 'Air_India') & (df['flight'].str.startswith('UK-'))].index
        if len(air_india_vistara) > 0:
            logger.info(f"[ERROR] {file_name}: Air India flights with Vistara flight numbers at indices: {list(air_india_vistara)}")
            error_indices.update(air_india_vistara)
            error_stats["air_india_vistara"] = len(air_india_vistara)
 
        temp_duration = pd.to_numeric(df['duration'], errors='coerce')
        zero_stops_long_duration = df[(df['stops'] == 'zero') & (temp_duration > 20)].index
        if len(zero_stops_long_duration) > 0:
            logger.info(f"[ERROR] {file_name}: Zero stops with duration > 20 hours at indices: {list(zero_stops_long_duration)}")
            error_indices.update(zero_stops_long_duration)
            error_stats["zero_stops_long_duration"] = len(zero_stops_long_duration)
    except Exception as e:
        logger.error(f"Error in get_unexpected_indices for {file_name}: {str(e)}")
        raise
    return list(error_indices), error_stats
 
# Tasks (only save_statistics is modified)
def read_data(**kwargs):
    """Randomly select a CSV file from the raw data folder."""
    logger = logging.getLogger("airflow.task")
    files = [f for f in os.listdir(RAW_DATA_FOLDER) if f.endswith('.csv')]
    if not files:
        logger.error(f"No CSV files found in {RAW_DATA_FOLDER}")
        raise FileNotFoundError(f"No CSV files found in {RAW_DATA_FOLDER}")
    
    file_name = random.choice(files)
    file_path = os.path.join(RAW_DATA_FOLDER, file_name)
    logger.info(f"Selected file: {file_name}")
    
    kwargs['ti'].xcom_push(key='file_path', value=file_path)
    kwargs['ti'].xcom_push(key='file_name', value=file_name)
    return file_path
 
def validate_data(**kwargs):
    """Validate the data using Great Expectations."""
    logger = logging.getLogger("airflow.task")
    file_path = kwargs['ti'].xcom_pull(key='file_path', task_ids='read_data')
    file_name = kwargs['ti'].xcom_pull(key='file_name', task_ids='read_data')
    
    logger.info(f"Validating file: {file_name} at {file_path}")
    
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File {file_path} not found")
    
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Read {len(df)} rows from {file_name}")
    except Exception as e:
        logger.error(f"Failed to read CSV {file_name}: {str(e)}")
        raise
    
    try:
        batch_request = RuntimeBatchRequest(
            datasource_name="flight_data",
            data_connector_name="runtime_data_connector",
            data_asset_name=file_name,
            runtime_parameters={"path": file_path},
            batch_identifiers={"default_identifier_name": "airflow_run"}
        )
        logger.info(f"Created batch request for {file_name}")
    except Exception as e:
        logger.error(f"Failed to create batch request for {file_name}: {str(e)}")
        raise
    
    try:
        results = context.run_checkpoint(
            checkpoint_name="checkpoint_1",
            batch_request=batch_request
        )
        logger.info(f"Ran checkpoint for {file_name}, success: {results['success']}")
    except Exception as e:
        logger.error(f"Failed to run checkpoint for {file_name}: {str(e)}")
        raise
    
    try:
        validation_results = results["run_results"]
        success = results["success"]
        failed_expectations = 0
        error_counts = {}
        
        for result_id, result in validation_results.items():
            if 'validation_result' in result:
                validation_result = result['validation_result']
                for expectation_result in validation_result['results']:
                    if not expectation_result['success']:
                        failed_expectations += 1
                        expectation_type = expectation_result['expectation_config']['expectation_type']
                        error_counts[expectation_type] = error_counts.get(expectation_type, 0) + 1
        
        logger.info(f"Validation results for {file_name}: success={success}, failed_expectations={failed_expectations}, error_counts={error_counts}")
    except Exception as e:
        logger.error(f"Failed to process validation results for {file_name}: {str(e)}")
        raise
    
    try:
        kwargs['ti'].xcom_push(key='success', value=success)
        kwargs['ti'].xcom_push(key='validation_results', value=validation_results)
        kwargs['ti'].xcom_push(key='failed_expectations', value=failed_expectations)
        kwargs['ti'].xcom_push(key='error_counts', value=error_counts)
        kwargs['ti'].xcom_push(key='data_frame', value=df.to_dict('records'))
        logger.info(f"Pushed validation results to XCom for {file_name}")
    except Exception as e:
        logger.error(f"Failed to push XCom data for {file_name}: {str(e)}")
        raise
    
    return success
 
def send_alerts(**kwargs):
    logger = logging.getLogger("airflow.task")
    logger.info("Starting send_alerts task")
    
    try:
        file_name = kwargs['ti'].xcom_pull(key='file_name', task_ids='read_data')
        success = kwargs['ti'].xcom_pull(key='success', task_ids='validate_data')
        failed_expectations = kwargs['ti'].xcom_pull(key='failed_expectations', task_ids='validate_data') or 0
        error_counts = kwargs['ti'].xcom_pull(key='error_counts', task_ids='validate_data') or {}
        logger.info(f"Pulled XCom data: file_name={file_name}, success={success}, failed_expectations={failed_expectations}")
        
        if file_name is None:
            logger.error("Missing file_name in XCom from read_data task")
            return
        if success is None:
            logger.error("Missing success status in XCom from validate_data task")
            return
    except Exception as e:
        logger.error(f"Failed to pull XCom data: {str(e)}")
        return
 
    logger.info(f"Preparing alert for {file_name}: success={success}")
    
    try:
        alert_file = os.path.join(project_root, "alerts", f"alert_{file_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt")
        os.makedirs(os.path.dirname(alert_file), exist_ok=True)
        logger.info(f"Alert file path: {alert_file}")
        
        if success:
            alert_title = f"Data Ingestion Success: {file_name}"
            alert_text = f"""
            [DATA INGESTION SUCCESS]
            File: {file_name}
            Status: All validations passed successfully.
            Data quality statistics saved to database.
            """
            color = "#008000"
        else:
            if failed_expectations > 10:
                criticality = "HIGH"
                color = "#FF0000"
            elif failed_expectations > 5:
                criticality = "MEDIUM"
                color = "#FFA500"
            else:
                criticality = "LOW"
                color = "#FFFF00"
            
            alert_text = f"""
            [DATA QUALITY ALERT] - {criticality} severity
            File: {file_name}
            Total failed expectations: {failed_expectations}
            Error breakdown:
            {pformat(error_counts, indent=2)}
            Please review the Data Docs for detailed validation results.
            """
            alert_title = f"Data Quality Alert: {file_name}"
 
        logger.info("Writing alert to file")
        with open(alert_file, 'w') as f:
            f.write(f"Title: {alert_title}\nColor: {color}\n\n{alert_text}")
        logger.info(f"Alert logged to {alert_file}")
 
        logger.info("Preparing to send alert to Teams")
        teams_payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": color,
            "summary": alert_title,
            "sections": [{
                "activityTitle": alert_title,
                "text": alert_text.replace("\n", "<br>")
            }]
        }
        headers = {'Content-Type': 'application/json'}
        for attempt in range(3):
            try:
                logger.info(f"Sending Teams request (attempt {attempt + 1})")
                response = requests.post(TEAMS_WEBHOOK_URL, headers=headers, json=teams_payload, timeout=10)
                if response.status_code == 200:
                    logger.info(f"Successfully sent alert to Microsoft Teams for {file_name}")
                    break
                else:
                    logger.warning(f"Teams request failed: {response.status_code} - {response.text}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Teams request failed (attempt {attempt + 1}): {str(e)}")
            if attempt < 2:
                time.sleep(5)
        else:
            logger.error(f"Failed to send alert to Teams for {file_name} after 3 attempts")
    except Exception as e:
        logger.error(f"Failed to prepare or send alert for {file_name}: {str(e)}")
        return
    logger.info("Completed send_alerts task")
 
def save_statistics(**kwargs):
    """Save data quality statistics to the database with merged error stats using psycopg2."""
    logger = logging.getLogger("airflow.task")
    
    # Pull data from XCom
    file_name = kwargs['ti'].xcom_pull(key='file_name', task_ids='read_data')
    file_path = kwargs['ti'].xcom_pull(key='file_path', task_ids='read_data')
    success = kwargs['ti'].xcom_pull(key='success', task_ids='validate_data')
    error_counts = kwargs['ti'].xcom_pull(key='error_counts', task_ids='validate_data')
    df_dict = kwargs['ti'].xcom_pull(key='data_frame', task_ids='validate_data')
    timestamp = kwargs['data_interval_start']  # Use data_interval_start
    
    # Reconstruct DataFrame
    try:
        df = pd.DataFrame(df_dict)
        total_rows = len(df)
    except Exception as e:
        logger.error(f"Failed to reconstruct DataFrame for {file_name}: {str(e)}")
        total_rows = 0
        df = pd.DataFrame()
        error_counts['reconstruction_error'] = error_counts.get('reconstruction_error', 0) + 1
    
    # Get custom validation statistics
    try:
        error_indices, custom_error_stats = get_unexpected_indices(df, file_name)
        
        # Define severity thresholds
        error_severity = {
            "spicejet_blank_price": "high" if custom_error_stats["spicejet_blank_price"] > 5 else "medium" if custom_error_stats["spicejet_blank_price"] > 0 else "none",
            "negative_days_left": "high" if custom_error_stats["negative_days_left"] > 5 else "medium" if custom_error_stats["negative_days_left"] > 0 else "none",
            "same_cities": "high" if custom_error_stats["same_cities"] > 2 else "medium" if custom_error_stats["same_cities"] > 0 else "none",
            "invalid_duration": "medium" if custom_error_stats["invalid_duration"] > 0 else "none",
            "premium_class": "medium" if custom_error_stats["premium_class"] > 0 else "none",
            "air_india_vistara": "low" if custom_error_stats["air_india_vistara"] > 0 else "none",
            "zero_stops_long_duration": "high" if custom_error_stats["zero_stops_long_duration"] > 0 else "none"
        }
    except Exception as e:
        logger.error(f"Error running custom validations for {file_name}: {str(e)}")
        error_indices = []
        custom_error_stats = {"validation_error": 1}
        error_severity = {"validation_error": "high"}
    
    # Calculate valid and invalid rows
    invalid_rows = len(set(error_indices))
    valid_rows = total_rows - invalid_rows
    is_critical = total_rows > 0 and invalid_rows == total_rows
    
    # Save to database using psycopg2
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                # Define insert query
                insert_query = sql.SQL("""
                    INSERT INTO data_quality_stats
                    (filename, total_rows, valid_rows, invalid_rows, error_counts,
                     is_critical, error_type, error_count, severity, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """)
                
                # Save summary row (error_type=NULL)
                cursor.execute(insert_query, (
                    file_name,
                    total_rows,
                    valid_rows,
                    invalid_rows,
                    json.dumps(error_counts),
                    is_critical,
                    None,  # error_type
                    None,  # error_count
                    None,  # severity
                    timestamp
                ))
                
                # Save one row per error type
                for error_type, error_count in custom_error_stats.items():
                    if error_count > 0:  # Only save non-zero errors
                        cursor.execute(insert_query, (
                            file_name,
                            total_rows,
                            valid_rows,
                            invalid_rows,
                            json.dumps(error_counts),
                            is_critical,
                            error_type,
                            error_count,
                            error_severity.get(error_type, "none"),
                            timestamp
                        ))
                
                conn.commit()
                logger.info(f"Saved statistics for {file_name}: {total_rows} rows, {invalid_rows} invalid, critical={is_critical}")
    except Exception as e:
        logger.error(f"Error saving statistics to database for {file_name}: {str(e)}")
        raise
 
def split_and_save_data(**kwargs):
    """Split and save data based on validation results and delete the original file."""
    logger = logging.getLogger("airflow.task")
    file_name = kwargs['ti'].xcom_pull(key='file_name', task_ids='read_data')
    file_path = kwargs['ti'].xcom_pull(key='file_path', task_ids='read_data')
    success = kwargs['ti'].xcom_pull(key='success', task_ids='validate_data')
    df_dict = kwargs['ti'].xcom_pull(key='data_frame', task_ids='validate_data')
    
    if not os.path.exists(file_path):
        logger.error(f"File not found for splitting: {file_path}")
        raise FileNotFoundError(f"File {file_path} not found for splitting")
    
    try:
        df = pd.DataFrame(df_dict)
    except Exception as e:
        logger.error(f"Failed to reconstruct DataFrame for {file_name}: {str(e)}")
        raise
    
    if success:
        target_path = os.path.join(GOOD_DATA_FOLDER, file_name)
        shutil.move(file_path, target_path)
        logger.info(f"Moved valid file to {target_path}")
    else:
        error_indices, _ = get_unexpected_indices(df, file_name)
        
        if error_indices:
            bad_df = df.loc[error_indices]
            good_df = df.drop(error_indices)
            
            bad_target_path = os.path.join(BAD_DATA_FOLDER, f"bad_{file_name}")
            bad_df.to_csv(bad_target_path, index=False)
            logger.info(f"Saved {len(bad_df)} invalid rows to {bad_target_path}")
            
            if not good_df.empty:
                good_target_path = os.path.join(GOOD_DATA_FOLDER, f"good_{file_name}")
                good_df.to_csv(good_target_path, index=False)
                logger.info(f"Saved {len(good_df)} valid rows to {good_target_path}")
        else:
            target_path = os.path.join(BAD_DATA_FOLDER, file_name)
            shutil.move(file_path, target_path)
            logger.info(f"Moved invalid file to {target_path}")
    
    # Delete the original file from RAW_DATA_FOLDER if it still exists
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted original file from raw data folder: {file_path}")
        else:
            logger.info(f"Original file {file_path} already moved or deleted.")
    except Exception as e:
        logger.error(f"Failed to delete original file {file_path}: {str(e)}")
        raise
 
# DAG Definition
default_args = {
    "owner": "tfd_team",
    "start_date": today('UTC').add(days=-1),
    "retries": 1,
    "execution_timeout": timedelta(seconds=300)
}
 
dag = DAG(
    "flight_data_ingestion_validation",
    default_args=default_args,
    schedule_interval="*/2 * * * *",
    catchup=False,
    description="Ingest and validate flight data using Great Expectations, and send alerts on teams",
)
 
with dag:
    read_data_task = PythonOperator(
        task_id="read_data",
        python_callable=read_data,
        provide_context=True
    )
 
    validate_data_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        provide_context=True
    )
 
    save_statistics_task = PythonOperator(
        task_id="save_statistics",
        python_callable=save_statistics,
        provide_context=True
    )
 
    send_alerts_task = PythonOperator(
        task_id="send_alerts",
        python_callable=send_alerts,
        provide_context=True
    )
 
    split_and_save_data_task = PythonOperator(
        task_id="save_file",
        python_callable=split_and_save_data,
        provide_context=True
    )
 
    # Task dependencies
    read_data_task >> validate_data_task
    validate_data_task >> [save_statistics_task, send_alerts_task, split_and_save_data_task]
