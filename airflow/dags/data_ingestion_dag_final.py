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

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from configFiles.config import DB_CONFIG, TEAMS_WEBHOOK_URL
from configFiles.dbCode import get_connection


# Paths
project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir))
RAW_DATA_FOLDER = os.path.join(project_root, "input_data", "raw_data")
GOOD_DATA_FOLDER = os.path.join(project_root, "output_data", "good_data")
BAD_DATA_FOLDER = os.path.join(project_root, "output_data", "bad_data")

# Ensure output directories exist
os.makedirs(GOOD_DATA_FOLDER, exist_ok=True)
os.makedirs(BAD_DATA_FOLDER, exist_ok=True)

# Great Expectations Context
context = DataContext(context_root_dir="/opt/gx")

# Helper Function
def get_unexpected_indices(df, file_name):
    """Identify data quality issues and return error indices and statistics."""
    logger = logging.getLogger("airflow.task")
    error_indices = set()
    error_stats = {
        "airline_name_null": 0,
        "negative_duration": 0,
        "same_cities": 0,
        "invalid_days_left": 0,
        "premium_class": 0,
        "air_india_vistara": 0,
        "zero_stops_long_duration": 0
    }
    
    try:
        # Check for null values in 'airline' column
        airline_name_null = df[df['airline'].isna()].index
        if len(airline_name_null) > 0:
            error_indices.update(airline_name_null)
            error_stats["airline_name_null"] = len(airline_name_null)


        # Negative duration values - convert to numeric first (safe conversion)
        duration_numeric = pd.to_numeric(df['duration'], errors='coerce')
        negative_duration = df[(duration_numeric < 0) & (duration_numeric.notna())].index
        if len(negative_duration) > 0:
            error_indices.update(negative_duration)
            error_stats["negative_duration"] = len(negative_duration)

        # Same source and destination cities
        same_cities = df[df['source_city'] == df['destination_city']].index
        if len(same_cities) > 0:
            error_indices.update(same_cities)
            error_stats["same_cities"] = len(same_cities)

        # Invalid days_left values (checking for non-numeric values like 'Yes', 'No')
        # Convert days_left to string safely and check for invalid values
        days_left_str = df['days_left'].astype(str)
        invalid_days_left = df[days_left_str.isin(['Yes', 'No'])].index
        if len(invalid_days_left) > 0:
            error_indices.update(invalid_days_left)
            error_stats["invalid_days_left"] = len(invalid_days_left)

        # Premium travel class
        premium_class = df[df['travel_class'] == 'Premium'].index
        if len(premium_class) > 0:
            error_indices.update(premium_class)
            error_stats["premium_class"] = len(premium_class)

        # Air India with Vistara flight numbers
        df['flight'] = df['flight'].astype(str)
        air_india_vistara = df[(df['airline'] == 'Air_India') & (df['flight'].str.startswith('UK-'))].index
        if len(air_india_vistara) > 0:
            error_indices.update(air_india_vistara)
            error_stats["air_india_vistara"] = len(air_india_vistara)

        # Zero stops with long duration
        temp_duration = pd.to_numeric(df['duration'], errors='coerce')
        zero_stops_long_duration = df[(df['stops'] == 'zero') & (temp_duration > 20) & (temp_duration.notna())].index
        if len(zero_stops_long_duration) > 0:
            error_indices.update(zero_stops_long_duration)
            error_stats["zero_stops_long_duration"] = len(zero_stops_long_duration)
    except Exception as e:
        logger.error(f"Error in get_unexpected_indices for {file_name}: {str(e)}")
        raise
    
    return list(error_indices), error_stats

# Tasks
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
    """Validate data using Great Expectations and custom validations."""
    logger = logging.getLogger("airflow.task")
    file_path = kwargs['ti'].xcom_pull(key='file_path', task_ids='read_data')
    file_name = kwargs['ti'].xcom_pull(key='file_name', task_ids='read_data')

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
        
        results = context.run_checkpoint(
            checkpoint_name="flight_checkpoint",
            batch_request=batch_request
        )
        
        success = results["success"]
        validation_results = results["run_results"]
        
        # Get validation URL
        specific_validation_url = None
        try:
            # Try to get URL from actions_results first
            for result in validation_results.values():
                if 'actions_results' in result and 'update_data_docs' in result['actions_results']:
                    data_docs_urls = result['actions_results']['update_data_docs'].get('local_site')
                    if data_docs_urls:
                        specific_validation_url = data_docs_urls
                        break
            
            # Fallback: search for any HTML file in validations directory
            if not specific_validation_url:
                validations_base_path = "/opt/gx/uncommitted/data_docs/local_site/validations/expectations_suite"
                if os.path.exists(validations_base_path):
                    for root, dirs, files in os.walk(validations_base_path):
                        for file in files:
                            if file.endswith('.html'):
                                specific_validation_url = f"file://{os.path.join(root, file)}"
                                break
                        if specific_validation_url:
                            break
        except Exception as e:
            logger.error(f"Error finding validation URL: {str(e)}")
        
        logger.info(f"Validation completed for {file_name}, success: {success}")
        kwargs['ti'].xcom_push(key='success', value=success)
        kwargs['ti'].xcom_push(key='data_frame', value=df.to_dict('records'))
        kwargs['ti'].xcom_push(key='data_docs_url', value=specific_validation_url)
        
    except Exception as e:
        logger.error(f"Failed to validate {file_name}: {str(e)}")
        raise
    
    return success

def save_statistics(**kwargs):
    """Save data quality statistics to database."""
    logger = logging.getLogger("airflow.task")
    
    file_name = kwargs['ti'].xcom_pull(key='file_name', task_ids='read_data')
    success = kwargs['ti'].xcom_pull(key='success', task_ids='validate_data')
    df_dict = kwargs['ti'].xcom_pull(key='data_frame', task_ids='validate_data')
    timestamp = datetime.now(timezone.utc)
    
    try:
        df = pd.DataFrame(df_dict)
        total_rows = len(df)
    except Exception as e:
        logger.error(f"Failed to reconstruct DataFrame for {file_name}: {str(e)}")
        raise
    
    # Custom validations
    try:
        error_indices, custom_error_stats = get_unexpected_indices(df, file_name)
        
        # Calculate severity levels
        error_severity = {
            "airline_name_null": "high" if custom_error_stats["airline_name_null"] > 5 else "medium" if custom_error_stats["airline_name_null"] > 0 else "none",
            "negative_duration": "high" if custom_error_stats["negative_duration"] > 5 else "medium" if custom_error_stats["negative_duration"] > 0 else "none",
            "same_cities": "high" if custom_error_stats["same_cities"] > 2 else "medium" if custom_error_stats["same_cities"] > 0 else "none",
            "invalid_days_left": "medium" if custom_error_stats["invalid_days_left"] > 0 else "none",
            "premium_class": "low" if custom_error_stats["premium_class"] > 0 else "none",
            "air_india_vistara": "medium" if custom_error_stats["air_india_vistara"] > 0 else "none",
            "zero_stops_long_duration": "high" if custom_error_stats["zero_stops_long_duration"] > 0 else "none"
        }
    except Exception as e:
        logger.error(f"Error running custom validations for {file_name}: {str(e)}")
        error_indices = []
        custom_error_stats = {"validation_error": 1}
        error_severity = {"validation_error": "high"}
    
    # Calculate statistics
    invalid_rows = len(set(error_indices))
    valid_rows = total_rows - invalid_rows
    error_count = sum(custom_error_stats.values())
    
    # Determine overall severity
    severities = [sev for sev in error_severity.values() if sev != "none"]
    overall_severity = None
    if severities:
        if "high" in severities:
            overall_severity = "high"
        elif "medium" in severities:
            overall_severity = "medium"
        else:
            overall_severity = "low"
    
    # Push XCom data for send_alerts (do this before database save to ensure it's always available)
    kwargs['ti'].xcom_push(key='custom_error_stats', value=custom_error_stats)
    kwargs['ti'].xcom_push(key='overall_severity', value=overall_severity)
    kwargs['ti'].xcom_push(key='error_count', value=error_count)
    
    # Save to database
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                insert_query = sql.SQL("""
                    INSERT INTO data_quality_stats
                    (filename, total_rows, valid_rows, invalid_rows, error_details, error_count, severity, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """)
                cursor.execute(insert_query, (
                    file_name, total_rows, valid_rows, invalid_rows,
                    json.dumps(custom_error_stats) if custom_error_stats else None,
                    error_count if error_count > 0 else None,
                    overall_severity, timestamp
                ))
                conn.commit()
                logger.info(f"Saved statistics for {file_name}: {total_rows} rows, {invalid_rows} invalid")
    except Exception as e:
        logger.error(f"Error saving statistics to database for {file_name}: {str(e)}")
        # Don't raise the exception - XCom data is already pushed, so alerts can still be sent
        logger.warning(f"Database save failed for {file_name}, but XCom data is available for alerts")

def send_alerts(**kwargs):
    """Send alerts to Teams and save to file."""
    logger = logging.getLogger("airflow.task")
    file_name = kwargs['ti'].xcom_pull(key='file_name', task_ids='read_data')
    success = kwargs['ti'].xcom_pull(key='success', task_ids='validate_data')
    df_dict = kwargs['ti'].xcom_pull(key='data_frame', task_ids='validate_data')
    data_docs_url = kwargs['ti'].xcom_pull(key='data_docs_url', task_ids='validate_data')
    
    # Calculate error statistics
    try:
        df = pd.DataFrame(df_dict)
        error_indices, custom_error_stats = get_unexpected_indices(df, file_name)
        
        # Calculate severity levels
        error_severity = {
            "airline_name_null": "high" if custom_error_stats["airline_name_null"] > 5 else "medium" if custom_error_stats["airline_name_null"] > 0 else "none",
            "negative_duration": "high" if custom_error_stats["negative_duration"] > 5 else "medium" if custom_error_stats["negative_duration"] > 0 else "none",
            "same_cities": "high" if custom_error_stats["same_cities"] > 2 else "medium" if custom_error_stats["same_cities"] > 0 else "none",
            "invalid_days_left": "medium" if custom_error_stats["invalid_days_left"] > 0 else "none",
            "premium_class": "low" if custom_error_stats["premium_class"] > 0 else "none",
            "air_india_vistara": "medium" if custom_error_stats["air_india_vistara"] > 0 else "none",
            "zero_stops_long_duration": "high" if custom_error_stats["zero_stops_long_duration"] > 0 else "none"
        }
        
        # Determine overall severity
        severities = [sev for sev in error_severity.values() if sev != "none"]
        overall_severity = None
        if severities:
            if "high" in severities:
                overall_severity = "high"
            elif "medium" in severities:
                overall_severity = "medium"
            else:
                overall_severity = "low"
        
        error_count = sum(custom_error_stats.values())
        
    except Exception as e:
        logger.error(f"Error calculating statistics for {file_name}: {str(e)}")
        custom_error_stats = {}
        error_count = 0
        overall_severity = None
    
    # Process data docs URL
    clickable_data_docs_url = "N/A"
    if data_docs_url:
        if data_docs_url.startswith("file:///opt/gx/"):
            web_base_url = "http://localhost:8081/gx"
            relative_path = data_docs_url.replace("file:///opt/gx/", "").replace("//", "/")
            clickable_data_docs_url = f"{web_base_url}/{relative_path}"
        else:
            clickable_data_docs_url = data_docs_url
    else:
        # Fallback: search for any HTML file
        validations_base_path = "/opt/gx/uncommitted/data_docs/local_site/validations/expectations_suite"
        if os.path.exists(validations_base_path):
            for root, dirs, files in os.walk(validations_base_path):
                for file in files:
                    if file.endswith('.html'):
                        clickable_data_docs_url = f"http://localhost:8081/gx/{os.path.join(root, file).replace('/opt/gx/', '')}"
                        break
                if clickable_data_docs_url != "N/A":
                    break
    
    # Create alert content
    if success:
        alert_title = f"Data Ingestion Success: {file_name}"
        alert_text = f"[DATA INGESTION SUCCESS]\nFile: {file_name}\nStatus: All validations passed successfully."
        color = "#008000"
    else:
        error_breakdown = "\n".join([f"{error_type}: {count}" for error_type, count in custom_error_stats.items() if count > 0])
        if not error_breakdown:
            error_breakdown = "Great Expectations validation failed - check Data Docs for details."
        
        data_docs_link = f'<a href="{clickable_data_docs_url}">View Data Docs</a>' if clickable_data_docs_url != "N/A" else "N/A"
        
        alert_title = f"Data Quality Alert: {file_name}"
        alert_text = f"""[DATA QUALITY ALERT] - {overall_severity if overall_severity else 'no issues detected'}
File: {file_name}
Great Expectations Validation: FAILED
Custom Validation Issues: {error_count}
Error breakdown:
{error_breakdown}
Please review the Data Docs for detailed validation results: {data_docs_link}"""
        
        color_map = {"high": "#FF0000", "medium": "#FFA500", "low": "#FFFF00"}
        color = color_map.get(overall_severity, "#FFA500") if overall_severity else "#FFA500"
    
    # Save alert to file
    try:
        alert_file = os.path.join(project_root, "alerts", f"alert_{file_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt")
        os.makedirs(os.path.dirname(alert_file), exist_ok=True)
        with open(alert_file, 'w') as f:
            f.write(f"Title: {alert_title}\nColor: {color}\n\n{alert_text}")
    except Exception as e:
        logger.error(f"Failed to write alert file: {str(e)}")
    
    # Send to Teams
    teams_text = alert_text.replace("\n", "<br>")
    teams_payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": color,
        "summary": alert_title,
        "sections": [{"activityTitle": alert_title, "text": teams_text}]
    }
    
    headers = {'Content-Type': 'application/json'}
    for attempt in range(3):
        try:
            response = requests.post(TEAMS_WEBHOOK_URL, headers=headers, json=teams_payload, timeout=10)
            if response.status_code == 200:
                logger.info(f"Alert sent to Teams for {file_name}")
                break
            else:
                logger.warning(f"Teams request failed: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Teams request failed (attempt {attempt + 1}): {str(e)}")
        if attempt < 2:
            time.sleep(5)
    else:
        logger.error(f"Failed to send alert to Teams for {file_name} after 3 attempts")

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
    
    # Delete the original file if it still exists
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted original file from raw data folder: {file_path}")
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
    "ingestion_job_flight_data_final",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    catchup=False,
    tags=["data_ingestion", "data_validation"],
    description="Ingest and validate flight data",
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