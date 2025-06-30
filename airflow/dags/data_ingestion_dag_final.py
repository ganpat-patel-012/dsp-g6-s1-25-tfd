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
        "spicejet_blank_price": 0,
        "negative_days_left": 0,
        "same_cities": 0,
        "invalid_duration": 0,
        "premium_class": 0,
        "air_india_vistara": 0,
        "zero_stops_long_duration": 0
    }
    
    try:
        # SpiceJet blank prices
        spicejet_blank_price = df[(df['airline'] == 'SpiceJet') & (df['price'].isna())].index
        if len(spicejet_blank_price) > 0:
            error_indices.update(spicejet_blank_price)
            error_stats["spicejet_blank_price"] = len(spicejet_blank_price)

        # Negative days left
        negative_days_left = df[df['days_left'] < 0].index
        if len(negative_days_left) > 0:
            error_indices.update(negative_days_left)
            error_stats["negative_days_left"] = len(negative_days_left)

        # Same source and destination cities
        same_cities = df[df['source_city'] == df['destination_city']].index
        if len(same_cities) > 0:
            error_indices.update(same_cities)
            error_stats["same_cities"] = len(same_cities)

        # Invalid duration values
        invalid_duration = df[df['duration'].astype(str).isin(['Yes', 'No'])].index
        if len(invalid_duration) > 0:
            error_indices.update(invalid_duration)
            error_stats["invalid_duration"] = len(invalid_duration)

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
        zero_stops_long_duration = df[(df['stops'] == 'zero') & (temp_duration > 20)].index
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
        
        # Get the specific validation URL for this run
        specific_validation_url = None
        try:
            validation_result_id = None
            run_name = None
            logger.info(f"Looking for validation result URL in run_results: {list(validation_results.keys())}")
            for result_id, result in validation_results.items():
                logger.info(f"Processing result_id: {result_id}")
                # Extract validation result ID
                if 'validation_result' in result and 'meta' in result['validation_result']:
                    validation_result_id = result['validation_result']['meta'].get('run_id', {}).get('run_name')
                    logger.info(f"Found validation_result_id: {validation_result_id}")
                # Extract run name
                if 'run_id' in result:
                    run_name = result['run_id'].get('run_name')
                    logger.info(f"Found run_name: {run_name}")
                # Look for the validation result in the run_results
                if 'actions_results' in result and 'update_data_docs' in result['actions_results']:
                    data_docs_urls = result['actions_results']['update_data_docs'].get('local_site')
                    logger.info(f"Data docs URLs from action: {data_docs_urls}")
                    if data_docs_urls:
                        specific_validation_url = data_docs_urls
                        break
            validations_base_path = "/opt/gx/uncommitted/data_docs/local_site/validations/expectations_suite"
            # Try to construct using validation_result_id
            if not specific_validation_url and validation_result_id:
                logger.info(f"Constructing URL using validation_result_id: {validation_result_id}")
                if os.path.exists(validations_base_path):
                    for item in os.listdir(validations_base_path):
                        item_path = os.path.join(validations_base_path, item)
                        logger.info(f"Checking directory: {item_path}")
                        if os.path.isdir(item_path) and validation_result_id in item:
                            logger.info(f"Found matching directory: {item}")
                            html_files = [f for f in os.listdir(item_path) if f.endswith('.html')]
                            if html_files:
                                html_file = html_files[0]
                                specific_validation_url = f"file:///opt/gx/uncommitted/data_docs/local_site/validations/expectations_suite/{item}/{html_file}"
                                logger.info(f"Constructed specific validation URL: {specific_validation_url}")
                                break
            # Try using run_name
            if not specific_validation_url and run_name:
                logger.info(f"Attempting to construct URL using run_name: {run_name}")
                if os.path.exists(validations_base_path):
                    for item in os.listdir(validations_base_path):
                        item_path = os.path.join(validations_base_path, item)
                        logger.info(f"Checking directory: {item_path}")
                        if os.path.isdir(item_path) and run_name in item:
                            logger.info(f"Found matching directory: {item}")
                            html_files = [f for f in os.listdir(item_path) if f.endswith('.html')]
                            if html_files:
                                html_file = html_files[0]
                                specific_validation_url = f"file:///opt/gx/uncommitted/data_docs/local_site/validations/expectations_suite/{item}/{html_file}"
                                logger.info(f"Constructed specific validation URL: {specific_validation_url}")
                                break
            # Fallback: most recent validation for this file
            if not specific_validation_url:
                logger.info("Attempting to find most recent validation for this specific file")
                if os.path.exists(validations_base_path):
                    validation_dirs = []
                    for item in os.listdir(validations_base_path):
                        item_path = os.path.join(validations_base_path, item)
                        if os.path.isdir(item_path):
                            validation_dirs.append((item_path, os.path.getmtime(item_path)))
                    validation_dirs.sort(key=lambda x: x[1], reverse=True)
                    if validation_dirs:
                        most_recent_dir, _ = validation_dirs[0]
                        html_files = [f for f in os.listdir(most_recent_dir) if f.endswith('.html')]
                        if html_files:
                            html_file = html_files[0]
                            dir_name = os.path.basename(most_recent_dir)
                            specific_validation_url = f"file:///opt/gx/uncommitted/data_docs/local_site/validations/expectations_suite/{dir_name}/{html_file}"
                            logger.info(f"Using most recent validation URL: {specific_validation_url}")
            # Final fallback: search for any HTML file in validations directory
            if not specific_validation_url:
                logger.info("Final fallback: searching for any HTML file in validations directory")
                if os.path.exists(validations_base_path):
                    for root, dirs, files in os.walk(validations_base_path):
                        for file in files:
                            if file.endswith('.html'):
                                specific_validation_url = f"file://{os.path.join(root, file)}"
                                logger.info(f"Found fallback HTML file: {specific_validation_url}")
                                break
                        if specific_validation_url:
                            break
        except Exception as e:
            logger.error(f"Error finding specific validation URL: {str(e)}")
        logger.info(f"Validation completed for {file_name}, success: {success}")
        logger.info(f"Final specific validation URL: {specific_validation_url}")
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
            "spicejet_blank_price": "high" if custom_error_stats["spicejet_blank_price"] > 5 else "medium" if custom_error_stats["spicejet_blank_price"] > 0 else "none",
            "negative_days_left": "high" if custom_error_stats["negative_days_left"] > 5 else "medium" if custom_error_stats["negative_days_left"] > 0 else "none",
            "same_cities": "high" if custom_error_stats["same_cities"] > 2 else "medium" if custom_error_stats["same_cities"] > 0 else "none",
            "invalid_duration": "medium" if custom_error_stats["invalid_duration"] > 0 else "none",
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
        raise
    
    # Push XCom data for send_alerts
    kwargs['ti'].xcom_push(key='custom_error_stats', value=custom_error_stats)
    kwargs['ti'].xcom_push(key='overall_severity', value=overall_severity)
    kwargs['ti'].xcom_push(key='error_count', value=error_count)

def send_alerts(**kwargs):
    """Send alerts to Teams and save to file."""
    logger = logging.getLogger("airflow.task")
    file_name = kwargs['ti'].xcom_pull(key='file_name', task_ids='read_data')
    success = kwargs['ti'].xcom_pull(key='success', task_ids='validate_data')
    custom_error_stats = kwargs['ti'].xcom_pull(key='custom_error_stats', task_ids='save_statistics')
    error_count = kwargs['ti'].xcom_pull(key='error_count', task_ids='save_statistics')
    overall_severity = kwargs['ti'].xcom_pull(key='overall_severity', task_ids='save_statistics')
    data_docs_url = kwargs['ti'].xcom_pull(key='data_docs_url', task_ids='validate_data')
    # Fallback to database if XCom data is missing
    if any(x is None for x in [custom_error_stats, error_count, overall_severity]):
        try:
            with get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT error_details, error_count, severity FROM data_quality_stats WHERE filename = %s ORDER BY timestamp DESC LIMIT 1",
                        (file_name,)
                    )
                    result = cursor.fetchone()
                    if result:
                        custom_error_stats = result[0] if isinstance(result[0], dict) else json.loads(result[0]) if result[0] else {}
                        error_count = result[1] if result[1] is not None else 0
                        overall_severity = result[2]
                    else:
                        logger.warning(f"No database entry found for {file_name}. Skipping alert.")
                        return
        except Exception as e:
            logger.error(f"Failed to query database for {file_name}: {str(e)}")
            return
    if overall_severity is None:
        overall_severity = "none"
    # Process data docs URL to make it clickable
    clickable_data_docs_url = "N/A"
    if data_docs_url:
        if data_docs_url.startswith("file:///opt/gx/"):
            web_base_url = "http://localhost:8081/gx"
            relative_path = data_docs_url.replace("file:///opt/gx/", "")
            relative_path = relative_path.replace("//", "/")
            clickable_data_docs_url = f"{web_base_url}/{relative_path}"
        else:
            clickable_data_docs_url = data_docs_url
    else:
        # Fallback: search for any HTML file in validations directory
        validations_base_path = "/opt/gx/uncommitted/data_docs/local_site/validations/expectations_suite"
        logger.info("No data_docs_url from XCom, searching for any HTML file in validations directory as fallback.")
        if os.path.exists(validations_base_path):
            found_html = None
            for root, dirs, files in os.walk(validations_base_path):
                for file in files:
                    if file.endswith('.html'):
                        found_html = os.path.join(root, file)
                        break
                if found_html:
                    break
            if found_html:
                clickable_data_docs_url = f"http://localhost:8081/gx/{found_html.replace('/opt/gx/', '')}"
                logger.info(f"Fallback clickable_data_docs_url: {clickable_data_docs_url}")
    # Create alert content (keeping original format)
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
        error_breakdown = "\n".join([f"{error_type}: {count}" for error_type, count in custom_error_stats.items() if count > 0])
        if not error_breakdown:
            error_breakdown = "No specific error types detected."
        
        # Create clickable link for data docs URL
        if clickable_data_docs_url != "N/A":
            data_docs_link = f'<a href="{clickable_data_docs_url}">View Data Docs</a>'
        else:
            data_docs_link = "N/A"
        
        alert_title = f"Data Quality Alert: {file_name}"
        alert_text = f"""
        [DATA QUALITY ALERT] - {overall_severity}
        File: {file_name}
        Total failed expectations: {error_count}
        Error breakdown:
        {error_breakdown}
        Please review the Data Docs for detailed validation results: {data_docs_link}
        """
        color_map = {"high": "#FF0000", "medium": "#FFA500", "low": "#FFFF00", "none": "#008000"}
        color = color_map.get(overall_severity, "#FFFF00")
    
    # Save alert to file
    try:
        alert_file = os.path.join(project_root, "alerts", f"alert_{file_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt")
        os.makedirs(os.path.dirname(alert_file), exist_ok=True)
        
        with open(alert_file, 'w') as f:
            f.write(f"Title: {alert_title}\nColor: {color}\n\n{alert_text}")
        logger.info(f"Alert logged to {alert_file}")
    except Exception as e:
        logger.error(f"Failed to write alert file: {str(e)}")
    

    # Create separate text for Teams with HTML formatting
    if success:
        teams_text = f"""
        [DATA INGESTION SUCCESS]
        File: {file_name}
        Status: All validations passed successfully.
        Data quality statistics saved to database.
        """
    else:
        error_breakdown_text = "\n".join([f"{error_type}: {count}" for error_type, count in custom_error_stats.items() if count > 0])
        if not error_breakdown_text:
            error_breakdown_text = "No specific error types detected."
        
        # Create clickable link for data docs URL
        if clickable_data_docs_url != "N/A":
            data_docs_link = f'<a href="{clickable_data_docs_url}">View Data Docs</a>'
        else:
            data_docs_link = "N/A"
        
        teams_text = f"""
        [DATA QUALITY ALERT] - {overall_severity}
        File: {file_name}
        Total failed expectations: {error_count}
        Error breakdown:
        {error_breakdown_text}
        Please review the Data Docs for detailed validation results: {data_docs_link}
        """
    
    teams_payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": color,
        "summary": alert_title,
        "sections": [{
            "activityTitle": alert_title,
            "text": teams_text.replace("\n", "<br>")
        }]
    }
    
    headers = {'Content-Type': 'application/json'}
    for attempt in range(3):
        try:
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