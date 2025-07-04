# 🕵️‍♂️ The Flight Detectives: Flight Price Prediction Project

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Data Pipeline & Architecture](#data-pipeline--architecture)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Airflow DAGs](#airflow-dags)
- [Model Training & Prediction](#model-training--prediction)
- [Data Validation & Quality](#data-validation--quality)
- [Database Schema](#database-schema)
- [Contributors](#contributors)
- [License](#license)

---

## Overview

**The Flight Detectives** is an end-to-end machine learning project that predicts flight prices using a robust data pipeline. The system automates data ingestion, validation, model training, and prediction, and provides a user-friendly web interface for real-time and batch predictions. The project leverages Apache Airflow for orchestration, Great Expectations for data quality, and is fully containerized with Docker.

---

## Features

- **Automated Data Ingestion**: Scheduled and validated via Airflow DAGs.
- **Data Quality Assurance**: Great Expectations checks and custom validation logic.
- **Machine Learning Model**: Random Forest Regressor trained on historical flight data.
- **Web Interface**: Streamlit app for single and batch predictions, and viewing prediction history.
- **REST API**: FastAPI backend for programmatic access and integration.
- **Database Storage**: PostgreSQL for storing predictions and data quality stats.
- **Monitoring**: Grafana dashboard (optional, for advanced monitoring).
- **Dockerized Deployment**: Easy setup and scaling with Docker Compose.

---

## Project Structure

```
.
├── airflow/                # Airflow configs, DAGs, and logs
│   └── dags/               # Airflow DAGs for data ingestion and prediction
├── configFiles/            # Configurations, FastAPI, DB, and prediction logic
├── data/                   # Raw and processed data (local)
├── database/               # Database files (if any)
├── ddl/                    # SQL scripts for table creation
├── gx/                     # Great Expectations configs and suites
├── mlModel/                # Trained ML model and training script
├── notebooks/              # Jupyter notebooks for EDA and validation
├── pages/                  # Streamlit app pages
├── images/                 # Team member images
├── Dockerfile              # Docker build file
├── docker-compose.yml      # Docker Compose setup
├── requirements.txt        # Python dependencies
├── runapp.sh               # Script to run the app
└── README.md               # Project documentation
```

---

## Data Pipeline & Architecture

**1. Data Ingestion & Validation**
- Raw flight data (CSV) is ingested via Airflow DAGs.
- Data is validated using Great Expectations and custom logic (e.g., null checks, value ranges, business rules).
- Validated data is split into "good" and "bad" datasets and statistics are logged.

**2. Model Training**
- The model is trained using a Random Forest Regressor pipeline (`mlModel/ml_model_train.py`).
- Preprocessing includes one-hot encoding for categorical features and passthrough for numeric features.
- The trained model is saved as `mlModel/flight_price_predictor.pkl`.

**3. Prediction**
- Batch predictions are scheduled via Airflow.
- Real-time and batch predictions are available via the Streamlit web app and FastAPI endpoints.
- All predictions are stored in PostgreSQL for history and analytics.

**4. Data Quality Monitoring**
- Data quality stats are stored in the database.
- Alerts are sent via Microsoft Teams webhook on validation failures.

---

## Setup Instructions


###  Run the Application

#### Using Docker Compose (Recommended)

```bash
docker-compose up --build
```

- Streamlit app: [http://localhost:8501](http://localhost:8501)
- FastAPI: [http://localhost:8000/docs](http://localhost:8000/docs)
- Airflow: [http://localhost:8080](http://localhost:8080)
- Grafana: [http://localhost:3000](http://localhost:3000) (admin/admin)
- Great Expectations Data Docs: [http://localhost:8081/gx](http://localhost:8081/gx)


## Usage

### Web Interface (Streamlit)

- **Single Prediction**: Enter flight details and get an instant price prediction.
- **Batch Prediction**: Upload a CSV file for multiple predictions at once.
- **Prediction History**: View all past predictions, filter by date and source.

### API Endpoints (FastAPI)

- **POST /predict**: Predict price for a single flight.
- **POST /predict_batch**: Predict prices for a batch of flights.
- **GET /past-predictions**: Retrieve prediction history (with filters).

API docs available at [http://localhost:8000/docs](http://localhost:8000/docs).

---

## Airflow DAGs

- **Data Ingestion DAG (`data_ingestion_dag_final.py`)**  
  - Runs every minute.
  - Picks a random raw CSV, validates it, logs data quality stats, splits into good/bad, and sends alerts on issues.

- **Prediction DAG (`prediction_dag_final.py`)**  
  - Runs every 2 minutes.
  - Picks new validated data, makes predictions using the trained model, and stores results in the database.

---

## Model Training & Prediction

- **Training Script**: `mlModel/ml_model_train.py`
  - Reads cleaned dataset, preprocesses features, trains a Random Forest model, and saves it as a pickle file.
- **Prediction**:  
  - Used by both the web app and Airflow DAGs for real-time and scheduled predictions.

---

## Data Validation & Quality

- **Great Expectations**:  
  - Configured in `gx/` with expectations defined in `gx/expectations/expectations_suite.json`.
  - Checks include: no null airlines, positive durations, valid city pairs, numeric days_left, no "Premium" class, and more.
- **Custom Validation**:  
  - Additional business rules are enforced in the Airflow DAGs and notebooks.
- **Data Quality Stats**:  
  - Stored in the `data_quality_stats` table for monitoring and analytics.
- **Alerts**:  
  - Sent to Microsoft Teams on validation failures.

---

## Database Schema

### `predictions` Table

| Column            | Type           | Description                        |
|-------------------|----------------|------------------------------------|
| id                | SERIAL         | Primary key                        |
| airline           | VARCHAR(50)    | Airline name                       |
| source_city       | VARCHAR(50)    | Source city                        |
| destination_city  | VARCHAR(50)    | Destination city                   |
| departure_time    | VARCHAR(50)    | Departure time slot                |
| arrival_time      | VARCHAR(50)    | Arrival time slot                  |
| travel_class      | VARCHAR(50)    | Travel class (Economy/Business)    |
| stops            | VARCHAR(50)    | Number of stops                    |
| duration          | NUMERIC(5,2)   | Flight duration (hours)            |
| days_left         | INT            | Days left until departure          |
| predicted_price   | NUMERIC(10,2)  | Predicted price                    |
| prediction_source | VARCHAR(50)    | WebApp/Scheduled                   |
| prediction_type   | VARCHAR(50)    | Single/Multiple/Scheduled          |
| prediction_time   | TIMESTAMP      | Time of prediction                 |

### `data_quality_stats` Table

| Column         | Type           | Description                        |
|----------------|----------------|------------------------------------|
| id             | SERIAL         | Primary key                        |
| filename       | VARCHAR(255)   | Name of the ingested file          |
| total_rows     | INTEGER        | Total rows in the file             |
| valid_rows     | INTEGER        | Number of valid rows               |
| invalid_rows   | INTEGER        | Number of invalid rows             |
| error_details  | JSONB          | Error breakdown                    |
| error_count    | INTEGER        | Total number of errors             |
| severity       | VARCHAR(20)    | Error severity (high/medium/low)   |
| timestamp      | TIMESTAMP      | Time of validation                 |

---

## Contributors

- **Ganpat Patel**: Streamlit, ML Model, FastAPI, PostgreSQL & Docker
- **JatinKumar Parmar**: Data Preparation, Data Ingestion & Validation DAG
- **Adnan Ali**: Airflow Data Prediction Job
- **Musa Ummar**: Airflow Prediction Job Scheduling First Defence & Grafana
- **Manoj Kumar**: Grafana



**Made with ❤️ and a whole lot of coffee ☕ by The Flight Detectives**

---