# 🕵️‍♂️ The Flight Detectives - Flight Price Prediction System

A comprehensive Machine Learning-powered web application for predicting flight prices with real-time data ingestion, validation and monitoring capabilities.

## 🎯 Project Overview

This project demonstrates a complete Data Science in Production pipeline, featuring:

- **Flight Price Prediction**: ML model to predict flight prices based on various features
- **Real-time Data Ingestion**: Continuous data validation and quality monitoring
- **Scheduled Predictions**: Automated prediction jobs using Apache Airflow
- **Monitoring Dashboards**: Real-time monitoring of data quality and model performance
- **Web Application**: Interface for making single & batch predictions and viewing history


## 🚀 Features

### ✈️ Flight Price Prediction
- **Single Prediction**: Predict flight prices for individual flights
- **Batch Prediction**: Upload CSV files for multiple predictions
- **Real-time API**: FastAPI service for model serving
- **Prediction History**: View and filter past predictions

### 📊 Data Quality Management
- **Automated Data Ingestion**: Ingest data every minute
- **Data Validation**: Uses Great Expectations for quality checks
- **Error Detection**: Identifies 7 types of data quality issues
- **Alert System**: Teams notifications for critical data problems

### 🔄 Automated Workflows
- **Data Ingestion DAG**: Validates and processes incoming data
- **Prediction DAG**: Makes scheduled predictions every 2 minutes
- **File Management**: Automatically sorts data into good/bad folders

### 📈 Monitoring & Analytics
- **Data Quality Dashboard**: Monitor ingested data problems
- **Prediction Analytics**: Track model performance and data drift
- **Real-time Updates**: Live dashboard updates with thresholds

## 🛠️ Technology Stack

| Component | Technology |
|-----------|------------|
| **Frontend** | Streamlit |
| **Backend API** | FastAPI |
| **Database** | PostgreSQL |
| **Data Validation** | Great Expectations |
| **Workflow Orchestration** | Apache Airflow |
| **Monitoring** | Grafana |
| **Containerization** | Docker |


## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/ganpat-patel-012/dsp-g6-s1-25-tfd.git
cd dsp-g6-s1-25-tfd
```

### 2. Start the Application
```bash
docker-compose up --build
```

### 3. Access the Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Streamlit Web App** | http://localhost:8501 | - |
| **FastAPI Documentation** | http://localhost:8000/docs | - |
| **Airflow Web UI** | http://localhost:8080 | admin/admin |
| **Grafana Dashboard** | http://localhost:3000 | admin/admin |


## 📁 Project Structure

```
dsp-g6-s1-25-tfd/
├── 📁 airflow/                 # Airflow DAGs and configuration
│   ├── dags/                  # Data ingestion and prediction DAGs
│   └── logs/                  # Airflow logs
├── 📁 configFiles/            # Configuration and utility files
│   ├── fastAPI.py            # FastAPI application
│   ├── makePrediction.py     # Prediction utilities
│   └── dbCode.py             # Database operations
├── 📁 ddl/                    # Database schema
├── 📁 grafana/                # Grafana dashboards and queries
│   ├── dashboards/           # Dashboard configurations
│   └── queries/              # SQL queries for dashboards
├── 📁 gx/                     # Great Expectations configuration
├── 📁 input_data/             # Raw data for ingestion
├── 📁 mlModel/                # Trained ML models
├── 📁 notebooks/              # Jupyter notebooks
├── 📁 output_data/            # Processed data (good/bad)
├── 📁 pages/                  # Streamlit pages
│   ├── Predict_Now.py        # Single and batch prediction
│   └── Predict_History.py    # Prediction history viewer
├── 📄 Home.py                 # Streamlit main page
├── 📄 docker-compose.yml      # Docker services configuration
├── 📄 requirements.txt        # Python dependencies
└── 📄 README.md               # This file
```

## 🔧 Configuration

### Environment Variables
The application uses the following key configurations:

- **Database**: PostgreSQL with user `tfd_user`, password `tfd_pass`, database `tfd_db`
- **API**: FastAPI running on port 8000
- **Web App**: Streamlit running on port 8501
- **Airflow**: Running on port 8080 with admin/admin credentials
- **Grafana**: Running on port 3000 with admin/admin credentials

### Data Quality Rules
The system validates data against these rules:

1. **Missing Airline Names**: High severity
2. **Negative Duration Values**: Medium severity  
3. **Same Source/Destination**: High severity
4. **Invalid Days Left**: Medium severity
5. **Premium Travel Class**: Low severity
6. **Air India with Vistara Flight Numbers**: Low severity
7. **Zero Stops with Long Duration**: High severity

## 📊 API Endpoints

### FastAPI Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/predict` | POST | Single flight price prediction |
| `/predict_batch` | POST | Batch flight price predictions |
| `/past-predictions` | GET | Retrieve prediction history |

## 🔄 Workflow Processes

### Data Ingestion Workflow
1. **Read Data**: Randomly selects CSV file from raw data folder
2. **Validate Data**: Uses Great Expectations for quality validation
3. **Save Statistics**: Stores validation statistics in database
4. **Send Alerts**: Generates HTML reports and sends Teams notifications
5. **Split & Save**: Separates good/bad data into respective folders

### Prediction Workflow
1. **Check for New Data**: Identifies newly ingested files
2. **Make Predictions**: Calls API for batch predictions
3. **Save Results**: Stores predictions in database

## 📈 Monitoring Dashboards

### Data Quality Monitoring Dashboard
- **Error Type Distribution**: Histogram of data quality issues
- **Validation Success Rate**: Percentage of valid vs invalid data
- **File Processing Statistics**: Files processed per time period
- **Error Severity Analysis**: Criticality levels of detected issues

### Prediction Analytics Dashboard
- **Prediction Volume**: Number of predictions over time
- **Price Distribution**: Histogram of predicted prices
- **Model Performance**: Accuracy and drift metrics
- **Source Analysis**: WebApp vs Scheduled predictions

## 👥 Team Members

| Name | Responsibilities |
|------|------------------|
| **Ganpat Patel** | Streamlit, ML Model, FastAPI, PostgreSQL & Docker |
| **JatinKumar Parmar** | Data Preparation, Data Ingestion & Validation DAG |
| **Adnan Ali** | Airflow Data Prediction Job |
| **Musa Ummar** | Airflow Prediction Job Scheduling First Defence & Grafana |
| **Manoj Kumar** | Grafana |
---

**Made with ❤️ and a whole lot of coffee ☕ by The Flight Detectives**