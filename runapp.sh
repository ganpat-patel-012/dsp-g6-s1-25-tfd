#!/bin/bash

# Install required Python packages
pip install -r requirements.txt

# Run Streamlit app in the background
streamlit run Home.py &

# Run FastAPI server
uvicorn configFiles.fastAPI:app --reload