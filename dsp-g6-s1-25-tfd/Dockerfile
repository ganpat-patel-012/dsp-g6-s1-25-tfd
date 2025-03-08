FROM python:3.9

WORKDIR /app

# Copy and install dependencies first (to leverage Docker layer caching)
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY . /app

# Expose the ports for Streamlit (8501) and FastAPI (8000)
EXPOSE 8501 8000 5433

# Run both FastAPI and Streamlit in parallel
CMD ["bash", "-c", "uvicorn configFiles.fastAPI:app --host 0.0.0.0 --port 8000 & streamlit run Home.py --server.port=8501 --server.address=0.0.0.0"]
