import psycopg2
from psycopg2 import sql
from datetime import datetime
from configFiles.config import DB_CONFIG

def get_connection():
    """Establishes a PostgreSQL connection."""
    return psycopg2.connect(**DB_CONFIG)

def insert_prediction(data):
    """Inserts prediction data into PostgreSQL, supporting both single and batch inserts."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                insert_query = sql.SQL("""
                    INSERT INTO predictions 
                    (airline, source_city, destination_city, departure_time, arrival_time, 
                    travel_class, stops, duration, days_left, predicted_price, 
                    prediction_source, prediction_time, prediction_type) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """)

                if isinstance(data, list):
                    # Batch insert for multiple predictions
                    values = [(
                        item["airline"], item["source_city"], item["destination_city"],
                        item["departure_time"], item["arrival_time"], item["travel_class"],
                        item["stops"], item["duration"], item["days_left"],
                        item["predicted_price"], item["prediction_source"], item["prediction_time"], item["prediction_type"]
                    ) for item in data]
                    cursor.executemany(insert_query, values)
                else:
                    # Single insert
                    cursor.execute(insert_query, (
                        data["airline"], data["source_city"], data["destination_city"],
                        data["departure_time"], data["arrival_time"], data["travel_class"],
                        data["stops"], data["duration"], data["days_left"],
                        data["predicted_price"], data["prediction_source"], datetime.now(), data["prediction_type"]
                    ))

            conn.commit()
        return "✅ Prediction(s) saved to database!"
    
    except Exception as e:
        return f"❌ Database Error: {e}"