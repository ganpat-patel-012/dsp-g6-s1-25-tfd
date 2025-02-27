DROP TABLE IF EXISTS predictions;CREATE TABLE predictions (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(50),
    source_city VARCHAR(50),
    destination_city VARCHAR(50),
    departure_time VARCHAR(50),
    arrival_time VARCHAR(50),
    travel_class VARCHAR(50),
    stops VARCHAR(10),
    duration NUMERIC(5,2),
    days_left INT,
    predicted_price NUMERIC(10,2),
    prediction_source VARCHAR(50),
    prediction_type VARCHAR(50),
    prediction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- remove all records : TRUNCATE TABLE predictions RESTART IDENTITY;