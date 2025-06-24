CREATE TABLE data_quality_stats (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    filename VARCHAR NOT NULL,
    total_rows INTEGER NOT NULL,
    valid_rows INTEGER NOT NULL,
    invalid_rows INTEGER NOT NULL,
    error_counts TEXT,
    is_critical BOOLEAN NOT NULL DEFAULT FALSE,
    error_type VARCHAR,
    error_count INTEGER,
    severity VARCHAR,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);