CREATE TABLE data_quality_stats (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    filename VARCHAR(255) NOT NULL,
    total_rows INTEGER NOT NULL CHECK (total_rows >= 0),
    valid_rows INTEGER NOT NULL CHECK (valid_rows >= 0),
    invalid_rows INTEGER NOT NULL CHECK (invalid_rows >= 0),
    error_details JSONB,
    error_count INTEGER CHECK (error_count IS NULL OR error_count >= 0),
    severity VARCHAR(20) CHECK (severity IN ('high', 'medium', 'low') OR severity IS NULL),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_row_counts CHECK (valid_rows + invalid_rows = total_rows)
);