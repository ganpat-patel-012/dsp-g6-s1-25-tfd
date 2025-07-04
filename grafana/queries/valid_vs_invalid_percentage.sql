-- Panel 1: Valid vs Invalid Rows Over Time
-- Displays percentage of valid and invalid rows from ingested files (Time series)
SELECT
  timestamp AS time,
  ROUND(100.0 * valid_rows / total_rows, 2) AS valid_percentage,
  ROUND(100.0 * invalid_rows / total_rows, 2) AS invalid_percentage
FROM data_quality_stats
WHERE $__timeFilter(timestamp)
ORDER BY time;


