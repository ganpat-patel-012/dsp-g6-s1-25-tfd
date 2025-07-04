-- Panel 2: Error Types in Last 10 Minutes
-- Select Histogram/Bar Chart

WITH exploded AS (
SELECT
    timestamp,
    jsonb_each_text(error_details) AS error_pair
FROM data_quality_stats
WHERE timestamp > now() - interval '10 minutes'
)
SELECT
error_pair.key AS error_type,
SUM(error_pair.value::int) AS error_count
FROM exploded
GROUP BY error_pair.key
ORDER BY error_count DESC;
