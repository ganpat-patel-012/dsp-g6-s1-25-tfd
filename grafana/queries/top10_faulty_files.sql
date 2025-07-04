-- Panel 4: Top 10 Faulty Files
-- Bar chart showing top 10 files with highest error_count (Displays the 10 files with the highest number of data errors

SELECT
filename,
error_count
FROM data_quality_stats
WHERE error_count IS NOT NULL
ORDER BY error_count DESC
LIMIT 10;