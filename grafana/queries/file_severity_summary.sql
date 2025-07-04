-- Panel 3: File Severity Levels
-- Pie chart showing count of files with low, medium, high severity

SELECT
severity,
COUNT(*) AS count
FROM data_quality_stats
WHERE $__timeFilter(timestamp)
GROUP BY severity;
