SELECT category, COUNT(*) AS event_count, SUM(amount) AS total_amount
FROM `test-project.analytics.events`
WHERE amount IS NOT NULL
GROUP BY category
HAVING COUNT(*) >= 2
ORDER BY total_amount DESC
LIMIT 2
