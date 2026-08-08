SELECT id, name
FROM `test-project.analytics.partitioned_events`
WHERE id IN (2, 3, 9)
  AND name LIKE 'prefix-%'
  AND event_date >= CAST('2026-08-02' AS DATE)
  AND event_at < TIMESTAMP '2026-08-04T00:00:00Z'
  AND nullable_id IS NOT NULL
ORDER BY id
