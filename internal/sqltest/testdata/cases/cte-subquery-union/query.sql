WITH selected AS (
  SELECT id, category
  FROM `test-project.analytics.events`
  WHERE id IN (1, 2)
  UNION ALL
  SELECT id, category
  FROM `test-project.analytics.events`
  WHERE id = 3
)
SELECT category, id
FROM (
  SELECT category, id
  FROM selected
  WHERE id > 1
) AS filtered
ORDER BY id
