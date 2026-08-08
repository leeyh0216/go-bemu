SELECT id, score
FROM `test-project.analytics.events`
WHERE (category = 'a' AND score BETWEEN 10 AND 30) OR score IS NULL
ORDER BY id
