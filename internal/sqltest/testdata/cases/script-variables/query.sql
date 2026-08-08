DECLARE threshold INT64 DEFAULT 10;
SET threshold = 20;
SELECT id
FROM `test-project.analytics.accounts`
WHERE balance >= threshold
ORDER BY id;
