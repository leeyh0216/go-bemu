UPDATE `test-project.analytics.accounts`
SET balance = 99
WHERE id = 1;
SELECT CAST('not-an-integer' AS INT64) AS broken;
