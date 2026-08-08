UPDATE `test-project.analytics.accounts`
SET status = 'active'
WHERE status = 'inactive' AND id IN (2, 3)
