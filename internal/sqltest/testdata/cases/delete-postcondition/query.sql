DELETE FROM `test-project.analytics.accounts`
WHERE status = 'inactive' AND id IN (2, 3)
