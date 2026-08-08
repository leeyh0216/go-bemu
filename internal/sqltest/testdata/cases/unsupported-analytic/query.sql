SELECT ROW_NUMBER() OVER (ORDER BY id) AS row_number
FROM `test-project.analytics.accounts`
