INSERT INTO `test-project.analytics.ingested_events` (id, name, _PARTITIONTIME)
VALUES (1, 'alpha', TIMESTAMP '2026-08-03T00:00:00Z');

INSERT INTO `test-project.analytics.ingested_events`
VALUES (2, 'default-partition');

SELECT *, _PARTITIONTIME AS partition_time, _PARTITIONDATE AS partition_date
FROM `test-project.analytics.ingested_events`
WHERE _PARTITIONDATE = DATE '2026-08-03'
ORDER BY id
