DECLARE partitions_to_delete DEFAULT (
  SELECT ARRAY_AGG(DISTINCT(DATE_TRUNC(partition_date, DAY)) IGNORE NULLS)
  FROM `test-project.analytics.temporary`
);
MERGE `test-project.analytics.destination` AS destination
USING `test-project.analytics.temporary` AS source
ON FALSE
WHEN NOT MATCHED BY SOURCE
  AND DATE_TRUNC(destination.partition_date, DAY) IN UNNEST(partitions_to_delete) THEN
  DELETE
WHEN NOT MATCHED BY TARGET THEN
  INSERT(id, partition_date, payload)
  VALUES(source.id, source.partition_date, source.payload)
