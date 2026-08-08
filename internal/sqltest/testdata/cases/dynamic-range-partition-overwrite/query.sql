DECLARE partitions_to_delete DEFAULT (
  SELECT ARRAY_AGG(
    DISTINCT(IFNULL(IF(partition_id >= 100, 0,
      RANGE_BUCKET(partition_id, GENERATE_ARRAY(0, 100, 10))), -1))
    IGNORE NULLS
  )
  FROM `test-project.analytics.temporary`
);
MERGE `test-project.analytics.destination` AS destination
USING `test-project.analytics.temporary` AS source
ON FALSE
WHEN NOT MATCHED BY SOURCE
  AND IFNULL(IF(destination.partition_id >= 100, 0,
    RANGE_BUCKET(destination.partition_id, GENERATE_ARRAY(0, 100, 10))), -1)
    IN UNNEST(partitions_to_delete) THEN
  DELETE
WHEN NOT MATCHED BY TARGET THEN
  INSERT(id, partition_id, payload)
  VALUES(source.id, source.partition_id, source.payload)
