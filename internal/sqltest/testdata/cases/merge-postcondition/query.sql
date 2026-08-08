MERGE `test-project.analytics.inventory` AS destination
USING `test-project.analytics.incoming` AS source
ON destination.id = source.id
WHEN MATCHED THEN
  UPDATE SET quantity = source.quantity
WHEN NOT MATCHED THEN
  INSERT (id, quantity) VALUES (source.id, source.quantity)
WHEN NOT MATCHED BY SOURCE AND destination.id = 4 THEN
  DELETE
