MERGE `test-project.analytics.inventory` AS destination
USING `test-project.analytics.adjustments` AS source
ON destination.id = source.id
WHEN MATCHED AND source.quantity > 0 THEN
  UPDATE SET quantity = source.quantity
WHEN MATCHED THEN
  UPDATE SET quantity = 0
