MERGE `test-project.analytics.inventory` AS destination
USING `test-project.analytics.adjustments` AS source
ON destination.id = source.id AND source.quantity > 0
WHEN MATCHED THEN
  UPDATE SET quantity = source.quantity
