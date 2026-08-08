SELECT customer.name, purchase.amount
FROM `test-project.analytics.customers` AS customer
INNER JOIN `test-project.analytics.orders` AS purchase
  ON customer.id = purchase.customer_id
WHERE purchase.amount IN (20, 30)
ORDER BY purchase.amount DESC
