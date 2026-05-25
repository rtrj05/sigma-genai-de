SELECT c.name, o.order_id
FROM customers c
JOIN orders o
ON c.customer_id = o.order_id;