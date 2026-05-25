SELECT o.order_id,
       o.amount
FROM orders o
WHERE o.amount > (
    SELECT AVG(amount)
    FROM orders
    WHERE customer_id = o.customer_id
);