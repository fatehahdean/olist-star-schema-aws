WITH orders_per_customer AS (
  SELECT c.customer_unique_id,
         COUNT(DISTINCT f.order_id) AS n_orders
  FROM olist_raw.fact_orders f
  JOIN olist_raw.dim_customer c ON f.customer_id = c.customer_id
  GROUP BY c.customer_unique_id
)
SELECT COUNT(*) AS customers,
       SUM(CASE WHEN n_orders > 1 THEN 1 ELSE 0 END) AS repeat_customers,
       ROUND(100.0 * SUM(CASE WHEN n_orders > 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS repeat_rate_pct
FROM orders_per_customer;
