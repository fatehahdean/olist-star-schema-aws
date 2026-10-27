SELECT c.customer_state AS state,
       COUNT(DISTINCT f.order_id) AS delivered_orders,
       ROUND(AVG(date_diff('day', f.estimated_delivery_ts, f.delivered_ts)), 2) AS avg_days_late
FROM olist_raw.fact_orders f
JOIN olist_raw.dim_customer c ON f.customer_id = c.customer_id
WHERE f.order_status = 'delivered'
  AND f.delivered_ts IS NOT NULL
  AND f.estimated_delivery_ts IS NOT NULL
GROUP BY c.customer_state
ORDER BY avg_days_late DESC;
