SELECT date_format(f.purchase_ts, '%Y-%m') AS month,
       c.customer_state AS state,
       ROUND(SUM(f.price), 2) AS item_revenue,
       ROUND(SUM(f.freight_value), 2) AS freight,
       ROUND(SUM(f.total_item_value), 2) AS total_revenue
FROM olist_raw.fact_orders f
JOIN olist_raw.dim_customer c ON f.customer_id = c.customer_id
GROUP BY 1, 2
ORDER BY 1, total_revenue DESC;
