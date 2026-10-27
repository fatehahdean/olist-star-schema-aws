SELECT p.category_name,
       COUNT(*) AS items_sold,
       ROUND(SUM(f.price), 2) AS revenue
FROM olist_raw.fact_orders f
JOIN olist_raw.dim_product p ON f.product_id = p.product_id
GROUP BY p.category_name
ORDER BY revenue DESC
LIMIT 10;
