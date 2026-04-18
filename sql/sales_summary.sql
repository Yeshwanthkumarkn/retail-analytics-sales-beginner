CREATE OR REPLACE TABLE retail_analytics.sales_summary AS
SELECT
  order_date,
  category,
  SUM(revenue) AS total_revenue,
  SUM(quantity) AS total_quantity
FROM retail_analytics.sales_cleaned
GROUP BY order_date, category;