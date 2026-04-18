CREATE TABLE retail_analytics.sales_cleaned (
  order_id STRING,
  product_id STRING,
  category STRING,
  price FLOAT64,
  quantity INT64,
  order_date DATE,
  revenue FLOAT64,
  load_date DATE
)
PARTITION BY load_date;