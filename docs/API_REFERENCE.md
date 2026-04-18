# Retail Sales Batch Pipeline - API Reference

## Pipeline Module (`pipeline/retail_sales.py`)

### Class: TransformData

Apache Beam DoFn for transforming raw sales data.

#### Methods

##### `process(element)`

Transforms a single CSV row into a structured record.

**Parameters:**
- `element` (str): CSV row as a comma-separated string

**Returns:**
- Generator yielding dictionaries with transformed data

**Behavior:**
- Skips header rows (where first field is 'order_id')
- Calculates revenue as `price × quantity`
- Adds current UTC timestamp as load_date
- Validates numeric fields

**Example:**
```python
transform = TransformData()
csv_row = "ORD001,PROD001,Electronics,100.00,2,2024-01-01"
result = list(transform.process(csv_row))
# Returns: [{
#   'order_id': 'ORD001',
#   'product_id': 'PROD001',
#   'category': 'Electronics',
#   'price': 100.0,
#   'quantity': 2,
#   'order_date': '2024-01-01',
#   'revenue': 200.0,
#   'load_date': '2024-01-18'
# }]
```

---

### Function: run()

Initializes and executes the Apache Beam pipeline.

#### Parameters:
- `argv` (list, optional): Command-line arguments

#### Command-Line Arguments:

| Argument | Required | Type | Description |
|----------|----------|------|-------------|
| `--input` | Yes | str | GCS path to input CSV file |
| `--output` | Yes | str | BigQuery table in format `project:dataset.table` |
| `--runner` | No | str | Runner type (`DirectRunner`, `DataflowRunner`) |
| `--project` | No | str | GCP project ID |
| `--region` | No | str | GCP region (default: us-central1) |
| `--temp_location` | No | str | GCS path for temporary files |
| `--staging_location` | No | str | GCS path for staging files |

#### Example Usage:

```bash
# Local execution
python pipeline/retail_sales.py \
  --input=data/sample_data.csv \
  --output=project:dataset.table

# Dataflow execution
python pipeline/retail_sales.py \
  --input=gs://bucket/raw/data.csv \
  --output=project:dataset.sales_cleaned \
  --runner=DataflowRunner \
  --project=my-project \
  --region=asia-south1 \
  --temp_location=gs://bucket/temp \
  --staging_location=gs://bucket/staging
```

---

## Input/Output Schemas

### Input CSV Schema

Expected format for input CSV files:

```
order_id,product_id,category,price,quantity,order_date
ORD001,PROD001,Electronics,99.99,2,2024-01-01
ORD002,PROD002,Clothing,29.99,1,2024-01-01
```

**Field Definitions:**

| Field | Type | Description |
|-------|------|-------------|
| order_id | String | Unique order identifier |
| product_id | String | Product SKU or identifier |
| category | String | Product category |
| price | Float | Unit price in decimal |
| quantity | Integer | Order quantity |
| order_date | String | Order date (YYYY-MM-DD) |

### Output BigQuery Schema

Table created: `dataset.sales_cleaned`

| Column | Type | Mode | Description |
|--------|------|------|-------------|
| order_id | STRING | REQUIRED | Unique order ID |
| product_id | STRING | REQUIRED | Product identifier |
| category | STRING | NULLABLE | Product category |
| price | FLOAT64 | REQUIRED | Unit price |
| quantity | INTEGER | REQUIRED | Order quantity |
| order_date | STRING | REQUIRED | Order date |
| revenue | FLOAT64 | REQUIRED | Calculated revenue (price × quantity) |
| load_date | STRING | REQUIRED | Pipeline execution date (YYYY-MM-DD) |

### Aggregation Schema

Table: `dataset.sales_summary`

| Column | Type | Mode | Description |
|--------|------|------|-------------|
| order_date | STRING | REQUIRED | Order date (YYYY-MM-DD) |
| category | STRING | REQUIRED | Product category |
| total_revenue | FLOAT64 | NULLABLE | Sum of revenue for date/category |
| total_quantity | INTEGER | NULLABLE | Sum of quantity for date/category |

---

## Error Handling

### Common Exceptions

#### Invalid Numeric Fields
- **Cause**: price or quantity cannot be converted to number
- **Behavior**: Record is skipped
- **Example**: `price="invalid"`

#### Missing Fields
- **Cause**: CSV row has fewer than 6 columns
- **Behavior**: IndexError raised, record skipped
- **Solution**: Validate CSV format before pipeline

#### BigQuery Connection Errors
- **Cause**: Invalid credentials or permissions
- **Behavior**: Pipeline fails
- **Solution**: Verify GCP authentication and IAM roles

---

## SQL Queries

### Create Sales Summary

File: `sql/sales_summary.sql`

```sql
CREATE OR REPLACE TABLE retail_analytics.sales_summary AS
SELECT
  order_date,
  category,
  SUM(revenue) AS total_revenue,
  SUM(quantity) AS total_quantity
FROM retail_analytics.sales_cleaned
GROUP BY order_date, category;
```

### Additional Useful Queries

#### Daily Revenue by Category
```sql
SELECT
  order_date,
  category,
  SUM(revenue) AS daily_revenue
FROM retail_analytics.sales_cleaned
GROUP BY order_date, category
ORDER BY order_date DESC, daily_revenue DESC;
```

#### Top Products by Revenue
```sql
SELECT
  product_id,
  category,
  SUM(revenue) AS product_revenue,
  COUNT(*) AS order_count
FROM retail_analytics.sales_cleaned
GROUP BY product_id, category
ORDER BY product_revenue DESC
LIMIT 20;
```

#### Category Performance
```sql
SELECT
  category,
  COUNT(*) AS order_count,
  SUM(quantity) AS total_quantity,
  SUM(revenue) AS total_revenue,
  AVG(revenue) AS avg_revenue
FROM retail_analytics.sales_cleaned
GROUP BY category;
```

---

## Environment Variables

### Required for Dataflow

- `GOOGLE_APPLICATION_CREDENTIALS`: Path to service account JSON key
- `GCP_PROJECT_ID`: GCP Project ID

### Optional for Configuration

- `DATAFLOW_REGION`: Default region for Dataflow jobs
- `DATAFLOW_MACHINE_TYPE`: VM machine type for workers

---

## Dependencies

### Python Packages

```
apache-beam[gcp]==2.54.0
google-cloud-bigquery
```

### GCP Services Required

- Cloud Dataflow API
- BigQuery API
- Cloud Storage API
- Cloud Resource Manager API

---

## Rate Limits & Quotas

### BigQuery
- 100,000 API calls per 100 seconds (per project)
- 10,000 concurrent queries per project
- Max row size: 100 MB
- Max table size: 10 TB

### Cloud Storage
- 5 TB/s write bandwidth
- 100 GETs/second per object

### Dataflow
- Max workers: 1000 (per job)
- Min workers: 1
- Max job duration: Limited by quota

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2024-01-18 | Initial release |
| 1.1 | 2024-02-15 | Added error handling |
| 1.2 | 2024-03-20 | Performance optimization |

---

## Migration Guide

### From Local to Dataflow

Change runner and add GCP parameters:

```bash
# Before (Local)
python pipeline/retail_sales.py \
  --input=data/sample_data.csv \
  --output=output

# After (Dataflow)
python pipeline/retail_sales.py \
  --input=gs://bucket/data.csv \
  --output=project:dataset.table \
  --runner=DataflowRunner \
  --project=project-id \
  --region=asia-south1 \
  --temp_location=gs://bucket/temp
```

