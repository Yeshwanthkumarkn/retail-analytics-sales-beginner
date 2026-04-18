# Retail Sales Batch Pipeline (GCP)

## 📌 Overview
This project demonstrates a batch data pipeline using Google Cloud Platform.

Pipeline Flow:
CSV → GCS → Dataflow → BigQuery → SQL Analytics

## 🧰 Tech Stack
- Google Cloud Storage
- Dataflow (Apache Beam)
- BigQuery

## 🚀 Setup Instructions

### 1. Enable APIs
- Dataflow API
- BigQuery API
- Cloud Storage API

### 2. Create Bucket
gsutil mb -l asia-south1 gs://<your-bucket-name>

### 3. Upload Data
gsutil cp data/sample_sales.csv gs://<your-bucket-name>/raw/

### 4. Create BigQuery Dataset
bq mk retail_analytics

### 5. Run Pipeline
bash scripts/run_pipeline.sh

### 6. Run Aggregation
bq query --use_legacy_sql=false < sql/create_summary.sql