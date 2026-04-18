#!/bin/bash

# Retail Sales Batch Pipeline Execution Script
# This script runs the Apache Beam pipeline on Google Cloud Dataflow
# 
# Prerequisites:
#   - gcloud SDK configured with credentials
#   - PROJECT_ID and BUCKET environment variables or set manually below
#   - Input data uploaded to GCS
#   - BigQuery dataset created
#
# Usage:
#   export PROJECT_ID=your-project-id
#   export BUCKET=your-bucket-name
#   bash scripts/run_pipeline.sh

# Set your GCP configuration (update with actual values)
PROJECT_ID=${PROJECT_ID:-YOUR_PROJECT_ID}
BUCKET=${BUCKET:-YOUR_BUCKET}
REGION=${REGION:-asia-south1}
DATASET=${DATASET:-retail_analytics}

# Validate required variables
if [ "$PROJECT_ID" = "YOUR_PROJECT_ID" ] || [ "$BUCKET" = "YOUR_BUCKET" ]; then
    echo "ERROR: Please set PROJECT_ID and BUCKET environment variables"
    echo "Usage: export PROJECT_ID=your-project-id && export BUCKET=your-bucket-name && bash scripts/run_pipeline.sh"
    exit 1
fi

echo "=========================================="
echo "Retail Sales Batch Pipeline"
echo "=========================================="
echo "Project ID: $PROJECT_ID"
echo "Bucket: $BUCKET"
echo "Region: $REGION"
echo "Dataset: $DATASET"
echo "=========================================="

# Run the pipeline
python pipeline/retail_sales.py \
  --project=$PROJECT_ID \
  --runner=DataflowRunner \
  --region=$REGION \
  --temp_location=gs://$BUCKET/temp \
  --staging_location=gs://$BUCKET/staging \
  --input=gs://$BUCKET/raw/sample_data.csv \
  --output=$PROJECT_ID:$DATASET.sales_cleaned

# Check exit status
if [ $? -eq 0 ]; then
    echo "=========================================="
    echo "✓ Pipeline execution completed successfully!"
    echo "=========================================="
else
    echo "=========================================="
    echo "✗ Pipeline execution failed!"
    echo "=========================================="
    exit 1
fi