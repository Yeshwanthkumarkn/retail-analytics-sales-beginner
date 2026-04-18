# Retail Sales Batch Pipeline - Deployment Guide

## 📋 Prerequisites

### Required Tools
- Google Cloud SDK (gcloud CLI)
- Python 3.8+
- gsutil (included with Google Cloud SDK)
- Git

### GCP Requirements
- Active GCP Project with billing enabled
- Appropriate IAM permissions:
  - Dataflow Developer
  - BigQuery Admin
  - Storage Admin
  - Service Account User

## 🚀 Step-by-Step Deployment

### Step 1: Set Environment Variables

```bash
export PROJECT_ID=your-gcp-project-id
export BUCKET_NAME=your-unique-bucket-name
export REGION=asia-south1
export DATASET_ID=retail_analytics
```

### Step 2: Enable Required APIs

```bash
gcloud services enable dataflow.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  cloudresourcemanager.googleapis.com \
  --project=$PROJECT_ID
```

### Step 3: Create GCS Bucket

```bash
gsutil mb -l $REGION gs://$BUCKET_NAME
```

**Bucket Structure Setup** (optional but recommended):
```bash
gsutil mb -l $REGION gs://$BUCKET_NAME/raw/
gsutil mb -l $REGION gs://$BUCKET_NAME/staging/
gsutil mb -l $REGION gs://$BUCKET_NAME/temp/
```

### Step 4: Upload Sample Data

```bash
# Copy sample data to GCS
gsutil cp data/sample_data.csv gs://$BUCKET_NAME/raw/

# Verify upload
gsutil ls -r gs://$BUCKET_NAME/raw/
```

### Step 5: Create BigQuery Dataset

```bash
bq mk --location=$REGION $DATASET_ID

# Verify dataset creation
bq ls
```

### Step 6: Install Python Dependencies

```bash
pip install -r requirements.txt
```

**Requirements**:
- apache-beam[gcp]==2.54.0
- google-cloud-bigquery

### Step 7: Create Service Account (Optional but Recommended)

```bash
# Create service account
gcloud iam service-accounts create dataflow-sa \
  --project=$PROJECT_ID

# Grant necessary roles
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:dataflow-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --role=roles/dataflow.worker

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:dataflow-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --role=roles/bigquery.dataEditor

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:dataflow-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --role=roles/storage.objectAdmin
```

### Step 8: Run the Pipeline

#### Option A: Using Python Script Directly

```bash
cd /path/to/retail-analytics-sales-beginner

python pipeline/retail_sales.py \
  --project=$PROJECT_ID \
  --runner=DataflowRunner \
  --region=$REGION \
  --temp_location=gs://$BUCKET_NAME/temp \
  --staging_location=gs://$BUCKET_NAME/staging \
  --input=gs://$BUCKET_NAME/raw/sample_data.csv \
  --output=$PROJECT_ID:$DATASET_ID.sales_cleaned
```

#### Option B: Using Bash Script

First, update `scripts/run_pipeline.sh`:
```bash
#!/bin/bash

PROJECT_ID=your-gcp-project-id
BUCKET=your-bucket-name
REGION=asia-south1
DATASET=retail_analytics

python pipeline/retail_sales.py \
  --project=$PROJECT_ID \
  --runner=DataflowRunner \
  --region=$REGION \
  --temp_location=gs://$BUCKET/temp \
  --staging_location=gs://$BUCKET/staging \
  --input=gs://$BUCKET/raw/sample_data.csv \
  --output=$PROJECT_ID:$DATASET.sales_cleaned
```

Then execute:
```bash
bash scripts/run_pipeline.sh
```

### Step 9: Monitor Pipeline Execution

```bash
# View Dataflow jobs
gcloud dataflow jobs list --region=$REGION

# Get detailed job information
gcloud dataflow jobs describe JOB_ID --region=$REGION

# Stream logs
gcloud dataflow jobs show JOB_ID --region=$REGION --log
```

### Step 10: Verify Data in BigQuery

```bash
# Check if table was created
bq ls $DATASET_ID

# Query sample records
bq query --use_legacy_sql=false \
  "SELECT * FROM $DATASET_ID.sales_cleaned LIMIT 10"

# Check record count
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as row_count FROM $DATASET_ID.sales_cleaned"
```

### Step 11: Create Summary Table

```bash
# Execute aggregation SQL
bq query --use_legacy_sql=false \
  < sql/sales_summary.sql

# Query summary results
bq query --use_legacy_sql=false \
  "SELECT * FROM $DATASET_ID.sales_summary"
```

## 🧪 Testing

### Local Testing (without Dataflow)

```bash
python pipeline/retail_sales.py \
  --runner=DirectRunner \
  --input=data/sample_data.csv \
  --output=$PROJECT_ID:$DATASET_ID.sales_cleaned_local
```

### Validate Pipeline with Sample Data

```bash
# Process small sample locally
head -100 data/sample_data.csv | python pipeline/retail_sales.py \
  --runner=DirectRunner \
  --input=- \
  --output=$PROJECT_ID:$DATASET_ID.sales_cleaned_test
```

## 📊 Verification Checklist

- [ ] GCP APIs enabled
- [ ] GCS bucket created and accessible
- [ ] Sample data uploaded to GCS
- [ ] BigQuery dataset created
- [ ] Python dependencies installed
- [ ] Dataflow job submitted successfully
- [ ] Pipeline completed without errors
- [ ] Data exists in `sales_cleaned` table
- [ ] Summary table created successfully
- [ ] Query results look correct

## 🔧 Troubleshooting

### Common Issues

**Issue**: Permission Denied Error
```bash
# Solution: Check service account permissions
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:*"
```

**Issue**: Pipeline Fails with Beam Exception
```bash
# Check logs
gcloud dataflow jobs show JOB_ID --region=$REGION --log | grep ERROR
```

**Issue**: BigQuery Table Not Found
```bash
# Verify dataset and table
bq ls -n 1000 $DATASET_ID
```

**Issue**: Out of Memory Errors
```bash
# Increase machine type in Dataflow
# Add: --machine_type=n1-standard-4 to pipeline command
```

## 📈 Next Steps

1. Set up scheduled jobs (Cloud Scheduler → Pub/Sub → Dataflow)
2. Configure monitoring and alerting
3. Create BigQuery dashboards
4. Implement incremental data loading
5. Add data quality checks
6. Set up CI/CD pipeline

## 🧹 Cleanup

To remove all resources and avoid charges:

```bash
# Delete Dataflow jobs (if still running)
gcloud dataflow jobs cancel JOB_ID --region=$REGION

# Delete BigQuery dataset
bq rm -r -d $DATASET_ID

# Delete GCS bucket
gsutil -m rm -r gs://$BUCKET_NAME

# Delete service account (if created)
gcloud iam service-accounts delete dataflow-sa@$PROJECT_ID.iam.gserviceaccount.com
```

