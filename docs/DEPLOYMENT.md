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

### Step 0: Authenticate with GCP

```bash
# Login to Google Cloud
gcloud auth login

# Set default project
gcloud config set project YOUR_PROJECT_ID

# Verify authentication
gcloud auth list
```

### Step 1: Set Environment Variables

```bash
export PROJECT_ID=your-gcp-project-id
export BUCKET_NAME=your-unique-bucket-name
export REGION=asia-south1
export DATASET_ID=retail_analytics

# Verify variables are set
echo "Project: $PROJECT_ID"
echo "Bucket: $BUCKET_NAME"
echo "Region: $REGION"
echo "Dataset: $DATASET_ID"
```

### Step 2: Enable Required APIs

```bash
gcloud services enable dataflow.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  cloudresourcemanager.googleapis.com \
  --project=$PROJECT_ID
```

### Step 3: Create GCS Bucket and Folder Structure

```bash
# Create main bucket
gsutil mb -l $REGION gs://$BUCKET_NAME

# Create folder structure (folders are auto-created, but explicit creation helps)
echo "" | gsutil cp - gs://$BUCKET_NAME/raw/.placeholder
echo "" | gsutil cp - gs://$BUCKET_NAME/staging/.placeholder
echo "" | gsutil cp - gs://$BUCKET_NAME/temp/.placeholder

# Verify bucket structure
gsutil ls -r gs://$BUCKET_NAME/
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

**Option A: Using pip (Recommended)**
```bash
# Upgrade pip first
pip install --upgrade pip

# Install requirements
pip install -r requirements.txt

# Verify installation
pip show apache-beam google-cloud-bigquery
```

**Option B: Using virtual environment (Best practice)**
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/macOS:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install requirements
pip install -r requirements.txt
```

**Requirements**:
- apache-beam[gcp]==2.54.0
- google-cloud-bigquery

### Step 7: Set Up Service Account (Optional but Recommended for Production)

```bash
# Create service account
gcloud iam service-accounts create dataflow-sa \
  --project=$PROJECT_ID \
  --description="Service account for Dataflow pipeline"

# Grant Dataflow worker role
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:dataflow-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --role=roles/dataflow.worker

# Grant BigQuery data editor role
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:dataflow-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --role=roles/bigquery.dataEditor

# Grant Cloud Storage object admin role
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:dataflow-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --role=roles/storage.objectAdmin

# Create and download key
gcloud iam service-accounts keys create dataflow-sa-key.json \
  --iam-account=dataflow-sa@$PROJECT_ID.iam.gserviceaccount.com

# Set credentials environment variable
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/dataflow-sa-key.json"
```

### Step 8: Run the Pipeline on Dataflow

#### Option A: Using Python Script Directly

```bash
# Navigate to project directory
cd /path/to/retail-analytics-sales-beginner

# Run pipeline on Dataflow
python pipeline/retail_sales.py \
  --project=$PROJECT_ID \
  --runner=DataflowRunner \
  --region=$REGION \
  --temp_location=gs://$BUCKET_NAME/temp \
  --staging_location=gs://$BUCKET_NAME/staging \
  --input=gs://$BUCKET_NAME/raw/sample_data.csv \
  --output=$PROJECT_ID:$DATASET_ID.sales_cleaned

# Note: The pipeline will submit the job and return the Job ID
```

#### Option B: Using Enhanced Bash Script (Recommended)

The script already has validation built-in:
```bash
# Set environment variables
export PROJECT_ID=your-project-id
export BUCKET=your-bucket-name

# Run the script
bash scripts/run_pipeline.sh
```

**Note**: The script validates that PROJECT_ID and BUCKET are set, creates necessary folders, and reports status.

### Step 9: Monitor Pipeline Execution

```bash
# View all Dataflow jobs in the region
gcloud dataflow jobs list --region=$REGION --status=active

# View job status with details (replace JOB_ID with actual job ID)
JOB_ID="your-job-id-from-step-8"
gcloud dataflow jobs describe $JOB_ID --region=$REGION

# Stream logs from Cloud Logging
gcloud logging read "resource.type=dataflow_step AND labels.job_id=$JOB_ID" \
  --region=$REGION \
  --limit=50 \
  --format=json

# Monitor job metrics
gcloud monitoring time-series list \
  --filter='metric.type="dataflow.googleapis.com/job/*"' \
  --format=table
```

**Alternative: View in Cloud Console**
- Open [Dataflow Dashboard](https://console.cloud.google.com/dataflow)
- Select your region
- Click on the job to see detailed metrics and logs

### Step 10: Verify Data in BigQuery

```bash
# Wait for pipeline to complete (check Step 9)
# Once complete, verify data:

# List tables in dataset
bq ls --dataset_id=$PROJECT_ID:$DATASET_ID

# Query sample records
bq query --use_legacy_sql=false \
  "SELECT * FROM \`$PROJECT_ID.$DATASET_ID.sales_cleaned\` LIMIT 10"

# Check record count
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as row_count FROM \`$PROJECT_ID.$DATASET_ID.sales_cleaned\`"

# Check data quality
bq query --use_legacy_sql=false \
  "SELECT 
     COUNT(*) as total_records,
     COUNT(DISTINCT order_date) as unique_dates,
     COUNT(DISTINCT category) as unique_categories,
     MIN(revenue) as min_revenue,
     MAX(revenue) as max_revenue,
     AVG(revenue) as avg_revenue
   FROM \`$PROJECT_ID.$DATASET_ID.sales_cleaned\`"
```

### Step 11: Create Summary Table

```bash
# Execute aggregation SQL to create summary table
bq query --use_legacy_sql=false \
  --project_id=$PROJECT_ID \
  < sql/sales_summary.sql

# Verify summary table was created
bq ls --dataset_id=$PROJECT_ID:$DATASET_ID

# Query summary results
bq query --use_legacy_sql=false \
  "SELECT * FROM \`$PROJECT_ID.$DATASET_ID.sales_summary\` LIMIT 10"

# Get aggregated metrics
bq query --use_legacy_sql=false \
  "SELECT 
     order_date,
     SUM(total_revenue) as daily_total_revenue,
     COUNT(*) as categories
   FROM \`$PROJECT_ID.$DATASET_ID.sales_summary\`
   GROUP BY order_date
   ORDER BY order_date DESC
   LIMIT 10"
```

## 🧪 Testing

### Local Testing (without Dataflow) - For Development

**Note**: DirectRunner requires BigQuery credentials and will write to BigQuery.

```bash
# Ensure GOOGLE_APPLICATION_CREDENTIALS is set
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"

# Test with local CSV file (writes to BigQuery)
python pipeline/retail_sales.py \
  --runner=DirectRunner \
  --input=data/sample_data.csv \
  --output=$PROJECT_ID:$DATASET_ID.sales_cleaned_test
```

### Test Data Quality with Sample Subset

```bash
# Create a small test file
head -100 data/sample_data.csv > data/sample_test.csv

# Run pipeline on test data
python pipeline/retail_sales.py \
  --runner=DirectRunner \
  --input=data/sample_test.csv \
  --output=$PROJECT_ID:$DATASET_ID.sales_cleaned_test

# Query test results
bq query --use_legacy_sql=false \
  "SELECT * FROM \`$PROJECT_ID.$DATASET_ID.sales_cleaned_test\` LIMIT 5"
```

### Error Handling Verification

The pipeline includes robust error handling:
- ✅ Validates CSV format (6 required fields)
- ✅ Handles type conversion errors (price, quantity)
- ✅ Validates numeric values (rejects negative prices/quantities)
- ✅ Logs all errors for debugging
- ✅ Continues processing on error (doesn't crash)
- ✅ Tracks metrics: records_processed, records_failed, headers_skipped

## 📊 Verification Checklist

### Pre-Deployment ✓
- [ ] GCP account and project created
- [ ] Billing enabled on GCP project
- [ ] Authenticated with `gcloud auth login`
- [ ] Google Cloud SDK installed and updated

### Infrastructure Setup ✓
- [ ] GCP APIs enabled (Dataflow, BigQuery, Storage)
- [ ] GCS bucket created and accessible
- [ ] GCS folder structure created (raw/, temp/, staging/)
- [ ] BigQuery dataset created
- [ ] Sample data uploaded to GCS
- [ ] Service account created (optional but recommended)
- [ ] Service account key downloaded and GOOGLE_APPLICATION_CREDENTIALS set

### Code Preparation ✓
- [ ] Python 3.8+ installed
- [ ] Virtual environment created (recommended)
- [ ] Python dependencies installed (`pip install -r requirements.txt`)
- [ ] Code syntax validated (no errors)

### Pipeline Execution ✓
- [ ] Environment variables set (PROJECT_ID, BUCKET_NAME, REGION, DATASET_ID)
- [ ] Dataflow job submitted successfully
- [ ] Pipeline job appears in Dataflow console
- [ ] Pipeline runs without errors (check logs)

### Results Verification ✓
- [ ] Data exists in `sales_cleaned` table
- [ ] Record count > 0
- [ ] Revenue values calculated correctly
- [ ] Load date populated
- [ ] Summary table created successfully
- [ ] Query results show aggregated data
- [ ] No data quality issues (check for NULLs, negatives)

## 🔧 Troubleshooting

### Issue 1: Authentication Error - "Permission denied" or "Invalid credentials"

**Symptoms**: Error message about missing credentials or permission denied

**Solutions**:
```bash
# Check if authenticated
gcloud auth list

# Login if needed
gcloud auth login

# Set application default credentials
gcloud auth application-default login

# Or use service account key
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"

# Verify credentials work
gcloud auth application-default print-access-token
```

---

### Issue 2: "Project not found" or "Invalid project ID"

**Symptoms**: Error mentioning project doesn't exist

**Solutions**:
```bash
# Verify project exists
gcloud projects list

# Set correct project
gcloud config set project YOUR_PROJECT_ID

# Verify it's set
gcloud config list --filter=core.project
```

---

### Issue 3: "Table not found" or "Dataset not found in BigQuery"

**Symptoms**: BigQuery dataset or table doesn't exist

**Solutions**:
```bash
# List datasets
bq ls

# List tables in dataset
bq ls $DATASET_ID

# Check table schema
bq show --schema --format=pretty $DATASET_ID.sales_cleaned

# If dataset missing, create it
bq mk --location=$REGION $DATASET_ID
```

---

### Issue 4: Pipeline Job Fails with "Insufficient Permissions"

**Symptoms**: Dataflow job fails with permission errors

**Solutions**:
```bash
# Check service account roles
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:dataflow-sa*"

# Verify specific permissions
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:dataflow-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --format="table(bindings.role)"
```

---

### Issue 5: "Out of Memory" or Pipeline Times Out

**Symptoms**: Workers crash with OOM or job takes too long

**Solutions**:
```bash
# Increase machine type (add to pipeline command)
--machine_type=n1-standard-4 \
--max_num_workers=10 \
--num_workers=2

# Or reduce data size and test again
head -1000 data/sample_data.csv > data/small_sample.csv

python pipeline/retail_sales.py \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=$REGION \
  --machine_type=n1-standard-2 \
  --num_workers=1 \
  --temp_location=gs://$BUCKET_NAME/temp \
  --staging_location=gs://$BUCKET_NAME/staging \
  --input=gs://$BUCKET_NAME/raw/small_sample.csv \
  --output=$PROJECT_ID:$DATASET_ID.sales_cleaned_test
```

---

### Issue 6: "Invalid CSV row" Warnings in Logs

**Symptoms**: Pipeline logs show warnings about invalid records

**Solutions**:
```bash
# This is normal - the pipeline skips bad records gracefully
# Check Dataflow metrics to see records processed vs failed

gcloud logging read "resource.type=dataflow_step" \
  --limit=100 \
  --format=json | grep -i "records_processed\|records_failed"

# Check data quality in BigQuery
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as total FROM \`$PROJECT_ID.$DATASET_ID.sales_cleaned\`"
```

---

### Issue 7: "No data" in BigQuery Table After Pipeline Completes

**Symptoms**: Pipeline shows success but BigQuery table is empty

**Solutions**:
```bash
# Check if table was created
bq ls $DATASET_ID

# Check table schema
bq show $DATASET_ID.sales_cleaned

# Recheck input data exists in GCS
gsutil ls -r gs://$BUCKET_NAME/raw/

# Check if file is empty
gsutil cat gs://$BUCKET_NAME/raw/sample_data.csv | wc -l

# Run pipeline again with logging
python pipeline/retail_sales.py \
  --runner=DirectRunner \
  --input=data/sample_data.csv \
  --output=$PROJECT_ID:$DATASET_ID.sales_cleaned_debug
```

---

### Issue 8: "Staging Location or Temp Location Permission Denied"

**Symptoms**: Error about staging or temp location access

**Solutions**:
```bash
# Verify bucket exists and is accessible
gsutil ls gs://$BUCKET_NAME/

# Verify service account has storage permissions
gcloud projects get-iam-policy $PROJECT_ID \
  --member=serviceAccount:dataflow-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --format="table(bindings.role)"

# Grant storage.admin if missing
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:dataflow-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --role=roles/storage.admin
```

---

### Getting Help

**Check Logs**:
```bash
# Dataflow job logs
gcloud logging read "resource.type=dataflow_step AND labels.job_id=$JOB_ID" \
  --limit=100 \
  --format=json

# Cloud Dataflow dashboard
# Open: https://console.cloud.google.com/dataflow
```

**Community Resources**:
- [Apache Beam Documentation](https://beam.apache.org/documentation/)
- [Google Dataflow Troubleshooting](https://cloud.google.com/dataflow/docs/troubleshoot)
- [BigQuery Troubleshooting](https://cloud.google.com/bigquery/docs/troubleshoot-queries)

## 📈 Next Steps

1. Set up scheduled jobs (Cloud Scheduler → Pub/Sub → Dataflow)
2. Configure monitoring and alerting
3. Create BigQuery dashboards
4. Implement incremental data loading
5. Add data quality checks
6. Set up CI/CD pipeline

## ✅ Code Features & Robustness

The pipeline includes enterprise-grade features:

### Error Handling
- ✅ Validates CSV structure (requires 6 fields)
- ✅ Type conversion error handling
- ✅ Validation for negative prices/quantities
- ✅ Empty field detection
- ✅ Graceful degradation (skips bad records, continues)

### Monitoring & Logging
- ✅ Comprehensive logging at DEBUG, INFO, WARNING, ERROR levels
- ✅ Beam metrics for tracking:
  - Records processed successfully
  - Records failed
  - Headers skipped
- ✅ All errors logged with context for debugging

### Code Quality
- ✅ Full docstrings and type hints
- ✅ PEP 257 compliant documentation
- ✅ Detailed inline comments
- ✅ Production-ready error handling

### Data Validation
- ✅ CSV format validation
- ✅ Numeric type validation
- ✅ Business logic validation (no negative values)
- ✅ Data quality checks

### Deployment Ready
- ✅ Tested with DirectRunner (local) and DataflowRunner (GCP)
- ✅ Supports both local and cloud execution
- ✅ Compatible with CI/CD pipelines
- ✅ Containerized with Dockerfile

## 🧹 Cleanup

To remove all resources and avoid charges:

```bash
# Delete Dataflow jobs (if still running)
gcloud dataflow jobs cancel $JOB_ID --region=$REGION

# Delete BigQuery dataset
bq rm -r -d $DATASET_ID

# Delete GCS bucket
gsutil -m rm -r gs://$BUCKET_NAME

# Delete service account (if created)
gcloud iam service-accounts delete dataflow-sa@$PROJECT_ID.iam.gserviceaccount.com

# Delete service account key
rm -f dataflow-sa-key.json

# Revoke application default credentials (if not needed)
gcloud auth application-default print-access-token >/dev/null 2>&1 && \
  gcloud auth application-default revoke || echo "Not set up"
```

