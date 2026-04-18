# Retail Sales Batch Pipeline - Development Guide

## 🛠️ Development Setup

### Local Environment Setup

1. **Clone Repository**
```bash
git clone <repository-url>
cd retail-analytics-sales-beginner
git checkout retail-sales-summary-batch
```

2. **Create Virtual Environment**
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate
```

3. **Install Dependencies**
```bash
pip install -r requirements.txt
```

### GCP Setup for Development

```bash
# Authenticate with GCP
gcloud auth application-default login

# Set default project
gcloud config set project YOUR_PROJECT_ID
```

## 📁 Project Structure

```
retail-analytics-sales-beginner/
├── data/                    # Sample data files
│   └── sample_data.csv     # Input CSV data
├── pipeline/               # Apache Beam pipeline code
│   ├── __init__.py
│   └── retail_sales.py     # Main pipeline logic
├── scripts/                # Automation scripts
│   ├── run_pipeline.sh     # Pipeline execution script
│   └── setup_gcp.sh        # GCP setup script
├── sql/                    # BigQuery SQL scripts
│   └── sales_summary.sql   # Aggregation query
├── docs/                   # Documentation
│   ├── ARCHITECTURE.md     # System design
│   ├── DEPLOYMENT.md       # Deployment guide
│   └── DEVELOPMENT.md      # This file
├── Dockerfile              # Container image definition
├── requirements.txt        # Python dependencies
├── README.md              # Project overview
└── .gitignore             # Git ignore rules
```

## 💡 Code Organization

### Pipeline Code (`pipeline/retail_sales.py`)

#### Main Components:

1. **TransformData Class**
   - Apache Beam DoFn for data transformation
   - Skips header row
   - Calculates revenue
   - Adds load timestamp

2. **run() Function**
   - Parses command-line arguments
   - Initializes Beam pipeline
   - Defines data transformation steps
   - Configures BigQuery output

### Key Methods

```python
class TransformData(beam.DoFn):
    def process(self, element):
        # element: CSV row as string
        # Yields: dict with transformed data
```

## 🔄 Development Workflow

### 1. Local Testing with DirectRunner

```bash
# Test with local runner (no GCP cost)
python pipeline/retail_sales.py \
  --runner=DirectRunner \
  --input=data/sample_data.csv \
  --output=local_output
```

### 2. Testing with Sample Data

```bash
# Create smaller test file
head -20 data/sample_data.csv > data/test_data.csv

# Run pipeline with test data
python pipeline/retail_sales.py \
  --runner=DirectRunner \
  --input=data/test_data.csv \
  --output=test_output
```

### 3. GCP Testing with Dataflow

```bash
python pipeline/retail_sales.py \
  --project=YOUR_PROJECT_ID \
  --runner=DataflowRunner \
  --region=asia-south1 \
  --temp_location=gs://YOUR_BUCKET/temp \
  --staging_location=gs://YOUR_BUCKET/staging \
  --input=gs://YOUR_BUCKET/raw/sample_data.csv \
  --output=YOUR_PROJECT_ID:retail_analytics.sales_cleaned
```

## 🧪 Testing & Validation

### Unit Testing Example

Create `tests/test_transform.py`:

```python
import unittest
from pipeline.retail_sales import TransformData

class TestTransformData(unittest.TestCase):
    def setUp(self):
        self.transform = TransformData()
    
    def test_header_skip(self):
        result = list(self.transform.process("order_id,product_id,category,price,quantity,order_date"))
        self.assertEqual(len(result), 0)
    
    def test_revenue_calculation(self):
        csv_row = "ORD001,PROD001,Electronics,100.00,2,2024-01-01"
        result = list(self.transform.process(csv_row))
        self.assertEqual(result[0]['revenue'], 200.0)

if __name__ == '__main__':
    unittest.main()
```

### Data Validation Queries

```sql
-- Check for nulls
SELECT * FROM retail_analytics.sales_cleaned 
WHERE order_id IS NULL OR revenue IS NULL;

-- Validate revenue calculation
SELECT 
  order_id,
  price * quantity as expected_revenue,
  revenue as actual_revenue,
  CASE 
    WHEN price * quantity = revenue THEN 'OK'
    ELSE 'MISMATCH'
  END as validation_status
FROM retail_analytics.sales_cleaned
LIMIT 20;

-- Check data volume
SELECT 
  COUNT(*) as total_records,
  COUNT(DISTINCT order_date) as unique_dates,
  COUNT(DISTINCT category) as unique_categories
FROM retail_analytics.sales_cleaned;
```

## 🔄 Git Workflow

### Feature Branch Workflow

```bash
# Create feature branch
git checkout -b retail-sales-summary-batch

# Make changes to code
# ... edit files ...

# Stage changes
git add .

# Commit changes
git commit -m "feat: add retail sales batch pipeline

- Implement Apache Beam pipeline
- Add BigQuery transformations
- Include deployment documentation"

# Push to remote
git push origin retail-sales-summary-batch

# Create Pull Request on GitHub
```

### Commit Message Convention

Follow conventional commits:
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation
- `test:` - Tests
- `refactor:` - Code refactoring
- `chore:` - Build/tooling

Example:
```bash
git commit -m "feat: add sales aggregation

- Create sales_summary table
- Add category-wise revenue calculations
- Optimize query performance"
```

## 📊 Performance Optimization

### Pipeline Optimization Tips

1. **Batch Size**: Adjust batch size for BigQuery writes
```python
# Add to pipeline options
pipeline_options.view_as(WriteToBigQueryOptions).method = 'STREAMING_INSERTS'
```

2. **Worker Configuration**
```bash
--num_workers=2 \
--max_num_workers=10 \
--machine_type=n1-standard-2 \
--worker_disk_size=100
```

3. **Data Partitioning**
```python
# Add partitioning to BigQuery write
write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
partition_fields=['order_date']
```

## 🐳 Docker Development

### Build Docker Image

```bash
docker build -t retail-analytics:latest .

# Build with tag
docker tag retail-analytics:latest \
  gcr.io/YOUR_PROJECT_ID/retail-analytics:latest
```

### Local Docker Testing

```bash
docker run -it \
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/key.json \
  -v ~/.config/gcloud:/tmp \
  retail-analytics:latest \
  python pipeline/retail_sales.py \
  --runner=DirectRunner \
  --input=/data/sample_data.csv \
  --output=local_output
```

## 🔍 Debugging

### Enable Debug Logging

```bash
# Add logging to pipeline
import logging
logging.getLogger().setLevel(logging.DEBUG)

# Run with debug output
python pipeline/retail_sales.py \
  --runner=DirectRunner \
  --input=data/sample_data.csv \
  --output=debug_output \
  --no_auth_local_fail=True
```

### Using print() for Debugging

```python
class TransformData(beam.DoFn):
    def process(self, element):
        print(f"Processing: {element}")  # For local debugging only
        # ... rest of code
```

## 📚 Resources

- [Apache Beam Documentation](https://beam.apache.org/documentation/)
- [Google Dataflow Documentation](https://cloud.google.com/dataflow/docs)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [GCP Python Client Library](https://cloud.google.com/python/docs)

## 🤝 Contributing

1. Create feature branch from `main`
2. Make changes and test thoroughly
3. Update documentation if needed
4. Commit with meaningful messages
5. Push branch and create Pull Request
6. Address review comments
7. Merge after approval

## 📝 Adding New Features

### Example: Adding Data Validation

1. **Create new DoFn in pipeline**
```python
class ValidateData(beam.DoFn):
    def process(self, element):
        try:
            # Validation logic
            yield element
        except Exception as e:
            # Log invalid records
            pass
```

2. **Add to pipeline**
```python
(
    p
    | "Read" >> beam.io.ReadFromText(args.input)
    | "Validate" >> beam.ParDo(ValidateData())
    | "Transform" >> beam.ParDo(TransformData())
    | "Write" >> beam.io.WriteToBigQuery(...)
)
```

3. **Test the change**
4. **Update documentation**
5. **Commit and push**

## 🐛 Known Issues & Limitations

- Local DirectRunner doesn't support all Dataflow features
- BigQuery write requires valid GCP credentials
- Large files may require increased Dataflow worker resources

## 📞 Support

For issues or questions:
1. Check existing documentation
2. Review error logs
3. Create GitHub issue with details
4. Contact development team

