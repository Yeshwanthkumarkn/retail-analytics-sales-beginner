#!/bin/bash

PROJECT_ID=YOUR_PROJECT_ID
BUCKET=YOUR_BUCKET

python pipeline/batch_pipeline.py \
  --project=$PROJECT_ID \
  --runner=DataflowRunner \
  --region=asia-south1 \
  --temp_location=gs://$BUCKET/temp \
  --staging_location=gs://$BUCKET/staging