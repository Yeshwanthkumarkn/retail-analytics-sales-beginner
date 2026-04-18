#!/bin/bash

PROJECT_ID=$1
BUCKET_NAME=$2

gcloud config set project $PROJECT_ID

gcloud services enable \
  dataflow.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com

gsutil mb -l asia-south1 gs://$BUCKET_NAME

bq mk retail_analytics