"""
Retail Sales Batch Pipeline - Apache Beam Implementation

This module implements a data processing pipeline using Apache Beam to:
1. Read sales transaction data from CSV files stored in Google Cloud Storage (GCS)
2. Transform and validate the data (calculate revenue, add load timestamp)
3. Write processed data to BigQuery for analytics

The pipeline is designed to run on Google Dataflow for scalable batch processing.

Example Usage:
    DirectRunner (local):
        python pipeline/retail_sales.py \\
            --runner=DirectRunner \\
            --input=gs://bucket/raw/sales.csv \\
            --output=project:dataset.sales_cleaned

    DataflowRunner (GCP):
        python pipeline/retail_sales.py \\
            --runner=DataflowRunner \\
            --project=my-project \\
            --region=asia-south1 \\
            --temp_location=gs://bucket/temp \\
            --staging_location=gs://bucket/staging \\
            --input=gs://bucket/raw/sales.csv \\
            --output=my-project:dataset.sales_cleaned
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.gcsio import GcsIO
from datetime import datetime
import argparse
import logging
import json
import uuid
from typing import Iterator, Dict, Any
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransformData(beam.DoFn):
    """
    Apache Beam DoFn for transforming raw sales transaction CSV data.
    
    This class processes individual CSV rows and performs the following operations:
    - Validates CSV structure (6 required fields)
    - Converts data types (price to float, quantity to int)
    - Calculates revenue (price * quantity)
    - Adds load timestamp
    - Handles errors gracefully with logging
    
    Expected input CSV format:
        order_id,product_id,category,price,quantity,order_date
        
    Output record schema:
        {
            'order_id': str,
            'product_id': str,
            'category': str,
            'price': float,
            'quantity': int,
            'order_date': str,
            'revenue': float,
            'load_date': str (YYYY-MM-DD)
        }
    """
    
    # Class-level counters for monitoring
    HEADERS_SKIPPED = beam.metrics.Metrics.counter('main', 'headers_skipped')
    RECORDS_PROCESSED = beam.metrics.Metrics.counter('main', 'records_processed')
    RECORDS_FAILED = beam.metrics.Metrics.counter('main', 'records_failed')

    def process(self, element: str) -> Iterator[Dict[str, Any]]:
        """
        Process a single CSV row and transform it into a structured record.
        
        Args:
            element (str): A comma-separated CSV row
            
        Yields:
            Dict[str, Any]: Transformed record with calculated fields, or None on error
            
        Raises:
            ValueError: If numeric fields cannot be converted
            IndexError: If CSV row has insufficient fields (logged, not raised)
        """
        try:
            # Split CSV row by comma delimiter
            fields = element.split(',')
            
            # Skip header row
            if fields[0].strip().lower() == 'order_id':
                logger.debug("Skipping header row")
                self.HEADERS_SKIPPED.inc()
                return
            
            # Validate minimum required fields (6 columns)
            if len(fields) < 6:
                error_msg = f"Invalid CSV row: expected 6 fields, got {len(fields)}. Row: {element[:100]}"
                logger.warning(error_msg)
                self.RECORDS_FAILED.inc()
                return
            
            # Extract and validate order_id
            order_id = fields[0].strip()
            if not order_id:
                logger.warning("Empty order_id encountered, skipping record")
                self.RECORDS_FAILED.inc()
                return
            
            # Extract product_id and category
            product_id = fields[1].strip()
            category = fields[2].strip()
            
            # Convert and validate price (field 3)
            try:
                price = float(fields[3].strip())
                if price < 0:
                    logger.warning(f"Negative price for order {order_id}: {price}, skipping")
                    self.RECORDS_FAILED.inc()
                    return
            except ValueError as e:
                logger.warning(f"Invalid price for order {order_id}: {fields[3]}. Error: {str(e)}")
                self.RECORDS_FAILED.inc()
                return
            
            # Convert and validate quantity (field 4)
            try:
                quantity = int(fields[4].strip())
                if quantity < 0:
                    logger.warning(f"Negative quantity for order {order_id}: {quantity}, skipping")
                    self.RECORDS_FAILED.inc()
                    return
            except ValueError as e:
                logger.warning(f"Invalid quantity for order {order_id}: {fields[4]}. Error: {str(e)}")
                self.RECORDS_FAILED.inc()
                return
            
            # Extract order date
            order_date = fields[5].strip()
            if not order_date:
                logger.warning(f"Empty order_date for order {order_id}, skipping")
                self.RECORDS_FAILED.inc()
                return
            
            # Calculate revenue
            revenue = price * quantity
            
            # Get current UTC date for load tracking
            load_date = datetime.utcnow().strftime('%Y-%m-%d')
            
            # Create output record
            output_record = {
                "order_id": order_id,
                "product_id": product_id,
                "category": category,
                "price": price,
                "quantity": quantity,
                "order_date": order_date,
                "revenue": revenue,
                "load_date": load_date
            }
            
            self.RECORDS_PROCESSED.inc()
            logger.debug(f"Successfully processed order {order_id} with revenue {revenue}")
            yield output_record
            
        except Exception as e:
            # Catch-all for unexpected errors
            logger.error(f"Unexpected error processing record: {element[:100]}. Error: {str(e)}", exc_info=True)
            self.RECORDS_FAILED.inc()
            return


class WriteToBigQueryCustom(beam.DoFn):
    """
    Custom BigQuery writer using google-cloud-bigquery client library.
    
    This bypasses the problematic WriteToBigQuery transform that has compatibility
    issues with Apache Beam 2.54.0. Uses BigQuery client library directly to:
    - Create table if it doesn't exist
    - Insert rows using BigQuery's Python client
    - Handle errors gracefully
    
    This approach avoids the isinstance(table, TableReference) error that occurs
    in beam.io.WriteToBigQuery's parse_table_reference() function.
    """
    
    ROWS_INSERTED = beam.metrics.Metrics.counter('main', 'rows_inserted')
    INSERT_ERRORS = beam.metrics.Metrics.counter('main', 'insert_errors')
    
    def __init__(self, table_id: str, batch_size: int = 1000):
        """
        Initialize the custom BigQuery writer.
        
        Args:
            table_id (str): BigQuery table ID in format PROJECT:DATASET.TABLE
            batch_size (int): Number of rows to batch before inserting
        """
        self.table_id = table_id
        self.batch_size = batch_size
        self.rows_buffer = []
        
    def start_bundle(self):
        """Initialize BigQuery client at bundle start."""
        self.client = bigquery.Client()
        self.rows_buffer = []
        
    def process(self, element: Dict[str, Any]):
        """
        Process records and buffer them for batch insertion.
        
        Args:
            element (Dict): Record to insert into BigQuery
            
        Yields:
            The element if successfully buffered
        """
        try:
            self.rows_buffer.append(element)
            
            # Flush buffer when it reaches batch size
            if len(self.rows_buffer) >= self.batch_size:
                self._flush_rows()
            
            yield element
            
        except Exception as e:
            logger.error(f"Error processing record for BigQuery: {str(e)}", exc_info=True)
            self.INSERT_ERRORS.inc()
    
    def finish_bundle(self):
        """Flush any remaining buffered rows at bundle end."""
        if self.rows_buffer:
            self._flush_rows()
    
    def _flush_rows(self):
        """
        Insert buffered rows into BigQuery using streaming insert.
        
        Uses insert_rows_json() which is more reliable than insert_rows()
        and doesn't trigger the parse_table_reference issue.
        """
        if not self.rows_buffer:
            return
        
        try:
            table = self.client.get_table(self.table_id)
            
            # Convert records to JSON-serializable format
            rows_json = [json.loads(json.dumps(row, default=str)) for row in self.rows_buffer]
            
            # Insert rows
            errors = self.client.insert_rows_json(table, rows_json)
            
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                self.INSERT_ERRORS.inc(len(errors))
            else:
                self.ROWS_INSERTED.inc(len(self.rows_buffer))
                logger.info(f"Successfully inserted {len(self.rows_buffer)} rows to {self.table_id}")
            
            # Clear buffer after flush
            self.rows_buffer = []
            
        except NotFound as e:
            logger.error(f"BigQuery table not found: {self.table_id}. {str(e)}")
            logger.info("Creating table...")
            self._create_table()
            # Retry insert after table creation
            if self.rows_buffer:
                self._insert_rows_safe()
        except Exception as e:
            logger.error(f"Error flushing rows to BigQuery: {str(e)}", exc_info=True)
            self.INSERT_ERRORS.inc(len(self.rows_buffer))
    
    def _insert_rows_safe(self):
        """Safely insert rows without creating table."""
        if not self.rows_buffer:
            return
        
        try:
            table = self.client.get_table(self.table_id)
            rows_json = [json.loads(json.dumps(row, default=str)) for row in self.rows_buffer]
            errors = self.client.insert_rows_json(table, rows_json)
            
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                self.INSERT_ERRORS.inc(len(errors))
            else:
                self.ROWS_INSERTED.inc(len(self.rows_buffer))
            
            self.rows_buffer = []
        except Exception as e:
            logger.error(f"Error in safe insert: {str(e)}", exc_info=True)
            self.INSERT_ERRORS.inc(len(self.rows_buffer))
    
    def _create_table(self):
        """Create BigQuery table with proper schema if it doesn't exist."""
        try:
            # Parse table reference
            if ':' in self.table_id:
                project_id, dataset_table = self.table_id.split(':', 1)
            else:
                project_id = self.client.project
                dataset_table = self.table_id
            
            dataset_id, table_name = dataset_table.split('.', 1)
            dataset_id_full = f"{project_id}.{dataset_id}"
            
            # Define schema
            schema = [
                bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("product_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("price", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("quantity", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("order_date", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("revenue", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("load_date", "STRING", mode="NULLABLE"),
            ]
            
            table = bigquery.Table(self.table_id, schema=schema)
            table = self.client.create_table(table)
            logger.info(f"Created table {self.table_id}")
            
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}", exc_info=True)


def run(argv=None) -> None:
    """
    Main pipeline execution function.
    
    Initializes and runs the Apache Beam pipeline with the following stages:
    1. Read: Load CSV data from GCS or local filesystem
    2. Transform: Apply business logic and data validation
    3. Write: Store processed data in BigQuery
    
    Args:
        argv (List[str], optional): Command-line arguments. If None, uses sys.argv
        
    Note:
        Requires GCP credentials for BigQuery output.
        Set GOOGLE_APPLICATION_CREDENTIALS environment variable to path of service account JSON key.
        
    Raises:
        SystemExit: On argument parsing error or pipeline execution failure
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Retail Sales Batch Pipeline - Transform CSV data to BigQuery',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Local testing
  python retail_sales.py --runner=DirectRunner --input=data/sample.csv --output=project:dataset.table
  
  # GCP Dataflow
  python retail_sales.py --runner=DataflowRunner --project=my-project --input=gs://bucket/raw.csv \\
    --output=my-project:dataset.table --region=asia-south1 \\
    --temp_location=gs://bucket/temp --staging_location=gs://bucket/staging
        """
    )
    
    # Add required arguments
    parser.add_argument(
        '--input',
        required=True,
        help='Input CSV file path (local or GCS path: gs://bucket/path/file.csv)'
    )
    parser.add_argument(
        '--output',
        required=True,
        help='BigQuery output table in format PROJECT:DATASET.TABLE'
    )
    
    # Parse arguments - remaining unknown args passed to beam
    args, pipeline_args = parser.parse_known_args(argv)
    
    # Validate BigQuery table reference format
    # Expected format: PROJECT:DATASET.TABLE
    table_ref = args.output.strip()
    
    if ':' not in table_ref or '.' not in table_ref.split(':', 1)[1]:
        raise ValueError(
            f"Invalid table reference format: {args.output}. "
            f"Expected format: PROJECT:DATASET.TABLE (e.g., my-project:my_dataset.my_table)"
        )
    
    logger.info(f"Pipeline Configuration:")
    logger.info(f"  Input: {args.input}")
    logger.info(f"  Output Table Reference: {table_ref}")
    logger.info(f"  Using Custom BigQuery Writer (bypasses WriteToBigQuery compatibility issue)")
    logger.info(f"  Pipeline args: {' '.join(pipeline_args)}")
    
    # Initialize Beam pipeline options
    options = PipelineOptions(pipeline_args)
    
    try:
        # Create and run pipeline with context manager (ensures proper cleanup)
        with beam.Pipeline(options=options) as p:
            
            logger.info("Starting pipeline execution...")
            
            # Define the pipeline stages
            (
                p
                | "Read CSV" >> beam.io.ReadFromText(args.input)
                | "Transform" >> beam.ParDo(TransformData())
                | "Write to BQ" >> beam.ParDo(WriteToBigQueryCustom(table_id=table_ref))
            )
            
        logger.info("Pipeline execution completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    # Execute pipeline
    run()