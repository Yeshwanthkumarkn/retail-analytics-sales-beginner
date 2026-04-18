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
from datetime import datetime
import argparse
import logging
from typing import Iterator, Dict, Any

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
    
    # Log pipeline configuration
    logger.info(f"Pipeline Configuration:")
    logger.info(f"  Input: {args.input}")
    logger.info(f"  Output: {args.output}")
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
                | "Write to BQ" >> beam.io.WriteToBigQuery(
                    args.output,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    # Use streaming inserts for better performance on smaller batches
                    # For large batches, consider batch loads via load jobs
                )
            )
            
        logger.info("Pipeline execution completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    # Execute pipeline
    run()