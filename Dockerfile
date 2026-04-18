# Dockerfile for Retail Sales Batch Pipeline
# Base image includes Apache Beam SDK for Python
FROM apache/beam_python3.10_sdk:2.54.0

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install -r requirements.txt

# Copy pipeline code
COPY pipeline/ ./pipeline/

# Set the entrypoint - this will be overridden by Dataflow when submitting jobs
ENTRYPOINT ["python", "pipeline/retail_sales.py"]

# Default CMD can be overridden with pipeline arguments
# Example: docker run image --input=gs://bucket/data.csv --output=project:dataset.table
CMD []