FROM apache/beam_python3.10_sdk:2.54.0

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY pipeline/ ./pipeline/

ENTRYPOINT ["python", "pipeline/retail_sales.py"]