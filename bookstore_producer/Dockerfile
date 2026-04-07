FROM python:3.11-slim

WORKDIR /app

COPY bookstore_producer.py /app/bookstore_producer.py
COPY increment_day_index.py /app/increment_day_index.py
COPY popularity_updater.py /app/popularity_updater.py
COPY catalog_loader.py /app/catalog_loader.py
COPY mariadb_books_and_books_history_tables_to_csv.py /app/mariadb_books_and_books_history_tables_to_csv.py
COPY requirements.txt /app/requirements.txt

RUN apt-get update && apt-get install -y \
    procps \
    libssl-dev \
    liblzma-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir polars deltalake


RUN mkdir -p /app/state && chmod -R 777 /app/state


ENV AWS_EC2_METADATA_DISABLED=true
ENV AWS_REGION=us-east-1
ENV AWS_ENDPOINT_URL_S3=http://minio:9000
ENV STATE_FILE=/app/state/day_index.txt
ENV KAFKA_BOOTSTRAP_SERVERS=redpanda:9092

CMD ["sleep", "infinity"]
