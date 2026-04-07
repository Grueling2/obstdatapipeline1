import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
AWS_BUCKET = os.getenv("AWS_BUCKET")

# Delta tables inside lakehouse/
DELTA_TABLES = [
    "bronze/raw_events",
    "silver/sales_items",
    "silver/returns_items",
]

# Ref files inside lakehouse/ref/
REF_FILES = [
    "ref/books.csv",
    "ref/books_history.csv",
]

TMP_DIR = "/tmp/lakehouse_sync"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_sync_minio_to_aws_s3",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
) as dag:

    clean_tmp = BashOperator(
        task_id="clean_tmp",
        bash_command=f"rm -rf {TMP_DIR} && mkdir -p {TMP_DIR}"
    )

    sync_tasks = []

    for table in DELTA_TABLES:
        table_name = table.replace("/", "_")

        task = BashOperator(
            task_id=f"sync_{table_name}",
            bash_command=f"""
echo "Syncing Delta table: {table}"

LOCAL_DIR={TMP_DIR}/{table}
MINIO_URI=s3://{MINIO_BUCKET}/{table}
AWS_URI=s3://{AWS_BUCKET}/lakehouse/{table}

mkdir -p $LOCAL_DIR

# 1. Copy Parquet files from MinIO (uses MinIO env vars automatically)
aws --endpoint-url={MINIO_ENDPOINT} s3 sync $MINIO_URI $LOCAL_DIR --exclude "*" --include "*.parquet"

# 2. Find latest checkpoint
CHECKPOINT=$(aws --endpoint-url={MINIO_ENDPOINT} s3 ls $MINIO_URI/_delta_log/ | grep checkpoint | sort | tail -n 1 | awk '{{print $4}}')

if [ -z "$CHECKPOINT" ]; then
    echo "ERROR: No checkpoint found for {table}"
    exit 1
fi

echo "Latest checkpoint: $CHECKPOINT"

# 3. Copy checkpoint file
mkdir -p $LOCAL_DIR/_delta_log
aws --endpoint-url={MINIO_ENDPOINT} s3 cp $MINIO_URI/_delta_log/$CHECKPOINT $LOCAL_DIR/_delta_log/

# Extract checkpoint version number
VERSION=$(echo $CHECKPOINT | sed 's/\\.checkpoint\\.parquet//')

# 4. Copy JSON logs up to checkpoint version
aws --endpoint-url={MINIO_ENDPOINT} s3 sync $MINIO_URI/_delta_log/ $LOCAL_DIR/_delta_log/ \
    --exclude "*" --include "*.json"

# Remove JSON logs newer than checkpoint
find $LOCAL_DIR/_delta_log -name "*.json" | while read f; do
    FILE_VERSION=$(basename "$f" .json)
    if [ "$FILE_VERSION" -gt "$VERSION" ]; then
        rm "$f"
    fi
done

# 5. Upload snapshot to AWS S3 using REAL AWS credentials
AWS_ACCESS_KEY_ID=$REAL_AWS_ACCESS_KEY_ID \
AWS_SECRET_ACCESS_KEY=$REAL_AWS_SECRET_ACCESS_KEY \
AWS_DEFAULT_REGION=$REAL_AWS_REGION \
aws s3 sync $LOCAL_DIR $AWS_URI
"""
        )

        sync_tasks.append(task)

    sync_ref = BashOperator(
        task_id="sync_ref_files",
        bash_command=f"""
for FILE in {' '.join(REF_FILES)}; do

    # 1. Download from MinIO using MinIO credentials (from .env)
    aws --endpoint-url={MINIO_ENDPOINT} s3 cp s3://{MINIO_BUCKET}/$FILE /tmp/$FILE

    # 2. Upload to AWS using REAL AWS credentials (from .env)
    AWS_ACCESS_KEY_ID=$REAL_AWS_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY=$REAL_AWS_SECRET_ACCESS_KEY \
    AWS_DEFAULT_REGION=$REAL_AWS_REGION \
    aws s3 cp /tmp/$FILE s3://{AWS_BUCKET}/lakehouse/$FILE

done
"""
    )

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {TMP_DIR}"
    )

    clean_tmp >> sync_tasks >> sync_ref >> cleanup
