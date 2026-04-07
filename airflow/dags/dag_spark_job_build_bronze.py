from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

local_tz = pendulum.timezone("America/Chicago")

default_args = {
    "owner": "will",
    "depends_on_past": False,
    "retries": 0,  # streaming jobs shouldn't retry
}

with DAG(
    dag_id="dag_spark_job_build_bronze",
    default_args=default_args,
    description="Start the Bronze Structured Streaming job (runs continuously)",
    schedule_interval=None,   # manual trigger only
    start_date=local_tz.datetime(2026, 3, 19, 10, 0, 0),
    catchup=False,
    max_active_runs=1,
) as dag:

    run_bronze_streaming = BashOperator(
        task_id="run_bronze_streaming",
        bash_command="""
        echo "Starting Bronze Structured Streaming job..."
        docker exec spark spark-submit \
          /opt/spark/jobs/build_bronze_raw_events_kafka_to_delta.py
        """
    )