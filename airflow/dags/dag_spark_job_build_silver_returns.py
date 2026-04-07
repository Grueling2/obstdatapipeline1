from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "will",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dag_spark_job_build_silver_returns",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
) as dag:

    run_silver_returns = BashOperator(
        task_id="run_silver_returns",
        bash_command="docker exec spark spark-submit /opt/spark/jobs/build_silver_returns_from_bronze_delta.py"
    )
