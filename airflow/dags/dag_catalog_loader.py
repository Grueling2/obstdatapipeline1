from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="dag_catalog_loader",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # manual trigger only
    catchup=False,
    default_args=default_args,
    tags=["catalog", "manual"],
):

    run_loader = BashOperator(
        task_id="run_catalog_loader",
        bash_command="docker exec bookstore_producer python3 /app/catalog_loader.py"
    )
