from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

local_tz = pendulum.timezone("America/Chicago")

default_args = {
    "owner": "will",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_run_bookstore_simulation_day_cycle",
    default_args=default_args,
    description="Simulated store day: start producer, run 30 minutes, stop producer, update popularity, increment day index, export metadata",
    schedule_interval="0 * * * *",
    start_date=local_tz.datetime(2026, 3, 21, 10, 0, 0),
    catchup=False,
    max_active_runs=1,
) as dag:

    start_producer = BashOperator(
        task_id="start_producer",
        bash_command="docker exec -d bookstore_producer python3 /app/bookstore_producer.py"
    )

    run_store_day = BashOperator(
        task_id="run_store_day",
        bash_command="sleep 1800"
    )

    stop_producer = BashOperator(
        task_id="stop_producer",
        bash_command="docker exec bookstore_producer pkill -f bookstore_producer.py || true"
    )

    popularity_update = BashOperator(
        task_id="popularity_update",
        bash_command="docker exec bookstore_producer python3 /app/popularity_updater.py"
    )

    increment_day = BashOperator(
        task_id="increment_day_index",
        bash_command="docker exec bookstore_producer python3 /app/increment_day_index.py"
    )

    export_mariadb_books_csvs_to_minio = BashOperator(
        task_id="export_mariadb_books_csvs_to_minio",
        bash_command="docker exec bookstore_producer python3 /app/mariadb_books_and_books_history_tables_to_csv.py"
    )

    start_producer >> run_store_day >> stop_producer >> popularity_update >> increment_day >> export_mariadb_books_csvs_to_minio