import os
import mysql.connector
import pandas as pd
from minio import Minio

TMP_DIR = "/app/tmp_exports"
BOOKS_CSV = f"{TMP_DIR}/books.csv"
BOOKS_HISTORY_CSV = f"{TMP_DIR}/books_history.csv"

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("MARIADB_HOST"),
        user=os.getenv("MARIADB_USER"),
        password=os.getenv("MARIADB_PASSWORD"),
        database=os.getenv("MARIADB_DATABASE"),
        port=int(os.getenv("MARIADB_PORT", 3306))
    )

def export_table(table_name, output_path):
    os.makedirs(TMP_DIR, exist_ok=True)
    conn = get_db_connection()
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    df.to_csv(output_path, index=False)
    conn.close()
    print(f"Exported {table_name} → {output_path}")

def upload_to_minio(local_path, object_name):
    client = Minio(
        os.getenv("MINIO_ENDPOINT_INTERNAL"),      # e.g. "minio:9000"
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )
    client.fput_object("lakehouse", object_name, local_path)
    print(f"Uploaded {local_path} → s3://lakehouse/{object_name}")

if __name__ == "__main__":
    export_table("books", BOOKS_CSV)
    export_table("books_history", BOOKS_HISTORY_CSV)

    upload_to_minio(BOOKS_CSV, "ref/books.csv")
    upload_to_minio(BOOKS_HISTORY_CSV, "ref/books_history.csv")

    print("MariaDB export → MinIO complete.")
