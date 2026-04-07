import os
import mysql.connector
import polars as pl
from datetime import date, timedelta

#Establish simulated day from day_index.txt


DAY_INDEX_PATH = "/app/state/day_index.txt"
BASE_DATE = date(2026, 3, 1)

with open(DAY_INDEX_PATH, "r") as f:
    day_index = int(f.read().strip())

SIM_DAY = BASE_DATE + timedelta(days=day_index)
SIM_DAY_STR = SIM_DAY.strftime("%Y-%m-%d")

print(f"Running popularity update for simulated day: {SIM_DAY_STR}")

#Connect to MariaDB

db = mysql.connector.connect(
    host=os.getenv("MARIADB_HOST"),
    port=int(os.getenv("MARIADB_PORT", "3306")),
    user=os.getenv("MARIADB_USER"),
    password=os.getenv("MARIADB_PASSWORD"),
    database=os.getenv("MARIADB_DATABASE")
)
cursor = db.cursor()

#Read Silver sales data for the simulated day

DELTA_SILVER_PATH = "s3://lakehouse/silver/sales_items"

storage_options = {
    "region": os.getenv("AWS_REGION"),
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "endpoint_url": os.getenv("AWS_ENDPOINT_URL_S3"),
    "allow_http": "true",
}

try:
    silver = pl.read_delta(DELTA_SILVER_PATH, storage_options=storage_options)
except Exception as e:
    print(f"No Delta table found yet or unreadable: {e}")
    silver = pl.DataFrame()

#If no sales, apply decay to all books

if silver.is_empty():
    print("No sales data found — applying decay only.")

    # DECAY FIRST
    decay_sql = """
        UPDATE books
        SET
            base_popularity = base_popularity * 0.995,
            real_world_popularity = real_world_popularity * 0.97,
            last_updated = %s
    """
    cursor.execute(decay_sql, (SIM_DAY_STR,))

    # SNAPSHOT AFTER UPDATES
    snapshot_sql = """
        INSERT INTO books_history
        (isbn, title, base_popularity, real_world_popularity, sales_popularity, snapshot_date)
        SELECT isbn, title, base_popularity, real_world_popularity, sales_popularity, %s
        FROM books
    """
    cursor.execute(snapshot_sql, (SIM_DAY_STR,))

    db.commit()
    cursor.close()
    db.close()
    print("Decay applied. Popularity update complete.")
    exit(0)

#Track units sold

sales = (
    silver
    .filter(pl.col("business_date") == pl.lit(SIM_DAY).cast(pl.Date))
    .group_by("isbn")
    .agg(pl.col("quantity").sum().alias("units_sold"))
)

print(f"Found {len(sales)} ISBNs sold on {SIM_DAY_STR}")

#Apply normal decay to all books

decay_sql = """
    UPDATE books
    SET
        base_popularity = base_popularity * 0.995,
        real_world_popularity = real_world_popularity * 0.97,
        last_updated = %s
"""

cursor.execute(decay_sql, (SIM_DAY_STR,))

#Update popularity for books that sold

update_sales_sql = """
    UPDATE books
    SET 
        sales_popularity = 0.8 * sales_popularity + 0.2 * %s
    WHERE isbn = %s
"""

for row in sales.iter_rows(named=True):
    isbn = row["isbn"]
    units = row["units_sold"]
    cursor.execute(update_sales_sql, (units, isbn))

#Save snapshot into MariaDB

snapshot_sql = """
    INSERT INTO books_history
    (isbn, title, base_popularity, real_world_popularity, sales_popularity, snapshot_date)
    SELECT isbn, title, base_popularity, real_world_popularity, sales_popularity, %s
    FROM books
"""

cursor.execute(snapshot_sql, (SIM_DAY_STR,))

db.commit()
cursor.close()
db.close()

print("Popularity update complete.")
