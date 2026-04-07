from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer

BRONZE_PATH = "s3a://lakehouse/bronze/raw_events"
SILVER_RETURNS_PATH = "s3a://lakehouse/silver/returns_items"
SILVER_RETURNS_CHECKPOINT = "s3a://lakehouse/checkpoints/silver_returns_items"

#Spark Session (Delta + S3A + Kafka configs come from spark-defaults.conf)

spark = (
    SparkSession.builder
    .appName("build_silver_returns_from_bronze_delta")
    .getOrCreate()
)

#Read Bronze as a Stream

bronze_stream = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)

#Filter events to just Returns

returns_stream = bronze_stream.filter(col("event_type") == "return")

#Explode items and normalize schema

items_stream = returns_stream.select(
    col("store_id"),
    col("customer_id"),
    col("business_date"),
    col("event_ts"),
    explode_outer(col("items")).alias("item")
)

silver_returns_stream = items_stream.select(
    col("store_id"),
    col("customer_id"),
    col("business_date"),
    col("event_ts"),
    col("item.isbn").alias("isbn"),
    col("item.title").alias("title"),
    col("item.genre").alias("genre"),
    col("item.quantity").cast("int").alias("quantity"),
    col("item.price").cast("double").alias("unit_price"),
    (col("item.quantity") * col("item.price")).alias("line_total")
)


#Silver Returns, append-only WRITE, partitioned by business_date

query = (
    silver_returns_stream.writeStream
    .format("delta")
    .outputMode("append")
    .partitionBy("business_date")
    .option("checkpointLocation", SILVER_RETURNS_CHECKPOINT)
    .option("path", SILVER_RETURNS_PATH)
    .trigger(processingTime="180 seconds")
    .start()
)

query.awaitTermination()
