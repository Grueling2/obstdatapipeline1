from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp, to_date
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

BRONZE_PATH = "s3a://lakehouse/bronze/raw_events"
CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/bronze_raw_events"

#Spark Session (Delta + S3A + Kafka configs come from spark-defaults.conf)

spark = (
    SparkSession.builder
    .appName("KafkaToDeltaRawEventsStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

#Schema for Kafka JSON payload

schema = StructType([
    StructField("event_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("store_id", StringType()),
    StructField("store_name", StringType()),
    StructField("customer_id", StringType()),
    StructField(
        "items",
        ArrayType(
            StructType([
                StructField("isbn", StringType()),
                StructField("title", StringType()),
                StructField("genre", StringType()),
                StructField("price", DoubleType()),
                StructField("quantity", IntegerType())
            ])
        )
    ),
    StructField("total_amount", DoubleType()),
    StructField("payment_method", StringType()),
    StructField("loyalty_member", BooleanType())
])

#Read Kafka as a stream

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "bookstore_events")
    .option("startingOffsets", "earliest")
    .option("kafka.consumer.group.id", "bronze-consumer")
    .option("failOnDataLoss", "false")
    .load()
)

#Parse and enrich
parsed_df = (
    kafka_df
    .select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("offset").alias("kafka_offset")
    )
    .select("data.*", "kafka_offset")
    .withColumn("ingest_ts", current_timestamp())
    .withColumn("event_ts", to_timestamp(col("timestamp")))
    .withColumn("business_date", to_date(col("event_ts")))
)

#Continuous Bronze streaming WRITE
print("Starting continuous Bronze streaming...")

query = (
    parsed_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .partitionBy("business_date")
    .start(BRONZE_PATH)
)

query.awaitTermination()
