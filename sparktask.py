

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count, from_json, col, to_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType
import time
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

def create_spark_session():
    """Create and configure Spark session for streaming"""
    return SparkSession.builder \
        .appName("EmojiStreamProcessor") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .getOrCreate()

def create_kafka_read_stream(spark):
    """Create input stream from Kafka"""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "emoji_topic") \
        .option("startingOffsets", "latest") \
        .load()

def process_emoji_stream():
    """Main function to process emoji streaming data"""
    spark = create_spark_session()
    
    # Define schema for JSON data
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("emoji_type", StringType(), True),
        StructField("timestamp", LongType(), True)  # Changed to LongType for Unix timestamp
    ])
    
    # Read from Kafka
    kafka_df = create_kafka_read_stream(spark)
    
    # Parse JSON data and convert timestamp
    value_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Convert Unix timestamp to timestamp type
    value_df = value_df.withColumn(
        "event_timestamp", 
        to_timestamp(col("timestamp") / 1000)  # Convert milliseconds to seconds
    )
    
    # Process data in 2-second windows
    windowed_counts = value_df \
        .withWatermark("event_timestamp", "2 seconds") \
        .groupBy(
            window("event_timestamp", "2 seconds"),
            "emoji_type"
        ) \
        .agg(count("*").alias("emoji_count"))
    
    # Apply scaling logic
    scaled_counts = windowed_counts \
        .withColumn(
            "scaled_count",
            expr("CASE WHEN emoji_count >= 1000 THEN 1 ELSE emoji_count END")
        )
    
    # Prepare output format
    output_df = scaled_counts.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "emoji_type",
        "emoji_count",
        "scaled_count"
    )
    
    # Write stream to console for debugging
    console_query = output_df \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .trigger(processingTime="2 seconds") \
        .start()
    
    # Write the processed data to Kafka
    kafka_query = output_df \
        .selectExpr("to_json(struct(*)) as value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "processed_emojis") \
        .option("checkpointLocation", "/tmp/checkpoint/") \
        .outputMode("update") \
        .trigger(processingTime="2 seconds") \
        .start()
    
    try:
        kafka_query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping stream processing...")
        console_query.stop()
        kafka_query.stop()
        spark.stop()

if __name__ == "__main__":
    process_emoji_stream()