
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import window, count, from_json, col, to_timestamp, expr
# from pyspark.sql.types import StructType, StructField, StringType, LongType
# import time
# import os
# from cluster_publisher import cluster_publish_aggregated_data  # Import the cluster publisher

# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

# def create_spark_session():
#     """Create and configure Spark session for micro-batch streaming"""
#     return SparkSession.builder \
#         .appName("EmojiStreamProcessor") \
#         .config("spark.streaming.stopGracefullyOnShutdown", "true") \
#         .config("spark.sql.shuffle.partitions", "2") \
#         .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
#         .config("spark.streaming.kafka.minBatchesToRetain", "2") \
#         .config("spark.streaming.backpressure.enabled", "true") \
#         .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
#         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
#         .getOrCreate()

# def create_kafka_read_stream(spark):
#     """Create input stream from Kafka with micro-batch settings"""
#     return spark \
#         .readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "localhost:9092") \
#         .option("subscribe", "emoji_topic") \
#         .option("startingOffsets", "latest") \
#         .option("maxOffsetsPerTrigger", 5000) \
#         .option("fetchOffset.numRetries", "3") \
#         .load()

# def process_emoji_stream():
#     """Process emoji streaming data in micro-batches"""
#     spark = create_spark_session()
    
#     # Define schema for JSON data
#     schema = StructType([
#         StructField("user_id", StringType(), True),
#         StructField("emoji_type", StringType(), True),
#         StructField("timestamp", LongType(), True)
#     ])
    
#     # Read from Kafka
#     kafka_df = create_kafka_read_stream(spark)
    
#     # Parse JSON data and handle timestamps
#     parsed_df = kafka_df.select(
#         from_json(col("value").cast("string"), schema).alias("data")
#     ).select("data.*")
    
#     # Convert millisecond timestamp to proper timestamp type
#     df_with_timestamp = parsed_df.withColumn(
#         "event_time",
#         to_timestamp(col("timestamp") / 1000)
#     )
    
#     # Process in 2-second windows with micro-batching
#     windowed_counts = df_with_timestamp \
#         .withWatermark("event_time", "2 seconds") \
#         .groupBy(
#             window("event_time", "2 seconds"),
#             "emoji_type"
#         ) \
#         .agg(count("*").alias("emoji_count"))
    
#     # Apply scaling logic (1000+ → 1)
#     scaled_counts = windowed_counts \
#         .withColumn(
#             "scaled_count",
#             expr("CASE WHEN emoji_count >= 1000 THEN 1 ELSE emoji_count END")
#         )
    
#     # Prepare output with window information
#     output_df = scaled_counts.select(
#         col("window.start").alias("window_start"),
#         col("window.end").alias("window_end"),
#         "emoji_type",
#         "emoji_count",
#         "scaled_count"
#     )
    
#     # Write to both console and Kafka with 2-second micro-batches
#     console_query = output_df \
#         .writeStream \
#         .format("console") \
#         .outputMode("update") \
#         .trigger(processingTime="2 seconds") \
#         .option("truncate", False) \
#         .start()
    
#     kafka_query = output_df \
#         .selectExpr("to_json(struct(*)) as value") \
#         .writeStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "localhost:9092") \
#         .option("topic", "processed_emojis") \
#         .option("checkpointLocation", "/tmp/checkpoint/") \
#         .outputMode("update") \
#         .trigger(processingTime="2 seconds") \
#         .start()

#     # Call the cluster publisher function to send the data in threads
#     def process_and_publish_batch(batch_df, batch_id):
#         aggregated_data = batch_df.collect()  # Collect the data
#         # Send the collected aggregated data to Kafka using the cluster publisher
#         cluster_publish_aggregated_data(aggregated_data)
    
#     monitoring_query = output_df \
#         .writeStream \
#         .foreachBatch(process_and_publish_batch) \
#         .trigger(processingTime="2 seconds") \
#         .start()
    
#     try:
#         spark.streams.awaitAnyTermination()
#     except KeyboardInterrupt:
#         print("Stopping stream processing...")
#         console_query.stop()
#         kafka_query.stop()
#         monitoring_query.stop()
#         spark.stop()

# if __name__ == "__main__":
#     process_emoji_stream()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import window, count, from_json, col, to_timestamp, expr
# from pyspark.sql.types import StructType, StructField, StringType, LongType
# import time
# import os
# from main_publisher import publish_data_to_kafka  # Import the main publisher

# # Set JAVA_HOME if required by your system
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

# def create_spark_session():
#     """Create and configure Spark session for micro-batch streaming"""
#     return SparkSession.builder \
#         .appName("EmojiStreamProcessor") \
#         .config("spark.streaming.stopGracefullyOnShutdown", "true") \
#         .config("spark.sql.shuffle.partitions", "2") \
#         .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
#         .config("spark.streaming.kafka.minBatchesToRetain", "2") \
#         .config("spark.streaming.backpressure.enabled", "true") \
#         .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
#         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
#         .getOrCreate()

# def create_kafka_read_stream(spark):
#     """Create input stream from Kafka with micro-batch settings"""
#     return spark \
#         .readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "localhost:9092") \
#         .option("subscribe", "emoji_topic") \
#         .option("startingOffsets", "latest") \
#         .option("maxOffsetsPerTrigger", 5000) \
#         .option("fetchOffset.numRetries", "3") \
#         .load()

# def process_emoji_stream():
#     """Process emoji streaming data in micro-batches"""
#     spark = create_spark_session()
    
#     # Define schema for JSON data
#     schema = StructType([
#         StructField("user_id", StringType(), True),
#         StructField("emoji_type", StringType(), True),
#         StructField("timestamp", LongType(), True)
#     ])
    
#     # Read from Kafka
#     kafka_df = create_kafka_read_stream(spark)
    
#     # Parse JSON data and handle timestamps
#     parsed_df = kafka_df.select(
#         from_json(col("value").cast("string"), schema).alias("data")
#     ).select("data.*")
    
#     # Convert millisecond timestamp to proper timestamp type
#     df_with_timestamp = parsed_df.withColumn(
#         "event_time",
#         to_timestamp(col("timestamp") / 1000)
#     )
    
#     # Process in 2-second windows with micro-batching
#     windowed_counts = df_with_timestamp \
#         .withWatermark("event_time", "2 seconds") \
#         .groupBy(
#             window("event_time", "2 seconds"),
#             "emoji_type"
#         ) \
#         .agg(count("*").alias("emoji_count"))
    
#     # Apply scaling logic (1000+ → 1)
#     scaled_counts = windowed_counts \
#         .withColumn(
#             "scaled_count",
#             expr("CASE WHEN emoji_count >= 1000 THEN 1 ELSE emoji_count END")
#         )
    
#     # Prepare output with window information
#     output_df = scaled_counts.select(
#         col("window.start").alias("window_start"),
#         col("window.end").alias("window_end"),
#         "emoji_type",
#         "emoji_count",
#         "scaled_count"
#     )
    
#     # Write to console for monitoring
#     console_query = output_df \
#         .writeStream \
#         .format("console") \
#         .outputMode("update") \
#         .trigger(processingTime="2 seconds") \
#         .option("truncate", False) \
#         .start()
    
#     # Write to Kafka topic "processed_emojis"
#     kafka_query = output_df \
#         .selectExpr("to_json(struct(*)) as value") \
#         .writeStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "localhost:9092") \
#         .option("topic", "processed_emojis") \
#         .option("checkpointLocation", "/tmp/checkpoint/") \
#         .outputMode("update") \
#         .trigger(processingTime="2 seconds") \
#         .start()

#     # Publish the aggregated data to the main publisher in each batch
#     def process_and_publish_batch(batch_df, batch_id):
#         aggregated_data = batch_df.collect()  # Collect the data from the current batch
#         # Publish each record to Kafka using the main publisher
#         for row in aggregated_data:
#             data = {
#                 "window_start": str(row.window_start),
#                 "window_end": str(row.window_end),
#                 "emoji_type": row.emoji_type,
#                 "emoji_count": row.emoji_count,
#                 "scaled_count": row.scaled_count
#             }
#             publish_data_to_kafka(data)

#     # Stream processing with batch publishing
#     monitoring_query = output_df \
#         .writeStream \
#         .foreachBatch(process_and_publish_batch) \
#         .trigger(processingTime="2 seconds") \
#         .start()
    
#     try:
#         spark.streams.awaitAnyTermination()
#     except KeyboardInterrupt:
#         print("Stopping stream processing...")
#         console_query.stop()
#         kafka_query.stop()
#         monitoring_query.stop()
#         spark.stop()

# if __name__ == "__main__":
#     process_emoji_stream()



from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count, from_json, col, to_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType
import time
import os
from main_publisher import publish_data_to_kafka  # Import the main publisher

# Set JAVA_HOME if required by your system
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

def create_spark_session():
    """Create and configure Spark session for micro-batch streaming"""
    return SparkSession.builder \
        .appName("EmojiStreamProcessor") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
        .config("spark.streaming.kafka.minBatchesToRetain", "2") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .getOrCreate()

def create_kafka_read_stream(spark):
    """Create input stream from Kafka with micro-batch settings"""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "emoji_topic") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 5000) \
        .option("fetchOffset.numRetries", "3") \
        .load()

def process_emoji_stream():
    """Process emoji streaming data in micro-batches"""
    spark = create_spark_session()
    
    # Define schema for JSON data
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("emoji_type", StringType(), True),
        StructField("timestamp", LongType(), True)
    ])
    
    # Read from Kafka
    kafka_df = create_kafka_read_stream(spark)
    
    # Parse JSON data and handle timestamps
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Convert millisecond timestamp to proper timestamp type
    df_with_timestamp = parsed_df.withColumn(
        "event_time",
        to_timestamp(col("timestamp") / 1000)
    )
    
    # Process in 2-second windows with micro-batching
    windowed_counts = df_with_timestamp \
        .withWatermark("event_time", "2 seconds") \
        .groupBy(
            window("event_time", "2 seconds"),
            "emoji_type"
        ) \
        .agg(count("*").alias("emoji_count"))
    
    # Calculate the scaled count
    scaled_counts = windowed_counts \
        .withColumn(
            "scaled_count",
            expr("CASE WHEN emoji_count >= 1000 THEN 1 ELSE emoji_count END")
        )
    
    # Prepare output with window information
    output_df = scaled_counts.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "emoji_type",
        "emoji_count",
        "scaled_count"
    )
    
    # Write to console for monitoring
    console_query = output_df \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .trigger(processingTime="2 seconds") \
        .option("truncate", False) \
        .start()
    
    # Write to Kafka topic "processed_emojis"
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

    # Publish the aggregated data to the main publisher in each batch
    def process_and_publish_batch(batch_df, batch_id):
        aggregated_data = batch_df.collect()  # Collect the data from the current batch
        # Publish each record to Kafka using the main publisher
        for row in aggregated_data:
            data = {
                "window_start": str(row.window_start),
                "window_end": str(row.window_end),
                "emoji_type": row.emoji_type,
                "emoji_count": row.emoji_count,
                "scaled_count": row.scaled_count
            }
            publish_data_to_kafka(data)

    # Stream processing with batch publishing
    monitoring_query = output_df \
        .writeStream \
        .foreachBatch(process_and_publish_batch) \
        .trigger(processingTime="2 seconds") \
        .start()
    
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping stream processing...")
        console_query.stop()
        kafka_query.stop()
        monitoring_query.stop()
        spark.stop()

if __name__ == "__main__":
    process_emoji_stream()