# from confluent_kafka import Producer
# import json
# import threading

# def send_to_kafka(data, topic):
#     """Send data to Kafka topic using confluent_kafka"""
    
#     # Kafka configuration
#     conf = {
#         'bootstrap.servers': 'localhost:9092',  # Kafka broker address
#         'client.id': 'emoji-producer'
#     }
    
#     # Create the producer instance
#     producer = Producer(conf)
    
#     # Callback function to confirm successful message delivery
#     def delivery_report(err, msg):
#         if err is not None:
#             print('Message delivery failed: {}'.format(err))
#         else:
#             print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
    
#     # Send data to Kafka
#     producer.produce(topic, json.dumps(data).encode('utf-8'), callback=delivery_report)
    
#     # Ensure all messages are sent before exiting
#     producer.flush()

# def publish_data_in_thread(data, topic):
#     """Publish data to Kafka in a separate thread"""
#     send_to_kafka(data, topic)

# def cluster_publish_aggregated_data(aggregated_data):
#     """Distribute aggregated data publishing across multiple threads"""
#     threads = []
#     for row in aggregated_data:
#         data = {
#             "window_start": row['window_start'],
#             "window_end": row['window_end'],
#             "emoji_type": row['emoji_type'],
#             "emoji_count": row['emoji_count'],
#             "scaled_count": row['scaled_count']
#         }
#         thread = threading.Thread(target=publish_data_in_thread, args=(data, "processed_emojis"))
#         threads.append(thread)
#         thread.start()

#     # Wait for all threads to complete
#     for thread in threads:
#         thread.join()

# if __name__ == "__main__":
#     # Example aggregated data for testing (you may ignore this part when used in Spark)
#     aggregated_data = [
#         {"window_start": "2024-11-12 10:00:00", "window_end": "2024-11-12 10:02:00", "emoji_type": "heart", "emoji_count": 1500, "scaled_count": 1},
#         {"window_start": "2024-11-12 10:02:00", "window_end": "2024-11-12 10:04:00", "emoji_type": "thumbs_up", "emoji_count": 900, "scaled_count": 900}
#     ]
    
#     # This part is for testing, in Spark you would call this function directly
#     cluster_publish_aggregated_data(aggregated_data)



# cluster_publisher.py
from kafka import KafkaProducer, KafkaConsumer
import json
import sys

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
MAIN_TOPIC = 'final_output_topic'  # Topic from the main publisher
CLUSTER_TOPIC = sys.argv[1] if len(sys.argv) > 1 else 'cluster1'

# Initialize Kafka Consumer to read from the main topic
consumer = KafkaConsumer(
    MAIN_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Initialize Kafka Producer for cluster topic
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Cluster Publisher started. Listening to {MAIN_TOPIC} and publishing to {CLUSTER_TOPIC}.")

def start_cluster_publisher():
    try:
        for message in consumer:
            data = message.value
            print(f"Received from main topic: {data}")
            
            # Forward data to cluster topic
            producer.send(CLUSTER_TOPIC, value=data)
            producer.flush()
            print(f"Published to cluster topic {CLUSTER_TOPIC}: {data}")
    except KeyboardInterrupt:
        print("Cluster Publisher stopped.")

if __name__ == "__main__":
    start_cluster_publisher()
