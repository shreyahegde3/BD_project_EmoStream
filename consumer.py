from confluent_kafka import Consumer
import json

# Kafka configuration
kafka_topic = "emoji_topic"
kafka_bootstrap_servers = "localhost:9092"
consumer_group_id = "emoji-consumer-group"

consumer = Consumer({
    'bootstrap.servers': kafka_bootstrap_servers,
    'group.id': consumer_group_id,
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([kafka_topic])

def consume_from_kafka():
    while True:
        try:
            # Poll for new messages
            msg = consumer.poll(timeout=2.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            data = json.loads(msg.value())
            yield data

        except KeyboardInterrupt:
            break

    consumer.close()