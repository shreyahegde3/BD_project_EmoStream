from confluent_kafka import Consumer, KafkaError
import json

registered_clients = set()

def consume_registrations():
    """Consume client registrations from the Kafka topic."""
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'registration-consumer-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['registration_topic'])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break

        registration_data = json.loads(msg.value().decode('utf-8'))
        client_id = registration_data.get("client_id")
        if client_id:
            registered_clients.add(client_id)
            print(f"Registered new client: {client_id}")

    consumer.close()

def consume_cluster_data(cluster_topic):
    """Consume cluster data and send it to registered clients."""
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': f'{cluster_topic}-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([cluster_topic])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break

        cluster_data = json.loads(msg.value().decode('utf-8'))
        print(f"Received from {cluster_topic}: {cluster_data}")

        if not registered_clients:
            print("No clients registered.")
        else:
            for client in registered_clients:
                print(f"Sending data to client {client}: {cluster_data}")

    consumer.close()

if __name__ == '__main__':
    import threading
    import sys

    # Start a thread to consume registrations
    registration_thread = threading.Thread(target=consume_registrations)
    registration_thread.start()

    # Start consuming cluster data
    cluster_topic = sys.argv[1] if len(sys.argv) > 1 else "cluster1"
    consume_cluster_data(cluster_topic)
