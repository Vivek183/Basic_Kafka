# consumer.py
from kafka import KafkaConsumer
import json

def create_consumer(topic):
    # Create consumer instance
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

def consume_messages(consumer):
    # Continuously consume messages
    print("Starting to consume messages...")
    try:
        for message in consumer:
            print(f"Received message:")
            print(f"Topic: {message.topic}")
            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")
            print(f"Key: {message.key}")
            print(f"Value: {message.value}")
            print("------------------------")
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

# Example usage
if __name__ == "__main__":
    topic = "test_topic"
    consumer = create_consumer(topic)
    consume_messages(consumer)