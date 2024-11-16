from kafka import KafkaProducer
import json
import time
from typing import Dict, Any

class MessageProducer:
    def __init__(self, bootstrap_servers: list = ['localhost:9092']):
        """
        Initialize the Kafka producer with configuration.
        
        Args:
            bootstrap_servers (list): List of Kafka broker addresses
        """
        self.producer = KafkaProducer(
            # List of Kafka brokers to connect to
            bootstrap_servers=bootstrap_servers,
            
            # Serialize messages to JSON format
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            
            # Additional configuration options
            acks='all',                 # Wait for all replicas to acknowledge
            retries=3,                  # Number of retries if sending fails
            retry_backoff_ms=1000,      # Wait time between retries
            max_in_flight_requests_per_connection=1,  # Prevent message reordering
            buffer_memory=33554432,     # 32MB buffer size
            compression_type='gzip'      # Compress messages for efficiency
        )

    def send_message(self, topic: str, message: Dict[str, Any]) -> None:
        """
        Send a message to specified Kafka topic.
        
        Args:
            topic (str): The Kafka topic to send to
            message (dict): The message to send
        """
        try:
            # Add timestamp to message
            message['timestamp'] = time.time()
            
            # Send message to Kafka
            future = self.producer.send(topic, value=message)
            
            # Wait for message to be sent
            record_metadata = future.get(timeout=10)
            
            print(f"""
Message sent successfully:
- Topic: {record_metadata.topic}
- Partition: {record_metadata.partition}
- Offset: {record_metadata.offset}
- Message: {message}
            """)
            
        except Exception as e:
            print(f"Error sending message: {str(e)}")

    def close(self):
        """
        Flush and close the producer.
        """
        try:
            self.producer.flush()  # Wait for all messages to be sent
            self.producer.close()  # Close the producer
            print("Producer closed successfully")
        except Exception as e:
            print(f"Error closing producer: {str(e)}")

# Example usage
if __name__ == "__main__":
    # Create producer instance
    producer = MessageProducer()
    
    # Example messages
    messages = [
        {"id": 1, "data": "First message"},
        {"id": 2, "data": "Second message"},
        {"id": 3, "data": "Third message"}
    ]
    
    # Send messages
    try:
        for msg in messages:
            producer.send_message("test_topic", msg)
            time.sleep(1)  # Wait 1 second between messages
    finally:
        producer.close()