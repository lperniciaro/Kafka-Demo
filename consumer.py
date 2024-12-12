import json
import time
from confluent_kafka import Consumer, Producer, KafkaException
from datetime import datetime
from collections import Counter

# Configuration for Kafka Consumer
consumer_config = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'user-login-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Configuration for Kafka Producer
producer_config = {
    'bootstrap.servers': 'localhost:29092'
}

# Initialize Kafka Consumer
consumer = Consumer(consumer_config)
consumer.subscribe(['user-login'])

# Initialize Kafka Producer
producer = Producer(producer_config)

# Data Aggregation Structures
app_version_counter = Counter()
device_type_counter = Counter()
locale_counter = Counter()

def send_to_new_topic(processed_message):
    try:
        producer.produce('processed-user-login', json.dumps(processed_message).encode('utf-8'))
        producer.flush()
        print(f"Sent to 'processed-user-login': {processed_message}")
    except Exception as e:
        print(f"Error sending message to new topic: {e}")

def handle_missing_fields(message):
    """Ensure all required fields are present, substituting defaults for missing fields."""
    required_fields = {
        'user_id': 'unknown',
        'app_version': 'unknown',
        'device_type': 'unknown',
        'locale': 'unknown',
        'timestamp': str(int(time.time()))  # Use current time if timestamp is missing
    }
    return {key: message.get(key, default) for key, default in required_fields.items()}

print("Kafka Consumer is running and consuming data...")

try:
    while True:
        # Poll for a message
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue  # No new messages, continue polling

        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                continue  # End of partition event
            else:
                print(f"Error: {msg.error()}")
                break

        # Process the message
        try:
            message_value = msg.value().decode('utf-8')
            raw_message = json.loads(message_value)

            # Handle missing fields
            message = handle_missing_fields(raw_message)

            # Extract fields
            user_id = message['user_id']
            app_version = message['app_version']
            device_type = message['device_type']
            locale = message['locale']
            timestamp = message['timestamp']

            # Convert timestamp to human-readable format
            event_time = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')

            # Update counters
            app_version_counter[app_version] += 1
            device_type_counter[device_type] += 1
            locale_counter[locale] += 1

            # Create processed message
            processed_message = {
                'user_id': user_id,
                'app_version': app_version,
                'device_type': device_type,
                'locale': locale,
                'event_time': event_time
            }

            # Send processed message to new topic
            send_to_new_topic(processed_message)

            # Print processed data
            print(f"Processed Event: {processed_message}")

        except Exception as e:
            print(f"Error processing message: {e}")

        # Periodically print insights
        if int(time.time()) % 10 == 0:  # Every 10 seconds
            print("\n--- Insights ---")
            print("Most Popular App Versions:", app_version_counter.most_common(3))
            print("Most Popular Device Types:", device_type_counter.most_common(3))
            print("Most Common Locales:", locale_counter.most_common(3))
            print("----------------\n")

except KeyboardInterrupt:
    print("\nStopping Kafka Consumer...")
finally:
    consumer.close()
