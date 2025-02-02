import json
import time
import os
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer, KafkaException
from datetime import datetime
from collections import Counter

load_dotenv('.env.prod') #Comment this line for local (non docker) testing

# Load environment variables
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'user-login')
GROUP_ID = os.getenv('GROUP_ID', 'user-login-consumer-group')

# Configuration for Kafka Consumer
consumer_config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
}

# Configuration for Kafka Producer
producer_config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS
}

# Initialize Kafka Consumer
consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC])

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

def send_aggregations_to_topic():
    try:
        aggregations = {
            'most_popular_app_versions': app_version_counter.most_common(3),
            'most_popular_device_types': device_type_counter.most_common(3),
            'most_common_locales': locale_counter.most_common(3),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        producer.produce('aggregated-user-login', json.dumps(aggregations).encode('utf-8'))
        producer.flush()
        print(f"Sent to 'aggregated-user-login': {aggregations}")
    except Exception as e:
        print(f"Error sending aggregations to new topic: {e}")

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

        # Periodically print insights and send aggregations
        if int(time.time()) % 10 == 0:  # Every 10 seconds
            print("\n--- Insights ---")
            print("Most Popular App Versions:", app_version_counter.most_common(3))
            print("Most Popular Device Types:", device_type_counter.most_common(3))
            print("Most Common Locales:", locale_counter.most_common(3))
            print("----------------\n")
            send_aggregations_to_topic()

except KeyboardInterrupt:
    print("\nStopping Kafka Consumer...")
finally:
    consumer.close()
