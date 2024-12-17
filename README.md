# Kafka-Demo

# Real-Time Streaming Data Pipeline using Kafka and Docker

## Overview
Real-time streaming data pipeline using **Kafka** and **Docker**. Streaming data from Kafka is consumed in Python where aggregation metrics are calculated and reingested in a Kafka. The metrics are sent to the new Kafka topic processed-user-login.

processed-user-login:
If a required field is missing, it will be replaced by the string: 'unknown'
This 'cleans' the user data so it can later be aggregated.

aggregated-user-login:
Every 10 seconds aggregations of key data points are sent to this topic to view trends.


## Prerequisites

- **Docker Desktop** (https://docs.docker.com/get-docker/)
- **Python 3.9+** (https://www.python.org/)
- **Microsoft Visual C++ 14.0 Build Tools** (https://visualstudio.microsoft.com/visual-cpp-build-tools/)

## Project Setup

### Step 1: Configure environment variables
.env.prod is preconfigured for docker environment, set custom variables as needed for local development
BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=user-login
GROUP_ID=user-login-consumer-group

### Step 2: Docker compose set up
Kafka, the data generator, and the kafka consumer are build into the docker compose file.
To start:

```bash
docker compose up --build
```

Kafka will be running on `localhost:29092` for local development, and `kafka:9092` in docker. The data generator will produce messages in the `user-login` topic.

The python Kafka consumer will run in it's own Docker container. The metrics aggregation will be processed into the `user-login-processed` topic.

Sample output:

```
Kafka Consumer is running and consuming data...
Processed Event: {'user_id': 'user-12', 'app_version': '1.4', 'device_type': 'device-1', 'locale': 'en-US', 'event_time': '2024-06-26 12:15:01'}
Sent to 'processed-user-login': {'user_id': 'user-12', 'app_version': '1.4', 'device_type': 'device-1', 'locale': 'en-US', 'event_time': '2024-06-26 12:15:01'}

--- Insights ---
Most Popular App Versions: [('1.4', 10), ('1.3', 7), ('1.2', 5)]
Most Popular Device Types: [('device-1', 15), ('device-0', 12)]
Most Common Locales: [('en-US', 20)]
----------------
Sent to 'aggregated-user-login': {'most_popular_app_versions': [('1.4', 10), ('1.3', 7), ('1.2', 5)], 'most_popular_device_types': [('device-1', 15), ('device-0', 12)], 'most_common_locales': [('en-US', 20)], 'timestamp': '2024-06-26 12:15:10'}
```