from kafka import KafkaProducer
import pandas as pd
import time
import json

print('init')

# Kafka Producer Config with Acknowledgment
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",  # Use "kafka" inside Docker, "localhost" outside Docker
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks='all'  # Ensures messages are fully written
)

print('init producer')

# Load taxi trip dataset (replace with actual dataset path)
df = pd.read_csv("./data/sample_sorted_data.csv", header=None) # Adjust the path if needed

print('init df')

# Simulate streaming the data row by row
for _, row in df.iterrows():
    trip_data = row.to_dict()

    # Send data to Kafka topic and wait for acknowledgment
    future = producer.send("taxi-trips", value=trip_data)
    try:
        record_metadata = future.get(timeout=10)  # Wait for Kafka confirmation
        print(f"Message sent to partition {record_metadata.partition} at offset {record_metadata.offset}")
    except Exception as e:
        print(f"Error sending message: {e}")

    # Simulate delay (e.g., sending 1 trip per second)
    time.sleep(1)

producer.close()