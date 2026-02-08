from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import os

def create_producer(max_retries=10, retry_delay=3):
    """Create Kafka producer with retry logic"""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers="kafka:9092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                max_block_ms=5000,
                request_timeout_ms=10000
            )
            print(f"✅ Connected to Kafka on attempt {attempt}")
            return producer
        except NoBrokersAvailable as e:
            if attempt == max_retries:
                print(f"❌ Failed to connect to Kafka after {max_retries} attempts")
                raise
            print(f"⏳ Kafka not ready yet... retrying in {retry_delay}s (attempt {attempt}/{max_retries})")
            time.sleep(retry_delay)

# Create producer with retry
producer = create_producer()

# Try multiple possible data file locations
possible_paths = [
    "/kafka/data/car_booking.json",      # Docker volume mount
    "/tmp/car_booking.json",             # Copied by Jenkins
    "data/car_booking.json",             # Relative path
    "./data/car_booking.json"            # Alternative relative
]

data_file = None
for path in possible_paths:
    if os.path.exists(path):
        data_file = path
        print(f"✅ Found data file at: {path}")
        break

if not data_file:
    print(f"❌ Data file not found in any of these locations:")
    for path in possible_paths:
        print(f"   - {path}")
    raise FileNotFoundError("car_booking.json not accessible")

# Read data
try:
    with open(data_file) as f:
        records = json.load(f)
    print(f"📖 Loaded {len(records)} records from file")
except Exception as e:
    print(f"❌ Error reading file: {e}")
    raise

# Send records to Kafka
topic = "car-bookings"
sent_count = 0

for record in records:
    try:
        future = producer.send(topic, record)
        future.get(timeout=10)
        print(f"✅ Sent: {record.get('booking_id', 'unknown')}")
        sent_count += 1
        time.sleep(0.5)
    except Exception as e:
        print(f"❌ Failed to send record {record.get('booking_id')}: {e}")

# Flush and close
producer.flush()
producer.close()

print(f"\n🎉 Successfully sent {sent_count}/{len(records)} records to topic '{topic}'")