from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time

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

# Read data
try:
    with open("/kafka/data/car_booking.json") as f:
        records = json.load(f)
    print(f"📖 Loaded {len(records)} records from file")
except FileNotFoundError:
    print("❌ Data file not found at /kafka/data/car_booking.json")
    raise

# Send records to Kafka
topic = "car-bookings"  # Match the topic created in Jenkinsfile
sent_count = 0

for record in records:
    try:
        future = producer.send(topic, record)
        future.get(timeout=10)  # Wait for confirmation
        print(f"✅ Sent: {record.get('booking_id', 'unknown')}")
        sent_count += 1
        time.sleep(0.5)  # Reduced delay for faster processing
    except Exception as e:
        print(f"❌ Failed to send record {record.get('booking_id')}: {e}")

# Flush and close
producer.flush()
producer.close()

print(f"\n🎉 Successfully sent {sent_count}/{len(records)} records to topic '{topic}'")
