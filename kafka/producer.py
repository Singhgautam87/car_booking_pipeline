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
                request_timeout_ms=10000,

                # ✅ Performance settings
                batch_size=65536,        # 64KB batch (default 16KB)
                linger_ms=10,            # 10ms wait to fill batch
                compression_type='snappy',  # compress karo
                buffer_memory=67108864,  # 64MB buffer
                acks=1,                  # sirf leader ack kare
            )
            print(f"✅ Connected to Kafka on attempt {attempt}")
            return producer
        except NoBrokersAvailable:
            if attempt == max_retries:
                print(f"❌ Failed to connect to Kafka after {max_retries} attempts")
                raise
            print(f"⏳ Kafka not ready... retrying in {retry_delay}s (attempt {attempt}/{max_retries})")
            time.sleep(retry_delay)

# Create producer
producer = create_producer()

# Data file dhundho
possible_paths = [
    "/kafka/data/car_booking.json",
    "/tmp/car_booking.json",
    "data/car_booking.json",
    "./data/car_booking.json"
]

data_file = None
for path in possible_paths:
    if os.path.exists(path):
        data_file = path
        print(f"✅ Found data file at: {path}")
        break

if not data_file:
    print("❌ Data file not found in any of these locations:")
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

# ✅ Send records — ASYNC, no sleep, batch me
topic = "car-bookings"
sent_count = 0
failed_count = 0
futures = []

print(f"🚀 Sending {len(records)} records...")
start_time = time.time()

for record in records:
    try:
        # ✅ future.get() mat karo — async bhejo
        future = producer.send(topic, record)
        futures.append((future, record.get('booking_id', 'unknown')))
        sent_count += 1

        # Har 1000 records pe progress dikhao
        if sent_count % 1000 == 0:
            print(f"📤 Sent {sent_count}/{len(records)} records...")

    except Exception as e:
        print(f"❌ Failed to send record {record.get('booking_id')}: {e}")
        failed_count += 1

# ✅ Sab messages flush karo ek baar me
print("⏳ Flushing all messages to Kafka...")
producer.flush()

# Optional: errors check karo
error_count = 0
for future, booking_id in futures:
    try:
        future.get(timeout=10)  # ab check karo jab sab bhej diye
    except Exception as e:
        print(f"❌ Delivery failed for {booking_id}: {e}")
        error_count += 1

producer.close()

elapsed = time.time() - start_time
print(f"\n🎉 Done in {elapsed:.1f} seconds!")
print(f"✅ Sent: {sent_count} | ❌ Failed: {failed_count + error_count} | Topic: '{topic}'")