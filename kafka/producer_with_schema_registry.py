"""
Car Booking Kafka Producer — Schema Registry Integrated
Schema Registry validate karta hai ki har message correct format mein hai
"""

import json
import time
import random
import requests
from kafka import KafkaProducer
from datetime import datetime, timedelta

# ================================================================
# SCHEMA REGISTRY CONFIG
# ================================================================
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "car-bookings"

# Avro-style JSON Schema — yeh Schema Registry mein register hoga
CAR_BOOKING_SCHEMA = {
    "type": "record",
    "name": "CarBooking",
    "namespace": "com.carbooking.pipeline",
    "doc": "Car booking event schema v1",
    "fields": [
        {"name": "booking_id",        "type": "string"},
        {"name": "customer_id",       "type": "string"},
        {"name": "customer_name",     "type": "string"},
        {"name": "car_id",            "type": "string"},
        {"name": "model",             "type": "string"},
        {"name": "category",          "type": "string"},
        {"name": "pickup_location",   "type": "string"},
        {"name": "drop_location",     "type": "string"},
        {"name": "booking_date",      "type": "string"},
        {"name": "return_date",       "type": "string"},
        {"name": "price_per_day",     "type": "double"},
        {"name": "rental_days",       "type": "int"},
        {"name": "payment_amount",    "type": "double"},
        {"name": "payment_method",    "type": "string"},
        {"name": "payment_id",        "type": "string"},
        {"name": "loyalty_tier",      "type": "string"},
        {"name": "insurance_coverage","type": "string"},
        {"name": "status",            "type": "string"},
        {"name": "event_timestamp",   "type": "string"},
    ]
}


# ================================================================
# SCHEMA REGISTRY — Register + Validate
# ================================================================
def register_schema():
    """Schema Registry mein schema register karo"""
    subject = f"{TOPIC}-value"
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"

    payload = {"schema": json.dumps(CAR_BOOKING_SCHEMA)}
    try:
        response = requests.post(
            url,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            data=json.dumps(payload),
            timeout=10
        )
        if response.status_code in [200, 409]:  # 409 = already exists
            schema_id = response.json().get("id", "existing")
            print(f"✅ Schema registered/verified — ID: {schema_id}")
            return True
        else:
            print(f"⚠️  Schema Registry response: {response.status_code} — {response.text}")
            return False
    except requests.exceptions.ConnectionError:
        print("⚠️  Schema Registry not available — running without schema validation")
        return False


def validate_message(message: dict) -> bool:
    """Message ka structure schema ke against validate karo"""
    required_fields = {f["name"] for f in CAR_BOOKING_SCHEMA["fields"]}
    message_fields = set(message.keys())
    missing = required_fields - message_fields
    if missing:
        print(f"❌ Schema violation — Missing fields: {missing}")
        return False
    return True


# ================================================================
# DATA GENERATION
# ================================================================
CARS = [
    {"car_id": "C001", "model": "Swift",     "category": "Hatchback",  "price": 1200},
    {"car_id": "C002", "model": "Innova",    "category": "SUV",        "price": 2500},
    {"car_id": "C003", "model": "Fortuner",  "category": "SUV",        "price": 3500},
    {"car_id": "C004", "model": "Nexon",     "category": "Compact SUV","price": 1800},
    {"car_id": "C005", "model": "Creta",     "category": "SUV",        "price": 2200},
    {"car_id": "C006", "model": "Baleno",    "category": "Hatchback",  "price": 1100},
    {"car_id": "C007", "model": "City",      "category": "Sedan",      "price": 1600},
    {"car_id": "C008", "model": "Ciaz",      "category": "Sedan",      "price": 1500},
    {"car_id": "C009", "model": "Ertiga",    "category": "MPV",        "price": 1900},
    {"car_id": "C010", "model": "Scorpio",   "category": "SUV",        "price": 2800},
]

LOCATIONS = [
    "Mumbai", "Delhi", "Bangalore", "Pune", "Chennai",
    "Hyderabad", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow"
]

LOYALTY_TIERS   = ["silver", "gold", "platinum"]
PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "net_banking", "cash"]
INSURANCE       = ["basic", "comprehensive", "premium", "none"]
STATUSES        = ["confirmed", "completed", "cancelled", "pending"]


def generate_booking(booking_num: int) -> dict:
    car = random.choice(CARS)
    rental_days = random.randint(1, 14)
    booking_date = datetime.now() - timedelta(days=random.randint(0, 90))
    return_date  = booking_date + timedelta(days=rental_days)

    return {
        "booking_id":         f"BK{booking_num:05d}",
        "customer_id":        f"CUST{random.randint(1000, 9999)}",
        "customer_name":      random.choice(["Rahul Sharma", "Priya Singh", "Amit Patel",
                                              "Neha Gupta", "Raj Kumar", "Sunita Verma",
                                              "Vikram Joshi", "Pooja Mehta"]),
        "car_id":             car["car_id"],
        "model":              car["model"],
        "category":           car["category"],
        "pickup_location":    random.choice(LOCATIONS),
        "drop_location":      random.choice(LOCATIONS),
        "booking_date":       booking_date.strftime("%Y-%m-%d"),
        "return_date":        return_date.strftime("%Y-%m-%d"),
        "price_per_day":      float(car["price"]),
        "rental_days":        rental_days,
        "payment_amount":     float(car["price"] * rental_days),
        "payment_method":     random.choice(PAYMENT_METHODS),
        "payment_id":         f"PAY{random.randint(100000, 999999)}",
        "loyalty_tier":       random.choice(LOYALTY_TIERS),
        "insurance_coverage": random.choice(INSURANCE),
        "status":             random.choice(STATUSES),
        "event_timestamp":    datetime.now().isoformat(),
    }


# ================================================================
# PRODUCER
# ================================================================
def run_producer(total_messages: int = 1000):
    print("=" * 50)
    print("🚀 Car Booking Kafka Producer — Schema Registry Integrated")
    print("=" * 50)

    # Step 1: Schema register karo
    schema_ok = register_schema()

    # Step 2: car_booking.json se data load karo
    json_path = "/tmp/car_booking.json"
    bookings = []
    try:
        with open(json_path, "r") as f:
            raw = json.load(f)
            bookings = raw if isinstance(raw, list) else [raw]
        print(f"✅ Loaded {len(bookings)} records from {json_path}")
    except Exception as e:
        print(f"⚠️  Could not load {json_path}: {e} — using generated data")
        bookings = [generate_booking(i) for i in range(1, total_messages + 1)]

    # Step 3: Kafka Producer initialize
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        max_block_ms=30000,
    )

    sent   = 0
    failed = 0
    total  = len(bookings)

    print(f"\n📤 Sending {total} messages to topic: {TOPIC}")
    print("-" * 50)

    for i, message in enumerate(bookings, 1):
        try:
            if "event_timestamp" not in message:
                message["event_timestamp"] = datetime.now().isoformat()

            if schema_ok and not validate_message(message):
                failed += 1
                continue

            booking_id = str(message.get("booking_id", f"BK{i:05d}"))
            future = producer.send(TOPIC, key=booking_id, value=message)
            future.get(timeout=10)
            sent += 1

            if i % 100 == 0:
                print(f"  ✅ Sent {sent}/{total} messages...")

        except Exception as e:
            print(f"  ❌ Error sending message {i}: {e}")
            failed += 1

    producer.flush()
    producer.close()

    print("\n" + "=" * 50)
    print(f"✅ Producer complete!")
    print(f"   Sent:   {sent}")
    print(f"   Failed: {failed}")
    print(f"   Topic:  {TOPIC}")
    print("=" * 50)


if __name__ == "__main__":
    run_producer()