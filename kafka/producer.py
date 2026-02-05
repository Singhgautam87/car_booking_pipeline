from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open("C:\Users\gautam\car-booking-pipeline\kafka\data\car_booking.json") as f:
    records = json.load(f)

for record in records:
    producer.send("raw_car_booking", record)
    print("Sent:", record["booking_id"])
    time.sleep(1)

producer.flush()
producer.close()
