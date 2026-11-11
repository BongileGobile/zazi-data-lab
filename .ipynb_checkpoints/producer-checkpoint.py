from kafka import KafkaProducer
import json, time, random

# Connect to your local Kafka broker
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

cities = ["Cape Town", "Durban", "Joburg", "Pretoria"]
names = ["Zazi", "Amahle", "Kwena", "Nathi"]

print("🚀 Sending random transactions to Kafka topic: zazi-stream")

while True:
    event = {
        "customer_id": random.randint(100, 999),
        "name": random.choice(names),
        "city": random.choice(cities),
        "purchase_value": random.randint(100, 600)
    }
    producer.send("zazi-stream", event)
    print("Sent:", event)
    time.sleep(2)  # every 2 seconds

