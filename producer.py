"""
=============================================================================
COMPONENT: Real-Time Event Publisher (Producer)
ARCHITECTURE ROLE: Simulates a real-time event streaming source (AWS MSK / Kinesis).
=============================================================================

WHY THIS DESIGN?
1. Connection Resilience: Kafka containers take a few seconds to boot. Adding an 
   explicit retry loop prevents startup crashes and ensures fault-tolerant startup order.
2. Structured Payloads: Emitting dictionary events (customer ID, South African city, 
   and purchase value) allows us to test downstream transformations and partitioning.
3. Controlled Throughput: Using `time.sleep(2)` regulates event velocity, preventing 
   local resource exhaustion while mimicking steady-state streaming data flow.
"""

import json          # WHY: Serializes Python dictionaries into raw JSON byte strings for network transport.
import logging       # WHY: Replaces print statements with professional log levels (INFO, WARNING) and timestamps.
import random        # WHY: Generates synthetic test data to simulate real-world user banking transactions.
import time          # WHY: Controls the emission interval between consecutive published events.
from kafka import KafkaProducer          # WHY: Core kafka-python client used to connect to brokers and send messages.
from kafka.errors import NoBrokersAvailable  # WHY: Exception handler for container startup race conditions.

# Configure professional logging for operational observability
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "zazi-stream"

def create_producer():
    """
    Initializes Kafka producer with retry logic.
    WHY: Handles containerized environments where the app starts faster than the Kafka broker.
    """
    retries = 5
    delay = 3
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Connecting to Kafka broker at {KAFKA_BOOTSTRAP_SERVERS} (Attempt {attempt}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8") # WHY: Converts python dict to encoded bytes
            )
            logging.info("Successfully connected to Kafka broker.")
            return producer
        except NoBrokersAvailable:
            logging.warning(f"Kafka broker not available yet. Retrying in {delay} seconds...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka broker after multiple attempts. Is Kafka running?")

def generate_transaction():
    """Generates a mock financial transaction payload with regional relevance."""
    cities = ["Cape Town", "Durban", "Joburg", "Pretoria"]
    names = ["Zazi", "Amahle", "Kwena", "Nathi"]
    
    return {
        "customer_id": random.randint(100, 999),
        "name": random.choice(names),
        "city": random.choice(cities),
        "purchase_value": round(random.uniform(100.0, 600.0), 2)
    }

def main():
    producer = create_producer()
    logging.info(f"🚀 Starting continuous event stream to topic: {KAFKA_TOPIC}")
    
    try:
        while True:
            event = generate_transaction()
            # Asynchronously push event to the Pub/Sub topic
            producer.send(KAFKA_TOPIC, value=event)
            logging.info(f"Published Event -> {json.dumps(event)}")
            time.sleep(2)  # Emit an event every 2 seconds
    except KeyboardInterrupt:
        logging.info("Stopping producer gracefully...")
    finally:
        producer.close()
        logging.info("Kafka producer closed.")

if __name__ == "__main__":
    main()
