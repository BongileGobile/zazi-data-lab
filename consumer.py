"""
=============================================================================
COMPONENT: Real-Time Event Subscriber (Consumer) & Dual-Sink Stager
ARCHITECTURE ROLE: Pulls Kafka stream, transforms data, writes to DB and S3 Lakehouse.
=============================================================================

WHY THIS DESIGN?
1. Dual-Sink Pattern: Relational DB (Postgres) gives instant transactional visibility; 
   Object Storage (S3 Parquet) powers analytical data lake workloads.
2. Micro-Batching: Writing individual records to S3 creates high API overhead ("small file problem"). 
   Batching every 5 records optimizes storage performance and compression.
3. In-Memory Buffering (`BytesIO`): Avoids writing temporary files to MacBook disk space, 
   saving I/O overhead and preventing local disk bloat.
"""

import json          # WHY: Deserializes raw byte payloads from Kafka back into Python dictionaries.
import logging       # WHY: Standardized logging to track consumption offsets and error rollbacks.
import time          # WHY: Generates epoch timestamps (`int(time.time())`) for unique S3 Parquet file naming.
from io import BytesIO  # WHY: In-memory binary stream buffer allowing Pandas to serialize Parquet directly to S3 via boto3 without local disk writes.
import boto3         # WHY: AWS SDK for Python, used here to communicate with LocalStack S3 (`http://localhost:4566`).
import pandas as pd  # WHY: Data manipulation library used to convert record lists into tabular DataFrames for Parquet export.
import psycopg2      # WHY: PostgreSQL database adapter for executing SQL inserts and transaction management.
from kafka import KafkaConsumer          # WHY: Core consumer client that polls Kafka topics and manages consumer groups.
from kafka.errors import NoBrokersAvailable  # WHY: Exception handler for broker startup synchronization.

# Configure professional logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "zazi-stream"
BUCKET_NAME = "zazi-data-bucket"

def connect_kafka():
    """
    Connects consumer to Kafka with retry logic.
    WHY: Ensures resilient startup order independence in container environments.
    """
    retries = 5
    delay = 3
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Connecting consumer to Kafka at {KAFKA_BOOTSTRAP_SERVERS} (Attempt {attempt}/{retries})...")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")), # WHY: Automatically decodes incoming bytes to JSON dicts
                auto_offset_reset="earliest",                                # WHY: Reads from the beginning of the topic if no offset is stored
                group_id="zazi-consumer"                                     # WHY: Consumer group ID ensures balanced partition reading & offset tracking
            )
            logging.info("Successfully connected to Kafka.")
            return consumer
        except NoBrokersAvailable:
            logging.warning(f"Kafka broker not ready. Retrying in {delay} seconds...")
            time.sleep(delay)
    raise RuntimeError("Could not connect consumer to Kafka after multiple attempts.")

def connect_postgres():
    """
    Connects to PostgreSQL sink with retry logic.
    WHY: Relational databases take longer to initialize schemas than app containers; retries prevent crash-loops.
    """
    retries = 5
    delay = 3
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Connecting to PostgreSQL at localhost:5432 (Attempt {attempt}/{retries})...")
            conn = psycopg2.connect(
                dbname="zazi_lab",
                user="postgres",
                password="postgres",
                host="localhost",
                port="5432"
            )
            logging.info("Successfully connected to PostgreSQL.")
            return conn
        except psycopg2.OperationalError:
            logging.warning(f"PostgreSQL not ready yet. Retrying in {delay} seconds...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to PostgreSQL after multiple attempts.")

def init_s3_bucket(s3_client):
    """Ensures the LocalStack S3 bucket exists before attempting data uploads."""
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logging.info(f"S3 Bucket '{BUCKET_NAME}' verified.")
    except Exception:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        logging.info(f"Created S3 Bucket '{BUCKET_NAME}' in LocalStack.")

def main():
    consumer = connect_kafka()
    conn = connect_postgres()
    cursor = conn.cursor()

    # LocalStack S3 client configuration mimicking AWS S3 locally
    s3 = boto3.client(
        "s3",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        endpoint_url="http://localhost:4566" # WHY: Points boto3 to the local LocalStack container instead of real AWS
    )
    init_s3_bucket(s3)

    logging.info("📡 Consumer listening for messages on topic: %s", KAFKA_TOPIC)
    
    records = []
    try:
        for message in consumer:
            data = message.value
            
            # --- Data Enrichment Layer ---
            # WHY: Demonstrates real-time stream processing (calculating South African VAT and customer segmentation)
            data["purchase_value_with_vat"] = round(data["purchase_value"] * 1.15, 2)
            data["segment"] = "High" if data["purchase_value"] >= 300 else "Low"
            
            logging.info(f"Received -> Customer: {data['customer_id']} | City: {data['city']} | Value (Incl. VAT): R{data['purchase_value_with_vat']}")
            
            # --- Sink 1: Relational Database Insert ---
            try:
                cursor.execute("""
                    INSERT INTO stream_spend (customer_id, name, city, purchase_value, purchase_value_with_vat, segment)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    data["customer_id"],
                    data["name"],
                    data["city"],
                    data["purchase_value"],
                    data["purchase_value_with_vat"],
                    data["segment"]
                ))
                conn.commit()
            except Exception as e:
                logging.error(f"❌ DB Insert Error: {e}")
                conn.rollback() # WHY: Rollback transaction to maintain database state consistency on failure
             
            # --- Sink 2: Object Storage Parquet Micro-Batching ---
            records.append(data)
            if len(records) >= 5:
                df = pd.DataFrame(records)
                buffer = BytesIO()
                key = f"stream-output/zazi_stream_{int(time.time())}.parquet"
                
                # WHY: Convert JSON batch to columnar Parquet format for high-speed analytical querying
                df.to_parquet(buffer, index=False)
                buffer.seek(0)
                s3.upload_fileobj(buffer, BUCKET_NAME, key)
                
                logging.info(f"✅ Uploaded batch of {len(records)} records to s3://{BUCKET_NAME}/{key}")
                records = []

    except KeyboardInterrupt:
        logging.info("Stopping consumer gracefully...")
    finally:
        cursor.close()
        conn.close()
        consumer.close()
        logging.info("Consumer connections closed.")

if __name__ == "__main__":
    main()
