import json, time
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Get CSV path from environment variable
CSV_PATH = os.getenv('CSV_PATH')

if not CSV_PATH:
    raise ValueError("CSV_PATH not found in .env file!")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10
)

df = pd.read_csv(CSV_PATH)
print(f"Loaded {len(df)} rows from CSV")

for i, row in df.head(1000).iterrows():   # limit for demo
    record = row.fillna('').to_dict()
    producer.send('transactions', value=record)
    print(f"Sent record {i}")
    time.sleep(0.5)
