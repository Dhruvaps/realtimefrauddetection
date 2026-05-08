# -*- coding: utf-8 -*-
"""
Real-time Fraud Detection Producer
Generates synthetic bank transactions and streams them to Kafka.
Simulates a mix of legitimate (~80%) and fraudulent (~20%) transactions.
"""

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import random
import sys
import uuid
from datetime import datetime

# Connect to Kafka
print("Connecting to Kafka at localhost:9092 ...")
producer = None
for attempt in range(1, 6):
    try:
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            acks="all",
            retries=3,
        )
        producer.bootstrap_connected()
        print("[OK] Connected to Kafka!")
        break
    except NoBrokersAvailable:
        print("[WARN] Attempt {}/5 - broker not available, retrying in 3s...".format(attempt))
        if attempt == 5:
            print("[ERROR] Cannot reach Kafka. Make sure Kafka is running on localhost:9092")
            sys.exit(1)
        time.sleep(3)

TOPIC         = "transactions"
SEND_INTERVAL = 2  # seconds between messages

print("\nStreaming transactions to Kafka topic '{}' every {}s".format(TOPIC, SEND_INTERVAL))
print("Press Ctrl+C to stop\n")

tx_count = 0
try:
    while True:
        # ~20% chance of a fraudulent transaction
        is_fraud_sim = random.random() < 0.20

        if is_fraud_sim:
            amount    = random.randint(5000, 10000)
            frequency = random.randint(15, 20)
        else:
            amount    = random.randint(10, 2000)
            frequency = random.randint(1, 8)

        tx = {
            "tx_id":     str(uuid.uuid4())[:8].upper(),
            "amount":    amount,
            "frequency": frequency,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        future = producer.send(TOPIC, value=tx)
        future.get(timeout=10)

        tx_count += 1
        tag = "HIGH RISK" if is_fraud_sim else "normal   "
        print("[{:>4}] Sent {} | TxID={} | Amt=${:>6} | Freq={:>2}".format(
            tx_count, tag, tx["tx_id"], tx["amount"], tx["frequency"]))

        time.sleep(SEND_INTERVAL)

except KeyboardInterrupt:
    print("\nProducer stopped. Total sent: {}".format(tx_count))
finally:
    if producer:
        producer.flush()
        producer.close()