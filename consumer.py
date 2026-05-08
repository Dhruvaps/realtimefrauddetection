# -*- coding: utf-8 -*-
"""
Real-time Fraud Detection Consumer
Uses kafka-python directly (no PySpark/JVM) for reliable real-time streaming.
Reads transactions from Kafka, runs ML model predictions, prints results.
"""

from kafka import KafkaConsumer
import json
import pickle
import pandas as pd
import sys
import time
from datetime import datetime

# Load model
print("Loading fraud detection model...")
try:
    model = pickle.load(open("model.pkl", "rb"))
    print("[OK] Model loaded successfully")
except FileNotFoundError:
    print("[ERROR] model.pkl not found. Run: python train_model.py")
    sys.exit(1)

# Kafka consumer
print("Connecting to Kafka at localhost:9092 ...")
consumer = None
for attempt in range(1, 6):
    try:
        consumer = KafkaConsumer(
            "transactions",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="fraud-detector-group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            consumer_timeout_ms=10000,
        )
        topics = consumer.topics()
        print("[OK] Connected to Kafka. Topics: {}".format(topics))
        break
    except Exception as e:
        print("[WARN] Attempt {}/5 failed: {}".format(attempt, e))
        if attempt == 5:
            print("[ERROR] Cannot connect to Kafka. Make sure Kafka is running.")
            sys.exit(1)
        time.sleep(3)

# Stats tracking
stats = {"total": 0, "fraud": 0, "legit": 0}

print("\n" + "=" * 55)
print("     REAL-TIME FRAUD DETECTION CONSUMER")
print("=" * 55)
print("Listening for transactions... (Ctrl+C to stop)\n")

try:
    for msg in consumer:
        data      = msg.value
        amount    = data.get("amount", 0)
        frequency = data.get("frequency", 1)
        tx_id     = data.get("tx_id", "N/A")
        ts        = datetime.now().strftime("%H:%M:%S")

        # ML prediction
        df = pd.DataFrame([[amount, frequency]], columns=["amount", "frequency"])
        prediction = model.predict(df)[0]
        proba      = model.predict_proba(df)[0]
        fraud_prob = round(float(proba[1]) * 100, 1) if len(proba) > 1 else "N/A"

        stats["total"] += 1
        label = "FRAUD" if prediction == 1 else "LEGIT"
        if prediction == 1:
            stats["fraud"] += 1
        else:
            stats["legit"] += 1

        alert = "*** FRAUD ALERT ***" if prediction == 1 else "[ OK ] CLEAR"
        print("[{}] TxID={} | Amt=${:>6} | Freq={:>2} | FraudProb={}% | {}".format(
            ts, tx_id, amount, frequency, fraud_prob, alert))
        print("       Stats -> Total={} | Fraud={} | Legit={}".format(
            stats["total"], stats["fraud"], stats["legit"]))
        print()

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")
except Exception as e:
    print("\n[ERROR] Unexpected error: {}".format(e))
finally:
    if consumer:
        consumer.close()
    print("\nFinal Stats: Total={} | Fraud={} | Legit={}".format(
        stats["total"], stats["fraud"], stats["legit"]))