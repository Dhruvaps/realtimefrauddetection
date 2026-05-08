# -*- coding: utf-8 -*-
"""
Fraud Detection Model Trainer
Uses a synthetic dataset to train a RandomForest classifier.
Replace the dataset with real transaction data for production use.
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
import pickle
import random

random.seed(42)
np.random.seed(42)

# ── Generate synthetic dataset ───────────────────────────────────────────────
# Legitimate transactions: low amount, low frequency
legit_n = 800
legit = pd.DataFrame({
    "amount":    np.random.randint(10, 2000, size=legit_n),
    "frequency": np.random.randint(1, 8,    size=legit_n),
    "fraud":     0,
})

# Fraudulent transactions: high amount AND/OR high frequency
fraud_n = 200
fraud = pd.DataFrame({
    "amount":    np.random.randint(4000, 10000, size=fraud_n),
    "frequency": np.random.randint(12, 20,      size=fraud_n),
    "fraud":     1,
})

df = pd.concat([legit, fraud], ignore_index=True).sample(frac=1, random_state=42)

X = df[["amount", "frequency"]]
y = df["fraud"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# ── Train model ──────────────────────────────────────────────────────────────
print("Training RandomForest classifier...")
model = RandomForestClassifier(
    n_estimators=100,
    max_depth=8,
    random_state=42,
    class_weight="balanced",
)
model.fit(X_train, y_train)

# ── Evaluate ─────────────────────────────────────────────────────────────────
y_pred = model.predict(X_test)
acc    = accuracy_score(y_test, y_pred)
print(f"\nAccuracy: {acc * 100:.1f}%")
print("\nClassification Report:")
print(classification_report(y_test, y_pred, target_names=["Legit", "Fraud"]))

# -- Save model ---------------------------------------------------------------
pickle.dump(model, open("model.pkl", "wb"))
print("Model saved to model.pkl")   