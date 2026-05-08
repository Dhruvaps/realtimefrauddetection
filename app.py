"""
Real-Time Fraud Detection Dashboard
A beautiful Streamlit app that:
  - Connects to Kafka and consumes live transactions
  - Runs ML model predictions in real-time
  - Shows live statistics, charts, and recent transactions table
"""

import streamlit as st
import pickle
import pandas as pd
import json
import threading
import time
import random
from collections import deque
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ── Page config (must be first Streamlit call) ────────────────────────────────
st.set_page_config(
    page_title="FraudShield | Real-Time Detection",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;800&display=swap');

  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

  /* Dark gradient background */
  .stApp {
    background: linear-gradient(135deg, #0a0e1a 0%, #0d1b2a 50%, #0a1628 100%);
    color: #e2e8f0;
  }

  /* Metric cards */
  div[data-testid="metric-container"] {
    background: linear-gradient(135deg, rgba(255,255,255,0.05), rgba(255,255,255,0.02));
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 16px;
    padding: 20px 24px;
    backdrop-filter: blur(10px);
  }
  div[data-testid="metric-container"] label {
    color: #94a3b8 !important;
    font-size: 0.8rem !important;
    letter-spacing: 0.05em !important;
    text-transform: uppercase !important;
  }
  div[data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: #f1f5f9 !important;
    font-size: 2rem !important;
    font-weight: 700 !important;
  }

  /* Sidebar */
  section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0d1b2a 0%, #111827 100%);
    border-right: 1px solid rgba(255,255,255,0.08);
  }

  /* Headers */
  h1, h2, h3 { color: #f1f5f9 !important; }

  /* Fraud rows */
  .fraud-row { background-color: rgba(239,68,68,0.15) !important; }

  /* Status badge */
  .badge-fraud {
    background: linear-gradient(135deg, #dc2626, #991b1b);
    color: white; padding: 2px 10px; border-radius: 20px; font-size: 0.75rem; font-weight: 600;
  }
  .badge-legit {
    background: linear-gradient(135deg, #059669, #065f46);
    color: white; padding: 2px 10px; border-radius: 20px; font-size: 0.75rem; font-weight: 600;
  }

  /* Buttons */
  .stButton > button {
    background: linear-gradient(135deg, #3b82f6, #2563eb);
    color: white; border: none; border-radius: 8px;
    font-weight: 600; padding: 0.5rem 1.5rem;
    transition: all 0.2s;
  }
  .stButton > button:hover {
    background: linear-gradient(135deg, #60a5fa, #3b82f6);
    transform: translateY(-1px); box-shadow: 0 4px 12px rgba(59,130,246,0.4);
  }

  /* Dataframe */
  .stDataFrame { border-radius: 12px; overflow: hidden; }

  /* Divider */
  hr { border-color: rgba(255,255,255,0.08) !important; }

  /* Alerts */
  .fraud-alert {
    background: linear-gradient(135deg, rgba(239,68,68,0.2), rgba(220,38,38,0.1));
    border-left: 4px solid #ef4444;
    border-radius: 8px; padding: 12px 16px; margin: 8px 0;
    animation: pulse 1s ease-in-out;
  }
  @keyframes pulse {
    0%   { opacity: 0.7; }
    50%  { opacity: 1.0; }
    100% { opacity: 0.7; }
  }
</style>
""", unsafe_allow_html=True)

# ── Load Model ────────────────────────────────────────────────────────────────
@st.cache_resource
def load_model():
    try:
        return pickle.load(open("model.pkl", "rb"))
    except FileNotFoundError:
        return None

model = load_model()

# ── Session State Initialization ──────────────────────────────────────────────
if "transactions" not in st.session_state:
    st.session_state.transactions = deque(maxlen=200)
if "kafka_running" not in st.session_state:
    st.session_state.kafka_running = False
if "kafka_thread" not in st.session_state:
    st.session_state.kafka_thread = None
if "kafka_connected" not in st.session_state:
    st.session_state.kafka_connected = False
if "kafka_error" not in st.session_state:
    st.session_state.kafka_error = ""
if "last_fraud_alert" not in st.session_state:
    st.session_state.last_fraud_alert = None

# ── Background Kafka Thread ───────────────────────────────────────────────────
_tx_buffer: list = []  # shared buffer between thread and main

def kafka_worker():
    global _tx_buffer
    try:
        consumer = KafkaConsumer(
            "transactions",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="streamlit-fraud-detector",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            consumer_timeout_ms=2000,
        )
        st.session_state.kafka_connected = True
        st.session_state.kafka_error = ""

        while st.session_state.kafka_running:
            msgs = consumer.poll(timeout_ms=1000, max_records=20)
            for tp, records in msgs.items():
                for record in records:
                    data = record.value
                    amount    = data.get("amount", 0)
                    frequency = data.get("frequency", 1)
                    tx_id     = data.get("tx_id", "N/A")
                    timestamp = data.get("timestamp", datetime.utcnow().isoformat())

                    if model:
                        df_row = pd.DataFrame([[amount, frequency]], columns=["amount", "frequency"])
                        pred   = model.predict(df_row)[0]
                        proba  = model.predict_proba(df_row)[0]
                        fraud_prob = round(float(proba[1]) * 100, 1) if len(proba) > 1 else 0.0
                    else:
                        pred, fraud_prob = 0, 0.0

                    tx = {
                        "tx_id":      tx_id,
                        "amount":     amount,
                        "frequency":  frequency,
                        "fraud":      int(pred),
                        "fraud_prob": fraud_prob,
                        "timestamp":  timestamp,
                    }
                    _tx_buffer.append(tx)

        consumer.close()
        st.session_state.kafka_connected = False

    except NoBrokersAvailable:
        st.session_state.kafka_connected = False
        st.session_state.kafka_error = "❌ Kafka not reachable. Start Kafka and try again."
        st.session_state.kafka_running = False
    except Exception as e:
        st.session_state.kafka_connected = False
        st.session_state.kafka_error = f"❌ Error: {str(e)}"
        st.session_state.kafka_running = False

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🛡️ FraudShield")
    st.markdown("---")

    # Kafka connection status
    if st.session_state.kafka_running:
        st.success("🟢 Kafka Live")
    elif st.session_state.kafka_error:
        st.error(st.session_state.kafka_error)
    else:
        st.warning("⚪ Kafka Disconnected")

    col1, col2 = st.columns(2)
    with col1:
        if not st.session_state.kafka_running:
            if st.button("▶ Start", use_container_width=True):
                st.session_state.kafka_running = True
                t = threading.Thread(target=kafka_worker, daemon=True)
                st.session_state.kafka_thread = t
                t.start()
                st.rerun()
    with col2:
        if st.session_state.kafka_running:
            if st.button("⏹ Stop", use_container_width=True):
                st.session_state.kafka_running = False
                st.rerun()

    st.markdown("---")

    # Manual transaction test
    st.markdown("### 🧪 Manual Test")
    amount    = st.slider("Amount ($)",    10, 10000, 500)
    frequency = st.slider("Frequency",    1,  20,     3)

    if st.button("🔍 Check", use_container_width=True):
        if model:
            df_row     = pd.DataFrame([[amount, frequency]], columns=["amount", "frequency"])
            pred       = model.predict(df_row)[0]
            proba      = model.predict_proba(df_row)[0]
            fraud_prob = round(float(proba[1]) * 100, 1) if len(proba) > 1 else 0.0
            if pred == 1:
                st.error(f"🚨 FRAUD! ({fraud_prob}% probability)")
            else:
                st.success(f"✅ Legitimate ({100 - fraud_prob:.1f}% clean)")
        else:
            st.error("Model not loaded. Run train_model.py first.")

    st.markdown("---")
    if st.button("🗑️ Clear History", use_container_width=True):
        st.session_state.transactions.clear()
        st.rerun()

    st.markdown("---")
    st.markdown("**📋 Setup Guide**")
    st.markdown("""
1. `python train_model.py`
2. Start Zookeeper
3. Start Kafka
4. `python producer.py`
5. Click ▶ Start above
""")

# ── Pull buffered transactions into session state ─────────────────────────────
if _tx_buffer:
    for tx in _tx_buffer:
        st.session_state.transactions.appendleft(tx)
        if tx["fraud"] == 1:
            st.session_state.last_fraud_alert = tx
    _tx_buffer.clear()

# ── Main Layout ───────────────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center; padding: 20px 0 10px 0;">
  <h1 style="font-size:2.5rem; font-weight:800; background: linear-gradient(135deg, #3b82f6, #06b6d4);
     -webkit-background-clip:text; -webkit-text-fill-color:transparent; margin:0;">
    🛡️ FraudShield
  </h1>
  <p style="color:#64748b; font-size:1rem; margin:4px 0 0 0;">
    Real-Time Transaction Fraud Detection &bull; Powered by ML + Apache Kafka
  </p>
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# ── Fraud Alert Banner ────────────────────────────────────────────────────────
if st.session_state.last_fraud_alert:
    fa = st.session_state.last_fraud_alert
    st.markdown(f"""
    <div class="fraud-alert">
      🚨 <strong>Latest Fraud Alert</strong> &nbsp;|&nbsp;
      TxID: <code>{fa['tx_id']}</code> &nbsp;|&nbsp;
      Amount: <strong>${fa['amount']:,}</strong> &nbsp;|&nbsp;
      Frequency: <strong>{fa['frequency']}</strong> &nbsp;|&nbsp;
      Fraud Prob: <strong>{fa['fraud_prob']}%</strong>
    </div>
    """, unsafe_allow_html=True)

# ── KPI Metrics ───────────────────────────────────────────────────────────────
txns = list(st.session_state.transactions)
total      = len(txns)
frauds     = sum(1 for t in txns if t["fraud"] == 1)
legits     = total - frauds
fraud_rate = round((frauds / total * 100), 1) if total > 0 else 0.0
avg_amount = int(sum(t["amount"] for t in txns) / total) if total > 0 else 0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("📊 Total Transactions", f"{total:,}")
c2.metric("🚨 Fraud Detected",     f"{frauds:,}", delta=f"+{frauds}" if frauds > 0 else None, delta_color="inverse")
c3.metric("✅ Legitimate",          f"{legits:,}")
c4.metric("📈 Fraud Rate",          f"{fraud_rate}%")
c5.metric("💰 Avg Amount",          f"${avg_amount:,}")

st.markdown("---")

# ── Charts ────────────────────────────────────────────────────────────────────
if txns:
    df_all = pd.DataFrame(txns)

    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.markdown("### 📊 Fraud vs Legitimate")
        pie_data = pd.DataFrame({
            "Category": ["Legitimate", "Fraud"],
            "Count":    [legits, frauds],
        })
        st.bar_chart(pie_data.set_index("Category"))

    with col_right:
        st.markdown("### 💰 Transaction Amounts (Last 50)")
        recent_50 = df_all.head(50).copy()
        recent_50 = recent_50.sort_index(ascending=False).reset_index(drop=True)
        chart_df = recent_50[["amount"]].copy()
        st.line_chart(chart_df)

    # Fraud probability distribution
    st.markdown("### 🎯 Fraud Probability Distribution (Last 100)")
    recent_100 = df_all.head(100).copy()
    st.bar_chart(recent_100[["fraud_prob"]].rename(columns={"fraud_prob": "Fraud Probability (%)"}))

else:
    st.info("📡 No transactions yet. Start the Kafka consumer or use the Manual Test panel.")

st.markdown("---")

# ── Transaction Table ─────────────────────────────────────────────────────────
st.markdown("### 📋 Recent Transactions")
if txns:
    display_limit = min(50, total)
    df_display = pd.DataFrame(txns[:display_limit]).copy()
    df_display["status"] = df_display["fraud"].apply(
        lambda x: "🚨 FRAUD" if x == 1 else "✅ Legit"
    )
    df_display["fraud_prob"] = df_display["fraud_prob"].apply(lambda x: f"{x:.1f}%")
    df_display["amount"]     = df_display["amount"].apply(lambda x: f"${x:,}")
    df_display = df_display[["timestamp", "tx_id", "amount", "frequency", "fraud_prob", "status"]]
    df_display.columns = ["Timestamp", "TxID", "Amount", "Frequency", "Fraud Prob", "Status"]
    st.dataframe(df_display, use_container_width=True, height=400)
else:
    st.info("No transactions to display yet.")

# ── Auto-refresh every 2 seconds when streaming ───────────────────────────────
if st.session_state.kafka_running:
    time.sleep(2)
    st.rerun()