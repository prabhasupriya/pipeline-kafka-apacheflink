import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
import os
import time

st.set_page_config(page_title="MNC Real-Time Dashboard", layout="wide")
st.title("📊 Real-Time Feature Store Monitor")

# Initialize data storage in session state
if 'feature_data' not in st.session_state:
    st.session_state.feature_data = []

# Cache the consumer so it doesn't restart on every rerun
@st.cache_resource
def get_kafka_consumer():
    try:
        return KafkaConsumer(
            'feature-store',
            bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='dashboard-view-final',
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            consumer_timeout_ms=500  # Crucial: stops poll from blocking indefinitely
        )
    except Exception as e:
        st.error(f"Kafka Connection Init Failed: {e}")
        return None

placeholder = st.empty()
consumer = get_kafka_consumer()

if consumer:
    # Standard message fetching block without an infinite blocking while loop
    message_pack = consumer.poll(timeout_ms=500)
    
    for tp, messages in message_pack.items():
        for message in messages:
            if message.value not in st.session_state.feature_data:
                st.session_state.feature_data.append(message.value)
            
            # Keep only the last 15 records
            if len(st.session_state.feature_data) > 15:
                st.session_state.feature_data.pop(0)
    
    # Update the UI cleanly
    if st.session_state.feature_data:
        df = pd.DataFrame(st.session_state.feature_data)
        with placeholder.container():
            st.success("✅ Stream Active: Receiving Features from Flink")
            st.table(df)
    else:
        with placeholder.container():
            st.info("Waiting for data from Flink pipeline... (Make sure Flink job is running at localhost:8081)")

    # Auto rerun the page smoothly every 1 second to pull new items from consumer
    time.sleep(1)
    st.rerun()