import json
import time
import os
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Pre-load some metadata so the JOIN works
metadata = [
    {"content_id": "v1", "category": "Tech", "creator_id": "c1"},
    {"content_id": "v2", "category": "Music", "creator_id": "c2"},
    {"content_id": "v3", "category": "Edu", "creator_id": "c3"}
]

print("Sending Metadata...")
for m in metadata:
    producer.send('content-metadata', m)

print("Streaming User Events...")
while True:
    event = {
        "user_id": f"u{random.randint(1, 10)}",
        "content_id": f"v{random.randint(1, 3)}",
        "event_type": random.choice(["click", "view"]),
        "dwell_time_ms": random.randint(500, 5000),
        "ts_str": time.strftime('%Y-%m-%dT%H:%M:%SZ')
    }
    producer.send('user-events', event)
    time.sleep(0.5)