import json
import time
import os
import random
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
 
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
 
# ---- User archetypes for realistic simulation ----
ARCHETYPES = {
    "binge_watcher":  {"event_weights": {"view": 0.6, "like": 0.2, "share": 0.1, "click": 0.1}, "dwell_range": (3000, 8000)},
    "news_scanner":   {"event_weights": {"click": 0.5, "view": 0.4, "like": 0.05, "share": 0.05}, "dwell_range": (500, 2000)},
    "social_sharer":  {"event_weights": {"share": 0.4, "like": 0.3, "view": 0.2, "click": 0.1}, "dwell_range": (1000, 4000)},
    "casual_browser": {"event_weights": {"view": 0.5, "click": 0.3, "like": 0.1, "share": 0.1}, "dwell_range": (800, 3000)},
}
 
# Fixed user IDs mapped to archetypes (deterministic so test_user_id is stable)
USERS = {
    "u001": "binge_watcher",
    "u002": "news_scanner",
    "u003": "social_sharer",
    "u004": "casual_browser",
    "u005": "binge_watcher",
    "u006": "news_scanner",
    "u007": "social_sharer",
    "u008": "casual_browser",
    "u009": "binge_watcher",
    "u010": "casual_browser",
}
 
CONTENT_IDS = ["c001", "c002", "c003", "c004", "c005"]
 
CONTENT_METADATA = [
    {"content_id": "c001", "category": "Technology", "creator_id": "cr01",
     "publish_timestamp": "2024-01-15T09:00:00Z"},
    {"content_id": "c002", "category": "Music",      "creator_id": "cr02",
     "publish_timestamp": "2024-01-15T10:00:00Z"},
    {"content_id": "c003", "category": "Education",  "creator_id": "cr03",
     "publish_timestamp": "2024-01-15T11:00:00Z"},
    {"content_id": "c004", "category": "Sports",     "creator_id": "cr04",
     "publish_timestamp": "2024-01-15T12:00:00Z"},
    {"content_id": "c005", "category": "Technology", "creator_id": "cr01",
     "publish_timestamp": "2024-01-15T13:00:00Z"},
]
 
 
def wait_for_kafka(servers: str, retries: int = 30, delay: int = 5) -> KafkaProducer:
    """Retry connecting to Kafka until it's ready."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            print(f"✅ Connected to Kafka at {servers}")
            return producer
        except NoBrokersAvailable:
            print(f"⏳ Kafka not ready... attempt {attempt + 1}/{retries}")
            time.sleep(delay)
    raise RuntimeError(f"Cannot connect to Kafka at {servers}")
 
 
def weighted_choice(weights: dict) -> str:
    """Pick a key from a dict of {item: weight}."""
    items = list(weights.keys())
    probs = list(weights.values())
    return random.choices(items, weights=probs, k=1)[0]
 
 
def make_timestamp(late: bool = False) -> str:
    """
    Returns ISO 8601 timestamp.
    If late=True, timestamp is 35–90 seconds in the past (simulates late events).
    Accelerated time: wall-clock 1 second = simulated 60 seconds.
    """
    now = datetime.now(timezone.utc)
    if late:
        # Deliberately late: 35–90 seconds behind current event time
        delay = timedelta(seconds=random.randint(35, 90))
        ts = now - delay
    else:
        ts = now
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
 
 
def main():
    producer = wait_for_kafka(KAFKA_SERVERS)
 
    # --- 1. Publish content metadata (keyed by content_id for compaction) ---
    print("📤 Publishing content metadata...")
    for meta in CONTENT_METADATA:
        producer.send(
            "content-metadata",
            key=meta["content_id"],
            value=meta,
        )
    producer.flush()
    print(f"✅ Published {len(CONTENT_METADATA)} metadata records.")
 
    # --- 2. Stream user events continuously ---
    print("📡 Streaming user events (Ctrl+C to stop)...")
    event_count = 0
    late_count = 0
 
    user_ids = list(USERS.keys())
 
    while True:
        user_id = random.choice(user_ids)
        content_id = random.choice(CONTENT_IDS)
        archetype = ARCHETYPES[USERS[user_id]]
 
        event_type = weighted_choice(archetype["event_weights"])
        dwell_time = random.randint(*archetype["dwell_range"])
 
        # 5% of events are deliberately late (35–90 seconds behind)
        is_late = random.random() < 0.05
        timestamp = make_timestamp(late=is_late)
 
        event = {
            "user_id":       user_id,
            "content_id":    content_id,
            "event_type":    event_type,
            "dwell_time_ms": dwell_time,
            "timestamp":     timestamp,
        }
 
        producer.send("user-events", key=user_id, value=event)
        event_count += 1
        if is_late:
            late_count += 1
 
        if event_count % 100 == 0:
            late_pct = (late_count / event_count) * 100
            print(
                f"📊 Sent {event_count} events | "
                f"Late: {late_count} ({late_pct:.1f}%) | "
                f"Last: {event_type} by {user_id} on {content_id}"
            )
 
        # 0.2s sleep → ~5 events/sec; with 60x accelerated time that's
        # 300 simulated events/minute, enough to fill 1-hour windows quickly.
        time.sleep(0.2)
 
 
if __name__ == "__main__":
    main()