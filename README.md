# Real-Time ML Feature Engineering Pipeline

A production-grade, real-time feature engineering pipeline built with **Apache Kafka**, **Apache Flink (PyFlink)**, and **Streamlit**. This system ingests high-velocity user interaction streams, computes ML features using event-time windowing, and serves them to a live monitoring dashboard — all orchestrated with a single Docker Compose command.

---

## Architecture Overview

```
┌─────────────────┐     user-events      ┌─────────────────────┐
│  Data Producer  │ ──────────────────▶  │                     │
│  (Python)       │                      │   Apache Kafka      │
│                 │ ──────────────────▶  │   (Event Bus)       │
└─────────────────┘  content-metadata    │                     │
                                         └──────────┬──────────┘
                                                    │
                                         ┌──────────▼──────────┐
                                         │   Apache Flink      │
                                         │   (PyFlink Job)     │
                                         │                     │
                                         │  • Tumbling Windows │
                                         │  • Sliding Windows  │
                                         │  • Stream Joins     │
                                         │  • Watermarking     │
                                         └──────────┬──────────┘
                                                    │ feature-store
                                         ┌──────────▼──────────┐
                                         │  Streamlit Dashboard│
                                         │  (localhost:8501)   │
                                         └─────────────────────┘
```

### Services

| Service | Image | Purpose |
|---|---|---|
| `zookeeper` | confluentinc/cp-zookeeper:7.4.0 | Kafka coordination |
| `kafka` | confluentinc/cp-kafka:7.4.0 | Event streaming backbone |
| `kafka-setup` | confluentinc/cp-kafka:7.4.0 | Creates topics on startup |
| `flink-jobmanager` | flink:1.17.1-scala_2.12 | Flink cluster coordinator |
| `flink-taskmanager` | flink:1.17.1-scala_2.12 | Flink task execution |
| `flink-pipeline` | Custom PyFlink image | Submits and runs feature job |
| `producer` | Custom Python image | Simulates user events |
| `dashboard` | Custom Streamlit image | Live feature monitoring UI |

---

## Kafka Topics

| Topic | Partitions | Cleanup Policy | Purpose |
|---|---|---|---|
| `user-events` | 3 | delete | Raw user interaction stream |
| `content-metadata` | 1 | compact | Content lookup table (KTable) |
| `feature-store` | 1 | compact | Computed ML features (key-value store) |

---

## Message Schemas

### user-events
```json
{
  "user_id": "u001",
  "content_id": "c001",
  "event_type": "click",
  "dwell_time_ms": 3200,
  "timestamp": "2026-05-24T10:00:00Z"
}
```

### content-metadata (key: content_id)
```json
{
  "content_id": "c001",
  "category": "Technology",
  "creator_id": "cr01",
  "publish_timestamp": "2024-01-15T09:00:00Z"
}
```

### feature-store (key: entity_id:feature_name)
```json
{
  "entity_id": "u001:click_rate",
  "feature_name": "click_rate",
  "feature_value": "0.35",
  "computed_at": "2026-05-24 10:59:59"
}
```

---

## Computed Features

| Feature | Window Type | Size | Slide | Description |
|---|---|---|---|---|
| `click_rate` | Tumbling | 1 hour | — | Fraction of click events per user |
| `avg_dwell_time` | Tumbling | 1 hour | — | Average dwell_time_ms per user |
| `engagement_rate` | Sliding (HOP) | 15 min | 5 min | (likes+shares)/views per content |
| `category_affinity_score` | Tumbling | 1 hour | — | Event count per user per category |

---

## Prerequisites

- Docker Desktop (v4.0+) with Docker Compose v2
- At least **6 GB RAM** allocated to Docker
- On Windows: WSL2 enabled

### Windows WSL2 Memory Configuration

Create or edit `%USERPROFILE%\.wslconfig`:
```ini
[wsl2]
memory=6GB
processors=4
```

Then restart WSL2:
```cmd
wsl --shutdown
```

---

## Quick Start

### Step 1 — Clone the repository
```bash
git clone <your-repo-url>
cd pipeline-kafka-apachaflink
```

### Step 2 — Configure environment variables
```bash
cp .env.example .env
```

The defaults work out of the box for local Docker deployment. Edit `.env` only if you need custom ports or external Kafka.

### Step 3 — Verify the Kafka connector JAR is present
```
flink/
└── lib/
    └── flink-sql-connector-kafka-3.0.1-1.17.jar   ← must exist
```

If missing, download it:
```bash
# Linux / macOS / WSL2
mkdir -p flink/lib
curl -L -o flink/lib/flink-sql-connector-kafka-3.0.1-1.17.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.17/flink-sql-connector-kafka-3.0.1-1.17.jar

# Windows CMD
mkdir flink\lib
curl -L -o flink\lib\flink-sql-connector-kafka-3.0.1-1.17.jar ^
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.17/flink-sql-connector-kafka-3.0.1-1.17.jar
```

### Step 4 — Build and launch the full stack
```bash
docker-compose up --build -d
```

Allow **3–5 minutes** for all services to reach healthy status.

### Step 5 — Verify all services are healthy
```bash
docker-compose ps
```

Expected output:
```
NAME                                STATUS
pipeline-...-zookeeper-1            Up (healthy)
pipeline-...-kafka-1                Up (healthy)
pipeline-...-kafka-setup-1          Exited (0)      ← normal, runs once
pipeline-...-flink-jobmanager-1     Up (healthy)
pipeline-...-flink-taskmanager-1    Up (healthy)
pipeline-...-flink-pipeline-1       Up
pipeline-...-producer-1             Up (healthy)
pipeline-...-dashboard-1            Up (healthy)
```

### Step 6 — Confirm features are being computed
```bash
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic feature-store \
  --from-beginning
```

You should see JSON feature records within 60–90 seconds (after the first tumbling window fires).

### Step 7 — Open the interfaces

| Interface | URL |
|---|---|
| **Streamlit Dashboard** | http://localhost:8501 |
| **Flink Web UI** | http://localhost:8081 |

---

## Using the Dashboard

### Entity Feature Viewer
1. Open http://localhost:8501
2. Scroll to **Entity Feature Viewer**
3. Enter a user ID (e.g., `u001`) or content ID (e.g., `c001`)
4. Click **Search** to see all latest feature values for that entity

### Pipeline Health Metrics
The top section displays:
- **Total Features Computed** — cumulative count of feature records written
- **Late Events Dropped** — events that arrived after their window closed
- **Watermark Lag** — estimated lag between wall-clock and pipeline event-time
- **Last Feature Update** — timestamp of the most recent feature write

### Feature Freshness
Shows time-since-last-update for `click_rate`, `avg_dwell_time`, `engagement_rate`, and `category_affinity_score`. Green = fresh (<2 min), orange = stale.

---

## Configuration Reference

All configuration is managed via environment variables. See `.env.example` for the full list.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address (internal) |
| `USER_EVENTS_TOPIC` | `user-events` | Topic for raw user interactions |
| `METADATA_TOPIC` | `content-metadata` | Topic for content metadata |
| `FEATURE_STORE_TOPIC` | `feature-store` | Topic for computed features |
| `FLINK_JOBMANAGER_HOST` | `flink-jobmanager` | Flink JobManager hostname |
| `FLINK_JOBMANAGER_PORT` | `8081` | Flink REST API port |

---

## Data Producer Details

The producer (`producer/producer.py`) simulates realistic user behaviour:

### User Archetypes
| Archetype | Behaviour |
|---|---|
| `binge_watcher` | High view/like rate, long dwell time (3–8s) |
| `news_scanner` | High click rate, short dwell time (0.5–2s) |
| `social_sharer` | High share rate, medium dwell time (1–4s) |
| `casual_browser` | Mixed events, medium dwell time (0.8–3s) |

### Late Event Simulation
Exactly **5% of events** are sent with timestamps 35–90 seconds in the past, simulating real-world network delays and producer-side buffering. This is used to validate the Flink watermark strategy.

```python
is_late = random.random() < 0.05
if is_late:
    delay = timedelta(seconds=random.randint(35, 90))
    timestamp = datetime.now(timezone.utc) - delay
```

---

## Flink Pipeline Details

### Event-Time Processing
The pipeline uses **event-time semantics** — all aggregations are based on the `timestamp` field embedded in each Kafka message, not the wall-clock time when the message is processed. This ensures deterministic, reproducible results regardless of processing delays.

### Watermark Strategy
```sql
WATERMARK FOR row_time AS row_time - INTERVAL '30' SECOND
```
This tolerates up to 30 seconds of out-of-order arrival. Events more than 30 seconds late are dropped and counted in the "Late Events Dropped" metric.

### Window Definitions
```sql
-- Tumbling window (user features)
GROUP BY user_id, TUMBLE(row_time, INTERVAL '1' HOUR)

-- Sliding/Hopping window (content engagement)
GROUP BY content_id, HOP(row_time, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)
```

---

## Troubleshooting

### Features not appearing after 2 minutes
```bash
# Check producer is sending events
docker-compose logs producer

# Check Flink pipeline for errors
docker-compose logs flink-pipeline

# Verify topics exist and have messages
docker-compose exec kafka kafka-topics --bootstrap-server localhost:29092 --list
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 --topic user-events --max-messages 5
```

### Docker memory errors on Windows
```cmd
wsl --shutdown
# Then increase memory in %USERPROFILE%\.wslconfig and restart Docker Desktop
```

### Full clean restart
```bash
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
```

### Check individual service health
```bash
docker-compose logs zookeeper
docker-compose logs kafka
docker-compose logs flink-jobmanager
docker-compose logs flink-pipeline
```

---

## Submission Details

See `submission.json` for the test entity IDs used in automated evaluation:
```json
{
  "test_user_id": "u001",
  "test_content_id": "c001"
}
```

These IDs are actively generated by the producer and will have feature values visible in both the `feature-store` Kafka topic and the dashboard within 90 seconds of startup.

---

## Project Structure

```
pipeline-kafka-apachaflink/
├── docker-compose.yml          # Full stack orchestration
├── .env.example                # Environment variable documentation
├── README.md                   # This file
├── ANALYSIS.md                 # Batch vs streaming analysis + late event report
├── submission.json             # Evaluation configuration
├── flink/
│   ├── Dockerfile              # PyFlink 1.17.1 runtime image
│   ├── pipeline.py             # Feature engineering job (4 features)
│   └── lib/
│       └── flink-sql-connector-kafka-3.0.1-1.17.jar
├── producer/
│   ├── Dockerfile
│   ├── producer.py             # User event simulator with late events
│   └── requirements.txt
└── dashboard/
    ├── Dockerfile
    ├── app.py                  # Streamlit monitoring dashboard
    └── requirements.txt
```