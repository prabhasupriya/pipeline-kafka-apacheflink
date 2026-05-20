# MNC Real-Time Feature Engineering Pipeline (Kafka + Apache Flink + Streamlit)

A production-ready, distributed streaming architecture for real-time feature engineering. This pipeline ingests high-velocity user activity streams and content metadata from Apache Kafka, processes time-collapsed aggregate metrics using Apache Flink's PyFlink Table API, and surfaces computed features directly into an interactive Streamlit monitoring interface.

The system uses a standalone Python engine container (`flink-pipeline`) to natively submit job topologies directly to an active Flink processing cluster via the Flink Command Line Interface client. This architecture avoids resource-heavy local graph compilation inside the Python wrapper script, preventing host systems from locking up or running out of memory.

### Active Pipelines & Aggregations
* **User Average Dwell Time:** Computes average engagement duration (`dwell_time_ms`) across tumbling 1-minute time windows.
* **Content Popularity Score:** Tracks event volume velocity across hopping windows (30-second sliding step, 1-minute duration).
* **Cross-Stream Enrichments:** Joins real-time clickstreams with compacted content dimension metadata to map trending categories.

---

## 1. Project Directory Structure

Ensure your project directory looks exactly like this on your local system before initiating builds:

```text
pipeline-kafka-apachaflink/
├── docker-compose.yml       # Complete multi-container environment orchestration
├── flink/
│   ├── Dockerfile           # Optimized PyFlink runtime image (skips on-the-fly compilation)
│   ├── pipeline.py          # Native PyFlink stream transformations & table queries
│   └── flink-sql-connector-kafka-3.0.1-1.17.jar  # Distributed Kafka client driver
└── dashboard/
    ├── Dockerfile           # Streamlit UI runtime definition
    └── app.py               # Live-updating feature store validation dashboard
```
## 1. System Requirements & Hardware Allocation

* **Docker Desktop & Docker Compose** (v2.0+)
* **Windows Subsystem for Linux (WSL2)** with at least 6GB allocated RAM.

### WSL2 Resource Configuration
To prevent `Insufficient system resources` or `WSL EOF` crash loops during image creation, specify hard thresholds inside your Windows host system memory limits. Create or update your `%USERPROFILE%\.wslconfig` file:

```text
[wsl2]
memory=6GB
processors=4
```
## 1. Deployment & Execution Steps

### Step 1: Recover Frozen Background Engines (If locked up)
If the system throws `Insufficient system resources exist to complete the requested service`, terminate the leaked virtual interfaces completely:

```cmd
wsl --shutdown
## 1. Deployment & Execution Steps
```
### Step 2: Clear Stale Container Caches and Volumes
Wipe old data definitions, hanging cache volumes, and stopped processes completely before generating the fresh stack:

```bash
docker-compose down --remove-orphans
docker system prune -a --volumes -f
```
### Step 3: Build and Launch Cluster Runtime
Build the modified, high-speed Python image layers and execute all target tracking modules in background mode:

```bash
docker-compose up -d --build
```
### Step 4: Track Active Engine Interoperability
Ensure every required service container is operating healthily and passing operational loop requirements:

```bash
docker-compose ps
```
## 1. Port Mappings & Service Verification

Once verification commands indicate complete startup states, check the pipeline through the following interfaces:

| Service Interface | External URL | Internal Endpoint Alias | Purpose |
| :--- | :--- | :--- | :--- |
| **Flink Dashboard** | [http://localhost:8081](http://localhost:8081) | `flink-jobmanager:8081` | Monitor active streaming jobs, pipeline status, and task assignments. |
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | `dashboard:8501` | Live monitoring UI for validated real-time user metrics. |
| **Kafka Broker** | `localhost:9092` | `kafka:29092` | Core distributed pub-sub stream coordinator. |

---

##  Troubleshooting Guide

### Issue: "0 Running Jobs" but TaskManager is Active
* **Cause:** The pipeline wrapper script executed standard native initialization code inside a standalone process environment rather than through the Flink driver interface. This causes local dependency lookup failures before compilation ends.
* **Fix:** Use the Flink execution command framework natively mapped inside the `docker-compose.yml` service specification block:
  ```bash
  flink run -m flink-jobmanager:8081 -C file:///opt/flink/usrlib/flink-sql-connector-kafka-3.0.1-1.17.jar -py pipeline.py
  ```
  * **youtude video-[watch here](https://youtu.be/XKDj9lEk0SQ)