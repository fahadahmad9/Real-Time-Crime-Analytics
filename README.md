# Real-Time Crime Analytics & Intelligent Alert System

Big Data course project (team of 2, FAST-NUCES). A Lambda Architecture pipeline over Chicago public safety data: real-time anomaly alerts plus batch analytics and hotspot detection.

## Architecture

- **Batch layer (Spark):** Explicit StructType schemas across 5 Chicago datasets (crimes, arrests, violence, sex offenders, police stations). Computes crime trends, arrest rates, cross-dataset joins and correlations, and K-Means (k=10) geospatial hotspots.
- **Speed layer (Kafka + Storm):** A Kafka producer replays crime records as a live feed. A Storm topology (Parse → District → Window → Anomaly → Alert bolts) detects per-district spikes in sliding windows.
- **Serving layer:** PostgreSQL (trends, hotspots, correlations, alerts) and MongoDB (raw events, alert documents).
- **Dashboard:** Streamlit (live alerts, trends, hotspot map).
- **Infra:** Docker Compose (Zookeeper, Kafka, Spark master + worker, Storm Nimbus/Supervisor/UI, PostgreSQL, MongoDB).

## Tech Stack

Python, PySpark (MLlib), Apache Kafka, Apache Storm, PostgreSQL, MongoDB, Streamlit, Docker Compose

## Run

```bash
cd docker
docker compose up -d
```

Then: [producer command], [Storm topology submit command], [Spark job command], [dashboard command].

Settings (Kafka topic, window size, anomaly threshold, DB credentials) live in `config/config.yaml`. Place the datasets in `data/` (see Chicago Open Data Portal).

## Demo

Google Drive: https://drive.google.com/drive/folders/121TH0CZY1ug0H_ADs68gWcJB3BEt43wB?usp=sharing
