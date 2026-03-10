# 🚗 Car Booking Data Pipeline

A production-grade end-to-end data engineering pipeline for real-time car booking analytics — built with a **Medallion Architecture** (Bronze → Silver → Curated) using Apache Kafka, Apache Spark, Delta Lake, and a real-time monitoring dashboard.

---

## 📐 Architecture

```
[car_booking.json]
       │
       ▼
[Kafka Producer] ──── Schema Registry (Avro validation)
       │
       ▼
[Apache Kafka]  (topic: car-bookings, 3 partitions)
       │
       ▼
[Spark Ingest]  ──────────────── Bronze Layer (Delta Lake / MinIO)
                                  s3a://delta/raw/  (partitioned: year/month/day)
       │
       ▼
[Spark Transform]  ────────────  Silver Layer (Delta Lake / MinIO)
  - transform_customer.py         s3a://delta/transformed/customer/
  - transform_booking.py          s3a://delta/transformed/booking/  (partitioned: year/month)
  - transform_cars.py             s3a://delta/transformed/cars/
  - transform_payments.py         s3a://delta/transformed/payments/
       │
       ▼
[Spark Merge]  ────────────────  Curated Layer (Delta Lake / MinIO)
  - merge_data.py                 s3a://delta/curated/customer_booking/
       │
       ▼
[Write PostgreSQL]  ───────────  Serving Layer
  - write_postgres.py             customer_booking_staging
       │
       ▼
[Great Expectations]  ─────────  Data Quality Validation
  - 27+ expectations              Results → MySQL
       │
       ▼
[Real-time Dashboard]  ────────  http://localhost:8050
  - 6 tabs, 50+ charts
  - 30s auto-refresh
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka + Confluent Schema Registry |
| Processing | Apache Spark 3.4.2 + Delta Lake |
| Storage | MinIO (S3-compatible) |
| Serving | PostgreSQL 15 |
| Monitoring | MySQL 8 |
| Validation | Great Expectations 1.6.3 |
| Dashboard | Plotly Dash |
| CI/CD | Jenkins |
| Containerization | Docker + Docker Compose |

---

## 📁 Project Structure

```
car-booking-pipeline/
├── config/
│   └── config.json                    ← Single source of truth (all configs)
├── kafka/
│   ├── producer_with_schema_registry.py
│   └── data/
│       └── car_booking.json
├── spark/
│   ├── jobs/
│   │   ├── ingest_stream.py           ← Bronze layer
│   │   ├── transform_customer.py      ← Silver layer
│   │   ├── transform_booking.py       ← Silver layer
│   │   ├── transform_cars.py          ← Silver layer
│   │   ├── transform_payments.py      ← Silver layer
│   │   ├── merge_data.py              ← Curated layer
│   │   └── write_postgres.py          ← Serving layer
│   └── lib/
│       └── config_loader.py
├── great_expectations/
│   ├── jobs/
│   │   └── run_ge.py
│   ├── great_expectations/
│   │   └── expectations/
│   │       └── customer_booking_suite.json
│   ├── Dockerfile
│   └── great_expectations.yml
├── realtime_dashboard/
│   ├── dashboard.py
│   └── Dockerfile
├── sql/
│   ├── mysql.sql                      ← Monitoring tables
│   └── postgres.sql                   ← Staging table
├── jenkins/
│   ├── Jenkinsfile
│   └── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## ⚡ Key Features

### Medallion Architecture
- **Bronze** — Raw data from Kafka, partitioned by `year/month/day`
- **Silver** — Cleaned + transformed data with business logic per domain
- **Curated** — Merged master dataset ready for analytics

### Idempotency (MERGE)
Every Spark job uses Delta Lake `MERGE` — running the pipeline multiple times produces the same result, no duplicates.

### Business Transformations
| File | Key Transformations |
|---|---|
| `transform_customer.py` | email_domain, loyalty_rank, is_high_value_customer, points_tier |
| `transform_booking.py` | pickup_hour (24hr), pickup_slot, trip_type, route |
| `transform_cars.py` | price_tier, car_segment, insurer_type, has_full_insurance |
| `transform_payments.py` | payment_method normalize, amount_tier, is_split_payment, is_digital_payment |

### Data Quality (Great Expectations)
- 27+ expectations across all domains
- Results stored in MySQL — visible on dashboard
- DQ score alert if below 80%

### Real-time Dashboard (6 tabs)
- **Overview** — 7 KPIs, revenue trend, booking heatmap
- **Analytics** — Model scatter, location analysis, loyalty revenue
- **Observability** — Pipeline health monitoring
- **Pipeline** — SLA tracking, alerts, schema registry log
- **Data Quality** — GE validation results, failed records
- **Debug** — Raw data explorer

### CI/CD (Jenkins)
- Full pipeline: Cleanup → Start → Schema Register → Produce → Ingest → Transform → Merge → Write → Validate → Dashboard
- Email alerts on success and failure
- Per-stage duration tracking in MySQL

---

## 🚀 How to Run

### Prerequisites
- Docker Desktop
- Jenkins (with Docker pipeline plugin)
- Git

### Steps

**1. Clone the repo**
```bash
git clone https://github.com/your-username/car-booking-pipeline.git
cd car-booking-pipeline
```

**2. Update config**
```json
// config/config.json
{
  "project": {
    "alert_email": "your-email@gmail.com"   ← apna email
  }
}
```

**3. Run via Jenkins**
- Open Jenkins → `http://localhost:8080`
- New Pipeline → Point to this repo
- Build Now ✅

**4. Access services**

| Service | URL |
|---|---|
| Dashboard | http://localhost:8050 |
| Kafka UI | http://localhost:8081 |
| Schema Registry | http://localhost:8085 |
| MinIO | http://localhost:9001 |
| Spark UI | http://localhost:8082 |
| Jenkins | http://localhost:8080 |

---

## 🗄️ Database Schema

### MySQL (Monitoring)
```
pipeline_run_stats        ← Per-stage duration + records
pipeline_alerts           ← SUCCESS / FAILURE / DQ_WARNING alerts
schema_registry_log       ← Schema versions + compatibility
validation_results        ← GE suite-level results
validation_expectation_details ← Per-expectation results
validation_failed_records ← Failed record details
```

### PostgreSQL (Serving)
```
customer_booking_staging  ← Final merged dataset for dashboard
```

---

## 📊 Pipeline Flow (Jenkins Stages)

```
1.  Checkout Code
2.  Cleanup              → Fresh start, volumes deleted
3.  Start Containers     → All services up
4.  Start Schema Registry
5.  Initialize Databases → mysql.sql + postgres.sql
6.  Schema Registration  → car-bookings topic + Avro schema
7.  Kafka Producer       → car_booking.json → Kafka
8.  Setup MinIO Buckets
9.  Spark Ingest Stream  → Bronze layer
10. Transform Data       → Silver layer (4 jobs)
11. Merge Data           → Curated layer
12. Write PostgreSQL     → Staging table
13. Data Validation      → Great Expectations
14. Start Dashboard      → http://localhost:8050
```

---

## 🔧 Configuration

All configs centralized in `config/config.json`:

```json
{
  "minio":      { "endpoint", "access_key", "secret_key" },
  "kafka":      { "bootstrap_servers", "topic", "schema_registry_url" },
  "postgresql": { "jdbc_url", "user", "password" },
  "mysql":      { "jdbc_url", "user", "password" },
  "spark":      { "packages", "driver_memory", "executor_memory" },
  "delta_paths":{ "raw", "transformed/*", "merged" },
  "pipeline":   { "dq_threshold", "kafka_wait_seconds" }
}
```