"""
FastAPI Layer — Car Booking Pipeline
=====================================
Production-grade API between Dashboard and PostgreSQL/MySQL
Endpoints:
  /api/health              → pipeline health score
  /api/bookings/summary    → KPIs
  /api/bookings/daily      → daily trend
  /api/bookings/segments   → car segments
  /api/bookings/payments   → payment breakdown
  /api/bookings/loyalty    → loyalty tiers
  /api/ml/forecast         → Prophet demand forecast
  /api/ml/fraud            → fraud scores
  /api/ml/churn            → churn predictions
  /api/pipeline/stats      → Jenkins stage stats
  /api/pipeline/alerts     → pipeline alerts
  /api/quality/results     → GE validation results
  /api/quality/failed      → GE failed records
"""

import os
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================================================================
# CONFIG
# ================================================================
PG_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER','admin')}:"
    f"{os.getenv('POSTGRES_PASSWORD','admin')}"
    f"@{os.getenv('POSTGRES_HOST','postgres')}:5432/"
    f"{os.getenv('POSTGRES_DB','booking')}"
)
MYSQL_URL = (
    f"mysql+pymysql://"
    f"{os.getenv('MYSQL_USER','admin')}:"
    f"{os.getenv('MYSQL_PASSWORD','admin')}"
    f"@{os.getenv('MYSQL_HOST','mysql')}:3306/"
    f"{os.getenv('MYSQL_DATABASE','booking')}"
)

# Singleton connection pools
_pg_engine    = None
_mysql_engine = None

def pg_engine():
    global _pg_engine
    if _pg_engine is None:
        _pg_engine = create_engine(PG_URL, pool_size=5, max_overflow=3,
                                    pool_pre_ping=True, pool_recycle=300)
    return _pg_engine

def mysql_engine():
    global _mysql_engine
    if _mysql_engine is None:
        _mysql_engine = create_engine(MYSQL_URL, pool_size=3, max_overflow=2,
                                       pool_pre_ping=True, pool_recycle=300)
    return _mysql_engine

def query_pg(sql: str) -> List[Dict]:
    try:
        df = pd.read_sql(sql, pg_engine())
        return df.to_dict("records")
    except Exception as e:
        logger.error(f"[PG] {e}")
        raise HTTPException(status_code=500, detail=str(e))

def query_mysql(sql: str) -> List[Dict]:
    try:
        df = pd.read_sql(sql, mysql_engine())
        return df.to_dict("records")
    except Exception as e:
        logger.error(f"[MySQL] {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ================================================================
# APP
# ================================================================
app = FastAPI(
    title="Car Booking Pipeline API",
    description="Production API layer — Kafka → Spark → Delta Lake → PostgreSQL → FastAPI → Dashboard",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ================================================================
# HEALTH
# ================================================================
@app.get("/api/health", tags=["health"])
def health():
    """Pipeline health score — weighted formula"""
    try:
        # DQ score from MySQL
        dq = query_mysql("""
            SELECT COALESCE(AVG(success_rate), 100) AS dq_score
            FROM validation_results
            ORDER BY created_at DESC LIMIT 5
        """)
        dq_score = float(dq[0]["dq_score"]) if dq else 100.0

        # Failed stages
        stats = query_mysql("""
            SELECT COUNT(*) AS failed
            FROM pipeline_run_stats
            WHERE status = 'FAILED'
            AND started_at >= NOW() - INTERVAL 24 HOUR
        """)
        failed_stages = int(stats[0]["failed"]) if stats else 0

        # Active alerts
        alerts = query_mysql("""
            SELECT COUNT(*) AS cnt
            FROM pipeline_alerts
            WHERE alert_type = 'FAILURE'
            AND created_at >= NOW() - INTERVAL 24 HOUR
        """)
        critical_alerts = int(alerts[0]["cnt"]) if alerts else 0

        # Weighted health score
        score = 100
        score -= failed_stages * 15
        score -= critical_alerts * 10
        score -= max(0, (90 - dq_score) * 0.5)
        score  = max(0, min(100, round(score)))

        status = "HEALTHY" if score >= 90 else ("WARNING" if score >= 70 else "CRITICAL")

        return {
            "health_status":   status,
            "health_score":    score,
            "dq_score":        round(dq_score, 2),
            "failed_stages":   failed_stages,
            "critical_alerts": critical_alerts,
            "timestamp":       datetime.now().isoformat(),
        }
    except Exception as e:
        return {"health_status": "UNKNOWN", "error": str(e)}


@app.get("/", tags=["health"])
def root():
    return {
        "service": "Car Booking Pipeline API",
        "version": "1.0.0",
        "docs":    "/docs",
        "status":  "running",
    }


# ================================================================
# BOOKINGS
# ================================================================
@app.get("/api/bookings/summary", tags=["bookings"])
def bookings_summary():
    """KPI summary — total bookings, revenue, customers"""
    return query_pg("""
        SELECT
            COUNT(*)                              AS total_bookings,
            COUNT(DISTINCT customer_id)           AS total_customers,
            ROUND(SUM(payment_amount)::numeric,0) AS total_revenue,
            SUM(is_high_value_customer::int)      AS high_value_customers,
            SUM(is_digital_payment::int)          AS digital_payments,
            COUNT(DISTINCT route)                 AS unique_routes,
            COUNT(DISTINCT car_segment)           AS car_segments,
            COUNT(DISTINCT model)                 AS car_models
        FROM customer_booking_staging
    """)


@app.get("/api/bookings/daily", tags=["bookings"])
def bookings_daily(days: int = Query(default=90, ge=7, le=365)):
    """Daily trend — bookings + revenue"""
    return query_pg(f"""
        SELECT
            booking_date::date                    AS booking_date,
            COUNT(*)                              AS bookings,
            ROUND(SUM(payment_amount)::numeric,0) AS revenue
        FROM customer_booking_staging
        WHERE booking_date >= CURRENT_DATE - INTERVAL '{days} days'
        GROUP BY booking_date::date
        ORDER BY booking_date
    """)


@app.get("/api/bookings/segments", tags=["bookings"])
def bookings_segments():
    """Car segment distribution"""
    return query_pg("""
        SELECT car_segment,
               COUNT(*) AS bookings,
               ROUND(AVG(payment_amount)::numeric,0) AS avg_payment
        FROM customer_booking_staging
        GROUP BY car_segment ORDER BY bookings DESC
    """)


@app.get("/api/bookings/payments", tags=["bookings"])
def bookings_payments():
    """Payment method breakdown"""
    return query_pg("""
        SELECT payment_method,
               COUNT(*) AS bookings,
               ROUND(SUM(payment_amount)::numeric,0) AS revenue,
               SUM(is_digital_payment::int) AS digital_count
        FROM customer_booking_staging
        GROUP BY payment_method ORDER BY bookings DESC
    """)


@app.get("/api/bookings/loyalty", tags=["bookings"])
def bookings_loyalty():
    """Loyalty tier breakdown"""
    return query_pg("""
        SELECT loyalty_tier,
               COUNT(*) AS bookings,
               SUM(is_high_value_customer::int) AS high_value,
               ROUND(AVG(loyalty_points)::numeric,0) AS avg_points
        FROM customer_booking_staging
        GROUP BY loyalty_tier ORDER BY bookings DESC
    """)


@app.get("/api/bookings/routes", tags=["bookings"])
def bookings_routes(limit: int = Query(default=10, ge=1, le=50)):
    """Top routes by booking volume"""
    return query_pg(f"""
        SELECT route,
               COUNT(*) AS bookings,
               ROUND(AVG(payment_amount)::numeric,0) AS avg_payment,
               COUNT(DISTINCT trip_type) AS trip_types
        FROM customer_booking_staging
        GROUP BY route ORDER BY bookings DESC LIMIT {limit}
    """)


# ================================================================
# ML
# ================================================================
@app.get("/api/ml/forecast", tags=["ml"])
def ml_forecast():
    """Prophet demand forecast — 30 days"""
    try:
        return query_pg("""
            SELECT forecast_date, predicted_bookings,
                   lower_bound, upper_bound, mape
            FROM ml_demand_forecast
            ORDER BY forecast_date
        """)
    except Exception:
        return []


@app.get("/api/ml/fraud", tags=["ml"])
def ml_fraud(risk_label: Optional[str] = None, limit: int = Query(default=100, le=500)):
    """Fraud scores — filter by risk_label"""
    where = f"WHERE risk_label = '{risk_label}'" if risk_label else ""
    try:
        return query_pg(f"""
            SELECT booking_id, customer_id, customer_name,
                   route, trip_type, payment_amount,
                   fraud_risk_score, risk_label, pickup_hour
            FROM ml_fraud_scores
            {where}
            ORDER BY fraud_risk_score DESC LIMIT {limit}
        """)
    except Exception:
        return []


@app.get("/api/ml/fraud/summary", tags=["ml"])
def ml_fraud_summary():
    """Fraud risk distribution summary"""
    try:
        return query_pg("""
            SELECT risk_label,
                   COUNT(*) AS count,
                   ROUND(AVG(fraud_risk_score)::numeric,2) AS avg_score
            FROM ml_fraud_scores
            GROUP BY risk_label ORDER BY avg_score DESC
        """)
    except Exception:
        return []


@app.get("/api/ml/churn", tags=["ml"])
def ml_churn(risk: Optional[str] = None, limit: int = Query(default=100, le=500)):
    """Churn predictions"""
    where = f"WHERE churn_risk = '{risk}'" if risk else ""
    try:
        return query_pg(f"""
            SELECT customer_id, customer_name, loyalty_tier,
                   churn_probability, churn_risk, model_auc
            FROM ml_churn_scores
            {where}
            ORDER BY churn_probability DESC LIMIT {limit}
        """)
    except Exception:
        return []


@app.get("/api/ml/churn/summary", tags=["ml"])
def ml_churn_summary():
    """Churn risk distribution"""
    try:
        return query_pg("""
            SELECT churn_risk,
                   COUNT(*) AS customers,
                   ROUND(AVG(churn_probability)::numeric,3) AS avg_prob,
                   ROUND(AVG(model_auc)::numeric,3) AS model_auc
            FROM ml_churn_scores
            GROUP BY churn_risk ORDER BY avg_prob DESC
        """)
    except Exception:
        return []


# ================================================================
# PIPELINE
# ================================================================
@app.get("/api/pipeline/stats", tags=["pipeline"])
def pipeline_stats(limit: int = Query(default=30, le=100)):
    """Pipeline stage stats from MySQL"""
    return query_mysql(f"""
        SELECT stage_name, status, duration_seconds,
               records_processed, started_at
        FROM pipeline_run_stats
        ORDER BY started_at DESC LIMIT {limit}
    """)


@app.get("/api/pipeline/alerts", tags=["pipeline"])
def pipeline_alerts(hours: int = Query(default=24, ge=1, le=168)):
    """Pipeline alerts — last N hours"""
    return query_mysql(f"""
        SELECT alert_type, pipeline_name, stage_name,
               error_message, created_at
        FROM pipeline_alerts
        WHERE created_at >= NOW() - INTERVAL {hours} HOUR
        ORDER BY created_at DESC LIMIT 50
    """)


@app.get("/api/pipeline/runs", tags=["pipeline"])
def pipeline_runs():
    """Distinct pipeline runs"""
    return query_mysql("""
        SELECT run_id,
               MIN(started_at) AS started,
               MAX(started_at) AS last_stage,
               COUNT(*) AS total_stages,
               SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS passed,
               SUM(CASE WHEN status='FAILED'  THEN 1 ELSE 0 END) AS failed,
               SUM(duration_seconds) AS total_duration_s
        FROM pipeline_run_stats
        GROUP BY run_id ORDER BY started DESC LIMIT 10
    """)


# ================================================================
# DATA QUALITY
# ================================================================
@app.get("/api/quality/results", tags=["quality"])
def quality_results():
    """GE validation results"""
    return query_mysql("""
        SELECT run_identifier, total_expectations,
               passed_expectations, failed_expectations,
               success_rate, validation_status, created_at
        FROM validation_results
        ORDER BY created_at DESC LIMIT 10
    """)


@app.get("/api/quality/failed", tags=["quality"])
def quality_failed(limit: int = Query(default=100, le=500)):
    """GE failed records with column details"""
    return query_mysql(f"""
        SELECT run_identifier, expectation_type, column_name,
               failed_reason, booking_id, customer_name,
               loyalty_tier, payment_amount
        FROM validation_failed_records
        ORDER BY run_identifier DESC LIMIT {limit}
    """)


@app.get("/api/quality/failed/summary", tags=["quality"])
def quality_failed_summary():
    """Top failing columns"""
    return query_mysql("""
        SELECT column_name, expectation_type,
               COUNT(*) AS failure_count
        FROM validation_failed_records
        WHERE run_identifier = (
            SELECT run_identifier FROM validation_results
            ORDER BY created_at DESC LIMIT 1
        )
        GROUP BY column_name, expectation_type
        ORDER BY failure_count DESC LIMIT 10
    """)