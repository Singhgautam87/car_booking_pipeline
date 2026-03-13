"""
AI Agents — Car Booking Pipeline v2
=====================================
FIXES in this version:
  - FIX 1: Prophet date range fix — future forecast dates se match hoga
  - FIX 2: Fraud rate threshold — 10% → 30% (realistic)
  - FIX 3: MLflow health check — requests se pehle check karo
  - FIX 4: pd.read_sql warnings fix — SQLAlchemy engine use karo

PostgreSQL table — customer_booking_staging (36 columns):
  booking_id, customer_id, customer_name, email,
  loyalty_tier, loyalty_points, booking_date,
  pickup_location, pickup_time, drop_location, drop_time,
  pickup_hour, pickup_slot, trip_type, route,
  booking_year, booking_month, email_domain,
  loyalty_rank, is_high_value_customer, points_tier,
  car_id, model, price_per_day, insurance_provider,
  insurance_coverage, price_tier, has_full_insurance,
  insurer_type, car_segment,
  payment_id, payment_method, payment_amount,
  is_digital_payment, amount_tier, payment_count, is_split_payment

MySQL tables:
  pipeline_run_stats, pipeline_alerts, schema_registry_log,
  validation_results, validation_expectation_details, validation_failed_records

ML tables (PostgreSQL — from ml_models.py):
  ml_demand_forecast, ml_fraud_scores, ml_churn_scores
"""

import os
import sys
import json
import time
import logging
import psycopg2
import pymysql
import pandas as pd
from datetime import datetime
from typing import Optional

# ✅ FIX 4: SQLAlchemy engine import
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Config loader ───────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config       = load_config()
postgres_cfg = config.get_postgres_config()
mysql_cfg    = config.get_mysql_config()
tables       = config.get_tables()

ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
LANGSMITH_API_KEY   = os.getenv("LANGSMITH_API_KEY", "")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
STAGING_TABLE       = tables.get("staging", "customer_booking_staging")

PG_CONN_PARAMS = {
    "host":     postgres_cfg["host"],
    "port":     postgres_cfg["port"],
    "database": postgres_cfg["database"],
    "user":     postgres_cfg["user"],
    "password": postgres_cfg["password"],
}
MYSQL_CONN_PARAMS = {
    "host":     mysql_cfg["host"],
    "port":     int(mysql_cfg["port"]),
    "database": mysql_cfg["database"],
    "user":     mysql_cfg["user"],
    "password": mysql_cfg["password"],
}
PG_SQLALCHEMY_URL = (
    f"postgresql+psycopg2://{postgres_cfg['user']}:{postgres_cfg['password']}"
    f"@{postgres_cfg['host']}:{postgres_cfg['port']}/{postgres_cfg['database']}"
)

# ✅ FIX 4: SQLAlchemy engine — pd.read_sql warnings hatega
PG_ENGINE = create_engine(PG_SQLALCHEMY_URL)

def get_pg_conn():    return psycopg2.connect(**PG_CONN_PARAMS)
def get_mysql_conn(): return pymysql.connect(**MYSQL_CONN_PARAMS)


# ══════════════════════════════════════════════════════════════════
# LANGSMITH SETUP
# ══════════════════════════════════════════════════════════════════
def _setup_langsmith():
    if not LANGSMITH_API_KEY:
        logger.info("[LangSmith] Not configured — set LANGSMITH_API_KEY for LLM monitoring")
        return False
    try:
        os.environ["LANGCHAIN_TRACING_V2"]  = "true"
        os.environ["LANGCHAIN_API_KEY"]     = LANGSMITH_API_KEY
        os.environ["LANGCHAIN_PROJECT"]     = "car-booking-pipeline"
        os.environ["LANGCHAIN_ENDPOINT"]    = "https://api.smith.langchain.com"
        logger.info("[LangSmith] ✓ LLM monitoring enabled — traces at smith.langchain.com")
        return True
    except Exception as e:
        logger.warning(f"[LangSmith] Setup failed: {e}")
        return False

LANGSMITH_ENABLED = _setup_langsmith()


# ══════════════════════════════════════════════════════════════════
# MLFLOW SETUP
# ✅ FIX 3: Connection se pehle health check karo
# ══════════════════════════════════════════════════════════════════
def _get_mlflow_client():
    try:
        import mlflow
        import requests

        # ✅ FIX 3: Pehle MLflow ready hai ya nahi check karo
        try:
            resp = requests.get(
                f"{MLFLOW_TRACKING_URI}/api/2.0/mlflow/experiments/list",
                timeout=5
            )
            if resp.status_code != 200:
                raise Exception(f"MLflow not ready — HTTP {resp.status_code}")
        except requests.exceptions.ConnectionError:
            raise Exception("MLflow connection refused — service start nahi hua")

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment("car-booking-ml-models")
        logger.info(f"[MLflow] ✓ Connected — {MLFLOW_TRACKING_URI}")
        return mlflow

    except ImportError:
        logger.info("[MLflow] Not installed — pip install mlflow")
        return None
    except Exception as e:
        logger.warning(f"[MLflow] Connection failed: {e} — continuing without tracking")
        return None


# ══════════════════════════════════════════════════════════════════
# 1. TEXT-TO-SQL AGENT
# ══════════════════════════════════════════════════════════════════
class TextToSQLAgent:
    """Natural language → SQL on customer_booking_staging (36 cols)"""

    def __init__(self):
        if not ANTHROPIC_API_KEY:
            self._ai_enabled = False; return
        try:
            from langchain_anthropic import ChatAnthropic
            from langchain.agents import create_sql_agent
            from langchain.agents.agent_toolkits import SQLDatabaseToolkit
            from langchain.sql_database import SQLDatabase

            llm         = ChatAnthropic(model="claude-sonnet-4-20250514",
                                        api_key=ANTHROPIC_API_KEY, temperature=0)
            db          = SQLDatabase.from_uri(PG_SQLALCHEMY_URL,
                                               include_tables=[STAGING_TABLE])
            toolkit     = SQLDatabaseToolkit(db=db, llm=llm)
            self._agent = create_sql_agent(
                llm=llm, toolkit=toolkit, verbose=False,
                agent_type="openai-tools",
                prefix=f"""
                You are a data analyst for a car booking company.
                Table: {STAGING_TABLE} (36 columns)
                Key columns: booking_id, customer_id, customer_name, email,
                  loyalty_tier, loyalty_points, loyalty_rank,
                  is_high_value_customer, points_tier,
                  booking_date, booking_year, booking_month,
                  pickup_location, drop_location, route,
                  pickup_hour, pickup_slot, trip_type,
                  car_id, model, car_segment, price_per_day, price_tier,
                  insurance_coverage, has_full_insurance, insurer_type,
                  payment_method, payment_amount, amount_tier,
                  is_digital_payment, is_split_payment, payment_count
                Answer with: SQL used, result in plain English, business insight.
                """
            )
            self._ai_enabled = True
        except ImportError:
            self._ai_enabled = False

    def query(self, question: str, run_id: Optional[str] = None) -> dict:
        start = time.time()
        if self._ai_enabled:
            try:
                result  = self._agent.run(question)
                latency = round(time.time() - start, 2)
                logger.info(f"[TextToSQL] Claude answered in {latency}s")
                return {"status": "success", "question": question,
                        "answer": result, "mode": "claude_api",
                        "latency_seconds": latency,
                        "orchestration_run_id": run_id,
                        "timestamp": datetime.now().isoformat()}
            except Exception as e:
                logger.warning(f"[TextToSQL] Claude API failed: {e}")
        return self._sql_fallback(question, run_id)

    def _sql_fallback(self, question: str, run_id: Optional[str] = None) -> dict:
        """Direct pandas queries — ✅ FIX 4: PG_ENGINE use ho raha hai"""
        q = question.lower()
        try:
            if "route" in q:
                df = pd.read_sql(f"""
                    SELECT route, COUNT(*) AS bookings,
                           ROUND(AVG(payment_amount)::numeric,0) AS avg_payment
                    FROM {STAGING_TABLE}
                    GROUP BY route ORDER BY bookings DESC LIMIT 5
                """, PG_ENGINE)
                answer = f"Top routes:\n{df.to_string(index=False)}"

            elif "trip" in q or "trip_type" in q:
                df = pd.read_sql(f"""
                    SELECT trip_type, COUNT(*) AS bookings,
                           ROUND(AVG(payment_amount)::numeric,0) AS avg_payment
                    FROM {STAGING_TABLE}
                    GROUP BY trip_type ORDER BY bookings DESC
                """, PG_ENGINE)
                answer = f"Trip types:\n{df.to_string(index=False)}"

            elif "hour" in q or "busiest" in q or "pickup_hour" in q:
                df = pd.read_sql(f"""
                    SELECT pickup_hour, pickup_slot, COUNT(*) AS bookings
                    FROM {STAGING_TABLE}
                    GROUP BY pickup_hour, pickup_slot
                    ORDER BY bookings DESC LIMIT 5
                """, PG_ENGINE)
                answer = f"Busiest pickup hours:\n{df.to_string(index=False)}"

            elif "segment" in q or "car_segment" in q:
                df = pd.read_sql(f"""
                    SELECT car_segment, model, COUNT(*) AS bookings,
                           ROUND(AVG(price_per_day)::numeric,0) AS avg_price
                    FROM {STAGING_TABLE}
                    GROUP BY car_segment, model
                    ORDER BY bookings DESC LIMIT 8
                """, PG_ENGINE)
                answer = f"Car segments:\n{df.to_string(index=False)}"

            elif "payment" in q and ("method" in q or "digital" in q):
                df = pd.read_sql(f"""
                    SELECT payment_method, amount_tier,
                           COUNT(*) AS count,
                           ROUND(SUM(payment_amount)::numeric,0) AS total,
                           ROUND(AVG(payment_amount)::numeric,0) AS avg,
                           SUM(is_digital_payment::int) AS digital_count
                    FROM {STAGING_TABLE}
                    GROUP BY payment_method, amount_tier
                    ORDER BY count DESC
                """, PG_ENGINE)
                answer = f"Payment methods:\n{df.to_string(index=False)}"

            elif "loyalty" in q or "tier" in q or "high value" in q:
                df = pd.read_sql(f"""
                    SELECT loyalty_tier, points_tier,
                           COUNT(*) AS customers,
                           ROUND(AVG(loyalty_points)::numeric,0) AS avg_points,
                           SUM(is_high_value_customer::int) AS high_value_count,
                           ROUND(AVG(payment_amount)::numeric,0) AS avg_payment
                    FROM {STAGING_TABLE}
                    GROUP BY loyalty_tier, points_tier
                    ORDER BY avg_payment DESC
                """, PG_ENGINE)
                answer = f"Loyalty breakdown:\n{df.to_string(index=False)}"

            elif "insurance" in q:
                df = pd.read_sql(f"""
                    SELECT insurance_coverage, insurer_type,
                           COUNT(*) AS bookings,
                           SUM(has_full_insurance::int) AS full_coverage_count
                    FROM {STAGING_TABLE}
                    GROUP BY insurance_coverage, insurer_type
                    ORDER BY bookings DESC
                """, PG_ENGINE)
                answer = f"Insurance breakdown:\n{df.to_string(index=False)}"

            elif "revenue" in q or "amount" in q:
                df = pd.read_sql(f"""
                    SELECT COUNT(*) AS total_bookings,
                           ROUND(SUM(payment_amount)::numeric,0) AS total_revenue,
                           ROUND(AVG(payment_amount)::numeric,0) AS avg_booking,
                           SUM(is_split_payment::int) AS split_payments,
                           SUM(is_digital_payment::int) AS digital_payments
                    FROM {STAGING_TABLE}
                """, PG_ENGINE)
                answer = f"Revenue summary:\n{df.to_string(index=False)}"

            elif "month" in q or "trend" in q:
                df = pd.read_sql(f"""
                    SELECT booking_year, booking_month,
                           COUNT(*) AS bookings,
                           ROUND(SUM(payment_amount)::numeric,0) AS revenue
                    FROM {STAGING_TABLE}
                    GROUP BY booking_year, booking_month
                    ORDER BY booking_year, booking_month
                """, PG_ENGINE)
                answer = f"Monthly trend:\n{df.to_string(index=False)}"

            else:
                df = pd.read_sql(f"""
                    SELECT COUNT(*) AS total_bookings,
                           COUNT(DISTINCT customer_id) AS unique_customers,
                           COUNT(DISTINCT route) AS unique_routes,
                           COUNT(DISTINCT model) AS car_models,
                           ROUND(SUM(payment_amount)::numeric,0) AS total_revenue,
                           SUM(is_high_value_customer::int) AS high_value_customers
                    FROM {STAGING_TABLE}
                """, PG_ENGINE)
                answer = (f"Pipeline summary ({STAGING_TABLE}):\n"
                          f"{df.to_string(index=False)}\n\n"
                          f"Set ANTHROPIC_API_KEY for natural language answers.")

            return {"status": "success", "question": question, "answer": answer,
                    "mode": "sql_fallback", "orchestration_run_id": run_id,
                    "timestamp": datetime.now().isoformat()}
        except Exception as e:
            return {"status": "error", "question": question,
                    "answer": f"Query failed: {e}", "mode": "error",
                    "timestamp": datetime.now().isoformat()}


# ══════════════════════════════════════════════════════════════════
# 2. PIPELINE HEALTH AGENT
# ══════════════════════════════════════════════════════════════════
class PipelineHealthAgent:

    def get_pipeline_stats(self) -> pd.DataFrame:
        try:
            conn = get_mysql_conn()
            # ✅ FIX 4: MySQL ke liye direct connection — SQLAlchemy zarurat nahi
            df = pd.read_sql("""
                SELECT stage_name, status, duration_seconds,
                       records_processed, error_message, started_at
                FROM pipeline_run_stats
                ORDER BY started_at DESC LIMIT 30
            """, conn); conn.close(); return df
        except Exception as e:
            logger.error(f"[PipelineHealth] pipeline_run_stats: {e}"); return pd.DataFrame()

    def get_active_alerts(self) -> pd.DataFrame:
        try:
            conn = get_mysql_conn()
            df = pd.read_sql("""
                SELECT alert_type, pipeline_name, stage_name,
                       error_message, created_at
                FROM pipeline_alerts
                WHERE created_at >= NOW() - INTERVAL 24 HOUR
                ORDER BY created_at DESC
            """, conn); conn.close(); return df
        except Exception as e:
            logger.error(f"[PipelineHealth] pipeline_alerts: {e}"); return pd.DataFrame()

    def get_dq_results(self) -> pd.DataFrame:
        try:
            conn = get_mysql_conn()
            df = pd.read_sql("""
                SELECT run_identifier, total_expectations,
                       passed_expectations, failed_expectations,
                       success_rate, validation_status, created_at
                FROM validation_results
                ORDER BY created_at DESC LIMIT 5
            """, conn); conn.close(); return df
        except Exception as e:
            logger.error(f"[PipelineHealth] validation_results: {e}"); return pd.DataFrame()

    def analyze(self, run_id: Optional[str] = None) -> dict:
        start  = time.time()
        stats  = self.get_pipeline_stats()
        alerts = self.get_active_alerts()
        dq     = self.get_dq_results()

        failed_stages   = len(stats[stats["status"] == "FAILED"])         if not stats.empty  and "status"       in stats.columns  else 0
        critical_alerts = len(alerts[alerts["alert_type"] == "FAILURE"])   if not alerts.empty and "alert_type"   in alerts.columns else 0
        dq_score        = float(dq["success_rate"].mean())                 if not dq.empty     and "success_rate" in dq.columns     else 100.0

        if   failed_stages == 0 and critical_alerts == 0 and dq_score >= 90: health, score = "HEALTHY",  100
        elif failed_stages <= 1 or  critical_alerts <= 2 or  dq_score >= 70: health, score = "WARNING",  65
        else:                                                                  health, score = "CRITICAL", 30

        ai_analysis = (self._claude_explain(stats, alerts, dq)
                       if ANTHROPIC_API_KEY
                       else f"Status: {health} | Failed: {failed_stages} | "
                            f"Alerts: {critical_alerts} | DQ: {dq_score:.1f}%\n"
                            f"Set ANTHROPIC_API_KEY for AI analysis.")

        return {"health_status": health, "health_score": score,
                "failed_stages": failed_stages, "active_alerts": critical_alerts,
                "dq_score": dq_score, "ai_analysis": ai_analysis,
                "latency_seconds": round(time.time() - start, 2),
                "orchestration_run_id": run_id,
                "timestamp": datetime.now().isoformat()}

    def _claude_explain(self, stats, alerts, dq) -> str:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            msg = client.messages.create(
                model="claude-sonnet-4-20250514", max_tokens=500,
                messages=[{"role": "user", "content": f"""
Expert Data Engineer for car booking pipeline.
Stack: Kafka → Spark 3.4.2 → Delta Lake (Bronze/Silver/Curated) → PostgreSQL → Plotly Dash
CI/CD: Jenkins | DQ: Great Expectations (27+ rules) → MySQL | Staging: 36-column table

pipeline_run_stats:
{stats.to_string() if not stats.empty else "No data"}

pipeline_alerts (last 24h):
{alerts.to_string() if not alerts.empty else "No alerts"}

validation_results (Great Expectations):
{dq.to_string() if not dq.empty else "No DQ data"}

Provide:
1. HEALTH STATUS
2. ROOT CAUSE — which stage, why
3. BUSINESS IMPACT
4. FIX — exact steps
5. PREVENTION
"""}])
            return msg.content[0].text
        except Exception as e:
            return f"Claude error: {e}"


# ══════════════════════════════════════════════════════════════════
# 3. ANOMALY EXPLAINER AGENT
# ══════════════════════════════════════════════════════════════════
class AnomalyExplainerAgent:

    def explain(self, anomaly_record: dict, run_id: Optional[str] = None) -> dict:
        context = self._get_context()
        if not ANTHROPIC_API_KEY:
            return {"booking_id":  anomaly_record.get("booking_id", "?"),
                    "explanation": f"Risk score: {anomaly_record.get('fraud_risk_score')} | "
                                   f"Route: {anomaly_record.get('route')} | "
                                   f"Set ANTHROPIC_API_KEY for full explanation.",
                    "orchestration_run_id": run_id,
                    "timestamp": datetime.now().isoformat()}
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            msg = client.messages.create(
                model="claude-sonnet-4-20250514", max_tokens=400,
                messages=[{"role": "user", "content": f"""
Fraud analyst for car booking platform. Table: {STAGING_TABLE} (36 columns).

Suspicious Booking:
{json.dumps(anomaly_record, indent=2, default=str)}

Normal Patterns (from {STAGING_TABLE}):
{json.dumps(context, indent=2, default=str)}

Explain:
1. WHY suspicious — cite specific field values vs normal patterns
2. RISK LEVEL: Low/Medium/High/Critical
3. ACTION: Block/Review/Monitor/Allow
4. PATTERN: Known fraud type?
"""}])
            return {"booking_id":  anomaly_record.get("booking_id", "?"),
                    "explanation": msg.content[0].text,
                    "orchestration_run_id": run_id,
                    "timestamp": datetime.now().isoformat()}
        except Exception as e:
            return {"booking_id":  anomaly_record.get("booking_id", "?"),
                    "explanation": f"Error: {e}",
                    "timestamp": datetime.now().isoformat()}

    def _get_context(self) -> dict:
        try:
            # ✅ FIX 4: PG_ENGINE use ho raha hai
            df = pd.read_sql(f"""
                SELECT
                    ROUND(AVG(payment_amount)::numeric,0)   AS avg_payment,
                    ROUND(MAX(payment_amount)::numeric,0)   AS max_payment,
                    ROUND(AVG(price_per_day)::numeric,0)    AS avg_price_per_day,
                    ROUND(AVG(pickup_hour)::numeric,1)      AS avg_pickup_hour,
                    COUNT(DISTINCT route)                    AS unique_routes,
                    COUNT(DISTINCT trip_type)                AS trip_types,
                    SUM(is_split_payment::int)               AS split_payment_count,
                    SUM(is_digital_payment::int)             AS digital_payment_count,
                    SUM(is_high_value_customer::int)         AS high_value_customers,
                    COUNT(*)                                 AS total_bookings
                FROM {STAGING_TABLE}
            """, PG_ENGINE)
            return df.iloc[0].to_dict() if not df.empty else {}
        except Exception:
            return {}


# ══════════════════════════════════════════════════════════════════
# 4. BOOKING RAG AGENT
# ══════════════════════════════════════════════════════════════════
class BookingRAGAgent:

    COLLECTION = "car_booking_embeddings"

    def __init__(self):
        try:
            from langchain_community.vectorstores import PGVector
            from langchain_community.embeddings  import HuggingFaceEmbeddings
            self._embeddings  = HuggingFaceEmbeddings(
                model_name="sentence-transformers/all-MiniLM-L6-v2")
            self._vectorstore = PGVector(connection_string=PG_SQLALCHEMY_URL,
                                         embedding_function=self._embeddings,
                                         collection_name=self.COLLECTION)
            self._enabled = True
        except ImportError:
            self._enabled = False

    def index_bookings(self, limit: int = 1000) -> int:
        if not self._enabled: return 0
        from langchain.schema import Document

        # ✅ FIX 4: PG_ENGINE use ho raha hai
        df = pd.read_sql(f"""
            SELECT booking_id, customer_id, customer_name,
                   loyalty_tier, points_tier, is_high_value_customer,
                   model, car_segment, route, trip_type,
                   pickup_location, drop_location, pickup_hour, pickup_slot,
                   payment_method, payment_amount, amount_tier,
                   is_digital_payment, is_split_payment,
                   insurance_coverage, booking_date
            FROM {STAGING_TABLE} LIMIT {limit}
        """, PG_ENGINE)

        docs = []
        for _, row in df.iterrows():
            text = (
                f"Booking {row['booking_id']}: {row['customer_name']} "
                f"({row['loyalty_tier']}, {row['points_tier']}) booked "
                f"{row['car_segment']} {row['model']} for {row['trip_type']} trip. "
                f"Route: {row['route']} "
                f"({row['pickup_location']} → {row['drop_location']}). "
                f"Pickup: {row['pickup_slot']} (hour {row['pickup_hour']}). "
                f"Payment: {row['payment_method']} ₹{row['payment_amount']:,.0f} "
                f"({row['amount_tier']}). "
                f"Digital: {row['is_digital_payment']}. Split: {row['is_split_payment']}. "
                f"Insurance: {row['insurance_coverage']}. Date: {row['booking_date']}."
            )
            docs.append(Document(page_content=text,
                                 metadata={"booking_id": str(row["booking_id"])}))

        self._vectorstore.add_documents(docs)
        logger.info(f"[RAG] Indexed {len(docs)} bookings into pgvector")
        return len(docs)

    def answer(self, question: str, run_id: Optional[str] = None) -> dict:
        results = self._vectorstore.similarity_search(question, k=5) if self._enabled else []
        context = "\n".join([d.page_content for d in results])
        if not ANTHROPIC_API_KEY:
            return {"question": question,
                    "answer": f"Found {len(results)} similar bookings.\n{context[:300]}",
                    "timestamp": datetime.now().isoformat()}
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            msg = client.messages.create(
                model="claude-sonnet-4-20250514", max_tokens=300,
                messages=[{"role": "user", "content":
                           f"Booking records:\n{context}\n\nQuestion: {question}\n"
                           f"Give specific data-driven answer."}])
            return {"question": question, "answer": msg.content[0].text,
                    "source_bookings": [d.metadata.get("booking_id") for d in results],
                    "orchestration_run_id": run_id,
                    "timestamp": datetime.now().isoformat()}
        except Exception as e:
            return {"question": question, "answer": f"Error: {e}",
                    "timestamp": datetime.now().isoformat()}


# ══════════════════════════════════════════════════════════════════
# 5. MODEL EVALUATION AGENT
# ══════════════════════════════════════════════════════════════════
class ModelEvaluationAgent:

    def __init__(self):
        self.mlflow = _get_mlflow_client()

    def evaluate_all(self) -> dict:
        results = {}
        results["prophet"]          = self.evaluate_prophet()
        results["isolation_forest"] = self.evaluate_fraud()
        results["xgboost"]          = self.evaluate_churn()

        logger.info(f"[ModelEval] All models evaluated: {json.dumps(results, indent=2, default=str)}")
        return results

    def evaluate_prophet(self) -> dict:
        """
        ✅ FIX 1: Date range fix — last 30 days actual vs forecast match karega
        """
        try:
            # ✅ FIX 1: Last 30 days ka actual data
            actual = pd.read_sql(f"""
                SELECT booking_date::date AS date,
                       COUNT(*) AS actual_bookings
                FROM {STAGING_TABLE}
                WHERE booking_date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY booking_date::date
                ORDER BY date
            """, PG_ENGINE)

            # ✅ FIX 1: Forecast table se saari dates lo (date filter nahi)
            forecast = pd.read_sql("""
                SELECT forecast_date::date AS date,
                       predicted_bookings
                FROM ml_demand_forecast
                ORDER BY date
            """, PG_ENGINE) if self._table_exists("ml_demand_forecast") else pd.DataFrame()

            if actual.empty or forecast.empty:
                logger.warning("[ModelEval] Prophet: actual ya forecast data nahi mila")
                return {"status": "no_data", "mape": None}

            merged = actual.merge(forecast, on="date", how="inner")
            if merged.empty:
                logger.warning("[ModelEval] Prophet: actual aur forecast dates overlap nahi kar rahe")
                return {"status": "no_overlap", "mape": None,
                        "actual_dates": str(actual["date"].tolist()[:5]),
                        "forecast_dates": str(forecast["date"].tolist()[:5])}

            mape = float((
                (merged["actual_bookings"] - merged["predicted_bookings"]).abs()
                / merged["actual_bookings"].clip(lower=1)
            ).mean() * 100)

            accuracy = round(100 - mape, 1)

            if self.mlflow:
                try:
                    with self.mlflow.start_run(run_name=f"prophet_eval_{datetime.now().strftime('%Y%m%d_%H%M')}"):
                        self.mlflow.log_metric("prophet_mape",     round(mape, 2))
                        self.mlflow.log_metric("prophet_accuracy", accuracy)
                        self.mlflow.log_metric("eval_days",        len(merged))
                        self.mlflow.set_tag("model_type", "demand_forecasting")
                        self.mlflow.set_tag("algorithm",  "prophet")
                        logger.info(f"[MLflow] Prophet logged — MAPE: {mape:.1f}%")
                except Exception as e:
                    logger.warning(f"[MLflow] Prophet log failed: {e}")

            return {
                "status":    "success",
                "mape":      round(mape, 2),
                "accuracy":  accuracy,
                "eval_days": len(merged),
                "verdict":   "good" if mape < 15 else ("acceptable" if mape < 30 else "needs_retraining")
            }
        except Exception as e:
            logger.error(f"[ModelEval] Prophet eval failed: {e}")
            return {"status": "error", "error": str(e)}

    def evaluate_fraud(self) -> dict:
        """
        ✅ FIX 2: Fraud threshold realistic — 10% → 30%
        Isolation Forest contamination=0.1 default matlab 10% hamesha flagged hoga
        """
        try:
            if not self._table_exists("ml_fraud_scores"):
                return {"status": "no_data"}

            # ✅ FIX 4: PG_ENGINE use ho raha hai
            df = pd.read_sql("""
                SELECT risk_label,
                       COUNT(*) AS count,
                       ROUND(AVG(fraud_risk_score)::numeric, 3) AS avg_score
                FROM ml_fraud_scores
                GROUP BY risk_label
                ORDER BY avg_score DESC
            """, PG_ENGINE)

            total = pd.read_sql("SELECT COUNT(*) AS total FROM ml_fraud_scores", PG_ENGINE)

            total_count    = int(total["total"].iloc[0])
            critical_count = int(df[df["risk_label"] == "CRITICAL"]["count"].sum()) if not df.empty else 0
            high_count     = int(df[df["risk_label"] == "HIGH"]["count"].sum())     if not df.empty else 0
            fraud_rate     = round((critical_count + high_count) / max(total_count, 1) * 100, 2)

            if self.mlflow:
                try:
                    with self.mlflow.start_run(run_name=f"fraud_eval_{datetime.now().strftime('%Y%m%d_%H%M')}"):
                        self.mlflow.log_metric("fraud_rate_pct",     fraud_rate)
                        self.mlflow.log_metric("critical_bookings",  critical_count)
                        self.mlflow.log_metric("high_risk_bookings", high_count)
                        self.mlflow.log_metric("total_scored",       total_count)
                        self.mlflow.set_tag("model_type", "fraud_detection")
                        self.mlflow.set_tag("algorithm",  "isolation_forest")
                        logger.info(f"[MLflow] Fraud logged — rate: {fraud_rate}%")
                except Exception as e:
                    logger.warning(f"[MLflow] Fraud log failed: {e}")

            return {
                "status":         "success",
                "total_scored":   total_count,
                "critical_count": critical_count,
                "high_risk_count": high_count,
                "fraud_rate_pct": fraud_rate,
                # ✅ FIX 2: Realistic thresholds — Isolation Forest contamination consider karo
                "verdict": "alert"   if fraud_rate > 30
                      else "monitor" if fraud_rate > 15
                      else "normal"
            }
        except Exception as e:
            logger.error(f"[ModelEval] Fraud eval failed: {e}")
            return {"status": "error", "error": str(e)}

    def evaluate_churn(self) -> dict:
        try:
            if not self._table_exists("ml_churn_scores"):
                return {"status": "no_data"}

            # ✅ FIX 4: PG_ENGINE use ho raha hai
            auc_df = pd.read_sql("""
                SELECT model_auc, created_at
                FROM ml_churn_scores
                ORDER BY created_at DESC LIMIT 1
            """, PG_ENGINE)

            dist = pd.read_sql("""
                SELECT churn_risk,
                       COUNT(*) AS count,
                       ROUND(AVG(churn_probability)::numeric, 3) AS avg_prob
                FROM ml_churn_scores
                GROUP BY churn_risk
                ORDER BY avg_prob DESC
            """, PG_ENGINE)

            auc = float(auc_df["model_auc"].iloc[0]) if not auc_df.empty else 0.0

            high_risk  = int(dist[dist["churn_risk"].isin(["HIGH", "CRITICAL"])]["count"].sum()) if not dist.empty else 0
            total_ch   = int(dist["count"].sum()) if not dist.empty else 0
            churn_rate = round(high_risk / max(total_ch, 1) * 100, 2)

            if self.mlflow:
                try:
                    with self.mlflow.start_run(run_name=f"churn_eval_{datetime.now().strftime('%Y%m%d_%H%M')}"):
                        self.mlflow.log_metric("xgb_auc",               auc)
                        self.mlflow.log_metric("churn_rate_pct",        churn_rate)
                        self.mlflow.log_metric("high_risk_customers",   high_risk)
                        self.mlflow.log_metric("total_scored",          total_ch)
                        self.mlflow.set_tag("model_type", "churn_prediction")
                        self.mlflow.set_tag("algorithm",  "xgboost")
                        if auc < 0.70:
                            self.mlflow.set_tag("action_required", "RETRAIN")
                        logger.info(f"[MLflow] Churn logged — AUC: {auc:.3f}")
                except Exception as e:
                    logger.warning(f"[MLflow] Churn log failed: {e}")

            return {
                "status":              "success",
                "xgb_auc":             round(auc, 3),
                "churn_rate_pct":      churn_rate,
                "high_risk_customers": high_risk,
                "total_scored":        total_ch,
                "verdict": "good"          if auc >= 0.80
                      else "acceptable"    if auc >= 0.70
                      else "needs_retraining"
            }
        except Exception as e:
            logger.error(f"[ModelEval] Churn eval failed: {e}")
            return {"status": "error", "error": str(e)}

    def _table_exists(self, table: str) -> bool:
        """✅ FIX 4: PG_ENGINE use karo — connection object nahi"""
        try:
            pd.read_sql(f"SELECT 1 FROM {table} LIMIT 1", PG_ENGINE)
            return True
        except Exception:
            return False


# ══════════════════════════════════════════════════════════════════
# 6. BOOKING INTELLIGENCE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════
class BookingIntelligenceOrchestrator:

    def __init__(self):
        self.run_id        = f"orch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.sql_agent     = TextToSQLAgent()
        self.health_agent  = PipelineHealthAgent()
        self.anomaly_agent = AnomalyExplainerAgent()
        self.eval_agent    = ModelEvaluationAgent()
        logger.info(f"[Orchestrator] Initialized — run_id: {self.run_id}")

    def full_intelligence_report(self, question: str = "Give me a complete business intelligence report") -> dict:
        start = time.time()
        logger.info(f"[Orchestrator] Starting full report — run_id: {self.run_id}")

        logger.info("[Orchestrator] Step 1/4 — TextToSQL querying data...")
        sql_result = self.sql_agent.query(question, run_id=self.run_id)

        logger.info("[Orchestrator] Step 2/4 — PipelineHealth checking status...")
        health_result = self.health_agent.analyze(run_id=self.run_id)

        logger.info("[Orchestrator] Step 3/4 — AnomalyExplainer checking fraud...")
        fraud_result = self._get_top_fraud_booking()
        if fraud_result:
            fraud_explanation = self.anomaly_agent.explain(fraud_result, run_id=self.run_id)
        else:
            fraud_explanation = {"explanation": "No high-risk bookings detected — pipeline clean"}

        logger.info("[Orchestrator] Step 4/4 — ModelEval checking ML metrics...")
        eval_result = self.eval_agent.evaluate_all()

        logger.info("[Orchestrator] Synthesizing with Claude...")
        final_insight = self._synthesize(
            question, sql_result, health_result, fraud_explanation, eval_result)

        total_time = round(time.time() - start, 2)
        logger.info(f"[Orchestrator] Complete in {total_time}s — run_id: {self.run_id}")

        return {
            "run_id":            self.run_id,
            "question":          question,
            "final_insight":     final_insight,
            "data_result":       sql_result.get("answer", ""),
            "pipeline_health":   health_result.get("health_status", "UNKNOWN"),
            "pipeline_score":    health_result.get("health_score", 0),
            "dq_score":          health_result.get("dq_score", 0),
            "fraud_alert":       fraud_explanation.get("explanation", ""),
            "model_evaluation":  eval_result,
            "total_latency_sec": total_time,
            "agents_used":       ["TextToSQL", "PipelineHealth", "AnomalyExplainer", "ModelEvaluation", "Claude"],
            "timestamp":         datetime.now().isoformat(),
        }

    def quick_answer(self, question: str) -> dict:
        start      = time.time()
        sql_result = self.sql_agent.query(question, run_id=self.run_id)

        if not ANTHROPIC_API_KEY:
            return sql_result

        try:
            import anthropic
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            msg = client.messages.create(
                model="claude-sonnet-4-20250514", max_tokens=250,
                messages=[{"role": "user", "content": f"""
Car booking data analyst. Be concise and specific.

Raw data result:
{sql_result.get('answer', 'no data')}

Question: {question}

Give 2-3 sentence insight with specific numbers.
"""}])
            return {
                "status":          "success",
                "question":        question,
                "answer":          msg.content[0].text,
                "raw_data":        sql_result.get("answer", ""),
                "mode":            "orchestrated",
                "latency_seconds": round(time.time() - start, 2),
                "run_id":          self.run_id,
                "timestamp":       datetime.now().isoformat(),
            }
        except Exception as e:
            return sql_result

    def _get_top_fraud_booking(self) -> Optional[dict]:
        try:
            # ✅ FIX 4: PG_ENGINE use ho raha hai
            df = pd.read_sql(f"""
                SELECT f.booking_id, f.fraud_risk_score, f.risk_label,
                       s.customer_name, s.route, s.trip_type,
                       s.pickup_hour, s.payment_method, s.payment_amount,
                       s.is_split_payment, s.car_segment, s.loyalty_tier
                FROM ml_fraud_scores f
                JOIN {STAGING_TABLE} s ON f.booking_id = s.booking_id
                WHERE f.risk_label = 'CRITICAL'
                ORDER BY f.fraud_risk_score DESC
                LIMIT 1
            """, PG_ENGINE)
            return df.iloc[0].to_dict() if not df.empty else None
        except Exception as e:
            logger.warning(f"[Orchestrator] fraud booking fetch failed: {e}")
            return None

    def _synthesize(self, question, sql_result, health_result,
                    fraud_explanation, eval_result) -> str:
        if not ANTHROPIC_API_KEY:
            return (
                f"Pipeline: {health_result.get('health_status', 'UNKNOWN')} | "
                f"DQ: {health_result.get('dq_score', 0):.1f}% | "
                f"Prophet: {eval_result.get('prophet', {}).get('mape', '?')}% MAPE | "
                f"XGBoost AUC: {eval_result.get('xgboost', {}).get('xgb_auc', '?')} | "
                f"Set ANTHROPIC_API_KEY for AI synthesis."
            )
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

            prophet_mape = eval_result.get("prophet", {}).get("mape", "N/A")
            xgb_auc      = eval_result.get("xgboost", {}).get("xgb_auc", "N/A")
            fraud_rate   = eval_result.get("isolation_forest", {}).get("fraud_rate_pct", "N/A")

            msg = client.messages.create(
                model="claude-sonnet-4-20250514", max_tokens=600,
                messages=[{"role": "user", "content": f"""
You are the AI Intelligence layer for a car booking data pipeline.
You have just coordinated 4 specialized agents. Synthesize their findings.

QUESTION: {question}

AGENT RESULTS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. DATA AGENT (TextToSQL):
{sql_result.get('answer', 'no data')[:400]}

2. PIPELINE HEALTH AGENT:
Status: {health_result.get('health_status')}
DQ Score: {health_result.get('dq_score', 0):.1f}%
Failed stages: {health_result.get('failed_stages', 0)}

3. FRAUD AGENT (Isolation Forest):
Fraud rate: {fraud_rate}%
Top alert: {str(fraud_explanation.get('explanation', 'none'))[:200]}

4. MODEL EVALUATION AGENT:
Prophet MAPE: {prophet_mape}% (lower = better)
XGBoost AUC: {xgb_auc} (higher = better)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Give a 3-4 sentence executive summary combining all findings.
Be specific with numbers. Flag any concerns clearly.
"""}])
            return msg.content[0].text
        except Exception as e:
            return f"Synthesis error: {e}"


# ══════════════════════════════════════════════════════════════════
# QUICK TEST
# ══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  AI AGENTS v2 — Car Booking Pipeline")
    print("=" * 60)
    print(f"  PG       : {PG_CONN_PARAMS['host']}:{PG_CONN_PARAMS['port']}/{PG_CONN_PARAMS['database']}")
    print(f"  MySQL    : {MYSQL_CONN_PARAMS['host']}:{MYSQL_CONN_PARAMS['port']}/{MYSQL_CONN_PARAMS['database']}")
    print(f"  Table    : {STAGING_TABLE} (36 columns)")
    print(f"  Claude   : {'✓ enabled' if ANTHROPIC_API_KEY else '○ not set (SQL fallback)'}")
    print(f"  MLflow   : {MLFLOW_TRACKING_URI}")
    print(f"  LangSmith: {'✓ enabled' if LANGSMITH_ENABLED else '○ not configured'}")
    print("=" * 60 + "\n")

    print("1. TextToSQLAgent — routes")
    print(TextToSQLAgent().query("top routes by bookings")["answer"][:200])
    print()

    print("2. PipelineHealthAgent")
    h = PipelineHealthAgent().analyze()
    print(f"   Status: {h['health_status']} | DQ: {h['dq_score']:.1f}%")
    print()

    print("3. ModelEvaluationAgent")
    ev = ModelEvaluationAgent().evaluate_all()
    print(f"   Prophet MAPE  : {ev.get('prophet', {}).get('mape', 'N/A')}%")
    print(f"   Fraud rate    : {ev.get('isolation_forest', {}).get('fraud_rate_pct', 'N/A')}%")
    print(f"   XGBoost AUC   : {ev.get('xgboost', {}).get('xgb_auc', 'N/A')}")
    print()

    print("4. BookingIntelligenceOrchestrator — Full Report")
    orch   = BookingIntelligenceOrchestrator()
    report = orch.full_intelligence_report("What is the overall business performance?")
    print(f"   Run ID        : {report['run_id']}")
    print(f"   Pipeline      : {report['pipeline_health']}")
    print(f"   Total latency : {report['total_latency_sec']}s")
    print(f"   Agents used   : {', '.join(report['agents_used'])}")
    print(f"\n   FINAL INSIGHT:\n   {report['final_insight'][:300]}")