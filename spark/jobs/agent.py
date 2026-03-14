"""
AI Agents v3 — Car Booking Pipeline
=====================================
FIXES vs v2:
  - FIX 1: create_sql_agent removed — Direct Claude API (no LangChain dep)
  - FIX 2: Health score continuous weighted formula (not 100/65/30)
  - FIX 3: GE validation_failed_records → Claude (unique feature)
  - FIX 4: Parallel agent execution — ThreadPoolExecutor
  - FIX 5: _synthesize() includes GE column failures
  - FIX 6: BookingRAGAgent graceful fallback (pgvector not required)

Stack: Kafka → Spark → Delta Lake → PostgreSQL → Dash
MySQL: pipeline_run_stats, pipeline_alerts, validation_results,
       validation_expectation_details, validation_failed_records
"""

import os, sys, json, time, logging, psycopg2, pymysql
import pandas as pd
from datetime import datetime
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config       = load_config()
postgres_cfg = config.get_postgres_config()
mysql_cfg    = config.get_mysql_config()
tables       = config.get_tables()

# ✅ ML thresholds from config.json — single source of truth
_ml_cfg = config._config.get("ml", {}) if hasattr(config, "_config") else {}
CHURN_RETRAIN_AUC    = float(_ml_cfg.get("churn_retrain_auc",    0.70))
CHURN_THRESHOLD_DAYS = int(_ml_cfg.get("churn_threshold_days",   60))
FRAUD_ALERT_PCT      = float(_ml_cfg.get("fraud_alert_rate_pct", 30))
FRAUD_MONITOR_PCT    = float(_ml_cfg.get("fraud_monitor_rate_pct", 15))
PROPHET_RETRAIN_MAPE = float(_ml_cfg.get("prophet_retrain_mape", 30))

ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
LANGSMITH_API_KEY   = os.getenv("LANGSMITH_API_KEY", "")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
STAGING_TABLE       = tables.get("staging", "customer_booking_staging")

PG_CONN_PARAMS = {
    "host": postgres_cfg["host"], "port": postgres_cfg["port"],
    "database": postgres_cfg["database"],
    "user": postgres_cfg["user"], "password": postgres_cfg["password"],
}
MYSQL_CONN_PARAMS = {
    "host": mysql_cfg["host"], "port": int(mysql_cfg["port"]),
    "database": mysql_cfg["database"],
    "user": mysql_cfg["user"], "password": mysql_cfg["password"],
}
PG_SQLALCHEMY_URL = (
    f"postgresql+psycopg2://{postgres_cfg['user']}:{postgres_cfg['password']}"
    f"@{postgres_cfg['host']}:{postgres_cfg['port']}/{postgres_cfg['database']}"
)
PG_ENGINE = create_engine(PG_SQLALCHEMY_URL)

def get_pg_conn():    return psycopg2.connect(**PG_CONN_PARAMS)
def get_mysql_conn(): return pymysql.connect(**MYSQL_CONN_PARAMS)


# ── LangSmith setup ─────────────────────────────────────────────
def _setup_langsmith():
    if not LANGSMITH_API_KEY: return False
    try:
        os.environ["LANGCHAIN_TRACING_V2"] = "true"
        os.environ["LANGCHAIN_API_KEY"]    = LANGSMITH_API_KEY
        os.environ["LANGCHAIN_PROJECT"]    = "car-booking-pipeline"
        os.environ["LANGCHAIN_ENDPOINT"]   = "https://api.smith.langchain.com"
        logger.info("[LangSmith] ✓ enabled")
        return True
    except Exception as e:
        logger.warning(f"[LangSmith] {e}"); return False

LANGSMITH_ENABLED = _setup_langsmith()


# ── MLflow setup ─────────────────────────────────────────────────
def _get_mlflow_client():
    try:
        import mlflow, requests
        resp = requests.get(f"{MLFLOW_TRACKING_URI}/api/2.0/mlflow/experiments/list", timeout=5)
        if resp.status_code != 200:
            raise Exception(f"MLflow HTTP {resp.status_code}")
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment("car-booking-ml-models")
        logger.info(f"[MLflow] ✓ {MLFLOW_TRACKING_URI}")
        return mlflow
    except Exception as e:
        logger.warning(f"[MLflow] {e}"); return None


# ══════════════════════════════════════════════════════════════════
# 1. TEXT-TO-SQL AGENT
# FIX 1: Direct Claude API — no LangChain create_sql_agent
# ══════════════════════════════════════════════════════════════════
class TextToSQLAgent:
    """Natural language → SQL → Claude explains result"""

    SCHEMA = f"""
Table: customer_booking_staging (36 columns)
Columns: booking_id, customer_id, customer_name, email,
  loyalty_tier, loyalty_points, loyalty_rank, is_high_value_customer, points_tier,
  booking_date, booking_year, booking_month, email_domain,
  pickup_location, pickup_time, drop_location, drop_time,
  pickup_hour, pickup_slot, trip_type, route,
  car_id, model, price_per_day, price_tier, car_segment,
  insurance_provider, insurance_coverage, has_full_insurance, insurer_type,
  payment_id, payment_method, payment_amount, amount_tier,
  is_digital_payment, is_split_payment, payment_count
"""

    def query(self, question: str, run_id: Optional[str] = None) -> dict:
        start = time.time()

        # Get data via SQL fallback first
        data_result = self._sql_fallback(question, run_id)

        # Then use Claude to explain/enrich if API key available
        if ANTHROPIC_API_KEY and data_result.get("status") == "success":
            try:
                import anthropic
                client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
                msg = client.messages.create(
                    model="claude-sonnet-4-20250514", max_tokens=300,
                    messages=[{"role": "user", "content": f"""
Car booking data analyst. Be concise and specific with numbers.

Schema: {self.SCHEMA}

Raw SQL result:
{data_result.get('answer', 'no data')[:500]}

Question: {question}

Give 2-3 sentence business insight with specific numbers from the data.
"""}])
                return {
                    "status": "success", "question": question,
                    "answer": msg.content[0].text,
                    "raw_data": data_result.get("answer", ""),
                    "mode": "claude_direct",
                    "latency_seconds": round(time.time() - start, 2),
                    "orchestration_run_id": run_id,
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logger.warning(f"[TextToSQL] Claude failed: {e}")

        return data_result

    def _sql_fallback(self, question: str, run_id: Optional[str] = None) -> dict:
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
            elif "trip" in q:
                df = pd.read_sql(f"""
                    SELECT trip_type, COUNT(*) AS bookings,
                           ROUND(AVG(payment_amount)::numeric,0) AS avg_payment
                    FROM {STAGING_TABLE} GROUP BY trip_type ORDER BY bookings DESC
                """, PG_ENGINE)
                answer = f"Trip types:\n{df.to_string(index=False)}"
            elif "hour" in q or "busiest" in q:
                df = pd.read_sql(f"""
                    SELECT pickup_hour, pickup_slot, COUNT(*) AS bookings
                    FROM {STAGING_TABLE}
                    GROUP BY pickup_hour, pickup_slot ORDER BY bookings DESC LIMIT 5
                """, PG_ENGINE)
                answer = f"Busiest hours:\n{df.to_string(index=False)}"
            elif "segment" in q or "car" in q:
                df = pd.read_sql(f"""
                    SELECT car_segment, COUNT(*) AS bookings,
                           ROUND(AVG(price_per_day)::numeric,0) AS avg_price
                    FROM {STAGING_TABLE} GROUP BY car_segment ORDER BY bookings DESC
                """, PG_ENGINE)
                answer = f"Car segments:\n{df.to_string(index=False)}"
            elif "payment" in q:
                df = pd.read_sql(f"""
                    SELECT payment_method, COUNT(*) AS count,
                           ROUND(SUM(payment_amount)::numeric,0) AS total,
                           SUM(is_digital_payment::int) AS digital_count
                    FROM {STAGING_TABLE} GROUP BY payment_method ORDER BY count DESC
                """, PG_ENGINE)
                answer = f"Payments:\n{df.to_string(index=False)}"
            elif "loyalty" in q or "tier" in q or "high value" in q:
                df = pd.read_sql(f"""
                    SELECT loyalty_tier, COUNT(*) AS customers,
                           ROUND(AVG(loyalty_points)::numeric,0) AS avg_points,
                           SUM(is_high_value_customer::int) AS high_value_count,
                           ROUND(AVG(payment_amount)::numeric,0) AS avg_payment
                    FROM {STAGING_TABLE} GROUP BY loyalty_tier ORDER BY avg_payment DESC
                """, PG_ENGINE)
                answer = f"Loyalty:\n{df.to_string(index=False)}"
            elif "revenue" in q or "amount" in q:
                df = pd.read_sql(f"""
                    SELECT COUNT(*) AS bookings,
                           ROUND(SUM(payment_amount)::numeric,0) AS total_revenue,
                           ROUND(AVG(payment_amount)::numeric,0) AS avg_booking,
                           SUM(is_digital_payment::int) AS digital_payments
                    FROM {STAGING_TABLE}
                """, PG_ENGINE)
                answer = f"Revenue:\n{df.to_string(index=False)}"
            elif "fraud" in q or "risk" in q:
                df = pd.read_sql("""
                    SELECT risk_label, COUNT(*) AS count,
                           ROUND(AVG(fraud_risk_score)::numeric,1) AS avg_score
                    FROM ml_fraud_scores GROUP BY risk_label ORDER BY avg_score DESC
                """, PG_ENGINE)
                answer = f"Fraud scores:\n{df.to_string(index=False)}"
            elif "churn" in q:
                df = pd.read_sql("""
                    SELECT churn_risk, COUNT(*) AS customers,
                           ROUND(AVG(churn_probability)::numeric,3) AS avg_prob,
                           SUM(CASE WHEN is_high_value_customer THEN 1 ELSE 0 END) AS high_value_at_risk
                    FROM ml_churn_scores GROUP BY churn_risk ORDER BY avg_prob DESC
                """, PG_ENGINE)
                # Also get recency context
                recency = pd.read_sql(f"""
                    SELECT COUNT(DISTINCT customer_id) AS inactive_60d
                    FROM {STAGING_TABLE}
                    WHERE booking_date < CURRENT_DATE - INTERVAL '{CHURN_THRESHOLD_DAYS} days'
                """, PG_ENGINE)
                inactive = int(recency["inactive_60d"].iloc[0]) if not recency.empty else 0
                answer = f"Churn (recency threshold: {CHURN_THRESHOLD_DAYS}d | inactive: {inactive}):\n{df.to_string(index=False)}"
            elif "month" in q or "trend" in q:
                df = pd.read_sql(f"""
                    SELECT booking_year, booking_month, COUNT(*) AS bookings,
                           ROUND(SUM(payment_amount)::numeric,0) AS revenue
                    FROM {STAGING_TABLE}
                    GROUP BY booking_year, booking_month ORDER BY booking_year, booking_month
                """, PG_ENGINE)
                answer = f"Monthly trend:\n{df.to_string(index=False)}"
            else:
                df = pd.read_sql(f"""
                    SELECT COUNT(*) AS total_bookings,
                           COUNT(DISTINCT customer_id) AS unique_customers,
                           COUNT(DISTINCT route) AS unique_routes,
                           ROUND(SUM(payment_amount)::numeric,0) AS total_revenue,
                           SUM(is_high_value_customer::int) AS high_value_customers
                    FROM {STAGING_TABLE}
                """, PG_ENGINE)
                answer = f"Summary:\n{df.to_string(index=False)}"

            return {"status": "success", "question": question, "answer": answer,
                    "mode": "sql_fallback", "orchestration_run_id": run_id,
                    "timestamp": datetime.now().isoformat()}
        except Exception as e:
            return {"status": "error", "question": question,
                    "answer": f"Query error: {e}", "mode": "error",
                    "timestamp": datetime.now().isoformat()}


# ══════════════════════════════════════════════════════════════════
# 2. PIPELINE HEALTH AGENT
# FIX 2: Continuous health score (weighted formula)
# FIX 3: GE validation_failed_records → Claude (UNIQUE FEATURE)
# ══════════════════════════════════════════════════════════════════
class PipelineHealthAgent:

    def get_pipeline_stats(self) -> pd.DataFrame:
        try:
            conn = get_mysql_conn()
            df = pd.read_sql("""
                SELECT stage_name, status, duration_seconds,
                       records_processed, error_message, started_at
                FROM pipeline_run_stats ORDER BY started_at DESC LIMIT 30
            """, conn); conn.close(); return df
        except Exception as e:
            logger.error(f"[Health] stats: {e}"); return pd.DataFrame()

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
            logger.error(f"[Health] alerts: {e}"); return pd.DataFrame()

    def get_dq_results(self) -> pd.DataFrame:
        try:
            conn = get_mysql_conn()
            df = pd.read_sql("""
                SELECT run_identifier, total_expectations, passed_expectations,
                       failed_expectations, success_rate, validation_status, created_at
                FROM validation_results ORDER BY created_at DESC LIMIT 5
            """, conn); conn.close(); return df
        except Exception as e:
            logger.error(f"[Health] dq: {e}"); return pd.DataFrame()

    # ✅ FIX 3: UNIQUE — GE failed records + columns → Claude
    def get_failed_expectations(self) -> dict:
        """
        Fetch specific column failures + failed booking_ids from MySQL.
        This is the UNIQUE feature — Claude gets exact failure context.
        """
        try:
            conn = get_mysql_conn()
            # Top failed columns
            failed_cols = pd.read_sql("""
                SELECT column_name, expectation_type,
                       COUNT(*) AS failure_count,
                       MAX(failed_reason) AS sample_reason
                FROM validation_failed_records
                WHERE run_identifier = (
                    SELECT run_identifier FROM validation_results
                    ORDER BY created_at DESC LIMIT 1
                )
                GROUP BY column_name, expectation_type
                ORDER BY failure_count DESC LIMIT 10
            """, conn)

            # Sample failed booking_ids
            sample_bookings = pd.read_sql("""
                SELECT booking_id, column_name, failed_reason, payment_amount, loyalty_tier
                FROM validation_failed_records
                WHERE run_identifier = (
                    SELECT run_identifier FROM validation_results
                    ORDER BY created_at DESC LIMIT 1
                )
                ORDER BY failed_reason LIMIT 20
            """, conn)
            conn.close()

            return {
                "failed_columns": failed_cols.to_dict("records") if not failed_cols.empty else [],
                "sample_bookings": sample_bookings.to_dict("records") if not sample_bookings.empty else [],
                "total_failed_records": len(sample_bookings),
            }
        except Exception as e:
            logger.warning(f"[Health] GE failed records: {e}")
            return {"failed_columns": [], "sample_bookings": [], "total_failed_records": 0}

    def analyze(self, run_id: Optional[str] = None) -> dict:
        start  = time.time()
        stats  = self.get_pipeline_stats()
        alerts = self.get_active_alerts()
        dq     = self.get_dq_results()

        # ✅ FIX 3: Get GE failed records context
        ge_failures = self.get_failed_expectations()

        failed_stages   = len(stats[stats["status"] == "FAILED"])          if not stats.empty  and "status"       in stats.columns  else 0
        critical_alerts = len(alerts[alerts["alert_type"] == "FAILURE"])    if not alerts.empty and "alert_type"   in alerts.columns else 0
        dq_warnings     = len(alerts[alerts["alert_type"] == "DQ_WARNING"]) if not alerts.empty and "alert_type"   in alerts.columns else 0
        dq_score        = float(dq["success_rate"].mean())                  if not dq.empty     and "success_rate" in dq.columns     else 100.0

        # ✅ FIX 2: Continuous weighted health score
        health_score = 100
        health_score -= failed_stages * 15
        health_score -= critical_alerts * 10
        health_score -= dq_warnings * 5
        health_score -= max(0, (90 - dq_score) * 0.5)
        health_score  = max(0, min(100, round(health_score)))

        if   health_score >= 90: health = "HEALTHY"
        elif health_score >= 70: health = "WARNING"
        else:                    health = "CRITICAL"

        ai_analysis = (
            self._claude_explain(stats, alerts, dq, ge_failures)
            if ANTHROPIC_API_KEY
            else (
                f"Status: {health} | Score: {health_score}/100 | "
                f"Failed: {failed_stages} | Alerts: {critical_alerts} | DQ: {dq_score:.1f}%\n"
                f"Failed columns: {[f['column_name'] for f in ge_failures['failed_columns'][:3]]}\n"
                f"Set ANTHROPIC_API_KEY for AI analysis."
            )
        )

        return {
            "health_status":   health,
            "health_score":    health_score,
            "failed_stages":   failed_stages,
            "active_alerts":   critical_alerts,
            "dq_score":        dq_score,
            "ge_failures":     ge_failures,
            "ai_analysis":     ai_analysis,
            "latency_seconds": round(time.time() - start, 2),
            "orchestration_run_id": run_id,
            "timestamp": datetime.now().isoformat(),
        }

    def _claude_explain(self, stats, alerts, dq, ge_failures) -> str:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

            # FIX 3: GE failures in prompt
            ge_context = ""
            if ge_failures.get("failed_columns"):
                ge_context = f"""
GE Failed Columns (specific violations):
{json.dumps(ge_failures['failed_columns'][:5], indent=2, default=str)}

Sample Failed Bookings:
{json.dumps(ge_failures['sample_bookings'][:5], indent=2, default=str)}
"""

            msg = client.messages.create(
                model="claude-sonnet-4-20250514", max_tokens=500,
                messages=[{"role": "user", "content": f"""
Expert Data Engineer for car booking pipeline.
Stack: Kafka → Spark 3.4.2 → Delta Lake → PostgreSQL → Plotly Dash
CI/CD: Jenkins | DQ: Great Expectations | 36-column staging table

pipeline_run_stats:
{stats.head(10).to_string() if not stats.empty else "No data"}

pipeline_alerts (last 24h):
{alerts.to_string() if not alerts.empty else "No alerts"}

validation_results:
{dq.to_string() if not dq.empty else "No DQ data"}

{ge_context}

Provide:
1. HEALTH STATUS + SCORE reasoning
2. ROOT CAUSE — specific column/stage failures
3. BUSINESS IMPACT — which bookings affected
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
            return {"booking_id": anomaly_record.get("booking_id","?"),
                    "explanation": f"Risk: {anomaly_record.get('risk_label')} | "
                                   f"Score: {anomaly_record.get('fraud_risk_score')} | "
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
Fraud analyst for car booking platform.

Suspicious Booking:
{json.dumps(anomaly_record, indent=2, default=str)}

Normal Patterns:
{json.dumps(context, indent=2, default=str)}

Explain:
1. WHY suspicious — cite specific values vs normal patterns
2. RISK LEVEL: Low/Medium/High/Critical
3. ACTION: Block/Review/Monitor/Allow
4. PATTERN: Known fraud type (payment fraud / identity fraud / route manipulation)?
"""}])
            return {"booking_id": anomaly_record.get("booking_id","?"),
                    "explanation": msg.content[0].text,
                    "orchestration_run_id": run_id,
                    "timestamp": datetime.now().isoformat()}
        except Exception as e:
            return {"booking_id": anomaly_record.get("booking_id","?"),
                    "explanation": f"Error: {e}",
                    "timestamp": datetime.now().isoformat()}

    def _get_context(self) -> dict:
        try:
            df = pd.read_sql(f"""
                SELECT ROUND(AVG(payment_amount)::numeric,0) AS avg_payment,
                       ROUND(MAX(payment_amount)::numeric,0) AS max_payment,
                       ROUND(AVG(price_per_day)::numeric,0)  AS avg_price_per_day,
                       ROUND(AVG(pickup_hour)::numeric,1)    AS avg_pickup_hour,
                       COUNT(DISTINCT route)                  AS unique_routes,
                       SUM(is_split_payment::int)             AS split_payment_count,
                       SUM(is_digital_payment::int)           AS digital_payment_count,
                       COUNT(*)                               AS total_bookings
                FROM {STAGING_TABLE}
            """, PG_ENGINE)
            return df.iloc[0].to_dict() if not df.empty else {}
        except Exception:
            return {}


# ══════════════════════════════════════════════════════════════════
# 4. BOOKING RAG AGENT
# FIX 6: Graceful fallback — pgvector not required
# ══════════════════════════════════════════════════════════════════
class BookingRAGAgent:

    COLLECTION = "car_booking_embeddings"

    def __init__(self):
        self._enabled = False
        try:
            from langchain_community.vectorstores import PGVector
            from langchain_community.embeddings  import HuggingFaceEmbeddings

            # ✅ FIX 6: Check pgvector extension exists before connecting
            conn = get_pg_conn(); cur = conn.cursor()
            cur.execute("SELECT 1 FROM pg_extension WHERE extname='vector'")
            has_pgvector = cur.fetchone() is not None
            cur.close(); conn.close()

            if not has_pgvector:
                logger.warning("[RAG] pgvector extension not installed — RAG disabled")
                return

            self._embeddings  = HuggingFaceEmbeddings(
                model_name="sentence-transformers/all-MiniLM-L6-v2")
            self._vectorstore = PGVector(
                connection_string=PG_SQLALCHEMY_URL,
                embedding_function=self._embeddings,
                collection_name=self.COLLECTION)
            self._enabled = True
            logger.info("[RAG] ✓ pgvector enabled")
        except ImportError:
            logger.info("[RAG] LangChain not installed — RAG disabled")
        except Exception as e:
            logger.warning(f"[RAG] Init failed: {e}")

    def answer(self, question: str, run_id: Optional[str] = None) -> dict:
        if not self._enabled:
            # Graceful fallback — use direct SQL
            return TextToSQLAgent().query(question, run_id)

        results = self._vectorstore.similarity_search(question, k=5)
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
        logger.info(f"[ModelEval] Done: {json.dumps(results, indent=2, default=str)}")
        return results

    def evaluate_prophet(self) -> dict:
        try:
            actual = pd.read_sql(f"""
                SELECT booking_date::date AS date, COUNT(*) AS actual_bookings
                FROM {STAGING_TABLE}
                WHERE booking_date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY booking_date::date ORDER BY date
            """, PG_ENGINE)

            forecast = pd.read_sql("""
                SELECT forecast_date::date AS date, predicted_bookings, mape
                FROM ml_demand_forecast ORDER BY date
            """, PG_ENGINE) if self._table_exists("ml_demand_forecast") else pd.DataFrame()

            if actual.empty or forecast.empty:
                return {"status": "no_data", "mape": None}

            merged = actual.merge(forecast, on="date", how="inner")
            if merged.empty:
                return {"status": "no_overlap", "mape": None}

            mape = float((
                (merged["actual_bookings"] - merged["predicted_bookings"]).abs()
                / merged["actual_bookings"].clip(lower=1)
            ).mean() * 100)

            # Use stored CV mape if available
            stored_mape = forecast["mape"].dropna().iloc[0] if "mape" in forecast.columns and not forecast["mape"].dropna().empty else None
            final_mape  = stored_mape if stored_mape else round(mape, 2)

            if self.mlflow:
                try:
                    with self.mlflow.start_run(run_name=f"prophet_{datetime.now().strftime('%Y%m%d_%H%M')}"):
                        self.mlflow.log_metric("prophet_mape",     round(float(final_mape), 2))
                        self.mlflow.log_metric("prophet_accuracy", round(100 - float(final_mape), 1))
                        self.mlflow.set_tag("model_type", "demand_forecasting")
                except Exception as e:
                    logger.warning(f"[MLflow] {e}")

            return {"status": "success", "mape": round(float(final_mape), 2),
                    "accuracy": round(100 - float(final_mape), 1), "eval_days": len(merged),
                    "verdict": "good" if final_mape < 15 else ("acceptable" if final_mape < PROPHET_RETRAIN_MAPE else "needs_retraining")}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def evaluate_fraud(self) -> dict:
        try:
            if not self._table_exists("ml_fraud_scores"):
                return {"status": "no_data"}
            df    = pd.read_sql("SELECT risk_label, COUNT(*) AS count FROM ml_fraud_scores GROUP BY risk_label", PG_ENGINE)
            total = pd.read_sql("SELECT COUNT(*) AS total FROM ml_fraud_scores", PG_ENGINE)
            total_count    = int(total["total"].iloc[0])
            critical_count = int(df[df["risk_label"] == "CRITICAL"]["count"].sum()) if not df.empty else 0
            high_count     = int(df[df["risk_label"] == "HIGH"]["count"].sum())     if not df.empty else 0
            fraud_rate     = round((critical_count + high_count) / max(total_count, 1) * 100, 2)

            if self.mlflow:
                try:
                    with self.mlflow.start_run(run_name=f"fraud_{datetime.now().strftime('%Y%m%d_%H%M')}"):
                        self.mlflow.log_metric("fraud_rate_pct",    fraud_rate)
                        self.mlflow.log_metric("critical_bookings", critical_count)
                        self.mlflow.set_tag("model_type", "fraud_detection")
                except Exception as e:
                    logger.warning(f"[MLflow] {e}")

            return {"status": "success", "total_scored": total_count,
                    "critical_count": critical_count, "high_risk_count": high_count,
                    "fraud_rate_pct": fraud_rate,
                    "verdict": "alert" if fraud_rate > FRAUD_ALERT_PCT else ("monitor" if fraud_rate > FRAUD_MONITOR_PCT else "normal")}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def evaluate_churn(self) -> dict:
        try:
            if not self._table_exists("ml_churn_scores"):
                return {"status": "no_data"}
            auc_df = pd.read_sql("SELECT model_auc FROM ml_churn_scores ORDER BY created_at DESC LIMIT 1", PG_ENGINE)
            dist   = pd.read_sql("""
                SELECT churn_risk, COUNT(*) AS count,
                       ROUND(AVG(churn_probability)::numeric,3) AS avg_prob
                FROM ml_churn_scores GROUP BY churn_risk ORDER BY avg_prob DESC
            """, PG_ENGINE)
            auc        = float(auc_df["model_auc"].iloc[0]) if not auc_df.empty else 0.0
            high_risk  = int(dist[dist["churn_risk"].isin(["HIGH","CRITICAL"])]["count"].sum()) if not dist.empty else 0
            total_ch   = int(dist["count"].sum()) if not dist.empty else 0
            churn_rate = round(high_risk / max(total_ch, 1) * 100, 2)

            if self.mlflow:
                try:
                    with self.mlflow.start_run(run_name=f"churn_{datetime.now().strftime('%Y%m%d_%H%M')}"):
                        self.mlflow.log_metric("xgb_auc",             auc)
                        self.mlflow.log_metric("churn_rate_pct",      churn_rate)
                        self.mlflow.log_metric("high_risk_customers", high_risk)
                        self.mlflow.set_tag("model_type", "churn_prediction")
                        if auc < CHURN_RETRAIN_AUC:
                            self.mlflow.set_tag("action_required", "RETRAIN")
                            logger.warning(f"[MLflow] RETRAIN tag set — AUC {auc:.3f} < {CHURN_RETRAIN_AUC}")
                except Exception as e:
                    logger.warning(f"[MLflow] {e}")

            return {"status": "success", "xgb_auc": round(auc, 3),
                    "churn_rate_pct": churn_rate, "high_risk_customers": high_risk,
                    "total_scored": total_ch,
                    "verdict": "good" if auc >= 0.80 else ("acceptable" if auc >= CHURN_RETRAIN_AUC else "needs_retraining")}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def _table_exists(self, table: str) -> bool:
        try:
            pd.read_sql(f"SELECT 1 FROM {table} LIMIT 1", PG_ENGINE)
            return True
        except Exception:
            return False


# ══════════════════════════════════════════════════════════════════
# 6. BOOKING INTELLIGENCE ORCHESTRATOR
# FIX 4: Parallel agent execution — ThreadPoolExecutor
# FIX 5: _synthesize with GE context
# ══════════════════════════════════════════════════════════════════
class BookingIntelligenceOrchestrator:

    def __init__(self):
        self.run_id        = f"orch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.sql_agent     = TextToSQLAgent()
        self.health_agent  = PipelineHealthAgent()
        self.anomaly_agent = AnomalyExplainerAgent()
        self.eval_agent    = ModelEvaluationAgent()
        logger.info(f"[Orchestrator] run_id: {self.run_id}")

    def full_intelligence_report(self, question: str = "Give complete business intelligence") -> dict:
        start = time.time()
        logger.info(f"[Orchestrator] Starting parallel execution — run_id: {self.run_id}")

        # ✅ FIX 4: Parallel execution — 3-4x faster
        results = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(self.sql_agent.query, question, self.run_id):     "sql",
                executor.submit(self.health_agent.analyze, self.run_id):          "health",
                executor.submit(self.eval_agent.evaluate_all):                    "eval",
            }
            for future in as_completed(futures):
                key = futures[future]
                try:
                    results[key] = future.result()
                    logger.info(f"[Orchestrator] ✓ {key} done")
                except Exception as e:
                    logger.error(f"[Orchestrator] ✗ {key} failed: {e}")
                    results[key] = {"error": str(e)}

        # Fraud explanation after health (needs fraud data)
        fraud_booking = self._get_top_fraud_booking()
        results["fraud"] = (self.anomaly_agent.explain(fraud_booking, self.run_id)
                            if fraud_booking
                            else {"explanation": "No high-risk bookings detected"})

        # FIX 5: Synthesize with GE context
        final_insight = self._synthesize(question, results)
        total_time    = round(time.time() - start, 2)
        logger.info(f"[Orchestrator] Complete in {total_time}s (parallel)")

        return {
            "run_id":            self.run_id,
            "question":          question,
            "final_insight":     final_insight,
            "data_result":       results.get("sql",{}).get("answer",""),
            "pipeline_health":   results.get("health",{}).get("health_status","UNKNOWN"),
            "pipeline_score":    results.get("health",{}).get("health_score",0),
            "dq_score":          results.get("health",{}).get("dq_score",0),
            "ge_failures":       results.get("health",{}).get("ge_failures",{}),
            "fraud_alert":       results.get("fraud",{}).get("explanation",""),
            "model_evaluation":  results.get("eval",{}),
            "total_latency_sec": total_time,
            "agents_used":       ["TextToSQL","PipelineHealth","AnomalyExplainer","ModelEval","Claude"],
            "execution_mode":    "parallel",
            "timestamp":         datetime.now().isoformat(),
        }

    def quick_answer(self, question: str) -> dict:
        start      = time.time()
        sql_result = self.sql_agent.query(question, run_id=self.run_id)
        return {
            **sql_result,
            "latency_seconds": round(time.time() - start, 2),
            "run_id": self.run_id,
        }

    def _get_top_fraud_booking(self) -> Optional[dict]:
        try:
            df = pd.read_sql(f"""
                SELECT f.booking_id, f.fraud_risk_score, f.risk_label,
                       s.customer_name, s.route, s.trip_type,
                       s.pickup_hour, s.payment_method, s.payment_amount,
                       s.is_split_payment, s.car_segment, s.loyalty_tier
                FROM ml_fraud_scores f
                JOIN {STAGING_TABLE} s ON f.booking_id = s.booking_id
                WHERE f.risk_label = 'CRITICAL'
                ORDER BY f.fraud_risk_score DESC LIMIT 1
            """, PG_ENGINE)
            return df.iloc[0].to_dict() if not df.empty else None
        except Exception as e:
            logger.warning(f"[Orchestrator] fraud fetch: {e}"); return None

    def _synthesize(self, question: str, results: dict) -> str:
        health  = results.get("health", {})
        eval_r  = results.get("eval",   {})
        sql_r   = results.get("sql",    {})
        fraud_r = results.get("fraud",  {})

        # GE context for synthesis
        ge_failures = health.get("ge_failures", {})
        ge_summary  = ""
        if ge_failures.get("failed_columns"):
            cols = [f["column_name"] for f in ge_failures["failed_columns"][:3]]
            ge_summary = f"GE failed columns: {cols} | Failed records: {ge_failures.get('total_failed_records',0)}"

        if not ANTHROPIC_API_KEY:
            return (f"Pipeline: {health.get('health_status','?')} ({health.get('health_score',0)}/100) | "
                    f"DQ: {health.get('dq_score',0):.1f}% | {ge_summary} | "
                    f"Prophet MAPE: {eval_r.get('prophet',{}).get('mape','?')}% | "
                    f"XGBoost AUC: {eval_r.get('xgboost',{}).get('xgb_auc','?')} | "
                    f"Set ANTHROPIC_API_KEY for AI synthesis.")
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            msg = client.messages.create(
                model="claude-sonnet-4-20250514", max_tokens=600,
                messages=[{"role": "user", "content": f"""
AI Intelligence layer for car booking pipeline. 4 agents ran in parallel.

QUESTION: {question}

━━━━ AGENT RESULTS ━━━━
1. DATA (TextToSQL):
{sql_r.get('answer','no data')[:400]}

2. PIPELINE HEALTH:
Status: {health.get('health_status','?')} | Score: {health.get('health_score',0)}/100
DQ Score: {health.get('dq_score',0):.1f}%
{ge_summary}

3. FRAUD (Isolation Forest):
Rate: {eval_r.get('isolation_forest',{}).get('fraud_rate_pct','?')}%
Alert: {str(fraud_r.get('explanation','none'))[:200]}

4. ML MODELS:
Prophet MAPE: {eval_r.get('prophet',{}).get('mape','?')}%
XGBoost AUC: {eval_r.get('xgboost',{}).get('xgb_auc','?')}
Churn high-risk: {eval_r.get('xgboost',{}).get('high_risk_customers','?')}
━━━━━━━━━━━━━━━━━━━━━

3-4 sentence executive summary. Be specific with numbers. Flag concerns clearly.
"""}])
            return msg.content[0].text
        except Exception as e:
            return f"Synthesis error: {e}"


# ══════════════════════════════════════════════════════════════════
# QUICK TEST
# ══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n" + "="*60)
    print("  AI AGENTS v3 — Car Booking Pipeline")
    print("="*60)
    print(f"  Claude   : {'✓' if ANTHROPIC_API_KEY else '○ fallback'}")
    print(f"  LangSmith: {'✓' if LANGSMITH_ENABLED else '○'}")
    print(f"  MLflow   : {MLFLOW_TRACKING_URI}")
    print(f"  Mode     : Parallel (ThreadPoolExecutor)")
    print("="*60 + "\n")

    print("1. TextToSQLAgent (Direct Claude)")
    r = TextToSQLAgent().query("top routes by bookings")
    print(f"   Mode: {r.get('mode')} | {r.get('answer','')[:150]}\n")

    print("2. PipelineHealthAgent (with GE failures)")
    h = PipelineHealthAgent().analyze()
    print(f"   Score: {h['health_score']}/100 | Status: {h['health_status']}")
    print(f"   GE failures: {len(h['ge_failures'].get('failed_columns',[]))} columns\n")

    print("3. ModelEvaluationAgent")
    ev = ModelEvaluationAgent().evaluate_all()
    print(f"   Prophet MAPE: {ev.get('prophet',{}).get('mape','N/A')}%")
    print(f"   XGBoost AUC : {ev.get('xgboost',{}).get('xgb_auc','N/A')}\n")

    print("4. Orchestrator — Full Report (Parallel)")
    orch   = BookingIntelligenceOrchestrator()
    report = orch.full_intelligence_report("Business performance summary")
    print(f"   Run ID  : {report['run_id']}")
    print(f"   Health  : {report['pipeline_health']} ({report['pipeline_score']}/100)")
    print(f"   Latency : {report['total_latency_sec']}s ({report['execution_mode']})")
    print(f"   Insight : {report['final_insight'][:300]}")