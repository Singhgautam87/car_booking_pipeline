"""
ML Models — Car Booking Pipeline
Same config_loader pattern as write_to_postgres.py

Reads:  customer_booking_staging (PostgreSQL) — 36 columns
Writes: ml_demand_forecast, ml_fraud_scores, ml_churn_scores (PostgreSQL)

All 36 columns from sql/postgres.sql:
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
"""

import os
import sys
import warnings
import logging
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Exact same pattern as write_to_postgres.py ─────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config       = load_config()
postgres_cfg = config.get_postgres_config()
tables       = config.get_tables()

STAGING_TABLE = tables.get("staging", "customer_booking_staging")

PG_CONN_PARAMS = {
    "host":     postgres_cfg["host"],
    "port":     postgres_cfg["port"],
    "database": postgres_cfg["database"],
    "user":     postgres_cfg["user"],
    "password": postgres_cfg["password"],
}


def get_pg_conn():
    return psycopg2.connect(**PG_CONN_PARAMS)


def get_booking_data() -> pd.DataFrame:
    """Read all 36 columns from customer_booking_staging"""
    conn = get_pg_conn()
    try:
        df = pd.read_sql(f"""
            SELECT
                booking_id, customer_id, customer_name, email,
                loyalty_tier, loyalty_points, booking_date,
                loyalty_rank, is_high_value_customer, points_tier,
                pickup_location, pickup_time, drop_location, drop_time,
                pickup_hour, pickup_slot, trip_type, route,
                booking_year, booking_month, email_domain,
                car_id, model, price_per_day,
                insurance_provider, insurance_coverage,
                price_tier, has_full_insurance, insurer_type, car_segment,
                payment_id, payment_method, payment_amount,
                is_digital_payment, amount_tier, payment_count, is_split_payment
            FROM {STAGING_TABLE}
        """, conn)
        logger.info(f"📦 {len(df):,} records | {len(df.columns)} columns | from {STAGING_TABLE}")
        return df
    except Exception as e:
        logger.error(f"❌ Could not read {STAGING_TABLE}: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


# ═══════════════════════════════════════════════════
# MODEL 1 — DEMAND FORECASTING (Prophet)
# ═══════════════════════════════════════════════════

def run_demand_forecasting():
    logger.info("[Demand Forecasting] Starting...")
    try:
        from prophet import Prophet
    except ImportError:
        os.system("pip install prophet --quiet")
        from prophet import Prophet

    df = get_booking_data()

    if df.empty:
        logger.warning("[Demand Forecasting] No data — using synthetic.")
        base  = datetime.now() - timedelta(days=90)
        daily = pd.DataFrame({
            "ds": [base + timedelta(days=i) for i in range(90)],
            "y":  (np.random.poisson(150, 90) + np.sin(np.arange(90)*0.2)*20).clip(0)
        })
    else:
        df["booking_date"] = pd.to_datetime(df["booking_date"])
        daily = (df.groupby("booking_date")
                   .size()
                   .reset_index(name="y")
                   .rename(columns={"booking_date": "ds"}))

    daily = daily.sort_values("ds").reset_index(drop=True)

    model    = Prophet(yearly_seasonality=True, weekly_seasonality=True,
                       daily_seasonality=False, changepoint_prior_scale=0.05)
    model.fit(daily)
    forecast = model.predict(model.make_future_dataframe(periods=30))
    next30   = forecast.tail(30)

    conn = get_pg_conn(); cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ml_demand_forecast (
            id                 SERIAL PRIMARY KEY,
            forecast_date      DATE,
            predicted_bookings FLOAT,
            lower_bound        FLOAT,
            upper_bound        FLOAT,
            model_name         VARCHAR(50),
            created_at         TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("DELETE FROM ml_demand_forecast WHERE model_name = 'prophet'")
    for _, row in next30.iterrows():
        cur.execute("""
            INSERT INTO ml_demand_forecast
                (forecast_date, predicted_bookings, lower_bound, upper_bound, model_name)
            VALUES (%s,%s,%s,%s,%s)
        """, (row["ds"].date(), max(0.0, float(row["yhat"])),
              max(0.0, float(row["yhat_lower"])),
              max(0.0, float(row["yhat_upper"])), "prophet"))
    conn.commit(); cur.close(); conn.close()
    logger.info("✅ Demand Forecasting done — 30-day forecast saved.")
    return next30[["ds","yhat","yhat_lower","yhat_upper"]].to_dict("records")


# ═══════════════════════════════════════════════════
# MODEL 2 — FRAUD DETECTION (Isolation Forest)
# New features: pickup_hour, route, trip_type,
#               car_segment, is_digital_payment,
#               is_split_payment, amount_tier
# ═══════════════════════════════════════════════════

def run_fraud_detection():
    logger.info("[Fraud Detection] Starting...")
    from sklearn.ensemble    import IsolationForest
    from sklearn.preprocessing import LabelEncoder
    import joblib

    df = get_booking_data()
    if df.empty:
        logger.warning("[Fraud Detection] No data."); return []

    feature_df = df.copy()
    le         = LabelEncoder()

    cat_cols = ["loyalty_tier","payment_method","insurance_coverage","model",
                "trip_type","car_segment","amount_tier","price_tier",
                "insurer_type","pickup_slot","points_tier"]
    for col in cat_cols:
        if col in feature_df.columns:
            feature_df[col] = le.fit_transform(feature_df[col].astype(str))

    bool_cols = ["is_high_value_customer","has_full_insurance",
                 "is_digital_payment","is_split_payment"]
    for col in bool_cols:
        if col in feature_df.columns:
            feature_df[col] = feature_df[col].fillna(False).astype(int)

    # ✅ 19 features — all from new 36-column staging
    feature_cols = [c for c in [
        "payment_amount","price_per_day","loyalty_points","loyalty_rank",
        "pickup_hour","payment_count","booking_month",
        "loyalty_tier","payment_method","insurance_coverage",
        "model","trip_type","car_segment","amount_tier","price_tier",
        "is_digital_payment","is_split_payment",
        "is_high_value_customer","has_full_insurance"
    ] if c in feature_df.columns]

    X = feature_df[feature_cols].fillna(0)

    model = IsolationForest(n_estimators=200, contamination=0.05,
                            random_state=42, n_jobs=-1)
    model.fit(X)

    df["anomaly_score"]    = model.score_samples(X)
    df["is_fraud"]         = (model.predict(X) == -1).astype(int)
    s_min, s_max           = df["anomaly_score"].min(), df["anomaly_score"].max()
    df["fraud_risk_score"] = (
        (df["anomaly_score"] - s_max) / (s_min - s_max + 1e-9) * 100
    ).round(2).clip(0, 100)
    df["risk_label"]       = pd.cut(df["fraud_risk_score"],
                                    bins=[0,25,50,75,100],
                                    labels=["LOW","MEDIUM","HIGH","CRITICAL"],
                                    include_lowest=True)

    anomalies = df[df["is_fraud"] == 1]
    logger.info(f"[Fraud Detection] {len(anomalies)} suspicious ({len(anomalies)/len(df)*100:.1f}%)")

    conn = get_pg_conn(); cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ml_fraud_scores (
            id                 SERIAL PRIMARY KEY,
            booking_id         VARCHAR(20),
            customer_id        VARCHAR(20),
            customer_name      VARCHAR(100),
            route              VARCHAR(100),
            trip_type          VARCHAR(20),
            car_segment        VARCHAR(20),
            payment_amount     FLOAT,
            payment_method     VARCHAR(30),
            is_digital_payment BOOLEAN,
            is_split_payment   BOOLEAN,
            pickup_hour        INT,
            fraud_risk_score   FLOAT,
            risk_label         VARCHAR(20),
            is_fraud           INTEGER,
            anomaly_score      FLOAT,
            model_name         VARCHAR(50),
            created_at         TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("DELETE FROM ml_fraud_scores WHERE model_name = 'isolation_forest'")
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO ml_fraud_scores
                (booking_id, customer_id, customer_name,
                 route, trip_type, car_segment,
                 payment_amount, payment_method,
                 is_digital_payment, is_split_payment, pickup_hour,
                 fraud_risk_score, risk_label, is_fraud, anomaly_score, model_name)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (str(row.get("booking_id","")),    str(row.get("customer_id","")),
              str(row.get("customer_name","")),
              str(row.get("route","")),          str(row.get("trip_type","")),
              str(row.get("car_segment","")),
              float(row.get("payment_amount",0)), str(row.get("payment_method","")),
              bool(row.get("is_digital_payment",False)),
              bool(row.get("is_split_payment",False)),
              int(row.get("pickup_hour",0)    if pd.notna(row.get("pickup_hour")) else 0),
              float(row["fraud_risk_score"]),  str(row["risk_label"]),
              int(row["is_fraud"]),            float(row["anomaly_score"]),
              "isolation_forest"))
    conn.commit(); cur.close(); conn.close()
    os.makedirs("/tmp/models", exist_ok=True)
    joblib.dump(model, "/tmp/models/fraud_model.pkl")
    logger.info("✅ Fraud Detection done.")
    return anomalies[["booking_id","customer_id","fraud_risk_score","risk_label"]].to_dict("records")


# ═══════════════════════════════════════════════════
# MODEL 3 — CHURN PREDICTION (XGBoost)
# New features: loyalty_rank, is_high_value_customer,
#               trip_type, car_segment, points_tier
# ═══════════════════════════════════════════════════

def run_churn_prediction():
    logger.info("[Churn Prediction] Starting...")
    try:
        import xgboost as xgb
    except ImportError:
        os.system("pip install xgboost --quiet")
        import xgboost as xgb

    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing   import LabelEncoder
    from sklearn.metrics         import roc_auc_score
    import joblib

    df = get_booking_data()
    if df.empty:
        logger.warning("[Churn Prediction] No data."); return []

    # Churn label: low loyalty + low payment + not high value customer
    low_loyalty = df["loyalty_tier"].str.lower().isin(["bronze","new","silver"]) \
                  if "loyalty_tier" in df.columns else pd.Series([False]*len(df))
    avg_payment = df["payment_amount"].mean() if "payment_amount" in df.columns else 0
    low_payment = df["payment_amount"] < avg_payment if "payment_amount" in df.columns \
                  else pd.Series([True]*len(df))
    not_vip     = ~df["is_high_value_customer"].fillna(False).astype(bool) \
                  if "is_high_value_customer" in df.columns else pd.Series([True]*len(df))

    df["churn_label"] = (low_loyalty & low_payment & not_vip).astype(int)
    np.random.seed(42)
    df["churn_label"] = ((df["churn_label"] + np.random.binomial(1, 0.08, len(df))) > 0).astype(int)

    feature_df = df.copy()
    le         = LabelEncoder()

    cat_cols = ["loyalty_tier","payment_method","insurance_coverage","model",
                "trip_type","car_segment","amount_tier","price_tier",
                "points_tier","pickup_slot"]
    for col in cat_cols:
        if col in feature_df.columns:
            feature_df[col] = le.fit_transform(feature_df[col].astype(str))

    bool_cols = ["is_high_value_customer","has_full_insurance",
                 "is_digital_payment","is_split_payment"]
    for col in bool_cols:
        if col in feature_df.columns:
            feature_df[col] = feature_df[col].fillna(False).astype(int)

    # ✅ 18 features from new 36-column staging
    feature_cols = [c for c in [
        "payment_amount","price_per_day","loyalty_points","loyalty_rank",
        "pickup_hour","payment_count","booking_month",
        "loyalty_tier","payment_method","trip_type",
        "car_segment","amount_tier","price_tier","points_tier",
        "is_high_value_customer","is_digital_payment",
        "is_split_payment","has_full_insurance"
    ] if c in feature_df.columns]

    X = feature_df[feature_cols].fillna(0)
    y = df["churn_label"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y)

    model = xgb.XGBClassifier(
        n_estimators=200, max_depth=5, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        eval_metric="logloss", random_state=42, n_jobs=-1, verbosity=0)
    model.fit(X_train, y_train, eval_set=[(X_test,y_test)], verbose=False)

    auc = roc_auc_score(y_test, model.predict_proba(X_test)[:,1])
    logger.info(f"[Churn Prediction] XGBoost AUC: {auc:.3f}")

    df["churn_probability"] = model.predict_proba(X)[:,1]
    df["churn_risk"]        = pd.cut(df["churn_probability"],
                                     bins=[0,0.3,0.6,0.8,1.0],
                                     labels=["LOW","MEDIUM","HIGH","CRITICAL"],
                                     include_lowest=True)

    customer_churn = (df.groupby("customer_id")
                        .agg(churn_probability     =("churn_probability","max"),
                             customer_name         =("customer_name","first"),
                             loyalty_tier          =("loyalty_tier","first"),
                             points_tier           =("points_tier","first"),
                             is_high_value_customer=("is_high_value_customer","first"))
                        .reset_index())
    customer_churn["churn_risk"] = pd.cut(customer_churn["churn_probability"],
                                          bins=[0,0.3,0.6,0.8,1.0],
                                          labels=["LOW","MEDIUM","HIGH","CRITICAL"],
                                          include_lowest=True)

    conn = get_pg_conn(); cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ml_churn_scores (
            id                     SERIAL PRIMARY KEY,
            customer_id            VARCHAR(20),
            customer_name          VARCHAR(100),
            loyalty_tier           VARCHAR(20),
            points_tier            VARCHAR(20),
            is_high_value_customer BOOLEAN,
            churn_probability      FLOAT,
            churn_risk             VARCHAR(20),
            model_auc              FLOAT,
            model_name             VARCHAR(50),
            created_at             TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("DELETE FROM ml_churn_scores WHERE model_name = 'xgboost'")
    for _, row in customer_churn.iterrows():
        cur.execute("""
            INSERT INTO ml_churn_scores
                (customer_id, customer_name, loyalty_tier, points_tier,
                 is_high_value_customer, churn_probability, churn_risk,
                 model_auc, model_name)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (str(row["customer_id"]),  str(row["customer_name"]),
              str(row["loyalty_tier"]), str(row["points_tier"]),
              bool(row["is_high_value_customer"]),
              float(row["churn_probability"]),  str(row["churn_risk"]),
              float(auc), "xgboost"))
    conn.commit(); cur.close(); conn.close()
    os.makedirs("/tmp/models", exist_ok=True)
    joblib.dump(model, "/tmp/models/churn_model.pkl")

    high_risk = customer_churn[customer_churn["churn_risk"].isin(["HIGH","CRITICAL"])]
    logger.info(f"✅ Churn Prediction done. {len(high_risk)} high-risk customers.")
    return customer_churn.to_dict("records")


# ═══════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "="*55)
    print("  ML MODELS — Car Booking Pipeline")
    print("="*55)
    print(f"  Host  : {PG_CONN_PARAMS['host']}:{PG_CONN_PARAMS['port']}")
    print(f"  DB    : {PG_CONN_PARAMS['database']}")
    print(f"  Table : {STAGING_TABLE} (36 columns)")
    print("="*55 + "\n")

    for name, fn in [("Demand Forecasting", run_demand_forecasting),
                     ("Fraud Detection",    run_fraud_detection),
                     ("Churn Prediction",   run_churn_prediction)]:
        try:
            fn()
            print(f"✅ {name} complete\n")
        except Exception as e:
            logger.error(f"❌ {name} failed: {e}")
            import traceback; traceback.print_exc()

    print("="*55)
    print("  ALL MODELS DONE")
    print("="*55)