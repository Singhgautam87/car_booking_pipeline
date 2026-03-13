"""
Car Booking Pipeline Dashboard — v3
Architecture : dcc.Store → tab-specific callbacks → lazy tab loading
Stack        : Kafka → Spark → Delta Lake → PostgreSQL → Dash
New in v3    : BLANK SCREEN FIX — aggregated queries, not SELECT *
               Weighted Health Score 0-100
               AI Auto-Insights on page load
               GE failed records → Claude root cause analysis
"""
import os
import json
import requests
import dash
from dash import dcc, html, Input, Output, State, dash_table, ctx
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# ================================================================
# DB CONNECTIONS
# ================================================================
def get_pg_engine():
    host = os.getenv("POSTGRES_HOST", "postgres")
    return create_engine(
        f"postgresql+psycopg2://admin:admin@{host}:5432/booking",
        pool_pre_ping=True)

def get_mysql_engine():
    host = os.getenv("MYSQL_HOST", "mysql")
    return create_engine(
        f"mysql+mysqlconnector://admin:admin@{host}:3306/booking",
        pool_pre_ping=True)

def get_booking_data():
    """
    FIX: Aggregated summaries only — not SELECT * 30k rows
    Full data fetched per-tab only when needed (lazy loading)
    """
    try:
        engine = get_pg_engine()
        kpi = pd.read_sql("""
            SELECT COUNT(*) AS total_bookings,
                   COUNT(DISTINCT customer_id) AS total_customers,
                   ROUND(SUM(payment_amount)::numeric,0) AS total_revenue,
                   SUM(is_high_value_customer::int) AS high_value_customers,
                   SUM(is_digital_payment::int) AS digital_payments,
                   COUNT(DISTINCT route) AS unique_routes,
                   COUNT(DISTINCT car_segment) AS car_segments,
                   COUNT(DISTINCT model) AS car_models
            FROM customer_booking_staging
        """, engine)
        daily = pd.read_sql("""
            SELECT booking_date::date AS booking_date,
                   COUNT(*) AS bookings,
                   ROUND(SUM(payment_amount)::numeric,0) AS revenue
            FROM customer_booking_staging
            GROUP BY booking_date::date ORDER BY booking_date
        """, engine)
        segments = pd.read_sql("""
            SELECT car_segment, COUNT(*) AS bookings
            FROM customer_booking_staging
            GROUP BY car_segment ORDER BY bookings DESC
        """, engine)
        top_cars = pd.read_sql("""
            SELECT model, ROUND(SUM(payment_amount)::numeric,0) AS revenue
            FROM customer_booking_staging
            GROUP BY model ORDER BY revenue DESC LIMIT 7
        """, engine)
        payments = pd.read_sql("""
            SELECT payment_method, COUNT(*) AS bookings,
                   ROUND(SUM(payment_amount)::numeric,0) AS revenue
            FROM customer_booking_staging
            GROUP BY payment_method ORDER BY bookings DESC
        """, engine)
        loyalty = pd.read_sql("""
            SELECT loyalty_tier, COUNT(*) AS bookings,
                   SUM(is_high_value_customer::int) AS high_value
            FROM customer_booking_staging
            GROUP BY loyalty_tier ORDER BY bookings DESC
        """, engine)
        heatmap = pd.read_sql("""
            SELECT TO_CHAR(booking_date, 'Mon') AS month,
                   TRIM(TO_CHAR(booking_date, 'Day')) AS dow,
                   COUNT(*) AS bookings
            FROM customer_booking_staging
            GROUP BY TO_CHAR(booking_date, 'Mon'), TRIM(TO_CHAR(booking_date, 'Day'))
        """, engine)
        engine.dispose()
        return {
            "kpi": kpi.to_dict("records"),
            "daily": daily.to_dict("records"),
            "segments": segments.to_dict("records"),
            "top_cars": top_cars.to_dict("records"),
            "payments": payments.to_dict("records"),
            "loyalty": loyalty.to_dict("records"),
            "heatmap": heatmap.to_dict("records"),
        }
    except Exception as e:
        print(f"[POSTGRES ERROR] {e}")
        return {}

def get_tab_data(tab: str) -> pd.DataFrame:
    """Lazy load — full data only for active tab"""
    try:
        engine = get_pg_engine()
        queries = {
            "analytics": "SELECT booking_date, payment_amount, booking_id, amount_tier, model, price_per_day, insurance_coverage, insurer_type, loyalty_tier, payment_method FROM customer_booking_staging",
            "routes": "SELECT route, trip_type, car_segment, booking_id, payment_amount, pickup_hour, pickup_slot, booking_date, amount_tier FROM customer_booking_staging",
            "observability": "SELECT booking_date, booking_id, customer_id, pickup_hour, payment_amount FROM customer_booking_staging",
        }
        if tab not in queries:
            engine.dispose()
            return pd.DataFrame()
        df = pd.read_sql(queries[tab], engine)
        engine.dispose()
        return df
    except Exception as e:
        print(f"[TAB DATA ERROR] {tab}: {e}")
        return pd.DataFrame()

def get_monitoring_data():
    host = os.getenv("MYSQL_HOST", "mysql")
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://admin:admin@{host}:3306/booking",
            pool_pre_ping=True)
        val_df    = pd.read_sql("SELECT * FROM validation_results ORDER BY created_at DESC", engine)
        exp_df    = pd.read_sql("SELECT * FROM validation_expectation_details", engine)
        fail_df   = pd.read_sql("SELECT * FROM validation_failed_records LIMIT 500", engine)
        sla_df    = pd.read_sql("SELECT * FROM pipeline_run_stats ORDER BY started_at DESC", engine)
        alerts_df = pd.read_sql("SELECT * FROM pipeline_alerts ORDER BY created_at DESC LIMIT 50", engine)
        schema_df = pd.read_sql("SELECT * FROM schema_registry_log ORDER BY registered_at DESC", engine)
        engine.dispose()
        return val_df, exp_df, fail_df, sla_df, alerts_df, schema_df
    except Exception as e:
        print(f"[MYSQL ERROR] {e}")
        return (pd.DataFrame(),)*6

def get_ml_data():
    host = os.getenv("POSTGRES_HOST", "postgres")
    try:
        engine = create_engine(
            f"postgresql+psycopg2://admin:admin@{host}:5432/booking",
            pool_pre_ping=True)
        forecast_df = pd.read_sql("SELECT * FROM ml_demand_forecast ORDER BY forecast_date", engine) \
                      if _table_exists(engine, "ml_demand_forecast") else pd.DataFrame()
        fraud_df    = pd.read_sql("SELECT * FROM ml_fraud_scores ORDER BY fraud_risk_score DESC", engine) \
                      if _table_exists(engine, "ml_fraud_scores") else pd.DataFrame()
        churn_df    = pd.read_sql("SELECT * FROM ml_churn_scores ORDER BY churn_probability DESC", engine) \
                      if _table_exists(engine, "ml_churn_scores") else pd.DataFrame()
        engine.dispose()
        return forecast_df, fraud_df, churn_df
    except Exception as e:
        print(f"[ML DATA ERROR] {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def _table_exists(engine, table):
    try:
        from sqlalchemy import inspect as sqlinspect
        return table in sqlinspect(engine).get_table_names()
    except Exception:
        return False

# ================================================================
# THEME
# ================================================================
BG   = "#090d13"; CARD = "#0f1923"; SIDE = "#060a10"
B    = "#1e2d3d"; B2   = "#243447"
C1   = "#00d4ff"; C2   = "#00ff88"; C3   = "#ff4757"
C4   = "#a78bfa"; C5   = "#ffd43b"; C6   = "#74c0fc"
C7   = "#ff9f43"  # orange — new for AI tab
TX   = "#cdd9e5"; SUB  = "#6e7f8d"; MUT  = "#3d5166"
GR   = "rgba(30,45,61,0.5)"
MONO = "'JetBrains Mono','Fira Code','Courier New',monospace"

CBASE = dict(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor=CARD,
             font=dict(color=TX, family=MONO, size=11),
             margin=dict(l=50, r=20, t=46, b=38),
             title_font=dict(color=C1, size=12, family=MONO))
AXIS  = dict(gridcolor=GR, zerolinecolor=GR, color=SUB)

# ================================================================
# UI HELPERS
# ================================================================
def safe_df(data):
    try:   return pd.DataFrame(data) if data else pd.DataFrame()
    except: return pd.DataFrame()

def card(children, accent=B, mb="12px"):
    return html.Div(style={
        "backgroundColor":CARD,"borderRadius":"8px",
        "border":f"1px solid {B}","borderLeft":f"3px solid {accent}",
        "padding":"18px","marginBottom":mb}, children=children)

def kpi(label, value, color, icon, hint=""):
    return html.Div(style={
        "backgroundColor":CARD,"borderRadius":"8px",
        "border":f"1px solid {B}","borderTop":f"2px solid {color}",
        "padding":"16px","flex":"1","minWidth":"120px"}, children=[
        html.Div(style={"display":"flex","justifyContent":"space-between",
                        "alignItems":"center","marginBottom":"10px"}, children=[
            html.Span(icon, style={"fontSize":"18px"}),
            html.Span(label, style={"color":SUB,"fontSize":"9px","textTransform":"uppercase",
                                     "letterSpacing":"1.5px","textAlign":"right",
                                     "maxWidth":"90px","fontFamily":MONO}),
        ]),
        html.Div(str(value), style={"color":color,"fontSize":"24px","fontWeight":"700",
                                     "fontFamily":MONO,"lineHeight":"1","marginBottom":"4px"}),
        html.Div(hint, style={"color":MUT,"fontSize":"9px","fontFamily":MONO}) if hint else None,
    ])

def pill(text, color):
    try:  r,g,b = int(color[1:3],16), int(color[3:5],16), int(color[5:7],16); bg=f"rgba({r},{g},{b},0.12)"
    except: bg = "rgba(0,0,0,0.2)"
    return html.Span(text, style={
        "backgroundColor":bg,"color":color,"border":f"1px solid {color}",
        "borderRadius":"4px","padding":"2px 10px","fontSize":"9px",
        "fontFamily":MONO,"fontWeight":"600","letterSpacing":"0.5px"})

def section(title, sub=None):
    return html.Div(style={"marginBottom":"20px"}, children=[
        html.Div(style={"display":"flex","alignItems":"center","gap":"8px","marginBottom":"3px"}, children=[
            html.Span("▸", style={"color":C1,"fontSize":"13px"}),
            html.H2(title, style={"color":TX,"fontSize":"17px","fontWeight":"700",
                                   "margin":"0","fontFamily":MONO}),
        ]),
        html.Div(sub, style={"color":SUB,"fontSize":"10px","fontFamily":MONO,"paddingLeft":"21px"}) if sub else None,
    ])

def nav_style(active):
    return {"backgroundColor":"rgba(0,212,255,0.10)" if active else "transparent",
            "color":C1 if active else SUB,
            "border":f"1px solid {C1}" if active else "1px solid transparent",
            "padding":"8px 12px","borderRadius":"6px","cursor":"pointer",
            "fontSize":"11px","fontWeight":"600","fontFamily":MONO,
            "width":"100%","textAlign":"left","marginBottom":"3px"}

def empty(msg="no data"):
    fig = go.Figure()
    fig.add_annotation(text=f"[ {msg} ]", xref="paper", yref="paper",
                       x=0.5, y=0.5, showarrow=False,
                       font={"color":MUT,"size":12,"family":MONO})
    fig.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, height=220)
    return fig

def loading(children):
    return dcc.Loading(children=children, type="dot", color=C1, style={"minHeight":"100px"})

def split_bars(df, col, color):
    if df.empty or col not in df.columns:
        return [html.Div("no data", style={"color":MUT,"fontSize":"11px","fontFamily":MONO})]
    grp = (df.groupby(col)["booking_id"].count().reset_index()
           .assign(pct=lambda x: x["booking_id"]/x["booking_id"].sum()*100)
           .sort_values("pct", ascending=False))
    rows = []
    for _, r in grp.iterrows():
        rows.append(html.Div(style={"marginBottom":"10px"}, children=[
            html.Div(style={"display":"flex","justifyContent":"space-between","marginBottom":"3px"}, children=[
                html.Span(str(r[col]), style={"color":TX,"fontSize":"11px","fontFamily":MONO}),
                html.Span(f"{r['pct']:.1f}%", style={"color":color,"fontSize":"11px","fontFamily":MONO,"fontWeight":"700"}),
            ]),
            html.Div(style={"backgroundColor":B,"borderRadius":"2px","height":"3px"}, children=[
                html.Div(style={"backgroundColor":color,"width":f"{r['pct']:.1f}%","height":"100%","borderRadius":"2px","opacity":"0.8"}),
            ]),
        ]))
    return rows

# ================================================================
# APP
# ================================================================
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "car-booking · pipeline dashboard"

app.index_string = '''<!DOCTYPE html>
<html>
<head>
    {%metas%}<title>{%title%}</title>{%favicon%}{%css%}
    <style>
        * { box-sizing: border-box; }
        body { margin: 0; background: #090d13; }
        ::-webkit-scrollbar { width: 5px; }
        ::-webkit-scrollbar-track { background: #060a10; }
        ::-webkit-scrollbar-thumb { background: #1e2d3d; border-radius: 3px; }
        .Select-control { background: #0f1923 !important; border-color: #1e2d3d !important; }
        .Select-menu-outer { background: #0f1923 !important; border-color: #1e2d3d !important; }
        .Select-option { background: #0f1923 !important; color: #cdd9e5 !important; }
        .Select-option:hover { background: #1e2d3d !important; }
        .dash-spinner { color: #00d4ff !important; }
        textarea:focus { outline: none !important; }
    </style>
</head>
<body>{%app_entry%}
<footer>{%config%}{%scripts%}{%renderer%}</footer>
</body></html>'''

# ================================================================
# LAYOUT
# ================================================================
TABS = ["tab-overview","tab-analytics","tab-routes","tab-observability",
        "tab-pipeline","tab-quality","tab-ai","tab-debug"]

app.layout = html.Div(
    style={"backgroundColor":BG,"minHeight":"100vh","display":"flex",
           "flexDirection":"column","fontFamily":MONO,"color":TX},
    children=[
        # ── Stores ──────────────────────────────────────────────
        dcc.Store(id="store-booking",     storage_type="memory"),
        dcc.Store(id="store-validation",  storage_type="memory"),
        dcc.Store(id="store-expectation", storage_type="memory"),
        dcc.Store(id="store-failed",      storage_type="memory"),
        dcc.Store(id="store-sla",         storage_type="memory"),
        dcc.Store(id="store-alerts",      storage_type="memory"),
        dcc.Store(id="store-schema",      storage_type="memory"),
        dcc.Store(id="store-forecast",    storage_type="memory"),
        dcc.Store(id="store-fraud",       storage_type="memory"),
        dcc.Store(id="store-churn",       storage_type="memory"),
        dcc.Store(id="store-chat",        storage_type="memory", data=[]),
        dcc.Store(id="active-tab",        storage_type="memory", data="tab-overview"),

        # ── Topbar ──────────────────────────────────────────────
        html.Div(style={
            "backgroundColor":SIDE,"borderBottom":f"1px solid {B}",
            "padding":"0 24px","display":"flex","alignItems":"center",
            "justifyContent":"space-between","height":"52px",
            "position":"sticky","top":"0","zIndex":"200"}, children=[
            html.Div(style={"display":"flex","alignItems":"center","gap":"16px"}, children=[
                html.Div(style={"display":"flex","alignItems":"center","gap":"8px"}, children=[
                    html.Span("◈", style={"color":C1,"fontSize":"18px"}),
                    html.Span("car-booking-pipeline",
                              style={"color":TX,"fontWeight":"700","fontSize":"13px"}),
                ]),
                html.Div(style={"display":"flex","gap":"5px","alignItems":"center"}, children=[
                    pill("kafka",C2), html.Span("→",style={"color":MUT,"fontSize":"11px"}),
                    pill("spark",C2), html.Span("→",style={"color":MUT,"fontSize":"11px"}),
                    pill("delta-lake",C2), html.Span("→",style={"color":MUT,"fontSize":"11px"}),
                    pill("postgres",C2), html.Span("→",style={"color":MUT,"fontSize":"11px"}),
                    pill("ai·ml",C7),
                ]),
            ]),
            html.Div(style={"display":"flex","alignItems":"center","gap":"16px"}, children=[
                html.Span(id="alert-badge"),
                html.Span(id="store-status", style={"color":SUB,"fontSize":"10px"}),
                html.Span(id="last-updated", style={"color":MUT,"fontSize":"10px"}),
                html.Button("↻ refresh", id="refresh-btn", n_clicks=0, style={
                    "backgroundColor":"transparent","color":C1,
                    "border":f"1px solid {B2}","padding":"4px 14px",
                    "borderRadius":"4px","cursor":"pointer",
                    "fontSize":"10px","fontFamily":MONO}),
            ]),
            dcc.Interval(id="auto-refresh", interval=30000, n_intervals=0),
        ]),

        # ── Body ────────────────────────────────────────────────
        html.Div(style={"display":"flex","flex":"1","overflow":"hidden"}, children=[

            # ── Sidebar ─────────────────────────────────────────
            html.Div(style={
                "width":"188px","minWidth":"188px","backgroundColor":SIDE,
                "borderRight":f"1px solid {B}","padding":"18px 10px",
                "display":"flex","flexDirection":"column",
                "position":"sticky","top":"52px",
                "height":"calc(100vh - 52px)","overflowY":"auto"}, children=[
                html.Div("VIEWS", style={"color":MUT,"fontSize":"9px","fontWeight":"700",
                                          "letterSpacing":"2px","marginBottom":"10px","paddingLeft":"4px"}),
                html.Button("📊  overview",      id="tab-overview",      n_clicks=0, style=nav_style(True)),
                html.Button("📈  analytics",     id="tab-analytics",     n_clicks=0, style=nav_style(False)),
                html.Button("🗺️  routes",         id="tab-routes",        n_clicks=0, style=nav_style(False)),
                html.Button("🔭  observability", id="tab-observability", n_clicks=0, style=nav_style(False)),
                html.Button("⚡  pipeline",      id="tab-pipeline",      n_clicks=0, style=nav_style(False)),
                html.Button("🛡️  data quality",  id="tab-quality",       n_clicks=0, style=nav_style(False)),
                html.Button("🤖  ai · ml",       id="tab-ai",            n_clicks=0, style=nav_style(False)),
                html.Button("🐛  debug",         id="tab-debug",         n_clicks=0, style=nav_style(False)),

                html.Div(style={"borderTop":f"1px solid {B}","margin":"18px 0 14px"}),
                html.Div("FILTERS", style={"color":MUT,"fontSize":"9px","fontWeight":"700",
                                            "letterSpacing":"2px","marginBottom":"12px","paddingLeft":"4px"}),
                html.Div("loyalty_tier", style={"color":C6,"fontSize":"9px","marginBottom":"4px","paddingLeft":"2px"}),
                dcc.Dropdown(id="filter-loyalty",   options=[], placeholder="all", clearable=True, style={"fontSize":"11px","marginBottom":"10px"}),
                html.Div("payment_method", style={"color":C6,"fontSize":"9px","marginBottom":"4px","paddingLeft":"2px"}),
                dcc.Dropdown(id="filter-payment",   options=[], placeholder="all", clearable=True, style={"fontSize":"11px","marginBottom":"10px"}),
                html.Div("trip_type", style={"color":C6,"fontSize":"9px","marginBottom":"4px","paddingLeft":"2px"}),
                dcc.Dropdown(id="filter-trip",      options=[], placeholder="all", clearable=True, style={"fontSize":"11px","marginBottom":"10px"}),
                html.Div("car_segment", style={"color":C6,"fontSize":"9px","marginBottom":"4px","paddingLeft":"2px"}),
                dcc.Dropdown(id="filter-segment",   options=[], placeholder="all", clearable=True, style={"fontSize":"11px","marginBottom":"10px"}),
                html.Button("✕ clear filters", id="clear-filters", n_clicks=0, style={
                    "backgroundColor":"transparent","color":C3,"border":f"1px solid {B}",
                    "padding":"5px 8px","borderRadius":"4px","cursor":"pointer",
                    "fontSize":"10px","width":"100%"}),

                html.Div(style={"borderTop":f"1px solid {B}","margin":"18px 0 12px"}),
                html.Div(id="sidebar-meta", style={"color":MUT,"fontSize":"9px","lineHeight":"1.9"}),
            ]),

            # ── Content ─────────────────────────────────────────
            html.Div(style={"flex":"1","overflowY":"auto","padding":"28px",
                            "height":"calc(100vh - 52px)"}, children=[
                html.Div(id="content-overview",       style={"display":"block"}),
                html.Div(id="content-analytics",      style={"display":"none"}),
                html.Div(id="content-routes",         style={"display":"none"}),
                html.Div(id="content-observability",  style={"display":"none"}),
                html.Div(id="content-pipeline",       style={"display":"none"}),
                html.Div(id="content-quality",        style={"display":"none"}),
                html.Div(id="content-ai",             style={"display":"none"}),
                html.Div(id="content-debug",          style={"display":"none"}),
            ]),
        ]),
    ]
)


# ================================================================
# CB 1 — DATA STORE
# ================================================================
@app.callback(
    [Output("store-booking","data"), Output("store-validation","data"),
     Output("store-expectation","data"), Output("store-failed","data"),
     Output("store-sla","data"), Output("store-alerts","data"),
     Output("store-schema","data"), Output("store-forecast","data"),
     Output("store-fraud","data"), Output("store-churn","data"),
     Output("last-updated","children"), Output("store-status","children")],
    [Input("auto-refresh","n_intervals"), Input("refresh-btn","n_clicks")],
    prevent_initial_call=False,
)
def refresh_store(n, clicks):
    now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        # FIX: get_booking_data now returns aggregated dict, not full dataframe
        booking_dict                                        = get_booking_data()
        val_df, exp_df, fail_df, sla_df, alerts_df, sch_df = get_monitoring_data()
        forecast_df, fraud_df, churn_df                    = get_ml_data()
        ml_ok  = not forecast_df.empty or not fraud_df.empty or not churn_df.empty
        kpi_data = booking_dict.get("kpi", [{}])
        total_records = kpi_data[0].get("total_bookings", 0) if kpi_data else 0
        status = (f"✦ {total_records:,} records · "
                  f"{'✓ pg' if booking_dict else '✗ pg'} "
                  f"{'✓ mysql' if not val_df.empty else '✗ mysql'} "
                  f"{'✓ ml' if ml_ok else '○ ml'}")
        return (booking_dict,
                val_df.to_dict("records")      if not val_df.empty      else [],
                exp_df.to_dict("records")      if not exp_df.empty      else [],
                fail_df.to_dict("records")     if not fail_df.empty     else [],
                sla_df.to_dict("records")      if not sla_df.empty      else [],
                alerts_df.to_dict("records")   if not alerts_df.empty   else [],
                sch_df.to_dict("records")      if not sch_df.empty      else [],
                forecast_df.to_dict("records") if not forecast_df.empty else [],
                fraud_df.to_dict("records")    if not fraud_df.empty    else [],
                churn_df.to_dict("records")    if not churn_df.empty    else [],
                f"↻ {now}", status)
    except Exception as e:
        return {}, [], [], [], [], [], [], [], [], [], f"↻ {now}", f"✗ {str(e)[:40]}"


# ================================================================
# CB 2 — ALERT BADGE + FILTERS
# ================================================================
@app.callback(Output("alert-badge","children"), Input("store-alerts","data"))
def update_alert_badge(alerts_data):
    if not alerts_data: return None
    df = safe_df(alerts_data)
    if df.empty or "alert_type" not in df.columns: return None
    failures = len(df[df["alert_type"]=="FAILURE"])
    warnings = len(df[df["alert_type"]=="DQ_WARNING"])
    if failures > 0:
        return html.Span(f"🚨 {failures} failure{'s' if failures>1 else ''}",
                         style={"backgroundColor":"rgba(255,71,87,0.15)","color":C3,
                                "border":f"1px solid {C3}","borderRadius":"4px",
                                "padding":"3px 10px","fontSize":"10px","fontFamily":MONO,"fontWeight":"700"})
    elif warnings > 0:
        return html.Span(f"⚠️ {warnings} warning{'s' if warnings>1 else ''}",
                         style={"backgroundColor":"rgba(255,212,59,0.12)","color":C5,
                                "border":f"1px solid {C5}","borderRadius":"4px",
                                "padding":"3px 10px","fontSize":"10px","fontFamily":MONO,"fontWeight":"700"})
    return pill("● all clear", C2)

@app.callback(
    [Output("filter-loyalty","options"), Output("filter-payment","options"),
     Output("filter-trip","options"),    Output("filter-segment","options"),
     Output("sidebar-meta","children")],
    Input("store-booking","data"),
)
def update_filters(booking_data):
    # booking_data is now a dict of aggregated data
    if not booking_data or not isinstance(booking_data, dict):
        return [], [], [], [], html.Div("no data")

    kpi = booking_data.get("kpi", [{}])[0] if booking_data.get("kpi") else {}
    loyalty_df  = safe_df(booking_data.get("loyalty", []))
    payments_df = safe_df(booking_data.get("payments", []))
    segments_df = safe_df(booking_data.get("segments", []))

    # Options from aggregated data
    loy_opts = [{"label":v,"value":v} for v in sorted(loyalty_df["loyalty_tier"].dropna().unique())]                if not loyalty_df.empty and "loyalty_tier" in loyalty_df.columns else []
    pay_opts = [{"label":v,"value":v} for v in sorted(payments_df["payment_method"].dropna().unique())]                if not payments_df.empty and "payment_method" in payments_df.columns else []
    seg_opts = [{"label":v,"value":v} for v in sorted(segments_df["car_segment"].dropna().unique())]                if not segments_df.empty and "car_segment" in segments_df.columns else []

    # Trip types — fetch quickly
    try:
        engine = get_pg_engine()
        trips = pd.read_sql("SELECT DISTINCT trip_type FROM customer_booking_staging WHERE trip_type IS NOT NULL", engine)
        engine.dispose()
        trip_opts = [{"label":v,"value":v} for v in sorted(trips["trip_type"].tolist())]
    except Exception:
        trip_opts = []

    meta_lines = [
        f"records: {kpi.get('total_bookings', 0):,}",
        f"customers: {kpi.get('total_customers', 0):,}",
        f"routes: {kpi.get('unique_routes', 0):,}",
        f"segments: {kpi.get('car_segments', 0):,}",
        f"models: {kpi.get('car_models', 0):,}",
    ]
    return (loy_opts, pay_opts, trip_opts, seg_opts,
            html.Div([html.Div(l) for l in meta_lines if l]))


# ================================================================
# CB 3 — TAB VISIBILITY
# ================================================================
@app.callback(
    [Output("tab-overview","style"), Output("tab-analytics","style"),
     Output("tab-routes","style"),   Output("tab-observability","style"),
     Output("tab-pipeline","style"), Output("tab-quality","style"),
     Output("tab-ai","style"),       Output("tab-debug","style"),
     Output("content-overview","style"),      Output("content-analytics","style"),
     Output("content-routes","style"),        Output("content-observability","style"),
     Output("content-pipeline","style"),      Output("content-quality","style"),
     Output("content-ai","style"),            Output("content-debug","style"),
     Output("active-tab","data")],
    [Input("tab-overview","n_clicks"),     Input("tab-analytics","n_clicks"),
     Input("tab-routes","n_clicks"),       Input("tab-observability","n_clicks"),
     Input("tab-pipeline","n_clicks"),     Input("tab-quality","n_clicks"),
     Input("tab-ai","n_clicks"),           Input("tab-debug","n_clicks")],
    State("active-tab","data"),
    prevent_initial_call=False,
)
def switch_tab(ov,an,ro,ob,pi,qu,ai,de, current):
    triggered = ctx.triggered_id
    active    = triggered if triggered in TABS else (current or "tab-overview")
    nav_styles     = [nav_style(t==active) for t in TABS]
    content_map    = {t: t.replace("tab-","content-") for t in TABS}
    content_styles = [{"display":"block"} if content_map[t]==f"content-{active.replace('tab-','')}"
                       else {"display":"none"} for t in TABS]
    return (*nav_styles, *content_styles, active)


# ================================================================
# CB 4 — OVERVIEW
# ================================================================
@app.callback(
    Output("content-overview","children"),
    [Input("store-booking","data"), Input("store-sla","data"),
     Input("store-validation","data"), Input("store-alerts","data"),
     Input("filter-loyalty","value"), Input("filter-payment","value"),
     Input("filter-trip","value"),    Input("filter-segment","value"),
     Input("clear-filters","n_clicks")],
)
def render_overview(bk, sla, val, alerts, loy_f, pay_f, trip_f, seg_f, clr):
    if not bk or not isinstance(bk, dict):
        return html.Div("[ loading data... ]",
                        style={"color":MUT,"fontFamily":MONO,"fontSize":"12px","padding":"60px","textAlign":"center"})

    # Extract aggregated data from dict
    kpi_data   = bk.get("kpi", [{}])
    kpi        = kpi_data[0] if kpi_data else {}
    daily_df   = safe_df(bk.get("daily", []))
    seg_df     = safe_df(bk.get("segments", []))
    cars_df    = safe_df(bk.get("top_cars", []))
    pay_df     = safe_df(bk.get("payments", []))
    loy_df     = safe_df(bk.get("loyalty", []))
    heat_df    = safe_df(bk.get("heatmap", []))
    sla_df     = safe_df(sla)
    val_df     = safe_df(val)
    alerts_df  = safe_df(alerts)

    total_b    = int(kpi.get("total_bookings", 0))
    total_c    = int(kpi.get("total_customers", 0))
    total_rev  = float(kpi.get("total_revenue", 0))
    high_value = int(kpi.get("high_value_customers", 0))
    digital    = int(kpi.get("digital_payments", 0))
    dq_score   = float(val_df["success_rate"].mean()) if not val_df.empty and "success_rate" in val_df.columns else 0
    p_ok       = not sla_df.empty and len(sla_df[sla_df["status"]=="FAILED"])==0
    failures   = len(alerts_df[alerts_df["alert_type"]=="FAILURE"])    if not alerts_df.empty and "alert_type" in alerts_df.columns else 0
    warnings   = len(alerts_df[alerts_df["alert_type"]=="DQ_WARNING"]) if not alerts_df.empty and "alert_type" in alerts_df.columns else 0
    digital_pct= digital/total_b*100 if total_b > 0 else 0

    # Weighted Health Score 0-100
    health_score = 100
    if not sla_df.empty and "status" in sla_df.columns:
        failed_stages = len(sla_df[sla_df["status"] == "FAILED"])
        health_score -= failed_stages * 15
    health_score -= failures * 10
    health_score -= max(0, (90 - dq_score) * 0.5)
    health_score = max(0, min(100, round(health_score)))
    health_color = C2 if health_score >= 90 else (C5 if health_score >= 70 else C3)

    fbadge = html.Span(f"[ {total_b:,} records ]", style={"color":MUT,"fontSize":"10px","fontFamily":MONO})

    # Revenue sparkline — from daily aggregated data
    if not daily_df.empty and "booking_date" in daily_df.columns:
        fig_spark = go.Figure()
        fig_spark.add_trace(go.Scatter(
            x=daily_df["booking_date"], y=daily_df["revenue"], mode="lines",
            fill="tozeroy", line=dict(color=C2, width=2),
            fillcolor="rgba(0,255,136,0.05)"))
        fig_spark.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                font=dict(color=TX, family=MONO, size=11),
                                title=dict(text="revenue · daily trend", font=dict(color=C1, size=12)),
                                margin=dict(l=0,r=0,t=28,b=0), height=110,
                                xaxis=dict(visible=False), yaxis=dict(visible=False))
    else:
        fig_spark = empty("revenue — no data"); fig_spark.update_layout(height=110)

    # Car segment donut — from segments aggregated data
    if not seg_df.empty and "car_segment" in seg_df.columns:
        fig_seg = px.pie(seg_df, names="car_segment", values="bookings",
                          title="car segment mix", hole=0.6,
                          color_discrete_sequence=[C1,C2,C4,C5,C7])
        fig_seg.update_layout(**CBASE, height=260)
    else:
        fig_seg = empty("car segments"); fig_seg.update_layout(height=260)

    # Top cars by revenue — from top_cars aggregated data
    if not cars_df.empty and "model" in cars_df.columns:
        fig_cars = go.Figure(go.Bar(
            x=cars_df["revenue"], y=cars_df["model"], orientation="h",
            marker=dict(color=cars_df["revenue"],
                        colorscale=[[0,"#0f2027"],[0.5,C1],[1,C4]], showscale=False),
            text=[f"₹{v:,.0f}" for v in cars_df["revenue"]],
            textposition="outside", textfont=dict(color=SUB,size=9,family=MONO)))
        fig_cars.update_layout(**CBASE, title="top cars · revenue", height=260,
                               xaxis=AXIS, yaxis={"categoryorder":"total ascending"})
    else:
        fig_cars = empty("top cars"); fig_cars.update_layout(height=260)

    # Heatmap — from pre-aggregated heatmap data
    if not heat_df.empty and "month" in heat_df.columns:
        ht = heat_df.copy()
        ht["dow"] = pd.Categorical(ht["dow"],
            categories=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"], ordered=True)
        ht = ht.sort_values("dow")
        fig_heat = px.density_heatmap(ht, x="month", y="dow", z="bookings",
                                       title="heatmap · month × day of week",
                                       color_continuous_scale=[[0,BG],[0.4,C1],[1,C4]])
        fig_heat.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, coloraxis_showscale=False, height=250)
    else:
        fig_heat = empty("heatmap"); fig_heat.update_layout(height=250)

    # Payment split bars from aggregated data
    pay_bars = []
    if not pay_df.empty and "payment_method" in pay_df.columns:
        total_pay = pay_df["bookings"].sum()
        for _, r in pay_df.iterrows():
            pct = r["bookings"]/total_pay*100 if total_pay > 0 else 0
            pay_bars.append(html.Div(style={"marginBottom":"10px"}, children=[
                html.Div(style={"display":"flex","justifyContent":"space-between","marginBottom":"3px"}, children=[
                    html.Span(str(r["payment_method"]), style={"color":TX,"fontSize":"11px","fontFamily":MONO}),
                    html.Span(f"{pct:.1f}%", style={"color":C1,"fontSize":"11px","fontFamily":MONO,"fontWeight":"700"}),
                ]),
                html.Div(style={"backgroundColor":B,"borderRadius":"2px","height":"3px"}, children=[
                    html.Div(style={"backgroundColor":C1,"width":f"{pct:.1f}%","height":"100%","borderRadius":"2px","opacity":"0.8"}),
                ]),
            ]))

    loy_bars = []
    if not loy_df.empty and "loyalty_tier" in loy_df.columns:
        total_loy = loy_df["bookings"].sum()
        for _, r in loy_df.iterrows():
            pct = r["bookings"]/total_loy*100 if total_loy > 0 else 0
            loy_bars.append(html.Div(style={"marginBottom":"10px"}, children=[
                html.Div(style={"display":"flex","justifyContent":"space-between","marginBottom":"3px"}, children=[
                    html.Span(str(r["loyalty_tier"]), style={"color":TX,"fontSize":"11px","fontFamily":MONO}),
                    html.Span(f"{pct:.1f}%", style={"color":C5,"fontSize":"11px","fontFamily":MONO,"fontWeight":"700"}),
                ]),
                html.Div(style={"backgroundColor":B,"borderRadius":"2px","height":"3px"}, children=[
                    html.Div(style={"backgroundColor":C5,"width":f"{pct:.1f}%","height":"100%","borderRadius":"2px","opacity":"0.8"}),
                ]),
            ]))

    return loading(html.Div([
        html.Div(style={"display":"flex","justifyContent":"space-between",
                        "alignItems":"flex-start","marginBottom":"22px"}, children=[
            html.Div([section("overview","real-time pipeline analytics"), fbadge]),
            html.Div(style={"display":"flex","gap":"8px","flexWrap":"wrap"}, children=[
                pill("● healthy" if p_ok else "● issues", C2 if p_ok else C3),
                pill(f"dq: {dq_score:.1f}%", C2 if dq_score>=90 else (C5 if dq_score>=70 else C3)),
                pill(f"health: {health_score}/100", health_color),
                pill(f"🚨 {failures} failure{'s' if failures!=1 else ''}", C3) if failures > 0 else None,
                pill(f"⚠️ {warnings} warning{'s' if warnings!=1 else ''}", C5) if warnings > 0 and failures == 0 else None,
            ]),
        ]),

        # Health Score gauge — UNIQUE FEATURE
        card(accent=health_color, mb="14px", children=[
            html.Div(style={"display":"flex","alignItems":"center","gap":"24px"}, children=[
                html.Div(style={"flex":"1"}, children=[
                    html.Div("pipeline health score", style={"color":SUB,"fontSize":"9px","fontFamily":MONO,
                             "textTransform":"uppercase","letterSpacing":"1.5px","marginBottom":"6px"}),
                    html.Div(f"{health_score}/100", style={"color":health_color,"fontSize":"48px",
                             "fontWeight":"700","fontFamily":MONO,"lineHeight":"1"}),
                    html.Div(style={"backgroundColor":B,"borderRadius":"4px","height":"6px","marginTop":"10px","width":"100%"}, children=[
                        html.Div(style={"backgroundColor":health_color,"width":f"{health_score}%",
                                        "height":"100%","borderRadius":"4px",
                                        "transition":"width 0.5s ease"}),
                    ]),
                ]),
                html.Div(style={"display":"flex","gap":"20px"}, children=[
                    html.Div(style={"textAlign":"center"}, children=[
                        html.Div(f"{dq_score:.0f}%", style={"color":C4,"fontSize":"20px","fontWeight":"700","fontFamily":MONO}),
                        html.Div("data quality", style={"color":MUT,"fontSize":"9px","fontFamily":MONO}),
                    ]),
                    html.Div(style={"textAlign":"center"}, children=[
                        html.Div(str(failures), style={"color":C3 if failures>0 else C2,"fontSize":"20px","fontWeight":"700","fontFamily":MONO}),
                        html.Div("failures", style={"color":MUT,"fontSize":"9px","fontFamily":MONO}),
                    ]),
                    html.Div(style={"textAlign":"center"}, children=[
                        html.Div(str(warnings), style={"color":C5 if warnings>0 else C2,"fontSize":"20px","fontWeight":"700","fontFamily":MONO}),
                        html.Div("warnings", style={"color":MUT,"fontSize":"9px","fontFamily":MONO}),
                    ]),
                ]),
            ]),
        ]),

        html.Div(style={"display":"flex","gap":"10px","marginBottom":"14px","flexWrap":"wrap"}, children=[
            kpi("total bookings",   f"{total_b:,}",        C1, "📋"),
            kpi("customers",        f"{total_c:,}",        C2, "👤"),
            kpi("revenue",          f"₹{total_rev:,.0f}",  C3, "💰"),
            kpi("high value",       f"{high_value:,}",     C7, "⭐"),
            kpi("digital payments", f"{digital_pct:.1f}%", C6, "📱"),
            kpi("data quality",     f"{dq_score:.1f}%",    C4, "🛡️"),
            kpi("active alerts",    str(failures+warnings), C3 if failures>0 else (C5 if warnings>0 else C2), "🔔"),
        ]),
        card([dcc.Graph(figure=fig_spark, config={"displayModeBar":False})], accent=C2),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr","gap":"12px","marginBottom":"12px"}, children=[
            card([dcc.Graph(figure=fig_seg,  config={"displayModeBar":False})], accent=C1),
            card([dcc.Graph(figure=fig_cars, config={"displayModeBar":False})], accent=C5),
        ]),
        html.Div(style={"display":"grid","gridTemplateColumns":"1.5fr 1fr 1fr","gap":"12px"}, children=[
            card([dcc.Graph(figure=fig_heat, config={"displayModeBar":False})], accent=C4, mb="0"),
            card([html.Div("payment_method", style={"color":C6,"fontSize":"9px","marginBottom":"12px","textTransform":"uppercase","letterSpacing":"1px"}),
                  *pay_bars] if pay_bars else [html.Div("no payment data",style={"color":MUT})], accent=C1, mb="0"),
            card([html.Div("loyalty_tier", style={"color":C6,"fontSize":"9px","marginBottom":"12px","textTransform":"uppercase","letterSpacing":"1px"}),
                  *loy_bars] if loy_bars else [html.Div("no loyalty data",style={"color":MUT})], accent=C5, mb="0"),
        ]),
    ]))


# ================================================================
# CB 5 — ANALYTICS
# ================================================================
@app.callback(
    Output("content-analytics","children"),
    [Input("store-booking","data"),
     Input("filter-loyalty","value"), Input("filter-payment","value"),
     Input("filter-trip","value"),    Input("filter-segment","value"),
     Input("clear-filters","n_clicks")],
)
def render_analytics(bk, loy_f, pay_f, trip_f, seg_f, clr):
    # Lazy load full analytics data
    df_raw = get_tab_data("analytics")
    if df_raw.empty:
        return html.Div("[ loading data... ]", style={"color":MUT,"fontFamily":MONO,"padding":"60px","textAlign":"center"})
    df = _apply_filters(df_raw, loy_f, pay_f, trip_f, seg_f)
    fbadge = _filter_badge(df, df_raw)
    if df.empty:
        return card([html.Div("[ no data ]", style={"color":MUT,"padding":"60px","textAlign":"center"})])

    # Daily dual axis
    if "booking_date" in df.columns:
        daily = df.groupby("booking_date").agg(bookings=("booking_id","nunique"), revenue=("payment_amount","sum")).reset_index()
        fig_d = go.Figure()
        fig_d.add_trace(go.Bar(x=daily["booking_date"],y=daily["bookings"],name="bookings",marker_color=C1,opacity=0.8))
        fig_d.add_trace(go.Scatter(x=daily["booking_date"],y=daily["revenue"],name="revenue ₹",
                                    line=dict(color=C5,width=2),mode="lines+markers",marker=dict(size=4),yaxis="y2"))
        fig_d.update_layout(**CBASE, title="daily · bookings + revenue", xaxis=AXIS,
                             yaxis=dict(title="bookings",color=C1,showgrid=False),
                             yaxis2=dict(title="revenue ₹",overlaying="y",side="right",color=C5,showgrid=False),
                             legend=dict(orientation="h",y=1.12,x=0,font=dict(size=10)), height=300)
    else:
        fig_d = empty("daily trend"); fig_d.update_layout(height=300)

    # Amount tier + payment_count
    if "amount_tier" in df.columns:
        at = df.groupby("amount_tier").agg(bookings=("booking_id","count"),
                                            revenue=("payment_amount","sum")).reset_index()
        fig_at = px.bar(at, x="amount_tier", y="revenue", color="bookings",
                         title="amount_tier · revenue breakdown",
                         color_continuous_scale=[[0,"#0a1520"],[1,C7]])
        fig_at.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, coloraxis_showscale=False, height=260)
    else:
        fig_at = empty("amount tier"); fig_at.update_layout(height=260)

    # Model scatter
    if "model" in df.columns:
        cs = df.groupby("model").agg(bookings=("booking_id","count"),
                                      revenue=("payment_amount","sum"),
                                      avg_price=("price_per_day","mean")).reset_index()
        fig_sc = px.scatter(cs, x="bookings", y="revenue", size="avg_price", text="model",
                             title="model · bookings vs revenue",
                             color="avg_price", color_continuous_scale=[[0,CARD],[0.5,C6],[1,C4]])
        fig_sc.update_traces(textposition="top center", textfont=dict(color=SUB,size=9,family=MONO))
        fig_sc.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, coloraxis_showscale=False, height=280)
    else:
        fig_sc = empty("model scatter"); fig_sc.update_layout(height=280)

    # Insurance breakdown
    if "insurance_coverage" in df.columns:
        ins = df.groupby(["insurance_coverage","insurer_type"])["payment_amount"].sum().reset_index() \
              if "insurer_type" in df.columns \
              else df.groupby("insurance_coverage")["payment_amount"].sum().reset_index()
        fig_ins = px.bar(ins, x="insurance_coverage", y="payment_amount",
                          color="insurer_type" if "insurer_type" in ins.columns else "insurance_coverage",
                          title="insurance · coverage × insurer type",
                          color_discrete_sequence=[C1,C4,C2,C5])
        fig_ins.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, height=260)
    else:
        fig_ins = empty("insurance"); fig_ins.update_layout(height=260)

    # Loyalty × payment stacked
    if "loyalty_tier" in df.columns and "payment_method" in df.columns:
        lp = df.groupby(["loyalty_tier","payment_method"])["payment_amount"].sum().reset_index()
        lp.columns = ["tier","method","revenue"]
        fig_lp = px.bar(lp, x="tier", y="revenue", color="method",
                         title="revenue · loyalty_tier × payment_method", barmode="stack",
                         color_discrete_sequence=[C1,C2,C3,C5,C4])
        fig_lp.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, height=270)
    else:
        fig_lp = empty("revenue by tier"); fig_lp.update_layout(height=270)

    # Price distribution
    if "price_per_day" in df.columns:
        fig_pr = px.histogram(df, x="price_per_day", nbins=25,
                               title="price_per_day · distribution", color_discrete_sequence=[C6])
        fig_pr.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, height=250)
    else:
        fig_pr = empty("price distribution"); fig_pr.update_layout(height=250)

    return loading(html.Div([
        html.Div(style={"display":"flex","justifyContent":"space-between","alignItems":"center","marginBottom":"22px"},
                 children=[section("analytics","bookings · cars · revenue"), fbadge]),
        card([dcc.Graph(figure=fig_d, config={"displayModeBar":False})], accent=C1),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr","gap":"12px"}, children=[
            card([dcc.Graph(figure=fig_at, config={"displayModeBar":False})], accent=C7),
            card([dcc.Graph(figure=fig_sc, config={"displayModeBar":False})], accent=C4),
        ]),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr","gap":"12px","marginTop":"12px"}, children=[
            card([dcc.Graph(figure=fig_ins, config={"displayModeBar":False})], accent=C6),
            card([dcc.Graph(figure=fig_pr,  config={"displayModeBar":False})], accent=C2),
        ]),
        html.Div(style={"marginTop":"12px"},
                 children=[card([dcc.Graph(figure=fig_lp, config={"displayModeBar":False})], accent=C5)]),
    ]))


# ================================================================
# CB 6 — ROUTE INTELLIGENCE (uses new 36-col columns)
# ================================================================
@app.callback(
    Output("content-routes","children"),
    [Input("store-booking","data"),
     Input("filter-loyalty","value"), Input("filter-payment","value"),
     Input("filter-trip","value"),    Input("filter-segment","value"),
     Input("clear-filters","n_clicks")],
)
def render_routes(bk, loy_f, pay_f, trip_f, seg_f, clr):
    df_raw = get_tab_data("routes")
    if df_raw.empty or "route" not in df_raw.columns:
        return html.Div("[ loading data... ]", style={"color":MUT,"fontFamily":MONO,"padding":"60px","textAlign":"center"})
    df = _apply_filters(df_raw, loy_f, pay_f, trip_f, seg_f)
    fbadge = _filter_badge(df, df_raw)
    if df.empty:
        return card([html.Div("[ route data not available — check staging table ]",
                              style={"color":MUT,"padding":"60px","textAlign":"center"})])

    # Top routes
    rt = df.groupby("route").agg(bookings=("booking_id","count"),
                                   revenue=("payment_amount","sum"),
                                   avg_amount=("payment_amount","mean")).reset_index()
    rt = rt.nlargest(10, "bookings")
    fig_routes = go.Figure()
    fig_routes.add_trace(go.Bar(y=rt["route"],x=rt["bookings"],name="bookings",
                                  orientation="h",marker_color=C1,opacity=0.9))
    fig_routes.update_layout(**CBASE, title="top 10 routes · booking volume", height=340,
                              xaxis=AXIS, yaxis={"categoryorder":"total ascending"})

    # Trip type breakdown
    if "trip_type" in df.columns:
        tt = df.groupby(["trip_type","car_segment"]).agg(
            bookings=("booking_id","count"), revenue=("payment_amount","sum")).reset_index() \
             if "car_segment" in df.columns \
             else df.groupby("trip_type").agg(bookings=("booking_id","count")).reset_index()
        fig_trip = px.bar(tt, x="trip_type", y="bookings",
                           color="car_segment" if "car_segment" in tt.columns else "trip_type",
                           title="trip_type × car_segment",
                           barmode="stack",
                           color_discrete_sequence=[C1,C2,C4,C5,C7])
        fig_trip.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, height=280)
    else:
        fig_trip = empty("trip type"); fig_trip.update_layout(height=280)

    # Pickup hour heatmap — hour × day of week
    if "pickup_hour" in df.columns and "booking_date" in df.columns:
        ph = df.copy(); ph["booking_date"] = pd.to_datetime(ph["booking_date"])
        ph["dow"] = ph["booking_date"].dt.day_name()
        ph["dow"] = pd.Categorical(ph["dow"],
            categories=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"], ordered=True)
        hmap = ph.groupby(["pickup_hour","dow"])["booking_id"].count().reset_index()
        hmap.columns = ["hour","day","bookings"]
        fig_ph = px.density_heatmap(hmap, x="hour", y="day", z="bookings",
                                     title="pickup demand · hour × day of week",
                                     color_continuous_scale=[[0,BG],[0.3,C1],[0.7,C4],[1,C3]])
        fig_ph.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, coloraxis_showscale=False, height=300)
    else:
        fig_ph = empty("pickup heatmap"); fig_ph.update_layout(height=300)

    # Pickup slot breakdown
    if "pickup_slot" in df.columns:
        ps = df.groupby("pickup_slot")["payment_amount"].agg(["count","sum","mean"]).reset_index()
        ps.columns = ["slot","bookings","revenue","avg"]
        fig_slot = go.Figure()
        fig_slot.add_trace(go.Bar(x=ps["slot"],y=ps["bookings"],name="bookings",marker_color=C2))
        fig_slot.add_trace(go.Scatter(x=ps["slot"],y=ps["avg"],name="avg ₹",
                                       line=dict(color=C5,width=2),yaxis="y2",mode="lines+markers"))
        fig_slot.update_layout(**CBASE, title="pickup slot · volume + avg revenue",
                                xaxis=AXIS,
                                yaxis=dict(title="bookings",color=C2,showgrid=False),
                                yaxis2=dict(title="avg ₹",overlaying="y",side="right",color=C5,showgrid=False),
                                legend=dict(orientation="h",y=1.12,x=0,font=dict(size=10)), height=280)
    else:
        fig_slot = empty("pickup slot"); fig_slot.update_layout(height=280)

    # Route × amount_tier table
    route_table = []
    if "amount_tier" in df.columns:
        rt2 = df.groupby(["route","amount_tier"]).agg(
            bookings=("booking_id","count"),
            revenue=("payment_amount","sum")).reset_index().nlargest(20,"bookings")
        route_table = [
            card(accent=C7, mb="0", children=[
                html.Div("route intelligence · top 20 by volume",
                         style={"color":C7,"fontFamily":MONO,"fontWeight":"700","fontSize":"11px","marginBottom":"14px"}),
                dash_table.DataTable(
                    data=rt2.to_dict("records"),
                    columns=[{"name":c,"id":c} for c in rt2.columns],
                    style_table={"overflowX":"auto"},
                    style_cell={"backgroundColor":BG,"color":TX,"border":f"1px solid {B}",
                                 "textAlign":"left","padding":"8px 12px","fontSize":"10px","fontFamily":MONO},
                    style_header={"backgroundColor":SIDE,"color":C1,"fontWeight":"700","fontSize":"9px",
                                   "border":f"1px solid {B}","textTransform":"uppercase","letterSpacing":"1.5px"},
                    style_data_conditional=[{"if":{"row_index":"odd"},"backgroundColor":CARD}],
                    page_size=10, sort_action="native",
                ),
            ])
        ]

    return loading(html.Div([
        html.Div(style={"display":"flex","justifyContent":"space-between","alignItems":"center","marginBottom":"22px"},
                 children=[section("route intelligence","trip_type · pickup_hour · route patterns"), fbadge]),
        html.Div(style={"display":"flex","gap":"10px","marginBottom":"14px","flexWrap":"wrap"}, children=[
            kpi("unique routes",  str(df["route"].nunique()),         C1, "🗺️") if "route"        in df.columns else None,
            kpi("trip types",     str(df["trip_type"].nunique()),      C2, "🚗") if "trip_type"    in df.columns else None,
            kpi("peak hour",      str(df["pickup_hour"].mode()[0]) if not df.empty and "pickup_hour" in df.columns else "—", C5, "⏰"),
            kpi("top slot",       str(df["pickup_slot"].mode()[0]) if not df.empty and "pickup_slot" in df.columns else "—", C7, "🕐"),
        ]),
        card([dcc.Graph(figure=fig_routes, config={"displayModeBar":False})], accent=C1),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr","gap":"12px","marginTop":"12px"}, children=[
            card([dcc.Graph(figure=fig_trip, config={"displayModeBar":False})], accent=C4),
            card([dcc.Graph(figure=fig_slot, config={"displayModeBar":False})], accent=C5),
        ]),
        html.Div(style={"marginTop":"12px"},
                 children=[card([dcc.Graph(figure=fig_ph, config={"displayModeBar":False})], accent=C7)]),
        html.Div(style={"marginTop":"12px"}, children=route_table),
    ]))


# ================================================================
# CB 7 — OBSERVABILITY
# ================================================================
@app.callback(
    Output("content-observability","children"),
    [Input("store-booking","data"),
     Input("filter-loyalty","value"), Input("filter-payment","value"),
     Input("filter-trip","value"),    Input("filter-segment","value"),
     Input("clear-filters","n_clicks")],
)
def render_observability(bk, loy_f, pay_f, trip_f, seg_f, clr):
    df_raw = get_tab_data("observability")
    if df_raw.empty:
        return html.Div("[ loading data... ]", style={"color":MUT,"fontFamily":MONO,"padding":"60px","textAlign":"center"})
    df = _apply_filters(df_raw, loy_f, pay_f, trip_f, seg_f)
    fbadge = _filter_badge(df, df_raw)
    if df.empty:
        return card([html.Div("[ no data ]", style={"color":MUT,"padding":"60px","textAlign":"center"})])

    # Volume anomaly
    anom = pd.DataFrame()
    if "booking_date" in df.columns:
        dc = df.copy(); dc["booking_date"] = pd.to_datetime(dc["booking_date"])
        dly = dc.groupby("booking_date")["booking_id"].count().reset_index()
        dly.columns = ["date","count"]
        dly["avg7"]  = dly["count"].rolling(7,min_periods=1).mean()
        dly["std7"]  = dly["count"].rolling(7,min_periods=1).std().fillna(0)
        dly["upper"] = dly["avg7"]+2*dly["std7"]
        dly["lower"] = (dly["avg7"]-2*dly["std7"]).clip(lower=0)
        dly["anom"]  = (dly["count"]>dly["upper"])|(dly["count"]<dly["lower"])
        anom = dly[dly["anom"]]
        fig_vol = go.Figure()
        fig_vol.add_trace(go.Bar(x=dly["date"],y=dly["count"],name="records",marker_color=C1,opacity=0.7))
        fig_vol.add_trace(go.Scatter(x=dly["date"],y=dly["avg7"],name="7d avg",
                                      line=dict(color=C5,width=2,dash="dot")))
        if not anom.empty:
            fig_vol.add_trace(go.Scatter(x=anom["date"],y=anom["count"],mode="markers",
                                          name=f"anomaly ({len(anom)})",
                                          marker=dict(color=C3,size=12,symbol="x-thin",line=dict(width=2,color=C3))))
        fig_vol.update_layout(**CBASE, title=f"volume monitor · {len(anom)} anomalies",
                               xaxis=AXIS, yaxis=AXIS,
                               legend=dict(orientation="h",y=1.12,x=0,font=dict(size=10)), height=300)
    else:
        fig_vol = empty("volume monitor"); fig_vol.update_layout(height=300)

    # Null health
    key_cols = [c for c in ["booking_id","customer_id","car_id","payment_amount",
                             "route","trip_type","car_segment","pickup_hour",
                             "loyalty_tier","payment_method"] if c in df.columns]
    null_d   = [{"col":c,"pct":round(df[c].isnull().sum()/len(df)*100,2),
                 "st":"critical" if df[c].isnull().sum()/len(df)*100>5
                      else ("warning" if df[c].isnull().sum()>0 else "healthy")}
                for c in key_cols]
    null_df2 = pd.DataFrame(null_d)
    SC = {"healthy":C2,"warning":C5,"critical":C3}
    fig_null = go.Figure(go.Bar(
        x=null_df2["col"], y=null_df2["pct"],
        marker_color=[SC.get(s,C5) for s in null_df2["st"]],
        text=[f"{v:.1f}%" for v in null_df2["pct"]],
        textposition="outside", textfont=dict(color=TX,size=9,family=MONO)))
    fig_null.update_layout(**CBASE, title="null health · key columns",
                            xaxis=AXIS, yaxis=AXIS,
                            yaxis_range=[0,max(null_df2["pct"].max()*1.8,5)], height=250)
    fig_null.add_hline(y=5,line_dash="dot",line_color=C3,
                       annotation_text="5% threshold",annotation_font=dict(color=C3,size=9))

    # Pickup hour anomaly — hours with very few bookings
    if "pickup_hour" in df.columns:
        ph_cnt = df.groupby("pickup_hour")["booking_id"].count().reset_index()
        ph_cnt.columns = ["hour","bookings"]
        ph_mean = ph_cnt["bookings"].mean()
        ph_cnt["anom"] = ph_cnt["bookings"] < ph_mean * 0.3
        fig_hour = px.bar(ph_cnt, x="hour", y="bookings",
                           title="pickup hour · booking distribution",
                           color="anom",
                           color_discrete_map={True:C3, False:C1})
        fig_hour.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS,
                                showlegend=False, height=250)
    else:
        fig_hour = empty("pickup hour"); fig_hour.update_layout(height=250)

    # Freshness
    if "booking_date" in df.columns:
        df_f = df.copy(); df_f["booking_date"] = pd.to_datetime(df_f["booking_date"])
        monthly = (df_f.groupby(df_f["booking_date"].dt.to_period("M").astype(str))
                   ["booking_id"].count().reset_index())
        monthly.columns = ["month","records"]
        latest  = df_f["booking_date"].max()
        fig_fr  = px.bar(monthly, x="month", y="records",
                          title=f"freshness · latest={str(latest)[:10]}",
                          color="records", color_continuous_scale=[[0,"#0a1520"],[1,C2]])
        fig_fr.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, coloraxis_showscale=False, height=240)
    else:
        fig_fr = empty("freshness"); fig_fr.update_layout(height=240)
        latest = "N/A"

    dup_count = len(df)-df["booking_id"].nunique() if "booking_id" in df.columns else 0
    dup_rate  = dup_count/len(df)*100 if len(df)>0 else 0
    obs = [("volume anomalies", str(len(anom)),   C3 if len(anom)>0 else C2,   "detected"),
           ("duplicate rate",   f"{dup_rate:.2f}%", C3 if dup_rate>5 else C2, f"{dup_count:,} recs"),
           ("null columns",     str(len(null_df2[null_df2["pct"]>0])), C3 if any(null_df2["pct"]>0) else C2, "with missing"),
           ("latest record",    str(latest)[:10], C2, "most recent")]

    return loading(html.Div([
        html.Div(style={"display":"flex","justifyContent":"space-between","alignItems":"center","marginBottom":"22px"},
                 children=[section("observability","pipeline health · data quality signals"), fbadge]),
        html.Div(style={"display":"flex","gap":"10px","marginBottom":"14px","flexWrap":"wrap"},
                 children=[kpi(l,v,c,"📡",h) for l,v,c,h in obs]),
        card([dcc.Graph(figure=fig_vol,  config={"displayModeBar":False})], accent=C1),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr","gap":"12px","marginTop":"12px"}, children=[
            card([dcc.Graph(figure=fig_null, config={"displayModeBar":False})], accent=C5),
            card([dcc.Graph(figure=fig_hour, config={"displayModeBar":False})], accent=C3),
        ]),
        html.Div(style={"marginTop":"12px"},
                 children=[card([dcc.Graph(figure=fig_fr, config={"displayModeBar":False})], accent=C2)]),
    ]))


# ================================================================
# CB 8 — PIPELINE
# ================================================================
@app.callback(
    Output("content-pipeline","children"),
    [Input("store-sla","data"), Input("store-alerts","data"), Input("store-schema","data")],
)
def render_pipeline(sla, alerts, schema):
    sla_df    = safe_df(sla)
    alerts_df = safe_df(alerts)
    schema_df = safe_df(schema)

    if sla_df.empty:
        return card([html.Div("[ no pipeline run data — run jenkins ]",
                              style={"color":MUT,"padding":"60px","textAlign":"center"})])

    runs  = sla_df["run_id"].nunique()
    avg_d = sla_df["duration_seconds"].mean()
    recs  = sla_df["records_processed"].sum()
    s_rate= len(sla_df[sla_df["status"]=="SUCCESS"])/len(sla_df)*100

    sa = sla_df.groupby("stage_name")["duration_seconds"].agg(["mean","max"]).reset_index()
    sa.columns = ["stage","avg","max"]
    sa["stage"] = sa["stage"].str.replace("_"," ")
    fig_dur = go.Figure()
    fig_dur.add_trace(go.Bar(x=sa["stage"],y=sa["avg"],name="avg",marker_color=C1))
    fig_dur.add_trace(go.Bar(x=sa["stage"],y=sa["max"],name="max",marker_color=C3,opacity=0.5))
    fig_dur.update_layout(**CBASE, title="stage duration · avg vs max (s)", barmode="group",
                          xaxis=AXIS, yaxis=AXIS,
                          legend=dict(orientation="h",y=1.12,x=0,font=dict(size=10)), height=280)

    sc = sla_df["status"].value_counts().reset_index(); sc.columns = ["status","count"]
    fig_st = px.pie(sc, names="status", values="count", title="stage status",
                     color="status", color_discrete_map={"SUCCESS":C2,"FAILED":C3,"SKIPPED":MUT}, hole=0.65)
    fig_st.update_layout(**CBASE, height=280)

    latest_run = sla_df["run_id"].iloc[0]
    ltd  = sla_df[sla_df["run_id"]==latest_run].copy()
    ltd["stage_name"] = ltd["stage_name"].str.replace("_"," ")
    show = [c for c in ["stage_name","records_processed","duration_seconds","status","started_at"] if c in ltd.columns]
    cmap = {"stage_name":"stage","records_processed":"records","duration_seconds":"duration(s)",
            "status":"status","started_at":"started"}

    def _alert_row(row):
        atype  = str(row.get("alert_type",""));  stage  = str(row.get("stage_name",""))
        created= str(row.get("created_at",""));  errmsg = str(row.get("error_message",""))
        color  = C3 if atype=="FAILURE" else (C5 if atype=="DQ_WARNING" else C2)
        icon   = "🚨" if atype=="FAILURE" else ("⚠️" if atype=="DQ_WARNING" else "✅")
        return html.Div(style={"display":"flex","gap":"12px","alignItems":"flex-start",
                               "padding":"10px 14px","borderBottom":f"1px solid {B}","backgroundColor":BG}, children=[
            html.Span(icon, style={"fontSize":"14px","marginTop":"1px"}),
            html.Div(style={"flex":"1"}, children=[
                html.Div(style={"display":"flex","justifyContent":"space-between","marginBottom":"3px"}, children=[
                    html.Span(stage,  style={"color":color,"fontSize":"10px","fontFamily":MONO,"fontWeight":"700"}),
                    html.Span(created[:19], style={"color":MUT,"fontSize":"9px","fontFamily":MONO}),
                ]),
                html.Div(errmsg[:120], style={"color":TX,"fontSize":"10px","fontFamily":MONO}),
            ]),
            pill(atype, color),
        ])

    alerts_rows = [_alert_row(row) for _, row in alerts_df.head(10).iterrows()] if not alerts_df.empty \
                  else [html.Div("[ no alerts ]", style={"color":MUT,"fontSize":"11px","fontFamily":MONO,"padding":"20px","textAlign":"center"})]

    schema_rows = []
    if not schema_df.empty:
        for _, row in schema_df.iterrows():
            compatible = bool(row.get("is_compatible", True))
            schema_rows.append(html.Div(style={
                "display":"flex","justifyContent":"space-between","alignItems":"center",
                "padding":"10px 14px","borderBottom":f"1px solid {B}","backgroundColor":BG}, children=[
                html.Div(style={"display":"flex","gap":"12px","alignItems":"center"}, children=[
                    html.Span("🔀", style={"fontSize":"14px"}),
                    html.Div([html.Div(str(row.get("topic","")), style={"color":C1,"fontSize":"10px","fontFamily":MONO,"fontWeight":"700"}),
                              html.Div(str(row.get("schema_summary",""))[:80], style={"color":SUB,"fontSize":"9px","fontFamily":MONO})]),
                ]),
                html.Div(style={"display":"flex","gap":"8px","alignItems":"center"}, children=[
                    pill(f"v{row.get('schema_version','?')}", C4),
                    pill("compatible ✓" if compatible else "⚠ breaking", C2 if compatible else C3),
                    html.Span(str(row.get("registered_at",""))[:10], style={"color":MUT,"fontSize":"9px","fontFamily":MONO}),
                ]),
            ]))
    else:
        schema_rows = [html.Div("[ no schema registered ]", style={"color":MUT,"fontSize":"11px","fontFamily":MONO,"padding":"20px","textAlign":"center"})]

    return loading(html.Div([
        section("pipeline health","jenkins · spark · stage metrics"),
        html.Div(style={"display":"flex","gap":"10px","marginBottom":"14px"}, children=[
            kpi("pipeline runs", str(runs),       C1, "⚡"),
            kpi("avg duration",  f"{avg_d:.0f}s", C2, "⏱️"),
            kpi("records total", f"{recs:,}",     C5, "📦"),
            kpi("success rate",  f"{s_rate:.0f}%",C4, "✅"),
        ]),
        html.Div(style={"display":"grid","gridTemplateColumns":"2fr 1fr","gap":"12px"}, children=[
            card([dcc.Graph(figure=fig_dur, config={"displayModeBar":False})], accent=C1),
            card([dcc.Graph(figure=fig_st,  config={"displayModeBar":False})], accent=C4),
        ]),
        html.Div(style={"marginTop":"12px"}, children=[
            card(accent=C1, mb="0", children=[
                html.Div(style={"display":"flex","justifyContent":"space-between","alignItems":"center","marginBottom":"14px"}, children=[
                    html.Span("latest run · stage breakdown", style={"color":C1,"fontFamily":MONO,"fontWeight":"700","fontSize":"11px"}),
                    pill(f"run: {latest_run}", C6),
                ]),
                dash_table.DataTable(data=ltd[show].to_dict("records"),
                    columns=[{"name":cmap.get(c,c),"id":c} for c in show],
                    style_table={"overflowX":"auto"},
                    style_cell={"backgroundColor":BG,"color":TX,"border":f"1px solid {B}",
                                 "textAlign":"left","padding":"9px 14px","fontSize":"10px","fontFamily":MONO},
                    style_header={"backgroundColor":SIDE,"color":C1,"fontWeight":"700","fontSize":"9px",
                                   "border":f"1px solid {B}","textTransform":"uppercase","letterSpacing":"1.5px"},
                    style_data_conditional=[{"if":{"row_index":"odd"},"backgroundColor":CARD},
                                            {"if":{"filter_query":"{status} = SUCCESS"},"color":C2},
                                            {"if":{"filter_query":"{status} = FAILED"},"color":C3}],
                    page_size=10, sort_action="native"),
            ]),
        ]),
        html.Div(style={"marginTop":"12px"}, children=[
            card(accent=C3, mb="0", children=[
                html.Div(style={"display":"flex","justifyContent":"space-between","alignItems":"center","marginBottom":"14px"}, children=[
                    html.Span("pipeline alerts · last 10", style={"color":C3,"fontFamily":MONO,"fontWeight":"700","fontSize":"11px"}),
                    pill(f"{len(alerts_df)} total", C5) if not alerts_df.empty else None,
                ]),
                html.Div(style={"borderRadius":"6px","overflow":"hidden","border":f"1px solid {B}"}, children=alerts_rows),
            ]),
        ]),
        html.Div(style={"marginTop":"12px"}, children=[
            card(accent=C4, mb="0", children=[
                html.Div(style={"display":"flex","justifyContent":"space-between","alignItems":"center","marginBottom":"14px"}, children=[
                    html.Span("schema registry · kafka topics", style={"color":C4,"fontFamily":MONO,"fontWeight":"700","fontSize":"11px"}),
                    html.A("↗ localhost:8085", href="http://localhost:8085", target="_blank",
                           style={"color":C6,"fontSize":"9px","fontFamily":MONO}),
                ]),
                html.Div(style={"borderRadius":"6px","overflow":"hidden","border":f"1px solid {B}"}, children=schema_rows),
            ]),
        ]),
    ]))


# ================================================================
# CB 9 — DATA QUALITY
# ================================================================
@app.callback(
    Output("content-quality","children"),
    [Input("store-validation","data"), Input("store-expectation","data"), Input("store-failed","data")],
)
def render_quality(val, exp, fail):
    if not val:
        return html.Div("[ loading data... ]", style={"color":MUT,"fontFamily":MONO,"padding":"60px","textAlign":"center"})
    val_df  = safe_df(val)
    exp_df  = safe_df(exp)
    fail_df = safe_df(fail)

    if val_df.empty:
        return card([html.Div("[ no validation data — run great_expectations ]",
                              style={"color":MUT,"padding":"60px","textAlign":"center"})])

    avg_rate = float(val_df["success_rate"].mean())
    gc       = C2 if avg_rate>=90 else (C5 if avg_rate>=70 else C3)

    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number", value=avg_rate,
        title={"text":"data integrity score","font":{"color":SUB,"size":11,"family":MONO}},
        number={"suffix":"%","font":{"color":gc,"size":42,"family":MONO}},
        gauge={"axis":{"range":[0,100],"tickcolor":MUT,"tickfont":{"color":MUT,"size":9,"family":MONO}},
               "bar":{"color":gc,"thickness":0.25},"bgcolor":CARD,"borderwidth":0,
               "steps":[{"range":[0,70],"color":"#0d1520"},
                        {"range":[70,90],"color":"#0d1a14"},
                        {"range":[90,100],"color":"#0a1e10"}]},
    ))
    fig_gauge.update_layout(**CBASE, height=260)

    if not exp_df.empty:
        es = exp_df.groupby("expectation_type")["success"].agg(
            passed=lambda x: int(x.astype(bool).sum()),
            failed=lambda x: int((~x.astype(bool)).sum())).reset_index()
        es["rule"] = (es["expectation_type"]
                      .str.replace("expect_column_values_to_be_","")
                      .str.replace("expect_column_","").str.replace("_"," "))
        em = es.melt(id_vars=["expectation_type","rule"],
                     value_vars=["passed","failed"],var_name="status",value_name="count")
        fig_rules = px.bar(em, x="count", y="rule", color="status", orientation="h", barmode="group",
                            title="validation rules · pass vs fail",
                            color_discrete_map={"passed":C2,"failed":C3})
        fig_rules.update_layout(**CBASE, xaxis=AXIS, yaxis={"categoryorder":"total ascending"}, height=300)
    else:
        fig_rules = empty("validation rules"); fig_rules.update_layout(height=300)

    if "created_at" in val_df.columns:
        fig_hist = px.line(val_df, x="created_at", y="success_rate",
                            title="quality score · run history",
                            color_discrete_sequence=[C2], markers=True)
        fig_hist.add_hline(y=100,line_dash="dot",line_color=C3,
                           annotation_text="target: 100%",annotation_font=dict(color=C3,size=9))
        fig_hist.update_traces(line_width=2,marker_size=7)
        fig_hist.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, yaxis_range=[0,105], height=260)
    else:
        fig_hist = empty("quality history"); fig_hist.update_layout(height=260)

    vp = int(exp_df["success"].sum())                   if not exp_df.empty else 0
    vv = int((~exp_df["success"].astype(bool)).sum())   if not exp_df.empty else 0
    vf = len(fail_df)                                   if not fail_df.empty else 0

    return loading(html.Div([
        section("data quality","great expectations · validation results"),
        html.Div(style={"display":"flex","gap":"10px","marginBottom":"14px"}, children=[
            kpi("rules passed",    str(vp),             C2, "✅"),
            kpi("rules violated",  str(vv),             C3, "⚠️"),
            kpi("flagged records", f"{vf:,}",           C5, "🚨"),
            kpi("dq score",        f"{avg_rate:.1f}%",  gc, "🛡️"),
        ]),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 2fr","gap":"12px"}, children=[
            card([dcc.Graph(figure=fig_gauge, config={"displayModeBar":False})], accent=gc),
            card([dcc.Graph(figure=fig_hist,  config={"displayModeBar":False})], accent=C2),
        ]),
        html.Div(style={"marginTop":"12px"},
                 children=[card([dcc.Graph(figure=fig_rules, config={"displayModeBar":False})], accent=C1)]),
    ]))


# ================================================================
# CB 10 — AI · ML TAB
# Reads: ml_demand_forecast, ml_fraud_scores, ml_churn_scores
# ================================================================
@app.callback(
    Output("content-ai","children"),
    [Input("store-forecast","data"), Input("store-fraud","data"),
     Input("store-churn","data"),    Input("store-booking","data")],
)
def render_ai(forecast_data, fraud_data, churn_data, bk):
    forecast_df = safe_df(forecast_data)
    fraud_df    = safe_df(fraud_data)
    churn_df    = safe_df(churn_data)
    df          = safe_df(bk)

    ml_available = not forecast_df.empty or not fraud_df.empty or not churn_df.empty

    # ── KPIs ────────────────────────────────────────────────────
    next_day  = f"{forecast_df['predicted_bookings'].iloc[0]:.0f}" \
                if not forecast_df.empty and "predicted_bookings" in forecast_df.columns else "—"
    fraud_crit = len(fraud_df[fraud_df["risk_label"]=="CRITICAL"]) \
                 if not fraud_df.empty and "risk_label" in fraud_df.columns else 0
    fraud_high = len(fraud_df[fraud_df["risk_label"]=="HIGH"]) \
                 if not fraud_df.empty and "risk_label" in fraud_df.columns else 0
    churn_risk = len(churn_df[churn_df["churn_risk"].isin(["HIGH","CRITICAL"])]) \
                 if not churn_df.empty and "churn_risk" in churn_df.columns else 0
    xgb_auc   = f"{churn_df['model_auc'].iloc[0]:.3f}" \
                if not churn_df.empty and "model_auc" in churn_df.columns else "—"

    # ── 1. Demand Forecast ───────────────────────────────────────
    if not forecast_df.empty and "forecast_date" in forecast_df.columns:
        fig_fc = go.Figure()
        fig_fc.add_trace(go.Scatter(
            x=forecast_df["forecast_date"], y=forecast_df["upper_bound"],
            mode="lines", line=dict(color="rgba(0,0,0,0)"), showlegend=False, name="upper"))
        fig_fc.add_trace(go.Scatter(
            x=forecast_df["forecast_date"], y=forecast_df["lower_bound"],
            fill="tonexty", fillcolor="rgba(0,212,255,0.08)",
            line=dict(color="rgba(0,0,0,0)"), showlegend=False, name="confidence"))
        fig_fc.add_trace(go.Scatter(
            x=forecast_df["forecast_date"], y=forecast_df["predicted_bookings"],
            mode="lines+markers", name="forecast",
            line=dict(color=C1, width=2), marker=dict(size=5)))
        fig_fc.update_layout(**CBASE, title="demand forecast · 30 days (Prophet)",
                              xaxis=AXIS, yaxis=AXIS,
                              legend=dict(orientation="h",y=1.12,x=0,font=dict(size=10)), height=300)
    else:
        fig_fc = empty("demand forecast — run ml_models.py first")
        fig_fc.update_layout(height=300)

    # ── 2. Fraud Risk Distribution ───────────────────────────────
    if not fraud_df.empty and "risk_label" in fraud_df.columns:
        fr_cnt = fraud_df["risk_label"].value_counts().reset_index()
        fr_cnt.columns = ["risk","count"]
        fig_fraud_dist = px.pie(fr_cnt, names="risk", values="count",
                                 title="fraud risk distribution",
                                 hole=0.6,
                                 color="risk",
                                 color_discrete_map={"LOW":C2,"MEDIUM":C5,"HIGH":C7,"CRITICAL":C3})
        fig_fraud_dist.update_layout(**CBASE, height=280)
    else:
        fig_fraud_dist = empty("fraud distribution"); fig_fraud_dist.update_layout(height=280)

    # ── 3. Fraud Heatmap — pickup_hour × risk_label ──────────────
    if not fraud_df.empty and "pickup_hour" in fraud_df.columns and "risk_label" in fraud_df.columns:
        fh = fraud_df.groupby(["pickup_hour","risk_label"]).size().reset_index(name="count")
        fig_fraud_heat = px.density_heatmap(fh, x="pickup_hour", y="risk_label", z="count",
                                             title="fraud risk · hour × risk level",
                                             color_continuous_scale=[[0,BG],[0.4,C5],[1,C3]])
        fig_fraud_heat.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS,
                                      coloraxis_showscale=False, height=250)
    else:
        fig_fraud_heat = empty("fraud heatmap"); fig_fraud_heat.update_layout(height=250)

    # ── 4. Churn Funnel ──────────────────────────────────────────
    if not churn_df.empty and "churn_risk" in churn_df.columns:
        ch_cnt = churn_df["churn_risk"].value_counts().reset_index()
        ch_cnt.columns = ["risk","count"]
        order_map = {"LOW":0,"MEDIUM":1,"HIGH":2,"CRITICAL":3}
        ch_cnt["order"] = ch_cnt["risk"].map(order_map)
        ch_cnt = ch_cnt.sort_values("order")
        fig_churn = go.Figure(go.Funnel(
            y=ch_cnt["risk"], x=ch_cnt["count"],
            textinfo="value+percent initial",
            marker=dict(color=[C2,C5,C7,C3]),
            connector=dict(line=dict(color=B, width=1))))
        fig_churn.update_layout(**CBASE, title="churn risk funnel (XGBoost)",
                                 paper_bgcolor="rgba(0,0,0,0)", height=280)
    else:
        fig_churn = empty("churn funnel"); fig_churn.update_layout(height=280)

    # ── 5. Churn by Loyalty Tier ─────────────────────────────────
    if not churn_df.empty and "loyalty_tier" in churn_df.columns and "churn_probability" in churn_df.columns:
        cl = churn_df.groupby("loyalty_tier")["churn_probability"].agg(["mean","count"]).reset_index()
        cl.columns = ["tier","avg_prob","customers"]
        fig_cl = px.bar(cl, x="tier", y="avg_prob",
                         title="avg churn probability · by loyalty tier",
                         color="avg_prob",
                         color_continuous_scale=[[0,C2],[0.5,C5],[1,C3]],
                         text=[f"{v:.1%}" for v in cl["avg_prob"]])
        fig_cl.update_traces(textposition="outside", textfont=dict(color=TX,size=9))
        fig_cl.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS,
                              coloraxis_showscale=False, yaxis_range=[0,1], height=260)
    else:
        fig_cl = empty("churn by tier"); fig_cl.update_layout(height=260)

    # ── Top Fraud Alerts Table ───────────────────────────────────
    fraud_table_content = html.Div("[ no fraud data ]", style={"color":MUT,"fontFamily":MONO,"fontSize":"11px"})
    if not fraud_df.empty:
        top_fraud = fraud_df[fraud_df["risk_label"].isin(["HIGH","CRITICAL"])].head(20)
        if not top_fraud.empty:
            show = [c for c in ["booking_id","customer_name","route","trip_type",
                                 "payment_method","payment_amount","pickup_hour",
                                 "fraud_risk_score","risk_label"] if c in top_fraud.columns]
            fraud_table_content = dash_table.DataTable(
                data=top_fraud[show].to_dict("records"),
                columns=[{"name":c,"id":c} for c in show],
                style_table={"overflowX":"auto"},
                style_cell={"backgroundColor":BG,"color":TX,"border":f"1px solid {B}",
                             "textAlign":"left","padding":"8px 12px","fontSize":"10px","fontFamily":MONO},
                style_header={"backgroundColor":SIDE,"color":C1,"fontWeight":"700","fontSize":"9px",
                               "border":f"1px solid {B}","textTransform":"uppercase","letterSpacing":"1.5px"},
                style_data_conditional=[
                    {"if":{"row_index":"odd"},"backgroundColor":CARD},
                    {"if":{"filter_query":"{risk_label} = CRITICAL"},"color":C3,"fontWeight":"700"},
                    {"if":{"filter_query":"{risk_label} = HIGH"},"color":C7},
                ],
                page_size=10, sort_action="native",
            )

    # ── AI Chat section ──────────────────────────────────────────
    ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY","")
    chat_mode     = "claude" if ANTHROPIC_KEY else "sql_fallback"
    quick_questions = [
        "top 5 routes by bookings",
        "busiest pickup hours",
        "payment method breakdown",
        "high value customers by loyalty tier",
        "revenue by car segment",
    ]

    # AI Auto-Insights — UNIQUE FEATURE
    # Claude automatically generates 3 insights on page load
    auto_insights_section = card(accent=C4, mb="12px", children=[
        html.Div(style={"display":"flex","justifyContent":"space-between","alignItems":"center","marginBottom":"14px"}, children=[
            html.Span("🤖 ai auto-insights", style={"color":C4,"fontFamily":MONO,"fontWeight":"700","fontSize":"13px"}),
            html.Div(style={"display":"flex","gap":"6px"}, children=[
                pill("auto-generated", C4),
                pill("mysql · ge · ml", C6),
            ]),
        ]),
        html.Div(id="auto-insights-output", children=[
            html.Div("[ generating insights... ]",
                     style={"color":MUT,"fontSize":"11px","fontFamily":MONO,"textAlign":"center","padding":"20px"}),
        ]),
    ])

    ai_section = card(accent=C7, mb="0", children=[
        html.Div(style={"display":"flex","justifyContent":"space-between","alignItems":"center","marginBottom":"16px"}, children=[
            html.Span("ai data assistant", style={"color":C7,"fontFamily":MONO,"fontWeight":"700","fontSize":"13px"}),
            html.Div(style={"display":"flex","gap":"6px"}, children=[
                pill("claude api" if chat_mode=="claude" else "sql fallback",
                     C2 if chat_mode=="claude" else C5),
                pill("36 col staging", C6),
            ]),
        ]),

        # Quick questions
        html.Div("quick questions:", style={"color":SUB,"fontSize":"9px","fontFamily":MONO,"marginBottom":"8px","letterSpacing":"1px"}),
        html.Div(style={"display":"flex","gap":"6px","flexWrap":"wrap","marginBottom":"14px"}, children=[
            html.Button(q, id={"type":"quick-q","index":i}, n_clicks=0, style={
                "backgroundColor":"rgba(167,139,250,0.08)","color":C4,
                "border":f"1px solid {C4}","padding":"4px 10px","borderRadius":"4px",
                "cursor":"pointer","fontSize":"9px","fontFamily":MONO,
            }) for i, q in enumerate(quick_questions)
        ]),

        # Input
        html.Div(style={"display":"flex","gap":"8px","marginBottom":"14px"}, children=[
            dcc.Textarea(id="ai-input", placeholder="ask anything about your booking data...",
                         style={"flex":"1","backgroundColor":BG,"color":TX,
                                "border":f"1px solid {B2}","borderRadius":"4px",
                                "padding":"10px","fontSize":"11px","fontFamily":MONO,
                                "resize":"none","height":"48px"}),
            html.Button("ask →", id="ai-ask-btn", n_clicks=0, style={
                "backgroundColor":"rgba(255,159,67,0.1)","color":C7,
                "border":f"1px solid {C7}","padding":"8px 16px","borderRadius":"4px",
                "cursor":"pointer","fontSize":"11px","fontFamily":MONO,"fontWeight":"700",
                "whiteSpace":"nowrap"}),
        ]),

        # Chat history
        html.Div(id="ai-chat-output", style={
            "backgroundColor":BG,"borderRadius":"6px","border":f"1px solid {B}",
            "padding":"14px","minHeight":"120px","maxHeight":"400px","overflowY":"auto",
        }, children=[
            html.Div("[ ask a question to get started ]",
                     style={"color":MUT,"fontSize":"11px","fontFamily":MONO,"textAlign":"center","marginTop":"40px"}),
        ]),
    ])

    no_ml_banner = card([
        html.Div(style={"display":"flex","alignItems":"center","gap":"12px"}, children=[
            html.Span("⚠️", style={"fontSize":"20px"}),
            html.Div([
                html.Div("ML Models not yet run",
                         style={"color":C5,"fontFamily":MONO,"fontWeight":"700","fontSize":"12px"}),
                html.Div("Run jenkins pipeline → 'Run ML Models' stage will populate Prophet forecast, "
                         "Isolation Forest fraud scores, and XGBoost churn predictions",
                         style={"color":SUB,"fontFamily":MONO,"fontSize":"10px","marginTop":"4px"}),
            ]),
        ])
    ], accent=C5) if not ml_available else None

    return loading(html.Div([
        html.Div(style={"display":"flex","justifyContent":"space-between","alignItems":"flex-start","marginBottom":"22px"}, children=[
            section("ai · ml intelligence","prophet · isolation forest · xgboost · claude"),
            html.Div(style={"display":"flex","gap":"6px"}, children=[
                pill("prophet",           C1),
                pill("isolation forest",  C3),
                pill("xgboost",           C5),
                pill("claude api" if ANTHROPIC_KEY else "sql fallback", C7),
                html.A("↗ mlflow", href="http://localhost:5000", target="_blank",
                       style={"color":C6,"fontSize":"9px","fontFamily":MONO,
                              "border":f"1px solid {B}","padding":"2px 8px",
                              "borderRadius":"4px","textDecoration":"none"}),
            ]),
        ]),

        no_ml_banner,

        html.Div(style={"display":"flex","gap":"10px","marginBottom":"14px","flexWrap":"wrap"}, children=[
            kpi("next day forecast", next_day,      C1, "📈", "bookings predicted"),
            kpi("fraud critical",    str(fraud_crit),C3, "🚨", "needs review"),
            kpi("fraud high",        str(fraud_high),C7, "⚠️", "monitor"),
            kpi("churn at risk",     str(churn_risk),C5, "📉", "high+critical"),
            kpi("xgb auc",           xgb_auc,       C4, "🎯", "model accuracy"),
        ]),

        # Demand forecast full width
        card([dcc.Graph(figure=fig_fc, config={"displayModeBar":False})], accent=C1),

        # Fraud row
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1.5fr","gap":"12px","marginTop":"12px"}, children=[
            card([dcc.Graph(figure=fig_fraud_dist, config={"displayModeBar":False})], accent=C3),
            card([dcc.Graph(figure=fig_fraud_heat, config={"displayModeBar":False})], accent=C7),
        ]),

        # Churn row
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr","gap":"12px","marginTop":"12px"}, children=[
            card([dcc.Graph(figure=fig_churn, config={"displayModeBar":False})], accent=C5),
            card([dcc.Graph(figure=fig_cl,    config={"displayModeBar":False})], accent=C4),
        ]),

        # Top fraud alerts
        html.Div(style={"marginTop":"12px"}, children=[
            card(accent=C3, mb="12px", children=[
                html.Div(style={"display":"flex","justifyContent":"space-between","alignItems":"center","marginBottom":"14px"}, children=[
                    html.Span("top fraud alerts · high + critical",
                              style={"color":C3,"fontFamily":MONO,"fontWeight":"700","fontSize":"11px"}),
                    pill(f"{fraud_crit} critical · {fraud_high} high", C3),
                ]),
                fraud_table_content,
            ]),
        ]),

        # AI Auto-Insights
        html.Div(style={"marginTop":"12px"}, children=[auto_insights_section]),

        # AI Chat
        html.Div(style={"marginTop":"12px"}, children=[ai_section]),
    ]))


# ================================================================
# CB 11 — AI CHAT (handles ask button + quick questions)
# ================================================================
@app.callback(
    [Output("ai-chat-output","children"),
     Output("ai-input","value"),
     Output("store-chat","data")],
    [Input("ai-ask-btn","n_clicks"),
     Input({"type":"quick-q","index":dash.ALL},"n_clicks")],
    [State("ai-input","value"),
     State("store-booking","data"),
     State("store-chat","data")],
    prevent_initial_call=True,
)
def handle_ai_chat(ask_clicks, quick_clicks, user_input, bk, chat_history):
    triggered = ctx.triggered_id
    if not chat_history: chat_history = []

    # Which question was asked
    question = ""
    quick_qs = ["top 5 routes by bookings","busiest pickup hours","payment method breakdown",
                "high value customers by loyalty tier","revenue by car segment"]
    if isinstance(triggered, dict) and triggered.get("type") == "quick-q":
        question = quick_qs[triggered["index"]]
    elif triggered == "ai-ask-btn" and user_input and user_input.strip():
        question = user_input.strip()

    if not question:
        return _render_chat(chat_history), "", chat_history

    # Get answer
    answer = _get_ai_answer(question, bk)
    chat_history = chat_history + [{"q": question, "a": answer}]
    if len(chat_history) > 10:
        chat_history = chat_history[-10:]

    return _render_chat(chat_history), "", chat_history


def _get_ai_answer(question: str, bk) -> str:
    """
    BookingIntelligenceOrchestrator use karta hai:
    - Quick questions → quick_answer() (fast)
    - Full report → full_intelligence_report() (comprehensive)
    - Fallback → pandas SQL
    """
    ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY","")
    df = safe_df(bk)
    if df.empty:
        return "[ no data in staging table — run pipeline first ]"

    # Use Orchestrator if API key available
    if ANTHROPIC_KEY:
        try:
            import sys, os as _os
            # agent.py spark-master mein hai — dashboard mein import karo
            _sys_path = "/opt/spark-apps/jobs"
            if _sys_path not in sys.path:
                sys.path.insert(0, _sys_path)
                sys.path.insert(0, "/opt/spark-apps/lib")
            from agent import BookingIntelligenceOrchestrator
            orch   = BookingIntelligenceOrchestrator()
            result = orch.quick_answer(question)
            return result.get("answer", result.get("answer", "No answer"))
        except ImportError:
            pass  # agent.py not in path — use direct Claude
        except Exception as e:
            pass  # Fall through to direct Claude

        # Direct Claude fallback (agent.py not importable from dashboard)
        try:
            from anthropic import Anthropic
            client  = Anthropic(api_key=ANTHROPIC_KEY)
            context = df.describe(include="all").to_string() if not df.empty else "no data"
            sample  = df.head(3).to_string() if not df.empty else "no data"
            msg = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=400,
                messages=[{"role":"user","content":f"""
You are a data analyst for a car booking pipeline.
Table: customer_booking_staging (36 columns)

Column summary:
{context[:600]}

Sample rows:
{sample}

Question: {question}

Answer in 2-3 sentences with specific numbers. Be direct and concise.
"""}])
            return msg.content[0].text
        except Exception:
            pass

    # Pandas SQL fallback — no API key needed
    q = question.lower()
    try:
        if "route" in q:
            res = df.groupby("route")["booking_id"].count().nlargest(5).reset_index() \
                  if "route" in df.columns else pd.DataFrame()
            return f"Top routes:\n{res.to_string(index=False)}" if not res.empty else "route column not found"
        elif "hour" in q or "busiest" in q:
            res = df.groupby("pickup_hour")["booking_id"].count().nlargest(5).reset_index() \
                  if "pickup_hour" in df.columns else pd.DataFrame()
            return f"Busiest pickup hours:\n{res.to_string(index=False)}" if not res.empty else "pickup_hour not found"
        elif "payment" in q:
            res = df.groupby("payment_method").agg(
                count=("booking_id","count"),
                total=("payment_amount","sum")).reset_index() \
                  if "payment_method" in df.columns else pd.DataFrame()
            return f"Payment breakdown:\n{res.to_string(index=False)}" if not res.empty else "payment_method not found"
        elif "high value" in q or "loyal" in q:
            res = df.groupby("loyalty_tier").agg(
                customers=("customer_id","nunique"),
                high_value=("is_high_value_customer","sum"),
                avg_payment=("payment_amount","mean")).reset_index() \
                  if "loyalty_tier" in df.columns else pd.DataFrame()
            return f"Loyalty + high value:\n{res.to_string(index=False)}" if not res.empty else "loyalty_tier not found"
        elif "segment" in q or "car" in q:
            res = df.groupby("car_segment").agg(
                bookings=("booking_id","count"),
                revenue=("payment_amount","sum")).reset_index() \
                  if "car_segment" in df.columns else pd.DataFrame()
            return f"Car segment revenue:\n{res.to_string(index=False)}" if not res.empty else "car_segment not found"
        else:
            total_b = df["booking_id"].nunique()    if "booking_id"    in df.columns else len(df)
            total_r = df["payment_amount"].sum()    if "payment_amount" in df.columns else 0
            routes  = df["route"].nunique()         if "route"          in df.columns else "—"
            return (f"Summary: {total_b:,} bookings | ₹{total_r:,.0f} revenue | "
                    f"{routes} routes\nSet ANTHROPIC_API_KEY for natural language answers.")
    except Exception as e:
        return f"Query error: {e}"


def _render_chat(history):
    if not history:
        return html.Div("[ ask a question to get started ]",
                        style={"color":MUT,"fontSize":"11px","fontFamily":MONO,"textAlign":"center","marginTop":"40px"})
    items = []
    for turn in reversed(history):
        items.append(html.Div(style={"marginBottom":"16px"}, children=[
            html.Div(style={"display":"flex","gap":"8px","marginBottom":"6px","alignItems":"flex-start"}, children=[
                html.Span("▶", style={"color":C7,"fontSize":"10px","marginTop":"2px"}),
                html.Div(turn["q"], style={"color":C7,"fontSize":"11px","fontFamily":MONO,"fontWeight":"600"}),
            ]),
            html.Div(style={"display":"flex","gap":"8px","alignItems":"flex-start"}, children=[
                html.Span("◀", style={"color":C1,"fontSize":"10px","marginTop":"2px"}),
                html.Div(turn["a"], style={"color":TX,"fontSize":"11px","fontFamily":MONO,
                                            "whiteSpace":"pre-wrap","lineHeight":"1.6"}),
            ]),
            html.Div(style={"borderBottom":f"1px solid {B}","marginTop":"12px"}),
        ]))
    return items


# ================================================================
# CB 12 — DEBUG
# ================================================================
@app.callback(
    Output("content-debug","children"),
    [Input("store-booking","data"), Input("store-failed","data")],
)
def render_debug(bk, fail):
    # Debug tab fetches full schema info
    try:
        engine = get_pg_engine()
        df = pd.read_sql("SELECT * FROM customer_booking_staging LIMIT 1000", engine)
        engine.dispose()
    except Exception as e:
        df = pd.DataFrame()
        print(f"[DEBUG ERROR] {e}")
    fail_df = safe_df(fail)
    if df.empty:
        return html.Div("[ loading data... ]", style={"color":MUT,"fontFamily":MONO,"padding":"60px","textAlign":"center"})

    schema_items = []
    if not df.empty:
        for col in df.columns:
            n   = df[col].isnull().sum()
            pct = n/len(df)*100
            schema_items.append(html.Div(style={
                "backgroundColor":BG,"borderRadius":"6px","padding":"10px","border":f"1px solid {B}"}, children=[
                html.Div(col, style={"color":C1,"fontSize":"10px","fontFamily":MONO,"fontWeight":"700","marginBottom":"5px","wordBreak":"break-all"}),
                html.Div(str(df[col].dtype), style={"color":C4,"fontSize":"9px","fontFamily":MONO,"marginBottom":"3px"}),
                html.Div(f"null: {n} ({pct:.1f}%)", style={"color":C3 if pct>0 else MUT,"fontSize":"9px","fontFamily":MONO,"marginBottom":"2px"}),
                html.Div(f"unique: {df[col].nunique():,}", style={"color":SUB,"fontSize":"9px","fontFamily":MONO}),
            ]))

    null_bars = []
    if not df.empty:
        for col in df.columns:
            pct = df[col].isnull().sum()/len(df)*100
            null_bars.append(html.Div(style={"display":"flex","alignItems":"center","gap":"10px","marginBottom":"8px"}, children=[
                html.Span(col, style={"color":TX,"fontSize":"10px","fontFamily":MONO,"width":"180px",
                                       "overflow":"hidden","textOverflow":"ellipsis","whiteSpace":"nowrap"}),
                html.Div(style={"flex":"1","backgroundColor":B,"borderRadius":"2px","height":"4px"}, children=[
                    html.Div(style={"backgroundColor":C3 if pct>5 else (C5 if pct>0 else C2),
                                    "width":f"{min(pct,100):.1f}%","height":"100%","borderRadius":"2px"})]),
                html.Span(f"{pct:.1f}%", style={"color":C3 if pct>5 else (C5 if pct>0 else MUT),
                                                 "fontSize":"10px","fontFamily":MONO,"width":"36px","textAlign":"right"}),
            ]))

    fail_content = html.Div("[ no flagged records ]", style={"color":MUT,"fontFamily":MONO,"fontSize":"11px"})
    if not fail_df.empty:
        show = [c for c in ["run_identifier","expectation_type","column_name",
                             "failed_reason","booking_id","customer_name",
                             "loyalty_tier","payment_method","payment_amount"] if c in fail_df.columns]
        cmap = {"run_identifier":"run","expectation_type":"rule","column_name":"col",
                "failed_reason":"reason","booking_id":"booking","customer_name":"customer",
                "loyalty_tier":"tier","payment_method":"payment","payment_amount":"amount"}
        fail_content = dash_table.DataTable(
            data=fail_df[show].head(300).to_dict("records"),
            columns=[{"name":cmap.get(c,c),"id":c} for c in show],
            style_table={"overflowX":"auto"},
            style_cell={"backgroundColor":BG,"color":TX,"border":f"1px solid {B}",
                         "textAlign":"left","padding":"8px 12px","fontSize":"10px","fontFamily":MONO,
                         "maxWidth":"160px","overflow":"hidden","textOverflow":"ellipsis"},
            style_header={"backgroundColor":SIDE,"color":C1,"fontWeight":"700","fontSize":"9px",
                           "border":f"1px solid {B}","textTransform":"uppercase","letterSpacing":"1.5px"},
            style_data_conditional=[{"if":{"row_index":"odd"},"backgroundColor":CARD}],
            page_size=15, filter_action="native", sort_action="native",
        )

    return loading(html.Div([
        section("debug console","schema · nulls · flagged records"),
        card(accent=C1, mb="12px", children=[
            html.Div(f"schema inspector · customer_booking_staging ({len(df.columns) if not df.empty else 0} cols)",
                     style={"color":C1,"fontFamily":MONO,"fontWeight":"700","fontSize":"11px","marginBottom":"14px"}),
            html.Div(style={"display":"grid","gridTemplateColumns":"repeat(auto-fill,minmax(140px,1fr))","gap":"8px"},
                     children=schema_items),
        ]),
        card(accent=C5, mb="12px", children=[
            html.Div("null analysis · all columns",
                     style={"color":C5,"fontFamily":MONO,"fontWeight":"700","fontSize":"11px","marginBottom":"14px"}),
            *null_bars,
        ]),
        card(accent=C3, mb="0", children=[
            html.Div(style={"display":"flex","justifyContent":"space-between","alignItems":"center","marginBottom":"14px"}, children=[
                html.Span("flagged records explorer", style={"color":C3,"fontFamily":MONO,"fontWeight":"700","fontSize":"11px"}),
                pill(f"{len(fail_df):,} records", C5) if not fail_df.empty else None,
            ]),
            fail_content,
        ]),
    ]))


# ================================================================
# FILTER HELPERS
# ================================================================
def _apply_filters(df, loy_f, pay_f, trip_f, seg_f):
    if df.empty: return df
    if loy_f  and "loyalty_tier"    in df.columns: df = df[df["loyalty_tier"]    == loy_f]
    if pay_f  and "payment_method"  in df.columns: df = df[df["payment_method"]  == pay_f]
    if trip_f and "trip_type"       in df.columns: df = df[df["trip_type"]       == trip_f]
    if seg_f  and "car_segment"     in df.columns: df = df[df["car_segment"]     == seg_f]
    return df

def _filter_badge(df, df_raw):
    active = len(df) != len(df_raw)
    return html.Span(f"[ {len(df):,} / {len(df_raw):,} records ]",
                     style={"color":C5 if active else MUT,"fontSize":"10px","fontFamily":MONO})


# ================================================================
# CB 13 — AI AUTO-INSIGHTS
# UNIQUE: Claude automatically generates insights on page load
# Uses: ML scores + MySQL GE data + pipeline stats
# ================================================================
@app.callback(
    Output("auto-insights-output", "children"),
    [Input("store-forecast", "data"),
     Input("store-fraud", "data"),
     Input("store-churn", "data"),
     Input("store-validation", "data"),
     Input("store-failed", "data"),
     Input("store-sla", "data")],
    prevent_initial_call=False,
)
def generate_auto_insights(forecast_data, fraud_data, churn_data, val_data, failed_data, sla_data):
    ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

    forecast_df = safe_df(forecast_data)
    fraud_df    = safe_df(fraud_data)
    churn_df    = safe_df(churn_data)
    val_df      = safe_df(val_data)
    failed_df   = safe_df(failed_data)
    sla_df      = safe_df(sla_data)

    # Build context for Claude
    insights_context = {}

    # ML metrics
    if not forecast_df.empty and "predicted_bookings" in forecast_df.columns:
        insights_context["next_7d_avg_bookings"] = round(float(forecast_df["predicted_bookings"].head(7).mean()), 1)
        insights_context["peak_forecast_day"]    = str(forecast_df.loc[forecast_df["predicted_bookings"].idxmax(), "forecast_date"])
        insights_context["peak_forecast_value"]  = round(float(forecast_df["predicted_bookings"].max()), 0)

    if not fraud_df.empty and "risk_label" in fraud_df.columns:
        insights_context["fraud_critical_count"] = int((fraud_df["risk_label"] == "CRITICAL").sum())
        insights_context["fraud_high_count"]     = int((fraud_df["risk_label"] == "HIGH").sum())
        insights_context["total_fraud_scored"]   = len(fraud_df)
        if "route" in fraud_df.columns:
            top_fraud_route = fraud_df[fraud_df["risk_label"] == "CRITICAL"]["route"].mode()
            insights_context["top_fraud_route"] = str(top_fraud_route.iloc[0]) if not top_fraud_route.empty else "N/A"

    if not churn_df.empty and "churn_risk" in churn_df.columns:
        high_risk = churn_df[churn_df["churn_risk"].isin(["HIGH", "CRITICAL"])]
        insights_context["churn_high_risk_count"] = len(high_risk)
        insights_context["churn_model_auc"]       = round(float(churn_df["model_auc"].iloc[0]), 3) if "model_auc" in churn_df.columns else "N/A"
        if "loyalty_tier" in high_risk.columns and not high_risk.empty:
            insights_context["highest_churn_tier"] = str(high_risk["loyalty_tier"].mode().iloc[0])

    # GE validation data — MySQL
    if not val_df.empty and "success_rate" in val_df.columns:
        insights_context["dq_score"]        = round(float(val_df["success_rate"].mean()), 1)
        insights_context["total_rules"]     = int(val_df["total_expectations"].sum()) if "total_expectations" in val_df.columns else 0
        insights_context["failed_rules"]    = int(val_df["failed_expectations"].sum()) if "failed_expectations" in val_df.columns else 0

    # GE failed records — UNIQUE: specific column failures
    if not failed_df.empty:
        if "column_name" in failed_df.columns:
            top_failed_col = failed_df["column_name"].value_counts().head(3).to_dict()
            insights_context["top_failed_columns"] = top_failed_col
        insights_context["total_failed_records"] = len(failed_df)

    # Pipeline health
    if not sla_df.empty and "status" in sla_df.columns:
        insights_context["pipeline_success_rate"] = round(
            len(sla_df[sla_df["status"] == "SUCCESS"]) / len(sla_df) * 100, 1)
        insights_context["total_pipeline_runs"] = int(sla_df["run_id"].nunique()) if "run_id" in sla_df.columns else 0

    # No API key — show summary cards
    if not ANTHROPIC_KEY:
        items = []
        if insights_context.get("churn_high_risk_count"):
            items.append(("📉 Churn Risk",
                f"{insights_context['churn_high_risk_count']} customers at high/critical churn risk. "
                f"Model AUC: {insights_context.get('churn_model_auc', 'N/A')}. "
                f"Set ANTHROPIC_API_KEY for AI analysis.", C5))
        if insights_context.get("fraud_critical_count"):
            items.append(("🚨 Fraud Alert",
                f"{insights_context['fraud_critical_count']} CRITICAL + "
                f"{insights_context.get('fraud_high_count', 0)} HIGH risk bookings detected. "
                f"Top route: {insights_context.get('top_fraud_route', 'N/A')}.", C3))
        if insights_context.get("dq_score"):
            items.append(("🛡️ Data Quality",
                f"DQ Score: {insights_context['dq_score']}%. "
                f"{insights_context.get('failed_rules', 0)} rules violated. "
                f"Top failed: {list(insights_context.get('top_failed_columns', {}).keys())[:2]}.", C4))
        if not items:
            return html.Div("[ run pipeline first to generate insights ]",
                           style={"color":MUT,"fontSize":"11px","fontFamily":MONO,"textAlign":"center","padding":"20px"})
        return html.Div([
            html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr 1fr","gap":"12px"}, children=[
                html.Div(style={"backgroundColor":BG,"borderRadius":"6px","border":f"1px solid {B}",
                                "borderTop":f"2px solid {c}","padding":"14px"}, children=[
                    html.Div(title, style={"color":c,"fontSize":"11px","fontFamily":MONO,
                                           "fontWeight":"700","marginBottom":"8px"}),
                    html.Div(msg, style={"color":TX,"fontSize":"10px","fontFamily":MONO,"lineHeight":"1.6"}),
                ]) for title, msg, c in items
            ]),
        ])

    # Claude generates insights
    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=ANTHROPIC_KEY)

        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=600,
            messages=[{"role": "user", "content": f"""
You are an AI data analyst for a car booking pipeline.
Analyze this data and generate exactly 3 actionable business insights.

Pipeline & ML Metrics:
{json.dumps(insights_context, indent=2, default=str)}

Rules:
1. Each insight must have: title, specific numbers, business impact, recommended action
2. Focus on: churn risk, fraud patterns, data quality issues
3. Be specific — use exact numbers from the data
4. Format as JSON array:
[
  {{"title": "...", "insight": "...", "action": "...", "severity": "high/medium/low"}},
  {{"title": "...", "insight": "...", "action": "...", "severity": "high/medium/low"}},
  {{"title": "...", "insight": "...", "action": "...", "severity": "high/medium/low"}}
]
Return ONLY the JSON array, no other text.
"""}])

        raw = msg.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        insights_list = json.loads(raw.strip())

        sev_colors = {"high": C3, "medium": C5, "low": C2}
        sev_icons  = {"high": "🚨", "medium": "⚠️", "low": "✅"}

        return html.Div([
            html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr 1fr","gap":"12px"}, children=[
                html.Div(style={
                    "backgroundColor":BG,"borderRadius":"6px",
                    "border":f"1px solid {B}",
                    "borderTop":f"2px solid {sev_colors.get(ins.get('severity','medium'), C5)}",
                    "padding":"14px"}, children=[
                    html.Div(style={"display":"flex","alignItems":"center","gap":"6px","marginBottom":"8px"}, children=[
                        html.Span(sev_icons.get(ins.get("severity","medium"), "⚠️"), style={"fontSize":"14px"}),
                        html.Span(ins.get("title","Insight"), style={
                            "color":sev_colors.get(ins.get("severity","medium"), C5),
                            "fontSize":"11px","fontFamily":MONO,"fontWeight":"700"}),
                    ]),
                    html.Div(ins.get("insight",""), style={"color":TX,"fontSize":"10px","fontFamily":MONO,
                                                            "lineHeight":"1.6","marginBottom":"8px"}),
                    html.Div(style={"borderTop":f"1px solid {B}","paddingTop":"8px","marginTop":"4px"}, children=[
                        html.Span("→ ", style={"color":C1,"fontSize":"9px","fontFamily":MONO}),
                        html.Span(ins.get("action",""), style={"color":C6,"fontSize":"9px","fontFamily":MONO}),
                    ]),
                ]) for ins in insights_list[:3]
            ]),
        ])

    except Exception as e:
        return html.Div(f"[ insights error: {str(e)[:80]} ]",
                       style={"color":C3,"fontSize":"10px","fontFamily":MONO,"padding":"10px"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)