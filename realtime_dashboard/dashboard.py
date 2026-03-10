"""
Car Booking Pipeline Dashboard
Architecture: dcc.Store → tab-specific callbacks → no unnecessary re-renders
Stack: Kafka → Spark → Delta Lake → PostgreSQL → Dash
"""
import os
import dash
from dash import dcc, html, Input, Output, State, dash_table, ctx
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# ================================================================
# DB — only called from refresh_store()
# ================================================================
def get_booking_data():
    host = os.getenv("POSTGRES_HOST", "postgres")
    try:
        engine = create_engine(
            f"postgresql+psycopg2://admin:admin@{host}:5432/booking",
            pool_pre_ping=True
        )
        df = pd.read_sql("SELECT * FROM customer_booking_staging", engine)
        engine.dispose()
        return df
    except Exception as e:
        print(f"[POSTGRES ERROR] {e}")
        return pd.DataFrame()

def get_monitoring_data():
    host = os.getenv("MYSQL_HOST", "mysql")
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://admin:admin@{host}:3306/booking",
            pool_pre_ping=True
        )
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
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# ================================================================
# THEME
# ================================================================
BG      = "#090d13"
CARD    = "#0f1923"
SIDE    = "#060a10"
B       = "#1e2d3d"
B2      = "#243447"
C1      = "#00d4ff"   # cyan
C2      = "#00ff88"   # green
C3      = "#ff4757"   # red
C4      = "#a78bfa"   # purple
C5      = "#ffd43b"   # yellow
C6      = "#74c0fc"   # light blue
TX      = "#cdd9e5"
SUB     = "#6e7f8d"
MUT     = "#3d5166"
GR      = "rgba(30,45,61,0.5)"
MONO    = "'JetBrains Mono','Fira Code','Courier New',monospace"

CBASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor=CARD,
    font=dict(color=TX, family=MONO, size=11),
    margin=dict(l=50, r=20, t=46, b=38),
    title_font=dict(color=C1, size=12, family=MONO),
)

# Default axis — use where no custom axis needed
AXIS = dict(gridcolor=GR, zerolinecolor=GR, color=SUB)

# ================================================================
# UI HELPERS
# ================================================================
def safe_df(data):
    try:
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def card(children, accent=B, mb="12px"):
    return html.Div(style={
        "backgroundColor": CARD, "borderRadius": "8px",
        "border": f"1px solid {B}", "borderLeft": f"3px solid {accent}",
        "padding": "18px", "marginBottom": mb,
    }, children=children)

def kpi(label, value, color, icon, hint=""):
    return html.Div(style={
        "backgroundColor": CARD, "borderRadius": "8px",
        "border": f"1px solid {B}", "borderTop": f"2px solid {color}",
        "padding": "16px", "flex": "1", "minWidth": "120px",
    }, children=[
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
    try:
        r,g,b = int(color[1:3],16), int(color[3:5],16), int(color[5:7],16)
        bg = f"rgba({r},{g},{b},0.12)"
    except Exception:
        bg = "rgba(0,0,0,0.2)"
    return html.Span(text, style={
        "backgroundColor": bg, "color": color,
        "border": f"1px solid {color}", "borderRadius": "4px",
        "padding": "2px 10px", "fontSize": "9px",
        "fontFamily": MONO, "fontWeight": "600", "letterSpacing": "0.5px",
    })

def section(title, sub=None):
    return html.Div(style={"marginBottom":"20px"}, children=[
        html.Div(style={"display":"flex","alignItems":"center","gap":"8px","marginBottom":"3px"},
                 children=[
            html.Span("▸", style={"color":C1,"fontSize":"13px"}),
            html.H2(title, style={"color":TX,"fontSize":"17px","fontWeight":"700",
                                   "margin":"0","fontFamily":MONO}),
        ]),
        html.Div(sub, style={"color":SUB,"fontSize":"10px","fontFamily":MONO,
                              "paddingLeft":"21px"}) if sub else None,
    ])

def nav_style(active):
    return {
        "backgroundColor": "rgba(0,212,255,0.10)" if active else "transparent",
        "color": C1 if active else SUB,
        "border": f"1px solid {C1}" if active else "1px solid transparent",
        "padding": "8px 12px", "borderRadius": "6px", "cursor": "pointer",
        "fontSize": "11px", "fontWeight": "600", "fontFamily": MONO,
        "width": "100%", "textAlign": "left", "marginBottom": "3px",
    }

def empty(msg="no data"):
    fig = go.Figure()
    fig.add_annotation(text=f"[ {msg} ]", xref="paper", yref="paper",
                       x=0.5, y=0.5, showarrow=False,
                       font={"color":MUT,"size":12,"family":MONO})
    fig.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, height=220)
    return fig

def loading(children):
    return dcc.Loading(children=children, type="dot",
                       color=C1, style={"minHeight":"100px"})

def split_bars(df, col, color):
    if df.empty or col not in df.columns:
        return [html.Div("no data", style={"color":MUT,"fontSize":"11px","fontFamily":MONO})]
    grp = (df.groupby(col)["booking_id"].count().reset_index()
           .assign(pct=lambda x: x["booking_id"]/x["booking_id"].sum()*100)
           .sort_values("pct", ascending=False))
    rows = []
    for _, r in grp.iterrows():
        rows.append(html.Div(style={"marginBottom":"10px"}, children=[
            html.Div(style={"display":"flex","justifyContent":"space-between","marginBottom":"3px"},
                     children=[
                html.Span(str(r[col]), style={"color":TX,"fontSize":"11px","fontFamily":MONO}),
                html.Span(f"{r['pct']:.1f}%", style={"color":color,"fontSize":"11px",
                                                       "fontFamily":MONO,"fontWeight":"700"}),
            ]),
            html.Div(style={"backgroundColor":B,"borderRadius":"2px","height":"3px"}, children=[
                html.Div(style={"backgroundColor":color,"width":f"{r['pct']:.1f}%",
                                "height":"100%","borderRadius":"2px","opacity":"0.8"}),
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
    </style>
</head>
<body>{%app_entry%}
<footer>{%config%}{%scripts%}{%renderer%}</footer>
</body></html>'''

# ================================================================
# LAYOUT
# ================================================================
TABS = ["tab-overview","tab-analytics","tab-observability","tab-pipeline","tab-quality","tab-debug"]

app.layout = html.Div(
    style={"backgroundColor":BG,"minHeight":"100vh","display":"flex",
           "flexDirection":"column","fontFamily":MONO,"color":TX},
    children=[
        # ── Stores ──────────────────────────────────────────
        dcc.Store(id="store-booking",     storage_type="memory"),
        dcc.Store(id="store-validation",  storage_type="memory"),
        dcc.Store(id="store-expectation", storage_type="memory"),
        dcc.Store(id="store-failed",      storage_type="memory"),
        dcc.Store(id="store-sla",         storage_type="memory"),
        dcc.Store(id="store-alerts",      storage_type="memory"),
        dcc.Store(id="store-schema",      storage_type="memory"),
        dcc.Store(id="active-tab",        storage_type="memory", data="tab-overview"),

        # ── Topbar ──────────────────────────────────────────
        html.Div(style={
            "backgroundColor":SIDE,"borderBottom":f"1px solid {B}",
            "padding":"0 24px","display":"flex","alignItems":"center",
            "justifyContent":"space-between","height":"52px",
            "position":"sticky","top":"0","zIndex":"200",
        }, children=[
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
                    pill("postgres",C2),
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
                    "fontSize":"10px","fontFamily":MONO,
                }),
            ]),
            dcc.Interval(id="auto-refresh", interval=30000, n_intervals=0),
        ]),

        # ── Body ────────────────────────────────────────────
        html.Div(style={"display":"flex","flex":"1","overflow":"hidden"}, children=[

            # ── Sidebar ─────────────────────────────────────
            html.Div(style={
                "width":"188px","minWidth":"188px","backgroundColor":SIDE,
                "borderRight":f"1px solid {B}","padding":"18px 10px",
                "display":"flex","flexDirection":"column",
                "position":"sticky","top":"52px",
                "height":"calc(100vh - 52px)","overflowY":"auto",
            }, children=[
                html.Div("VIEWS", style={"color":MUT,"fontSize":"9px","fontWeight":"700",
                                          "letterSpacing":"2px","marginBottom":"10px",
                                          "paddingLeft":"4px"}),
                html.Button("📊  overview",       id="tab-overview",  n_clicks=0, style=nav_style(True)),
                html.Button("📈  analytics",      id="tab-analytics", n_clicks=0, style=nav_style(False)),
                html.Button("🔭  observability",  id="tab-observability", n_clicks=0, style=nav_style(False)),
                html.Button("⚡  pipeline",       id="tab-pipeline",  n_clicks=0, style=nav_style(False)),
                html.Button("🛡️  data quality",   id="tab-quality",   n_clicks=0, style=nav_style(False)),
                html.Button("🐛  debug",          id="tab-debug",     n_clicks=0, style=nav_style(False)),

                html.Div(style={"borderTop":f"1px solid {B}","margin":"18px 0 14px"}),
                html.Div("FILTERS", style={"color":MUT,"fontSize":"9px","fontWeight":"700",
                                            "letterSpacing":"2px","marginBottom":"12px",
                                            "paddingLeft":"4px"}),
                html.Div("loyalty_tier",       style={"color":C6,"fontSize":"9px","marginBottom":"4px","paddingLeft":"2px"}),
                dcc.Dropdown(id="filter-loyalty",   options=[], placeholder="all", clearable=True,
                             style={"fontSize":"11px","marginBottom":"10px"}),
                html.Div("payment_method",     style={"color":C6,"fontSize":"9px","marginBottom":"4px","paddingLeft":"2px"}),
                dcc.Dropdown(id="filter-payment",   options=[], placeholder="all", clearable=True,
                             style={"fontSize":"11px","marginBottom":"10px"}),
                html.Div("insurance_coverage", style={"color":C6,"fontSize":"9px","marginBottom":"4px","paddingLeft":"2px"}),
                dcc.Dropdown(id="filter-insurance", options=[], placeholder="all", clearable=True,
                             style={"fontSize":"11px","marginBottom":"10px"}),
                html.Button("✕ clear filters", id="clear-filters", n_clicks=0, style={
                    "backgroundColor":"transparent","color":C3,
                    "border":f"1px solid {B}","padding":"5px 8px",
                    "borderRadius":"4px","cursor":"pointer",
                    "fontSize":"10px","width":"100%",
                }),

                html.Div(style={"borderTop":f"1px solid {B}","margin":"18px 0 12px"}),
                html.Div(id="sidebar-meta", style={"color":MUT,"fontSize":"9px","lineHeight":"1.9"}),
            ]),

            # ── Content ─────────────────────────────────────
            html.Div(style={
                "flex":"1","overflowY":"auto","padding":"28px",
                "height":"calc(100vh - 52px)",
            }, children=[
                # Each tab has its own div — only active one visible
                html.Div(id="content-overview",    style={"display":"block"}),
                html.Div(id="content-analytics",   style={"display":"none"}),
                html.Div(id="content-observability", style={"display":"none"}),
                html.Div(id="content-pipeline",    style={"display":"none"}),
                html.Div(id="content-quality",     style={"display":"none"}),
                html.Div(id="content-debug",       style={"display":"none"}),
            ]),
        ]),
    ]
)


# ================================================================
# CB 1 — DATA STORE: single DB fetch, results → stores
# ================================================================
@app.callback(
    [Output("store-booking",    "data"),
     Output("store-validation", "data"),
     Output("store-expectation","data"),
     Output("store-failed",     "data"),
     Output("store-sla",        "data"),
     Output("store-alerts",     "data"),
     Output("store-schema",     "data"),
     Output("last-updated",     "children"),
     Output("store-status",     "children")],
    [Input("auto-refresh","n_intervals"),
     Input("refresh-btn", "n_clicks")],
    prevent_initial_call=False,
)
def refresh_store(n, clicks):
    now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        df                                        = get_booking_data()
        val_df, exp_df, fail_df, sla_df, alerts_df, schema_df = get_monitoring_data()
        status = (f"✦ {len(df):,} records  ·  "
                  f"{'✓ pg' if not df.empty else '✗ pg'}  "
                  f"{'✓ mysql' if not val_df.empty else '✗ mysql'}")
        return (
            df.to_dict("records")         if not df.empty         else [],
            val_df.to_dict("records")     if not val_df.empty     else [],
            exp_df.to_dict("records")     if not exp_df.empty     else [],
            fail_df.to_dict("records")    if not fail_df.empty    else [],
            sla_df.to_dict("records")     if not sla_df.empty     else [],
            alerts_df.to_dict("records")  if not alerts_df.empty  else [],
            schema_df.to_dict("records")  if not schema_df.empty  else [],
            f"↻ {now}", status,
        )
    except Exception as e:
        return [], [], [], [], [], [], [], f"↻ {now}", f"✗ {str(e)[:40]}"


# ================================================================
# CB 2a — ALERT BADGE: topbar mein active warnings dikhao
# ================================================================
@app.callback(
    Output("alert-badge", "children"),
    Input("store-alerts", "data"),
)
def update_alert_badge(alerts_data):
    if not alerts_data:
        return None
    alerts_df = safe_df(alerts_data)
    if alerts_df.empty or "alert_type" not in alerts_df.columns:
        return None

    failures = len(alerts_df[alerts_df["alert_type"] == "FAILURE"])
    warnings = len(alerts_df[alerts_df["alert_type"] == "DQ_WARNING"])

    if failures > 0:
        return html.Span(
            f"🚨 {failures} failure{'s' if failures>1 else ''}",
            style={"backgroundColor":"rgba(255,71,87,0.15)",
                   "color":C3,"border":f"1px solid {C3}",
                   "borderRadius":"4px","padding":"3px 10px",
                   "fontSize":"10px","fontFamily":MONO,"fontWeight":"700"},
        )
    elif warnings > 0:
        return html.Span(
            f"⚠️ {warnings} warning{'s' if warnings>1 else ''}",
            style={"backgroundColor":"rgba(255,212,59,0.12)",
                   "color":C5,"border":f"1px solid {C5}",
                   "borderRadius":"4px","padding":"3px 10px",
                   "fontSize":"10px","fontFamily":MONO,"fontWeight":"700"},
        )
    return pill("● all clear", C2)



@app.callback(
    [Output("filter-loyalty",  "options"),
     Output("filter-payment",  "options"),
     Output("filter-insurance","options"),
     Output("sidebar-meta",    "children")],
    Input("store-booking","data"),
)
def update_filters(booking_data):
    df = safe_df(booking_data)
    def opts(col):
        if df.empty or col not in df.columns: return []
        return [{"label":v,"value":v} for v in sorted(df[col].dropna().unique())]
    meta_lines = [
        f"records: {len(df):,}",
        f"columns: {len(df.columns)}",
        f"bookings: {df['booking_id'].nunique():,}" if "booking_id" in df.columns else "",
        f"customers: {df['customer_id'].nunique():,}" if "customer_id" in df.columns else "",
    ]
    return (opts("loyalty_tier"), opts("payment_method"), opts("insurance_coverage"),
            html.Div([html.Div(l) for l in meta_lines if l]))


# ================================================================
# CB 3 — TAB VISIBILITY + NAV STYLES
# ================================================================
@app.callback(
    [Output("tab-overview",       "style"),
     Output("tab-analytics",      "style"),
     Output("tab-observability",  "style"),
     Output("tab-pipeline",       "style"),
     Output("tab-quality",        "style"),
     Output("tab-debug",          "style"),
     Output("content-overview",   "style"),
     Output("content-analytics",  "style"),
     Output("content-observability", "style"),
     Output("content-pipeline",   "style"),
     Output("content-quality",    "style"),
     Output("content-debug",      "style"),
     Output("active-tab",         "data")],
    [Input("tab-overview",  "n_clicks"),
     Input("tab-analytics", "n_clicks"),
     Input("tab-observability", "n_clicks"),
     Input("tab-pipeline",  "n_clicks"),
     Input("tab-quality",   "n_clicks"),
     Input("tab-debug",     "n_clicks")],
    State("active-tab","data"),
    prevent_initial_call=False,
)
def switch_tab(ov,an,ml,pi,qu,de, current):
    triggered = ctx.triggered_id
    active    = triggered if triggered in TABS else (current or "tab-overview")
    nav_styles     = [nav_style(t==active) for t in TABS]
    content_styles = [{"display":"block"} if t.replace("tab-","content-")==f"content-{active.replace('tab-','')}"
                       else {"display":"none"} for t in TABS]
    return (*nav_styles, *content_styles, active)


# ================================================================
# CB 4 — OVERVIEW: booking + sla + validation stores
# ================================================================
@app.callback(
    Output("content-overview","children"),
    [Input("store-booking",   "data"),
     Input("store-sla",       "data"),
     Input("store-validation","data"),
     Input("store-alerts",    "data"),
     Input("filter-loyalty",  "value"),
     Input("filter-payment",  "value"),
     Input("filter-insurance","value"),
     Input("clear-filters",   "n_clicks")],
)
def render_overview(bk, sla, val, alerts, loy_f, pay_f, ins_f, clr):
    if not bk:
        return html.Div("[ loading data... ]",
                        style={"color":MUT,"fontFamily":MONO,"fontSize":"12px",
                               "padding":"60px","textAlign":"center"})
    df_raw     = safe_df(bk)
    sla_df     = safe_df(sla)
    val_df     = safe_df(val)
    alerts_df  = safe_df(alerts)
    df         = _apply_filters(df_raw, loy_f, pay_f, ins_f)
    fbadge     = _filter_badge(df, df_raw)

    total_b   = df["booking_id"].nunique()   if not df.empty and "booking_id"      in df.columns else 0
    total_c   = df["customer_id"].nunique()  if not df.empty and "customer_id"     in df.columns else 0
    total_rev = df["payment_amount"].sum()   if not df.empty and "payment_amount"  in df.columns else 0
    total_car = df["car_id"].nunique()       if not df.empty and "car_id"          in df.columns else 0
    avg_rev   = df["payment_amount"].mean()  if not df.empty and "payment_amount"  in df.columns else 0
    dq_score  = val_df["success_rate"].mean()if not val_df.empty and "success_rate" in val_df.columns else 0
    p_ok      = not sla_df.empty and len(sla_df[sla_df["status"]=="FAILED"])==0 if not sla_df.empty else False
    failures  = len(alerts_df[alerts_df["alert_type"]=="FAILURE"]) if not alerts_df.empty and "alert_type" in alerts_df.columns else 0
    warnings  = len(alerts_df[alerts_df["alert_type"]=="DQ_WARNING"]) if not alerts_df.empty and "alert_type" in alerts_df.columns else 0

    # Revenue sparkline
    if not df.empty and "booking_date" in df.columns:
        rd = df.groupby("booking_date")["payment_amount"].sum().reset_index()
        fig_spark = go.Figure()
        fig_spark.add_trace(go.Scatter(
            x=rd["booking_date"], y=rd["payment_amount"],
            mode="lines",
            fill="tozeroy",
            line=dict(color="#00ff88", width=2),
            fillcolor="white",
            opacity=0.05,
        ))
        fig_spark.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#cdd9e5", family="'JetBrains Mono',monospace", size=11),
            title=dict(text="revenue · daily", font=dict(color="#00d4ff", size=12)),
            margin=dict(l=0, r=0, t=28, b=0),
            height=110,
            xaxis=dict(visible=False, gridcolor="rgba(0,0,0,0)"),
            yaxis=dict(visible=False, gridcolor="rgba(0,0,0,0)"),
        )
    else:
        fig_spark = empty("revenue — no data"); fig_spark.update_layout(height=110)

    # Top cars
    if not df.empty and "model" in df.columns:
        tc = df.groupby("model")["payment_amount"].sum().nlargest(7).reset_index()
        fig_cars = go.Figure(go.Bar(
            x=tc["payment_amount"], y=tc["model"], orientation="h",
            marker=dict(color=tc["payment_amount"],
                        colorscale=[[0,"#0f2027"],[0.5,C1],[1,C4]],
                        showscale=False),
            text=[f"₹{v:,.0f}" for v in tc["payment_amount"]],
            textposition="outside",
            textfont=dict(color=SUB,size=9,family=MONO),
        ))
        fig_cars.update_layout(**CBASE, title="top cars · revenue", height=260,
                               xaxis=AXIS,
                               yaxis={"categoryorder":"total ascending"})
    else:
        fig_cars = empty("top cars — no data"); fig_cars.update_layout(height=260)

    # Heatmap
    if not df.empty and "booking_date" in df.columns:
        d2 = df.copy()
        d2["booking_date"] = pd.to_datetime(d2["booking_date"])
        d2["dow"]   = d2["booking_date"].dt.day_name()
        d2["month"] = d2["booking_date"].dt.strftime("%b")
        ht = d2.groupby(["month","dow"])["booking_id"].count().reset_index()
        ht.columns = ["Month","Day","Count"]
        ht["Day"] = pd.Categorical(ht["Day"],
            categories=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"],
            ordered=True)
        ht = ht.sort_values("Day")
        fig_heat = px.density_heatmap(ht, x="Month", y="Day", z="Count",
                                       title="heatmap · month × day of week",
                                       color_continuous_scale=[[0,BG],[0.4,C1],[1,C4]])
        fig_heat.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, coloraxis_showscale=False, height=250)
    else:
        fig_heat = empty("heatmap — no data"); fig_heat.update_layout(height=250)

    return loading(html.Div([
        html.Div(style={"display":"flex","justifyContent":"space-between",
                        "alignItems":"flex-start","marginBottom":"22px"}, children=[
            html.Div([section("overview","real-time pipeline analytics"), fbadge]),
            html.Div(style={"display":"flex","gap":"8px"}, children=[
                pill("● healthy" if p_ok else "● issues", C2 if p_ok else C3),
                pill(f"dq: {dq_score:.1f}%",
                     C2 if dq_score>=90 else (C5 if dq_score>=70 else C3)),
                pill(f"🚨 {failures} failure{'s' if failures!=1 else ''}", C3) if failures > 0 else None,
                pill(f"⚠️ {warnings} warning{'s' if warnings!=1 else ''}", C5) if warnings > 0 and failures == 0 else None,
            ]),
        ]),
        html.Div(style={"display":"flex","gap":"10px","marginBottom":"14px","flexWrap":"wrap"},
                 children=[
            kpi("total bookings",  f"{total_b:,}",        C1, "📋"),
            kpi("customers",       f"{total_c:,}",        C2, "👤"),
            kpi("revenue",         f"₹{total_rev:,.0f}",  C3, "💰"),
            kpi("unique cars",     f"{total_car:,}",      C5, "🚘"),
            kpi("avg booking",     f"₹{avg_rev:,.0f}",    C6, "📊"),
            kpi("data quality",    f"{dq_score:.1f}%",    C4, "🛡️"),
            kpi("active alerts",   str(failures+warnings), C3 if failures>0 else (C5 if warnings>0 else C2), "🔔"),
        ]),
        card([dcc.Graph(figure=fig_spark, config={"displayModeBar":False})], accent=C2),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1.5fr","gap":"12px","marginBottom":"12px"},
                 children=[
            card([dcc.Graph(figure=fig_cars, config={"displayModeBar":False})], accent=C5),
            card([dcc.Graph(figure=fig_heat, config={"displayModeBar":False})], accent=C4),
        ]),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr 1fr","gap":"12px"}, children=[
            card([html.Div("payment_method",     style={"color":C6,"fontSize":"9px","marginBottom":"12px",
                                                         "textTransform":"uppercase","letterSpacing":"1px"}),
                  *split_bars(df,"payment_method",C1)], accent=C1, mb="0"),
            card([html.Div("loyalty_tier",       style={"color":C6,"fontSize":"9px","marginBottom":"12px",
                                                         "textTransform":"uppercase","letterSpacing":"1px"}),
                  *split_bars(df,"loyalty_tier",C5)], accent=C5, mb="0"),
            card([html.Div("insurance_coverage", style={"color":C6,"fontSize":"9px","marginBottom":"12px",
                                                         "textTransform":"uppercase","letterSpacing":"1px"}),
                  *split_bars(df,"insurance_coverage",C2)], accent=C2, mb="0"),
        ]),
    ]))


# ================================================================
# CB 5 — ANALYTICS: only booking store
# ================================================================
@app.callback(
    Output("content-analytics","children"),
    [Input("store-booking",   "data"),
     Input("filter-loyalty",  "value"),
     Input("filter-payment",  "value"),
     Input("filter-insurance","value"),
     Input("clear-filters",   "n_clicks")],
)
def render_analytics(bk, loy_f, pay_f, ins_f, clr):
    if not bk:
        return html.Div("[ loading data... ]",
                        style={"color":MUT,"fontFamily":MONO,"fontSize":"12px",
                               "padding":"60px","textAlign":"center"})
    df_raw = safe_df(bk)
    df     = _apply_filters(df_raw, loy_f, pay_f, ins_f)
    fbadge = _filter_badge(df, df_raw)

    if df.empty:
        return card([html.Div("[ no data — run pipeline first ]",
                              style={"color":MUT,"padding":"60px","textAlign":"center"})])

    # Daily dual axis
    if "booking_date" in df.columns:
        daily = df.groupby("booking_date").agg(
            bookings=("booking_id","nunique"), revenue=("payment_amount","sum")).reset_index()
        fig_d = go.Figure()
        fig_d.add_trace(go.Bar(x=daily["booking_date"], y=daily["bookings"],
                                name="bookings", marker_color=C1, opacity=0.8))
        fig_d.add_trace(go.Scatter(x=daily["booking_date"], y=daily["revenue"],
                                    name="revenue ₹", line=dict(color=C5,width=2),
                                    mode="lines+markers", marker=dict(size=4), yaxis="y2"))
        fig_d.update_layout(**CBASE, title="daily · bookings + revenue",
                             xaxis=AXIS,
                             yaxis=dict(title="bookings",color=C1,showgrid=False),
                             yaxis2=dict(title="revenue ₹",overlaying="y",side="right",
                                         color=C5,showgrid=False),
                             legend=dict(orientation="h",y=1.12,x=0,font=dict(size=10)),
                             height=300)
    else:
        fig_d = empty("daily trend"); fig_d.update_layout(height=300)

    # Model scatter
    if "model" in df.columns:
        cs = df.groupby("model").agg(bookings=("booking_id","count"),
                                      revenue=("payment_amount","sum"),
                                      avg_price=("price_per_day","mean")).reset_index()
        fig_sc = px.scatter(cs, x="bookings", y="revenue", size="avg_price",
                             text="model", title="model · bookings vs revenue",
                             color="avg_price",
                             color_continuous_scale=[[0,CARD],[0.5,C6],[1,C4]])
        fig_sc.update_traces(textposition="top center",
                              textfont=dict(color=SUB,size=9,family=MONO))
        fig_sc.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, coloraxis_showscale=False, height=280)
    else:
        fig_sc = empty("model scatter"); fig_sc.update_layout(height=280)

    # Locations
    if "pickup_location" in df.columns:
        pu = df["pickup_location"].value_counts().head(8).reset_index()
        pu.columns = ["loc","n"]
        dr = df["drop_location"].value_counts().head(8).reset_index()
        dr.columns = ["loc","n"]
        fig_loc = go.Figure()
        fig_loc.add_trace(go.Bar(y=pu["loc"],x=pu["n"],name="pickup",
                                  orientation="h",marker_color=C2))
        fig_loc.add_trace(go.Bar(y=dr["loc"],x=dr["n"],name="drop",
                                  orientation="h",marker_color=C4,opacity=0.8))
        fig_loc.update_layout(**CBASE, title="locations · pickup vs drop",
                               barmode="group",
                               xaxis=AXIS,
                               yaxis={"categoryorder":"total ascending"}, height=280)
    else:
        fig_loc = empty("locations"); fig_loc.update_layout(height=280)

    # Revenue stacked
    if "loyalty_tier" in df.columns and "payment_method" in df.columns:
        lp = df.groupby(["loyalty_tier","payment_method"])["payment_amount"].sum().reset_index()
        lp.columns = ["tier","method","revenue"]
        fig_lp = px.bar(lp, x="tier", y="revenue", color="method",
                         title="revenue · loyalty_tier × payment_method",
                         barmode="stack",
                         color_discrete_sequence=[C1,C2,C3,C5,C4])
        fig_lp.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, height=270)
    else:
        fig_lp = empty("revenue by tier"); fig_lp.update_layout(height=270)

    # Price dist
    if "price_per_day" in df.columns:
        fig_pr = px.histogram(df, x="price_per_day", nbins=25,
                               title="price_per_day · distribution",
                               color_discrete_sequence=[C6])
        fig_pr.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, height=250)
    else:
        fig_pr = empty("price distribution"); fig_pr.update_layout(height=250)

    return loading(html.Div([
        html.Div(style={"display":"flex","justifyContent":"space-between",
                        "alignItems":"center","marginBottom":"22px"},
                 children=[section("analytics","bookings · cars · revenue"), fbadge]),
        card([dcc.Graph(figure=fig_d,  config={"displayModeBar":False})], accent=C1),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr","gap":"12px"},
                 children=[
            card([dcc.Graph(figure=fig_sc,  config={"displayModeBar":False})], accent=C4),
            card([dcc.Graph(figure=fig_loc, config={"displayModeBar":False})], accent=C2),
        ]),
        html.Div(style={"display":"grid","gridTemplateColumns":"1.5fr 1fr","gap":"12px","marginTop":"12px"},
                 children=[
            card([dcc.Graph(figure=fig_lp, config={"displayModeBar":False})], accent=C5),
            card([dcc.Graph(figure=fig_pr, config={"displayModeBar":False})], accent=C6),
        ]),
    ]))


# ================================================================
# CB 6 — OBSERVABILITY: only booking store
# ================================================================
@app.callback(
    Output("content-observability","children"),
    [Input("store-booking",   "data"),
     Input("filter-loyalty",  "value"),
     Input("filter-payment",  "value"),
     Input("filter-insurance","value"),
     Input("clear-filters",   "n_clicks")],
)
def render_observability(bk, loy_f, pay_f, ins_f, clr):
    if not bk:
        return html.Div("[ loading data... ]",
                        style={"color":MUT,"fontFamily":MONO,"fontSize":"12px",
                               "padding":"60px","textAlign":"center"})
    df_raw = safe_df(bk)
    df     = _apply_filters(df_raw, loy_f, pay_f, ins_f)
    fbadge = _filter_badge(df, df_raw)

    if df.empty:
        return card([html.Div("[ no data ]",style={"color":MUT,"padding":"60px","textAlign":"center"})])

    # Volume anomaly
    if "booking_date" in df.columns:
        dc = df.copy()
        dc["booking_date"] = pd.to_datetime(dc["booking_date"])
        dly = dc.groupby("booking_date")["booking_id"].count().reset_index()
        dly.columns = ["date","count"]
        dly["avg7"]  = dly["count"].rolling(7,min_periods=1).mean()
        dly["std7"]  = dly["count"].rolling(7,min_periods=1).std().fillna(0)
        dly["upper"] = dly["avg7"]+2*dly["std7"]
        dly["lower"] = (dly["avg7"]-2*dly["std7"]).clip(lower=0)
        dly["anom"]  = (dly["count"]>dly["upper"])|(dly["count"]<dly["lower"])
        anom = dly[dly["anom"]]
        fig_vol = go.Figure()
        fig_vol.add_trace(go.Scatter(
            x=list(dly["date"])+list(dly["date"])[::-1],
            y=list(dly["upper"])+list(dly["lower"])[::-1],
            fill="toself", fillcolor="rgba(0,0,0,0)",
            line=dict(color="rgba(0,0,0,0)"), name="2σ band",
            opacity=0.1))
        fig_vol.add_trace(go.Bar(x=dly["date"],y=dly["count"],
                                  name="records",marker_color=C1,opacity=0.7))
        fig_vol.add_trace(go.Scatter(x=dly["date"],y=dly["avg7"],
                                      name="7d avg",line=dict(color=C5,width=2,dash="dot")))
        if not anom.empty:
            fig_vol.add_trace(go.Scatter(x=anom["date"],y=anom["count"],mode="markers",
                                          name=f"anomaly ({len(anom)})",
                                          marker=dict(color=C3,size=12,symbol="x-thin",
                                                      line=dict(width=2,color=C3))))
        fig_vol.update_layout(**CBASE,
                               title=f"volume monitor · {len(anom)} anomaly detected",
                               xaxis=AXIS, yaxis=AXIS,
                               legend=dict(orientation="h",y=1.12,x=0,font=dict(size=10)),
                               height=300)
    else:
        fig_vol = empty("volume monitor"); fig_vol.update_layout(height=300)
        anom    = pd.DataFrame()

    # Null health
    key_cols = [c for c in ["booking_id","customer_id","car_id","payment_amount",
                             "pickup_location","drop_location","loyalty_tier","payment_method"]
                if c in df.columns]
    null_d = [{"col":c,
               "pct":round(df[c].isnull().sum()/len(df)*100,2),
               "st":"critical" if df[c].isnull().sum()/len(df)*100>5
                    else ("warning" if df[c].isnull().sum()>0 else "healthy")}
              for c in key_cols]
    null_df2 = pd.DataFrame(null_d)
    SC = {"healthy":C2,"warning":C5,"critical":C3}
    fig_null = go.Figure(go.Bar(
        x=null_df2["col"], y=null_df2["pct"],
        marker_color=[SC.get(s,C5) for s in null_df2["st"]],
        text=[f"{v:.1f}%" for v in null_df2["pct"]],
        textposition="outside", textfont=dict(color=TX,size=9,family=MONO),
    ))
    fig_null.update_layout(**CBASE, title="null health · per column",
                            xaxis=AXIS, yaxis=AXIS,
                            yaxis_range=[0,max(null_df2["pct"].max()*1.8,5)], height=250)
    fig_null.add_hline(y=5,line_dash="dot",line_color=C3,
                       annotation_text="5% threshold",
                       annotation_font=dict(color=C3,size=9))

    # Duplicates
    dup_count, dup_rate = 0, 0.0
    if "booking_id" in df.columns:
        dup_count = len(df)-df["booking_id"].nunique()
        dup_rate  = dup_count/len(df)*100
        dd = [{"col":c,"dups":len(df)-df[c].nunique(),
               "rate":round((len(df)-df[c].nunique())/len(df)*100,2)}
              for c in ["booking_id","customer_id","car_id","payment_id"] if c in df.columns]
        fig_dup = px.bar(pd.DataFrame(dd), x="col", y="dups",
                          title=f"duplicates · {dup_rate:.2f}% overall rate",
                          color="rate",
                          color_continuous_scale=[[0,C2],[0.05,C5],[0.2,C3]],
                          text="rate")
        fig_dup.update_traces(texttemplate="%{text:.1f}%",textposition="outside",
                               textfont=dict(color=TX,size=9))
        fig_dup.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, coloraxis_showscale=False, height=250)
    else:
        fig_dup = empty("duplicates"); fig_dup.update_layout(height=250)

    # Freshness
    if "booking_date" in df.columns:
        df_f = df.copy()
        df_f["booking_date"] = pd.to_datetime(df_f["booking_date"])
        latest  = df_f["booking_date"].max()
        monthly = (df_f.groupby(df_f["booking_date"].dt.to_period("M").astype(str))
                   ["booking_id"].count().reset_index())
        monthly.columns = ["month","records"]
        fig_fr = px.bar(monthly, x="month", y="records",
                         title="freshness · records by month",
                         color="records",
                         color_continuous_scale=[[0,"#0a1520"],[1,C2]])
        fig_fr.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, coloraxis_showscale=False, height=240)
    else:
        fig_fr  = empty("freshness"); fig_fr.update_layout(height=240)
        latest  = "N/A"

    obs = [
        ("volume_anomalies", str(len(anom)),  C3 if len(anom)>0 else C2, "detected" if len(anom)>0 else "✓ normal"),
        ("duplicate_rate",   f"{dup_rate:.2f}%", C3 if dup_rate>5 else (C5 if dup_rate>0 else C2), f"{dup_count:,} records"),
        ("null_columns",     str(len(null_df2[null_df2["pct"]>0])), C3 if len(null_df2[null_df2["pct"]>0])>0 else C2, "with missing"),
        ("latest_record",    str(latest)[:10], C2, "most recent"),
    ]
    return loading(html.Div([
        html.Div(style={"display":"flex","justifyContent":"space-between",
                        "alignItems":"center","marginBottom":"22px"},
                 children=[section("observability","pipeline health · data quality signals"), fbadge]),
        html.Div(style={"display":"flex","gap":"10px","marginBottom":"14px","flexWrap":"wrap"},
                 children=[kpi(l,v,c,"📡",h) for l,v,c,h in obs]),
        card([dcc.Graph(figure=fig_vol,  config={"displayModeBar":False})], accent=C1),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr","gap":"12px","marginTop":"12px"},
                 children=[
            card([dcc.Graph(figure=fig_null, config={"displayModeBar":False})], accent=C5),
            card([dcc.Graph(figure=fig_dup,  config={"displayModeBar":False})], accent=C3),
        ]),
        html.Div(style={"marginTop":"12px"},
                 children=[card([dcc.Graph(figure=fig_fr, config={"displayModeBar":False})], accent=C2)]),
    ]))


# ================================================================
# CB 7 — PIPELINE: sla + alerts + schema stores
# ================================================================
@app.callback(
    Output("content-pipeline","children"),
    [Input("store-sla",    "data"),
     Input("store-alerts", "data"),
     Input("store-schema", "data")],
)
def render_pipeline(sla, alerts, schema):
    sla_df     = safe_df(sla)
    alerts_df  = safe_df(alerts)
    schema_df  = safe_df(schema)

    if sla_df.empty:
        return card([html.Div("[ no pipeline run data — run jenkins ]",
                              style={"color":MUT,"padding":"60px","textAlign":"center"})])

    runs   = sla_df["run_id"].nunique()
    avg_d  = sla_df["duration_seconds"].mean()
    recs   = sla_df["records_processed"].sum()
    s_rate = len(sla_df[sla_df["status"]=="SUCCESS"])/len(sla_df)*100

    sa = sla_df.groupby("stage_name")["duration_seconds"].agg(["mean","max"]).reset_index()
    sa.columns = ["stage","avg","max"]
    sa["stage"] = sa["stage"].str.replace("_"," ")
    fig_dur = go.Figure()
    fig_dur.add_trace(go.Bar(x=sa["stage"],y=sa["avg"],name="avg",marker_color=C1))
    fig_dur.add_trace(go.Bar(x=sa["stage"],y=sa["max"],name="max",marker_color=C3,opacity=0.5))
    fig_dur.update_layout(**CBASE, title="stage duration · avg vs max (s)",
                          barmode="group",
                          xaxis=AXIS, yaxis=AXIS,
                          legend=dict(orientation="h",y=1.12,x=0,font=dict(size=10)),
                          height=280)

    sc = sla_df["status"].value_counts().reset_index()
    sc.columns = ["status","count"]
    fig_st = px.pie(sc, names="status", values="count",
                     title="stage status",
                     color="status",
                     color_discrete_map={"SUCCESS":C2,"FAILED":C3,"SKIPPED":MUT},
                     hole=0.65)
    fig_st.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, height=280)

    sr = (sla_df[sla_df["records_processed"]>0]
          .groupby("stage_name")["records_processed"].max().reset_index())
    sr.columns = ["stage","records"]
    sr["stage"] = sr["stage"].str.replace("_"," ")
    fig_rec = px.bar(sr, x="stage", y="records",
                      title="records processed · per stage",
                      color="records",
                      color_continuous_scale=[[0,"#0a1520"],[1,C2]])
    fig_rec.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, coloraxis_showscale=False, height=250)

    latest_run = sla_df["run_id"].iloc[0]
    ltd = sla_df[sla_df["run_id"]==latest_run].copy()
    ltd["stage_name"] = ltd["stage_name"].str.replace("_"," ")
    show = [c for c in ["stage_name","records_processed","duration_seconds","status","started_at"]
            if c in ltd.columns]
    cmap = {"stage_name":"stage","records_processed":"records",
            "duration_seconds":"duration(s)","status":"status","started_at":"started"}

    # ── Alerts section ──────────────────────────────────────────
    def _alert_row(row):
        atype   = str(row["alert_type"])    if "alert_type"    in row.index else ""
        stage   = str(row["stage_name"])    if "stage_name"    in row.index else ""
        created = str(row["created_at"])    if "created_at"    in row.index else ""
        errmsg  = str(row["error_message"]) if "error_message" in row.index else ""
        color = C3 if atype=="FAILURE" else (C5 if atype=="DQ_WARNING" else C2)
        icon  = "🚨" if atype=="FAILURE" else ("⚠️" if atype=="DQ_WARNING" else "✅")
        return html.Div(style={
            "display":"flex","gap":"12px","alignItems":"flex-start",
            "padding":"10px 14px","borderBottom":f"1px solid {B}",
            "backgroundColor":BG,
        }, children=[
            html.Span(icon, style={"fontSize":"14px","marginTop":"1px"}),
            html.Div(style={"flex":"1"}, children=[
                html.Div(style={"display":"flex","justifyContent":"space-between",
                                "alignItems":"center","marginBottom":"3px"}, children=[
                    html.Span(stage,
                              style={"color":color,"fontSize":"10px",
                                     "fontFamily":MONO,"fontWeight":"700"}),
                    html.Span(created[:19],
                              style={"color":MUT,"fontSize":"9px","fontFamily":MONO}),
                ]),
                html.Div(errmsg[:120],
                         style={"color":TX,"fontSize":"10px","fontFamily":MONO}),
            ]),
            pill(atype, color),
        ])

    alerts_rows = []
    if not alerts_df.empty:
        for _, row in alerts_df.head(10).iterrows():
            alerts_rows.append(_alert_row(row))
    else:
        alerts_rows = [html.Div("[ no alerts — pipeline running clean ]",
                                style={"color":MUT,"fontSize":"11px","fontFamily":MONO,
                                       "padding":"20px","textAlign":"center"})]

    # ── Schema Registry section ──────────────────────────────────
    schema_rows = []
    if not schema_df.empty:
        for _, row in schema_df.iterrows():
            compatible  = bool(row["is_compatible"]) if "is_compatible" in row.index else True
            topic       = str(row["topic"])          if "topic"          in row.index else ""
            summary     = str(row["schema_summary"]) if "schema_summary" in row.index else ""
            version     = row["schema_version"]      if "schema_version" in row.index else "?"
            reg_at      = str(row["registered_at"])  if "registered_at"  in row.index else ""
            schema_rows.append(html.Div(style={
                "display":"flex","justifyContent":"space-between","alignItems":"center",
                "padding":"10px 14px","borderBottom":f"1px solid {B}","backgroundColor":BG,
            }, children=[
                html.Div(style={"display":"flex","gap":"12px","alignItems":"center"}, children=[
                    html.Span("🔀", style={"fontSize":"14px"}),
                    html.Div([
                        html.Div(topic,
                                 style={"color":C1,"fontSize":"10px","fontFamily":MONO,"fontWeight":"700"}),
                        html.Div(summary[:80],
                                 style={"color":SUB,"fontSize":"9px","fontFamily":MONO}),
                    ]),
                ]),
                html.Div(style={"display":"flex","gap":"8px","alignItems":"center"}, children=[
                    pill(f"v{version}", C4),
                    pill("compatible ✓" if compatible else "⚠ breaking", C2 if compatible else C3),
                    html.Span(reg_at[:10],
                              style={"color":MUT,"fontSize":"9px","fontFamily":MONO}),
                ]),
            ]))
    else:
        schema_rows = [html.Div("[ no schema registered — run schema registration stage ]",
                                style={"color":MUT,"fontSize":"11px","fontFamily":MONO,
                                       "padding":"20px","textAlign":"center"})]

    return loading(html.Div([
        section("pipeline health","jenkins · spark · stage metrics"),
        html.Div(style={"display":"flex","gap":"10px","marginBottom":"14px"}, children=[
            kpi("pipeline runs",  str(runs),        C1, "⚡"),
            kpi("avg duration",   f"{avg_d:.0f}s",  C2, "⏱️"),
            kpi("records total",  f"{recs:,}",      C5, "📦"),
            kpi("success rate",   f"{s_rate:.0f}%", C4, "✅"),
        ]),
        html.Div(style={"display":"grid","gridTemplateColumns":"2fr 1fr","gap":"12px"},
                 children=[
            card([dcc.Graph(figure=fig_dur, config={"displayModeBar":False})], accent=C1),
            card([dcc.Graph(figure=fig_st,  config={"displayModeBar":False})], accent=C4),
        ]),
        html.Div(style={"marginTop":"12px"},
                 children=[card([dcc.Graph(figure=fig_rec, config={"displayModeBar":False})], accent=C2)]),
        html.Div(style={"marginTop":"12px"}, children=[
            card(accent=C1, mb="0", children=[
                html.Div(style={"display":"flex","justifyContent":"space-between",
                                "alignItems":"center","marginBottom":"14px"}, children=[
                    html.Span("latest run · stage breakdown",
                              style={"color":C1,"fontFamily":MONO,"fontWeight":"700","fontSize":"11px"}),
                    pill(f"run: {latest_run}", C6),
                ]),
                dash_table.DataTable(
                    data=ltd[show].to_dict("records"),
                    columns=[{"name":cmap.get(c,c),"id":c} for c in show],
                    style_table={"overflowX":"auto"},
                    style_cell={"backgroundColor":BG,"color":TX,"border":f"1px solid {B}",
                                 "textAlign":"left","padding":"9px 14px",
                                 "fontSize":"10px","fontFamily":MONO},
                    style_header={"backgroundColor":SIDE,"color":C1,"fontWeight":"700",
                                   "fontSize":"9px","border":f"1px solid {B}",
                                   "textTransform":"uppercase","letterSpacing":"1.5px"},
                    style_data_conditional=[
                        {"if":{"row_index":"odd"},"backgroundColor":CARD},
                        {"if":{"filter_query":"{status} = SUCCESS"},"color":C2},
                        {"if":{"filter_query":"{status} = FAILED"}, "color":C3},
                    ],
                    page_size=10, sort_action="native",
                ),
            ]),
        ]),

        # ── Pipeline Alerts ─────────────────────────────────────
        html.Div(style={"marginTop":"12px"}, children=[
            card(accent=C3, mb="0", children=[
                html.Div(style={"display":"flex","justifyContent":"space-between",
                                "alignItems":"center","marginBottom":"14px"}, children=[
                    html.Span("pipeline alerts · last 10",
                              style={"color":C3,"fontFamily":MONO,"fontWeight":"700","fontSize":"11px"}),
                    pill(f"{len(alerts_df)} total", C5) if not alerts_df.empty else None,
                ]),
                html.Div(style={"borderRadius":"6px","overflow":"hidden",
                                "border":f"1px solid {B}"}, children=alerts_rows),
            ]),
        ]),

        # ── Schema Registry ──────────────────────────────────────
        html.Div(style={"marginTop":"12px"}, children=[
            card(accent=C4, mb="0", children=[
                html.Div(style={"display":"flex","justifyContent":"space-between",
                                "alignItems":"center","marginBottom":"14px"}, children=[
                    html.Span("schema registry · kafka topics",
                              style={"color":C4,"fontFamily":MONO,"fontWeight":"700","fontSize":"11px"}),
                    html.A("↗ localhost:8085", href="http://localhost:8085",
                           target="_blank",
                           style={"color":C6,"fontSize":"9px","fontFamily":MONO}),
                ]),
                html.Div(style={"borderRadius":"6px","overflow":"hidden",
                                "border":f"1px solid {B}"}, children=schema_rows),
            ]),
        ]),
    ]))


# ================================================================
# CB 8 — DATA QUALITY: only validation stores
# ================================================================
@app.callback(
    Output("content-quality","children"),
    [Input("store-validation",  "data"),
     Input("store-expectation", "data"),
     Input("store-failed",      "data")],
)
def render_quality(val, exp, fail):
    if not val:
        return html.Div("[ loading data... ]",
                        style={"color":MUT,"fontFamily":MONO,"fontSize":"12px",
                               "padding":"60px","textAlign":"center"})
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
        gauge={"axis":{"range":[0,100],"tickcolor":MUT,
                       "tickfont":{"color":MUT,"size":9,"family":MONO}},
               "bar":{"color":gc,"thickness":0.25},
               "bgcolor":CARD,"borderwidth":0,
               "steps":[{"range":[0,70],"color":"#0d1520"},
                        {"range":[70,90],"color":"#0d1a14"},
                        {"range":[90,100],"color":"#0a1e10"}]},
    ))
    fig_gauge.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, height=260)

    if not exp_df.empty:
        es = exp_df.groupby("expectation_type")["success"].agg(
            passed=lambda x: int(x.astype(bool).sum()),
            failed=lambda x: int((~x.astype(bool)).sum())
        ).reset_index()
        es["rule"] = (es["expectation_type"]
                      .str.replace("expect_column_values_to_be_","")
                      .str.replace("expect_column_","")
                      .str.replace("_"," "))
        em = es.melt(id_vars=["expectation_type","rule"],
                     value_vars=["passed","failed"],
                     var_name="status", value_name="count")
        fig_rules = px.bar(em, x="count", y="rule", color="status",
                            orientation="h", barmode="group",
                            title="validation rules · pass vs fail",
                            color_discrete_map={"passed":C2,"failed":C3})
        fig_rules.update_layout(**CBASE,
                                 xaxis=AXIS,
                                 yaxis={"categoryorder":"total ascending"},
                                 height=300)
    else:
        fig_rules = empty("validation rules"); fig_rules.update_layout(height=300)

    if "created_at" in val_df.columns:
        fig_hist = px.line(val_df, x="created_at", y="success_rate",
                            title="quality score · run history",
                            color_discrete_sequence=[C2], markers=True)
        fig_hist.add_hline(y=100,line_dash="dot",line_color=C3,
                           annotation_text="target: 100%",
                           annotation_font=dict(color=C3,size=9))
        fig_hist.update_traces(line_width=2,marker_size=7)
        fig_hist.update_layout(**CBASE, xaxis=AXIS, yaxis=AXIS, yaxis_range=[0,105], height=260)
    else:
        fig_hist = empty("quality history"); fig_hist.update_layout(height=260)

    vp = int(exp_df["success"].sum())              if not exp_df.empty  else 0
    vv = int((~exp_df["success"].astype(bool)).sum()) if not exp_df.empty else 0
    vf = len(fail_df)                               if not fail_df.empty else 0

    return loading(html.Div([
        section("data quality","great expectations · validation results"),
        html.Div(style={"display":"flex","gap":"10px","marginBottom":"14px"}, children=[
            kpi("rules passed",   str(vp),        C2, "✅"),
            kpi("rules violated", str(vv),        C3, "⚠️"),
            kpi("flagged records",f"{vf:,}",      C5, "🚨"),
            kpi("dq score",       f"{avg_rate:.1f}%", gc, "🛡️"),
        ]),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 2fr","gap":"12px"},
                 children=[
            card([dcc.Graph(figure=fig_gauge, config={"displayModeBar":False})], accent=gc),
            card([dcc.Graph(figure=fig_hist,  config={"displayModeBar":False})], accent=C2),
        ]),
        html.Div(style={"marginTop":"12px"},
                 children=[card([dcc.Graph(figure=fig_rules, config={"displayModeBar":False})], accent=C1)]),
    ]))


# ================================================================
# CB 9 — DEBUG: booking + failed stores
# ================================================================
@app.callback(
    Output("content-debug","children"),
    [Input("store-booking","data"),
     Input("store-failed", "data")],
)
def render_debug(bk, fail):
    if not bk:
        return html.Div("[ loading data... ]",
                        style={"color":MUT,"fontFamily":MONO,"fontSize":"12px",
                               "padding":"60px","textAlign":"center"})
    df      = safe_df(bk)
    fail_df = safe_df(fail)

    schema_items = []
    if not df.empty:
        for col in df.columns:
            n   = df[col].isnull().sum()
            pct = n/len(df)*100
            schema_items.append(html.Div(style={
                "backgroundColor":BG,"borderRadius":"6px",
                "padding":"10px","border":f"1px solid {B}",
            }, children=[
                html.Div(col, style={"color":C1,"fontSize":"10px","fontFamily":MONO,
                                      "fontWeight":"700","marginBottom":"5px","wordBreak":"break-all"}),
                html.Div(str(df[col].dtype), style={"color":C4,"fontSize":"9px","fontFamily":MONO,"marginBottom":"3px"}),
                html.Div(f"null: {n} ({pct:.1f}%)",
                         style={"color":C3 if pct>0 else MUT,"fontSize":"9px","fontFamily":MONO,"marginBottom":"2px"}),
                html.Div(f"unique: {df[col].nunique():,}",
                         style={"color":SUB,"fontSize":"9px","fontFamily":MONO}),
            ]))
    else:
        schema_items = [html.Div("[ no data ]",style={"color":MUT,"fontFamily":MONO})]

    null_bars = []
    if not df.empty:
        for col in df.columns:
            pct = df[col].isnull().sum()/len(df)*100
            null_bars.append(html.Div(style={"display":"flex","alignItems":"center",
                                              "gap":"10px","marginBottom":"8px"}, children=[
                html.Span(col, style={"color":TX,"fontSize":"10px","fontFamily":MONO,
                                       "width":"180px","overflow":"hidden",
                                       "textOverflow":"ellipsis","whiteSpace":"nowrap"}),
                html.Div(style={"flex":"1","backgroundColor":B,"borderRadius":"2px","height":"4px"},
                         children=[html.Div(style={
                             "backgroundColor": C3 if pct>5 else (C5 if pct>0 else C2),
                             "width":f"{min(pct,100):.1f}%","height":"100%","borderRadius":"2px",
                         })]),
                html.Span(f"{pct:.1f}%", style={
                    "color": C3 if pct>5 else (C5 if pct>0 else MUT),
                    "fontSize":"10px","fontFamily":MONO,"width":"36px","textAlign":"right",
                }),
            ]))

    fail_content = html.Div("[ no flagged records ]",
                             style={"color":MUT,"fontFamily":MONO,"fontSize":"11px"})
    if not fail_df.empty:
        show = [c for c in ["run_identifier","expectation_type","column_name",
                             "failed_reason","booking_id","customer_name",
                             "loyalty_tier","payment_method","payment_amount"]
                if c in fail_df.columns]
        cmap = {"run_identifier":"run","expectation_type":"rule","column_name":"col",
                "failed_reason":"reason","booking_id":"booking","customer_name":"customer",
                "loyalty_tier":"tier","payment_method":"payment","payment_amount":"amount"}
        fail_content = dash_table.DataTable(
            data=fail_df[show].head(300).to_dict("records"),
            columns=[{"name":cmap.get(c,c),"id":c} for c in show],
            style_table={"overflowX":"auto"},
            style_cell={"backgroundColor":BG,"color":TX,"border":f"1px solid {B}",
                         "textAlign":"left","padding":"8px 12px",
                         "fontSize":"10px","fontFamily":MONO,
                         "maxWidth":"160px","overflow":"hidden","textOverflow":"ellipsis"},
            style_header={"backgroundColor":SIDE,"color":C1,"fontWeight":"700","fontSize":"9px",
                           "border":f"1px solid {B}","textTransform":"uppercase","letterSpacing":"1.5px"},
            style_data_conditional=[{"if":{"row_index":"odd"},"backgroundColor":CARD}],
            page_size=15, filter_action="native", sort_action="native",
        )

    return loading(html.Div([
        section("debug console","schema · nulls · flagged records"),
        card(accent=C1, mb="12px", children=[
            html.Div("schema inspector · customer_booking_staging",
                     style={"color":C1,"fontFamily":MONO,"fontWeight":"700",
                            "fontSize":"11px","marginBottom":"14px","letterSpacing":"0.5px"}),
            html.Div(style={"display":"grid",
                            "gridTemplateColumns":"repeat(auto-fill,minmax(140px,1fr))",
                            "gap":"8px"}, children=schema_items),
        ]),
        card(accent=C5, mb="12px", children=[
            html.Div("null analysis · all columns",
                     style={"color":C5,"fontFamily":MONO,"fontWeight":"700",
                            "fontSize":"11px","marginBottom":"14px","letterSpacing":"0.5px"}),
            *null_bars,
        ]),
        card(accent=C3, mb="0", children=[
            html.Div(style={"display":"flex","justifyContent":"space-between",
                            "alignItems":"center","marginBottom":"14px"}, children=[
                html.Span("flagged records explorer",
                          style={"color":C3,"fontFamily":MONO,"fontWeight":"700","fontSize":"11px"}),
                pill(f"{len(fail_df):,} records", C5) if not fail_df.empty else None,
            ]),
            fail_content,
        ]),
    ]))


# ================================================================
# FILTER HELPERS
# ================================================================
def _apply_filters(df, loy_f, pay_f, ins_f):
    if df.empty: return df
    if loy_f and "loyalty_tier"       in df.columns: df = df[df["loyalty_tier"]       == loy_f]
    if pay_f and "payment_method"     in df.columns: df = df[df["payment_method"]     == pay_f]
    if ins_f and "insurance_coverage" in df.columns: df = df[df["insurance_coverage"] == ins_f]
    return df

def _filter_badge(df, df_raw):
    active = len(df) != len(df_raw)
    return html.Span(
        f"[ {len(df):,} / {len(df_raw):,} records ]",
        style={"color":C5 if active else MUT,"fontSize":"10px","fontFamily":MONO}
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)