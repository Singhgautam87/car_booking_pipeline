import os
import dash
from dash import dcc, html, Input, Output, State, dash_table, ctx
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# ============================================================
# DB CONNECTIONS — Only called from refresh_store()
# ============================================================
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
        print(f"[ERROR] PostgreSQL: {e}")
        return pd.DataFrame()

def get_monitoring_data():
    host = os.getenv("MYSQL_HOST", "mysql")
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://admin:admin@{host}:3306/booking",
            pool_pre_ping=True
        )
        val_df  = pd.read_sql("SELECT * FROM validation_results ORDER BY created_at DESC", engine)
        exp_df  = pd.read_sql("SELECT * FROM validation_expectation_details", engine)
        fail_df = pd.read_sql("SELECT * FROM validation_failed_records LIMIT 500", engine)
        sla_df  = pd.read_sql("SELECT * FROM pipeline_run_stats ORDER BY started_at DESC", engine)
        engine.dispose()
        return val_df, exp_df, fail_df, sla_df
    except Exception as e:
        print(f"[ERROR] MySQL: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# ============================================================
# THEME — Terminal/Data Engineer Aesthetic
# ============================================================
BG      = "#090d13"
CARD_BG = "#0f1923"
SIDEBAR = "#060a10"
BORDER  = "#1e2d3d"
BORDER2 = "#243447"
ACCENT1 = "#00d4ff"   # cyan — primary
ACCENT2 = "#00ff88"   # green — success
ACCENT3 = "#ff4757"   # red — error
ACCENT4 = "#a78bfa"   # purple — special
ACCENT5 = "#ffd43b"   # yellow — warning
ACCENT6 = "#74c0fc"   # light blue
TEXT    = "#cdd9e5"
SUBTEXT = "#6e7f8d"
MUTED   = "#3d5166"
GRID    = "rgba(30,45,61,0.4)"

FONT_MONO = "'JetBrains Mono', 'Fira Code', 'Courier New', monospace"
FONT_UI   = "'Inter', 'Segoe UI', sans-serif"

CHART_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor=CARD_BG,
    font=dict(color=TEXT, family=FONT_MONO, size=11),
    margin=dict(l=50, r=20, t=48, b=40),
    title_font=dict(color=ACCENT1, size=12, family=FONT_MONO),
    xaxis=dict(gridcolor=GRID, zerolinecolor=GRID, color=SUBTEXT),
    yaxis=dict(gridcolor=GRID, zerolinecolor=GRID, color=SUBTEXT),
)

# ============================================================
# HELPERS
# ============================================================
def safe_df(data):
    """Store data → DataFrame safely"""
    try:
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def card(children, accent=BORDER, pad="18px", mb="0px"):
    return html.Div(
        style={
            "backgroundColor": CARD_BG,
            "borderRadius": "8px",
            "border": f"1px solid {accent}",
            "borderLeft": f"3px solid {accent}",
            "padding": pad,
            "marginBottom": mb,
        },
        children=children
    )

def metric_card(label, value, color, icon, hint=""):
    return html.Div(
        style={
            "backgroundColor": CARD_BG,
            "borderRadius": "8px",
            "border": f"1px solid {BORDER}",
            "borderTop": f"2px solid {color}",
            "padding": "16px",
            "flex": "1",
            "minWidth": "130px",
        },
        children=[
            html.Div(
                style={"display": "flex", "justifyContent": "space-between",
                       "alignItems": "center", "marginBottom": "12px"},
                children=[
                    html.Span(icon, style={"fontSize": "18px"}),
                    html.Span(label, style={
                        "color": SUBTEXT, "fontSize": "9px",
                        "textTransform": "uppercase", "letterSpacing": "1.5px",
                        "textAlign": "right", "maxWidth": "90px",
                        "fontFamily": FONT_MONO,
                    }),
                ]
            ),
            html.Div(str(value), style={
                "color": color, "fontSize": "26px",
                "fontWeight": "700", "fontFamily": FONT_MONO,
                "lineHeight": "1", "marginBottom": "6px",
            }),
            html.Div(hint, style={
                "color": MUTED, "fontSize": "10px",
                "fontFamily": FONT_MONO,
            }) if hint else None,
        ]
    )

def empty_chart(msg="No data available"):
    fig = go.Figure()
    fig.add_annotation(
        text=f"[ {msg} ]",
        xref="paper", yref="paper", x=0.5, y=0.5,
        showarrow=False, font={"color": MUTED, "size": 12, "family": FONT_MONO}
    )
    fig.update_layout(**CHART_BASE, height=200)
    return fig

def nav_btn(label, btn_id, active=False):
    return html.Button(
        label, id=btn_id, n_clicks=0,
        style={
            "backgroundColor": f"rgba(0,212,255,0.12)" if active else "transparent",
            "color": ACCENT1 if active else SUBTEXT,
            "border": f"1px solid {ACCENT1}" if active else "1px solid transparent",
            "padding": "9px 14px",
            "borderRadius": "6px",
            "cursor": "pointer",
            "fontSize": "11px",
            "fontWeight": "600",
            "fontFamily": FONT_MONO,
            "width": "100%",
            "textAlign": "left",
            "marginBottom": "3px",
            "letterSpacing": "0.5px",
        }
    )

def section_header(title, subtitle=None, color=ACCENT1):
    return html.Div(style={"marginBottom": "20px"}, children=[
        html.Div(style={"display": "flex", "alignItems": "center", "gap": "10px",
                        "marginBottom": "4px"}, children=[
            html.Span("▸", style={"color": color, "fontSize": "14px"}),
            html.H2(title, style={
                "color": TEXT, "fontSize": "18px", "fontWeight": "700",
                "margin": "0", "fontFamily": FONT_MONO,
            }),
        ]),
        html.Div(subtitle, style={
            "color": SUBTEXT, "fontSize": "11px",
            "fontFamily": FONT_MONO, "paddingLeft": "24px",
        }) if subtitle else None,
    ])

def status_pill(text, color):
    return html.Span(text, style={
        "backgroundColor": f"rgba({','.join(str(int(color.lstrip('#')[i:i+2],16)) for i in (0,2,4))},0.15)",
        "color": color,
        "border": f"1px solid {color}",
        "borderRadius": "4px",
        "padding": "2px 10px",
        "fontSize": "10px",
        "fontFamily": FONT_MONO,
        "fontWeight": "600",
        "letterSpacing": "0.5px",
    })

# ============================================================
# APP
# ============================================================
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "Car Booking · Data Pipeline"

app.index_string = '''
<!DOCTYPE html>
<html>
<head>
    {%metas%}
    <title>{%title%}</title>
    {%favicon%}
    {%css%}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <style>
        * { box-sizing: border-box; }
        body { margin: 0; background: #090d13; }
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: #060a10; }
        ::-webkit-scrollbar-thumb { background: #1e2d3d; border-radius: 3px; }
        .Select-control { background: #0f1923 !important; border-color: #1e2d3d !important; }
        .Select-menu-outer { background: #0f1923 !important; border-color: #1e2d3d !important; }
        .Select-option { background: #0f1923 !important; color: #cdd9e5 !important; }
        .Select-option:hover { background: #1e2d3d !important; }
        .VirtualizedSelectOption { background: #0f1923 !important; }
    </style>
</head>
<body>
    {%app_entry%}
    <footer>
        {%config%}
        {%scripts%}
        {%renderer%}
    </footer>
</body>
</html>
'''

# ============================================================
# LAYOUT
# ============================================================
app.layout = html.Div(
    style={"backgroundColor": BG, "minHeight": "100vh",
           "display": "flex", "flexDirection": "column",
           "fontFamily": FONT_UI, "color": TEXT},
    children=[
        # ── Data Stores (single source of truth) ─────────────
        dcc.Store(id="store-booking",     storage_type="memory"),
        dcc.Store(id="store-validation",  storage_type="memory"),
        dcc.Store(id="store-expectation", storage_type="memory"),
        dcc.Store(id="store-failed",      storage_type="memory"),
        dcc.Store(id="store-sla",         storage_type="memory"),
        dcc.Store(id="store-meta",        storage_type="memory"),

        # ── Top Bar ──────────────────────────────────────────
        html.Div(
            style={"backgroundColor": SIDEBAR,
                   "borderBottom": f"1px solid {BORDER}",
                   "padding": "0 24px",
                   "display": "flex", "alignItems": "center",
                   "justifyContent": "space-between",
                   "height": "52px", "position": "sticky",
                   "top": "0", "zIndex": "200"},
            children=[
                # Left: Pipeline name
                html.Div(style={"display": "flex", "alignItems": "center", "gap": "16px"},
                         children=[
                     html.Div(style={"display": "flex", "alignItems": "center", "gap": "8px"},
                              children=[
                         html.Div("◈", style={"color": ACCENT1, "fontSize": "18px"}),
                         html.Span("car-booking-pipeline", style={
                             "color": TEXT, "fontWeight": "700",
                             "fontSize": "13px", "fontFamily": FONT_MONO,
                         }),
                     ]),
                     html.Div(style={"display": "flex", "gap": "6px"}, children=[
                         status_pill("kafka", ACCENT2),
                         html.Span("→", style={"color": MUTED, "fontSize": "12px"}),
                         status_pill("spark", ACCENT2),
                         html.Span("→", style={"color": MUTED, "fontSize": "12px"}),
                         status_pill("delta-lake", ACCENT2),
                         html.Span("→", style={"color": MUTED, "fontSize": "12px"}),
                         status_pill("postgres", ACCENT2),
                     ]),
                 ]),
                # Right: Status + Refresh
                html.Div(style={"display": "flex", "alignItems": "center", "gap": "16px"},
                         children=[
                     html.Div(id="store-status", style={
                         "color": SUBTEXT, "fontSize": "11px", "fontFamily": FONT_MONO,
                     }),
                     html.Div(id="last-updated", style={
                         "color": MUTED, "fontSize": "10px", "fontFamily": FONT_MONO,
                     }),
                     html.Button("↻ refresh", id="refresh-btn", n_clicks=0, style={
                         "backgroundColor": "transparent",
                         "color": ACCENT1,
                         "border": f"1px solid {BORDER2}",
                         "padding": "4px 14px",
                         "borderRadius": "4px",
                         "cursor": "pointer",
                         "fontSize": "11px",
                         "fontFamily": FONT_MONO,
                         "letterSpacing": "0.5px",
                     }),
                 ]),
                dcc.Interval(id="auto-refresh", interval=30000, n_intervals=0),
            ]
        ),

        # ── Body ─────────────────────────────────────────────
        html.Div(style={"display": "flex", "flex": "1", "overflow": "hidden"},
                 children=[

            # ── Sidebar ──────────────────────────────────────
            html.Div(
                style={"width": "192px", "minWidth": "192px",
                       "backgroundColor": SIDEBAR,
                       "borderRight": f"1px solid {BORDER}",
                       "padding": "20px 10px",
                       "display": "flex", "flexDirection": "column",
                       "position": "sticky", "top": "52px",
                       "height": "calc(100vh - 52px)", "overflowY": "auto"},
                children=[
                    # Nav
                    html.Div("VIEWS", style={
                        "color": MUTED, "fontSize": "9px", "fontWeight": "700",
                        "letterSpacing": "2px", "marginBottom": "10px", "paddingLeft": "4px",
                        "fontFamily": FONT_MONO,
                    }),
                    nav_btn("📊  overview",      "tab-overview",  True),
                    nav_btn("📈  analytics",     "tab-analytics", False),
                    nav_btn("🔭  observability", "tab-ml",        False),
                    nav_btn("⚡  pipeline",      "tab-pipeline",  False),
                    nav_btn("🛡️  data quality",  "tab-quality",   False),
                    nav_btn("🐛  debug",          "tab-debug",     False),

                    # Divider
                    html.Div(style={"borderTop": f"1px solid {BORDER}",
                                    "margin": "20px 0 16px 0"}),

                    # Filters
                    html.Div("FILTERS", style={
                        "color": MUTED, "fontSize": "9px", "fontWeight": "700",
                        "letterSpacing": "2px", "marginBottom": "12px", "paddingLeft": "4px",
                        "fontFamily": FONT_MONO,
                    }),
                    html.Div("loyalty_tier", style={
                        "color": ACCENT6, "fontSize": "9px", "marginBottom": "4px",
                        "fontFamily": FONT_MONO, "paddingLeft": "2px",
                    }),
                    dcc.Dropdown(id="filter-loyalty", options=[], placeholder="all",
                                 clearable=True,
                                 style={"fontSize": "11px", "marginBottom": "10px",
                                        "fontFamily": FONT_MONO}),
                    html.Div("payment_method", style={
                        "color": ACCENT6, "fontSize": "9px", "marginBottom": "4px",
                        "fontFamily": FONT_MONO, "paddingLeft": "2px",
                    }),
                    dcc.Dropdown(id="filter-payment", options=[], placeholder="all",
                                 clearable=True,
                                 style={"fontSize": "11px", "marginBottom": "10px",
                                        "fontFamily": FONT_MONO}),
                    html.Div("insurance_coverage", style={
                        "color": ACCENT6, "fontSize": "9px", "marginBottom": "4px",
                        "fontFamily": FONT_MONO, "paddingLeft": "2px",
                    }),
                    dcc.Dropdown(id="filter-insurance", options=[], placeholder="all",
                                 clearable=True,
                                 style={"fontSize": "11px", "marginBottom": "10px",
                                        "fontFamily": FONT_MONO}),
                    html.Button("✕ clear", id="clear-filters", n_clicks=0, style={
                        "backgroundColor": "transparent",
                        "color": ACCENT3,
                        "border": f"1px solid {BORDER}",
                        "padding": "5px 8px", "borderRadius": "4px",
                        "cursor": "pointer", "fontSize": "10px",
                        "fontFamily": FONT_MONO, "width": "100%",
                    }),

                    # Meta
                    html.Div(style={"borderTop": f"1px solid {BORDER}",
                                    "margin": "20px 0 12px 0"}),
                    html.Div(id="sidebar-meta", style={
                        "color": MUTED, "fontSize": "9px",
                        "fontFamily": FONT_MONO, "lineHeight": "1.8",
                    }),
                ]
            ),

            # ── Content ──────────────────────────────────────
            html.Div(
                style={"flex": "1", "overflowY": "auto", "padding": "28px",
                       "height": "calc(100vh - 52px)"},
                children=[html.Div(id="tab-content")]
            ),
        ]),
    ]
)


# ============================================================
# CALLBACK 1: DATA STORE REFRESH
# Yeh sirf ek jagah DB se baat karta hai
# Baaki sab callbacks sirf Store se padhte hain
# ============================================================
@app.callback(
    [Output("store-booking",     "data"),
     Output("store-validation",  "data"),
     Output("store-expectation", "data"),
     Output("store-failed",      "data"),
     Output("store-sla",         "data"),
     Output("store-meta",        "data"),
     Output("last-updated",      "children"),
     Output("store-status",      "children")],
    [Input("auto-refresh", "n_intervals"),
     Input("refresh-btn",  "n_clicks")],
    prevent_initial_call=False,
)
def refresh_store(n, clicks):
    """Single DB fetch — results Store mein save hote hain"""
    now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        df = get_booking_data()
        val_df, exp_df, fail_df, sla_df = get_monitoring_data()

        meta = {
            "booking_rows":    len(df),
            "booking_cols":    len(df.columns) if not df.empty else 0,
            "val_runs":        len(val_df),
            "sla_stages":      len(sla_df),
            "refreshed_at":    now,
            "pg_status":       "connected" if not df.empty else "empty",
            "mysql_status":    "connected" if not val_df.empty else "empty",
        }

        status = (f"✦ {len(df):,} records  ·  "
                  f"{'✓ pg' if not df.empty else '✗ pg'}  "
                  f"{'✓ mysql' if not val_df.empty else '✗ mysql'}")

        return (
            df.to_dict("records")      if not df.empty      else [],
            val_df.to_dict("records")  if not val_df.empty  else [],
            exp_df.to_dict("records")  if not exp_df.empty  else [],
            fail_df.to_dict("records") if not fail_df.empty else [],
            sla_df.to_dict("records")  if not sla_df.empty  else [],
            meta,
            f"↻ {now}",
            status,
        )
    except Exception as e:
        return [], [], [], [], [], {}, f"↻ {now}", f"✗ error: {str(e)[:40]}"


# ============================================================
# CALLBACK 2: FILTER OPTIONS (Store se — DB nahi)
# ============================================================
@app.callback(
    [Output("filter-loyalty",   "options"),
     Output("filter-payment",   "options"),
     Output("filter-insurance", "options"),
     Output("sidebar-meta",     "children")],
    [Input("store-booking", "data"),
     Input("store-meta",    "data")],
)
def update_filters_and_meta(booking_data, meta):
    df = safe_df(booking_data)

    # Dynamic options from actual data
    def make_opts(series):
        return [{"label": v, "value": v}
                for v in sorted(series.dropna().unique())] if not series.empty else []

    loy_opts = make_opts(df["loyalty_tier"])       if "loyalty_tier"       in df.columns else []
    pay_opts = make_opts(df["payment_method"])     if "payment_method"     in df.columns else []
    ins_opts = make_opts(df["insurance_coverage"]) if "insurance_coverage" in df.columns else []

    # Sidebar meta
    m = meta or {}
    meta_lines = [
        f"records: {m.get('booking_rows', 0):,}",
        f"cols: {m.get('booking_cols', 0)}",
        f"val_runs: {m.get('val_runs', 0)}",
        f"pg: {m.get('pg_status','—')}",
        f"mysql: {m.get('mysql_status','—')}",
    ]
    meta_el = html.Div([html.Div(l) for l in meta_lines])

    return loy_opts, pay_opts, ins_opts, meta_el


# ============================================================
# CALLBACK 3: MAIN RENDER
# Saare Stores as Input — tab switch pe sab data available
# ============================================================
@app.callback(
    [Output("tab-overview",  "style"),
     Output("tab-analytics", "style"),
     Output("tab-ml",        "style"),
     Output("tab-pipeline",  "style"),
     Output("tab-quality",   "style"),
     Output("tab-debug",     "style"),
     Output("tab-content",   "children")],
    [Input("tab-overview",    "n_clicks"),
     Input("tab-analytics",   "n_clicks"),
     Input("tab-ml",          "n_clicks"),
     Input("tab-pipeline",    "n_clicks"),
     Input("tab-quality",     "n_clicks"),
     Input("tab-debug",       "n_clicks"),
     Input("store-booking",   "data"),
     Input("store-validation","data"),
     Input("store-expectation","data"),
     Input("store-failed",    "data"),
     Input("store-sla",       "data"),
     Input("filter-loyalty",  "value"),
     Input("filter-payment",  "value"),
     Input("filter-insurance","value"),
     Input("clear-filters",   "n_clicks")],
)
def render_tab(ov, an, ml, pi, qu, de,
               booking_data, val_data, exp_data, fail_data, sla_data,
               loy_f, pay_f, ins_f, clear_n):

    triggered = ctx.triggered_id or "tab-overview"
    TAB_IDS   = ["tab-overview","tab-analytics","tab-ml",
                 "tab-pipeline","tab-quality","tab-debug"]
    active    = triggered if triggered in TAB_IDS else "tab-overview"

    if triggered == "clear-filters":
        loy_f = pay_f = ins_f = None

    # Nav styles
    styles = [nav_btn("", t, t==active)["props"]["style"] for t in TAB_IDS]

    # Load all data from stores — ONE place
    df_raw  = safe_df(booking_data)
    val_df  = safe_df(val_data)
    exp_df  = safe_df(exp_data)
    fail_df = safe_df(fail_data)
    sla_df  = safe_df(sla_data)

    # Apply filters
    df = df_raw.copy()
    if loy_f and "loyalty_tier"       in df.columns: df = df[df["loyalty_tier"]       == loy_f]
    if pay_f and "payment_method"     in df.columns: df = df[df["payment_method"]     == pay_f]
    if ins_f and "insurance_coverage" in df.columns: df = df[df["insurance_coverage"] == ins_f]

    filtered = loy_f or pay_f or ins_f
    fbadge = html.Span(
        f"[ {len(df):,} / {len(df_raw):,} records ]",
        style={"color": ACCENT5 if filtered else MUTED,
               "fontSize": "10px", "fontFamily": FONT_MONO}
    )

    # Render active tab — sab data available hai
    if   active == "tab-overview":  content = tab_overview(df, df_raw, val_df, sla_df, fbadge)
    elif active == "tab-analytics": content = tab_analytics(df, fbadge)
    elif active == "tab-ml":        content = tab_observability(df, fbadge)
    elif active == "tab-pipeline":  content = tab_pipeline(sla_df)
    elif active == "tab-quality":   content = tab_quality(val_df, exp_df, fail_df)
    elif active == "tab-debug":     content = tab_debug(df, fail_df, exp_df)
    else:                           content = tab_overview(df, df_raw, val_df, sla_df, fbadge)

    return (*styles, content)


# ============================================================
# TAB 1 — OVERVIEW
# ============================================================
def tab_overview(df, df_raw, val_df, sla_df, fbadge):
    # Metrics
    total_b   = df["booking_id"].nunique()      if not df.empty and "booking_id"      in df.columns else 0
    total_c   = df["customer_id"].nunique()     if not df.empty and "customer_id"     in df.columns else 0
    total_rev = df["payment_amount"].sum()      if not df.empty and "payment_amount"  in df.columns else 0
    total_cars= df["car_id"].nunique()          if not df.empty and "car_id"          in df.columns else 0
    avg_rev   = df["payment_amount"].mean()     if not df.empty and "payment_amount"  in df.columns else 0
    dq_score  = val_df["success_rate"].mean()   if not val_df.empty and "success_rate" in val_df.columns else 0
    pipeline_ok = (not sla_df.empty and
                   len(sla_df[sla_df["status"]=="FAILED"]) == 0) if not sla_df.empty else False

    # Revenue sparkline
    if not df.empty and "booking_date" in df.columns:
        rd = df.groupby("booking_date")["payment_amount"].sum().reset_index()
        fig_spark = go.Figure()
        fig_spark.add_trace(go.Scatter(
            x=rd["booking_date"], y=rd["payment_amount"],
            mode="lines", fill="tozeroy",
            line=dict(color=ACCENT2, width=2),
            fillcolor="rgba(0,255,136,0.07)"
        ))
        fig_spark.update_layout(
            **{**CHART_BASE, "margin": dict(l=0,r=0,t=28,b=0)},
            title="revenue · daily trend",
            xaxis={"visible": False, "gridcolor": "transparent"},
            yaxis={"visible": False, "gridcolor": "transparent"},
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=120,
        )
    else:
        fig_spark = empty_chart("revenue trend — no data")
        fig_spark.update_layout(height=120)

    # Bookings by car model
    if not df.empty and "model" in df.columns:
        tc = df.groupby("model")["payment_amount"].sum().nlargest(7).reset_index()
        fig_top = go.Figure(go.Bar(
            x=tc["payment_amount"], y=tc["model"],
            orientation="h",
            marker=dict(
                color=tc["payment_amount"],
                colorscale=[[0,"#0f2027"],[0.5,ACCENT1],[1,ACCENT4]],
                showscale=False,
            ),
            text=[f"₹{v:,.0f}" for v in tc["payment_amount"]],
            textposition="outside",
            textfont=dict(color=SUBTEXT, size=9, family=FONT_MONO),
        ))
        fig_top.update_layout(
            **CHART_BASE, title="top · car models by revenue",
            yaxis={"categoryorder":"total ascending"},
            height=280,
        )
    else:
        fig_top = empty_chart("car models — no data")
        fig_top.update_layout(height=280)

    # Heatmap
    if not df.empty and "booking_date" in df.columns:
        df2 = df.copy()
        df2["booking_date"] = pd.to_datetime(df2["booking_date"])
        df2["dow"]   = df2["booking_date"].dt.day_name()
        df2["month"] = df2["booking_date"].dt.strftime("%b")
        heat = df2.groupby(["month","dow"])["booking_id"].count().reset_index()
        heat.columns = ["Month","Day","Count"]
        day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        heat["Day"] = pd.Categorical(heat["Day"], categories=day_order, ordered=True)
        heat = heat.sort_values("Day")
        fig_heat = px.density_heatmap(
            heat, x="Month", y="Day", z="Count",
            title="booking heatmap · month × day",
            color_continuous_scale=[[0,BG],[0.4,ACCENT1],[1,ACCENT4]],
        )
        fig_heat.update_layout(**CHART_BASE, coloraxis_showscale=False, height=260)
    else:
        fig_heat = empty_chart("heatmap — no data")
        fig_heat.update_layout(height=260)

    # Split tables
    def split_table(df, col, color):
        if df.empty or col not in df.columns:
            return [html.Div("no data", style={"color":MUTED,"fontSize":"11px"})]
        grp = (df.groupby(col)["booking_id"].count().reset_index()
               .assign(pct=lambda x: x["booking_id"]/x["booking_id"].sum()*100)
               .sort_values("pct", ascending=False))
        rows = []
        for _, r in grp.iterrows():
            bar_w = r["pct"]
            rows.append(html.Div(style={"marginBottom":"10px"}, children=[
                html.Div(style={"display":"flex","justifyContent":"space-between","marginBottom":"3px"},
                         children=[
                     html.Span(str(r[col]), style={"color":TEXT,"fontSize":"11px","fontFamily":FONT_MONO}),
                     html.Span(f"{r['pct']:.1f}%", style={"color":color,"fontSize":"11px",
                                                            "fontFamily":FONT_MONO,"fontWeight":"700"}),
                 ]),
                html.Div(style={"backgroundColor":BORDER,"borderRadius":"2px","height":"3px"}, children=[
                    html.Div(style={"backgroundColor":color,"width":f"{bar_w:.1f}%",
                                    "height":"100%","borderRadius":"2px",
                                    "opacity":"0.8"}),
                ]),
            ]))
        return rows

    return html.Div([
        # Header
        html.Div(style={"display":"flex","justifyContent":"space-between",
                        "alignItems":"flex-start","marginBottom":"24px"},
                 children=[
            html.Div([
                section_header("overview", "pipeline · real-time analytics"),
                fbadge,
            ]),
            html.Div(style={"display":"flex","gap":"8px"}, children=[
                status_pill("● healthy" if pipeline_ok else "● issues",
                            ACCENT2 if pipeline_ok else ACCENT3),
                status_pill(f"dq: {dq_score:.1f}%",
                            ACCENT2 if dq_score>=90 else (ACCENT5 if dq_score>=70 else ACCENT3)),
            ]),
        ]),

        # KPI Row
        html.Div(style={"display":"flex","gap":"10px","marginBottom":"16px","flexWrap":"wrap"},
                 children=[
                     metric_card("total bookings",  f"{total_b:,}",        ACCENT1, "📋"),
                     metric_card("customers",        f"{total_c:,}",        ACCENT2, "👤"),
                     metric_card("revenue",          f"₹{total_rev:,.0f}",  ACCENT3, "💰"),
                     metric_card("unique cars",      f"{total_cars:,}",     ACCENT5, "🚘"),
                     metric_card("avg booking",      f"₹{avg_rev:,.0f}",    ACCENT6, "📊"),
                     metric_card("data quality",     f"{dq_score:.1f}%",    ACCENT4, "🛡️"),
                 ]),

        # Sparkline full width
        card([dcc.Graph(figure=fig_spark, config={"displayModeBar":False})],
             accent=ACCENT2, mb="12px"),

        # Top cars + Heatmap
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1.5fr",
                        "gap":"12px","marginBottom":"12px"},
                 children=[
                     card([dcc.Graph(figure=fig_top,  config={"displayModeBar":False})], accent=ACCENT5),
                     card([dcc.Graph(figure=fig_heat, config={"displayModeBar":False})], accent=ACCENT4),
                 ]),

        # Split cards
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr 1fr","gap":"12px"},
                 children=[
                     card([
                         html.Div("payment_method", style={"color":ACCENT6,"fontSize":"9px",
                                                            "fontFamily":FONT_MONO,"marginBottom":"14px",
                                                            "letterSpacing":"1px","textTransform":"uppercase"}),
                         *split_table(df, "payment_method", ACCENT1),
                     ], accent=ACCENT1),
                     card([
                         html.Div("loyalty_tier", style={"color":ACCENT6,"fontSize":"9px",
                                                          "fontFamily":FONT_MONO,"marginBottom":"14px",
                                                          "letterSpacing":"1px","textTransform":"uppercase"}),
                         *split_table(df, "loyalty_tier", ACCENT5),
                     ], accent=ACCENT5),
                     card([
                         html.Div("insurance_coverage", style={"color":ACCENT6,"fontSize":"9px",
                                                                "fontFamily":FONT_MONO,"marginBottom":"14px",
                                                                "letterSpacing":"1px","textTransform":"uppercase"}),
                         *split_table(df, "insurance_coverage", ACCENT2),
                     ], accent=ACCENT2),
                 ]),
    ])


# ============================================================
# TAB 2 — ANALYTICS
# ============================================================
def tab_analytics(df, fbadge):
    if df.empty:
        return card([html.Div("[ no data — run pipeline first ]",
                              style={"color":MUTED,"padding":"60px","textAlign":"center",
                                     "fontFamily":FONT_MONO})])

    # Daily dual axis
    if "booking_date" in df.columns:
        daily = df.groupby("booking_date").agg(
            bookings=("booking_id","nunique"),
            revenue=("payment_amount","sum")
        ).reset_index()
        fig_daily = go.Figure()
        fig_daily.add_trace(go.Bar(
            x=daily["booking_date"], y=daily["bookings"],
            name="bookings", marker_color=ACCENT1, opacity=0.8, yaxis="y"
        ))
        fig_daily.add_trace(go.Scatter(
            x=daily["booking_date"], y=daily["revenue"],
            name="revenue", line=dict(color=ACCENT5,width=2),
            mode="lines+markers", marker=dict(size=4), yaxis="y2"
        ))
        fig_daily.update_layout(
            **CHART_BASE, title="daily · bookings + revenue",
            yaxis=dict(title="bookings", color=ACCENT1, showgrid=False),
            yaxis2=dict(title="revenue ₹", overlaying="y", side="right",
                        color=ACCENT5, showgrid=False),
            legend=dict(orientation="h", y=1.1, x=0, font=dict(size=10)),
            height=300,
        )
    else:
        fig_daily = empty_chart("daily trend — no data")
        fig_daily.update_layout(height=300)

    # Scatter
    if "model" in df.columns:
        cs = df.groupby("model").agg(
            bookings=("booking_id","count"),
            revenue=("payment_amount","sum"),
            avg_price=("price_per_day","mean")
        ).reset_index()
        fig_sc = px.scatter(
            cs, x="bookings", y="revenue",
            size="avg_price", text="model",
            title="model · bookings vs revenue",
            color="avg_price",
            color_continuous_scale=[[0,CARD_BG],[0.5,ACCENT6],[1,ACCENT4]],
        )
        fig_sc.update_traces(textposition="top center",
                              textfont=dict(color=SUBTEXT,size=9,family=FONT_MONO))
        fig_sc.update_layout(**CHART_BASE, coloraxis_showscale=False, height=280)
    else:
        fig_sc = empty_chart("model scatter — no data")
        fig_sc.update_layout(height=280)

    # Locations
    if "pickup_location" in df.columns:
        pu = df["pickup_location"].value_counts().head(8).reset_index()
        pu.columns = ["loc","count"]
        dr = df["drop_location"].value_counts().head(8).reset_index()
        dr.columns = ["loc","count"]
        fig_loc = go.Figure()
        fig_loc.add_trace(go.Bar(y=pu["loc"], x=pu["count"], name="pickup",
                                  orientation="h", marker_color=ACCENT2))
        fig_loc.add_trace(go.Bar(y=dr["loc"], x=dr["count"], name="drop",
                                  orientation="h", marker_color=ACCENT4, opacity=0.8))
        fig_loc.update_layout(**CHART_BASE, title="locations · pickup vs drop",
                               barmode="group", yaxis={"categoryorder":"total ascending"},
                               height=280)
    else:
        fig_loc = empty_chart("locations — no data")
        fig_loc.update_layout(height=280)

    # Revenue by tier + payment stacked
    if "loyalty_tier" in df.columns and "payment_method" in df.columns:
        lp = df.groupby(["loyalty_tier","payment_method"])["payment_amount"].sum().reset_index()
        lp.columns = ["tier","method","revenue"]
        fig_lp = px.bar(lp, x="tier", y="revenue", color="method",
                         title="revenue · tier × payment method",
                         barmode="stack",
                         color_discrete_sequence=[ACCENT1,ACCENT2,ACCENT3,ACCENT5,ACCENT4])
        fig_lp.update_layout(**CHART_BASE, height=280)
    else:
        fig_lp = empty_chart("tier revenue — no data")
        fig_lp.update_layout(height=280)

    # Price histogram
    if "price_per_day" in df.columns:
        fig_price = px.histogram(df, x="price_per_day",
                                  title="price_per_day · distribution",
                                  nbins=25, color_discrete_sequence=[ACCENT6])
        fig_price.update_traces(marker_line_width=0)
        fig_price.update_layout(**CHART_BASE, height=250)
    else:
        fig_price = empty_chart("price distribution — no data")
        fig_price.update_layout(height=250)

    return html.Div([
        html.Div(style={"marginBottom":"24px","display":"flex",
                        "justifyContent":"space-between","alignItems":"center"},
                 children=[section_header("analytics","booking · cars · revenue"), fbadge]),
        card([dcc.Graph(figure=fig_daily, config={"displayModeBar":False})],
             accent=ACCENT1, mb="12px"),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr",
                        "gap":"12px","marginBottom":"12px"},
                 children=[
                     card([dcc.Graph(figure=fig_sc,  config={"displayModeBar":False})], accent=ACCENT4),
                     card([dcc.Graph(figure=fig_loc, config={"displayModeBar":False})], accent=ACCENT2),
                 ]),
        html.Div(style={"display":"grid","gridTemplateColumns":"1.5fr 1fr",
                        "gap":"12px"},
                 children=[
                     card([dcc.Graph(figure=fig_lp,    config={"displayModeBar":False})], accent=ACCENT5),
                     card([dcc.Graph(figure=fig_price, config={"displayModeBar":False})], accent=ACCENT6),
                 ]),
    ])


# ============================================================
# TAB 3 — OBSERVABILITY
# ============================================================
def tab_observability(df, fbadge):
    if df.empty:
        return card([html.Div("[ no data — run pipeline first ]",
                              style={"color":MUTED,"padding":"60px","textAlign":"center",
                                     "fontFamily":FONT_MONO})])

    # Volume anomaly detection
    if "booking_date" in df.columns:
        dc = df.copy()
        dc["booking_date"] = pd.to_datetime(dc["booking_date"])
        daily = dc.groupby("booking_date")["booking_id"].count().reset_index()
        daily.columns = ["date","count"]
        daily["avg7"] = daily["count"].rolling(7, min_periods=1).mean()
        daily["std7"] = daily["count"].rolling(7, min_periods=1).std().fillna(0)
        daily["upper"] = daily["avg7"] + 2*daily["std7"]
        daily["lower"] = (daily["avg7"] - 2*daily["std7"]).clip(lower=0)
        daily["anomaly"] = (daily["count"] > daily["upper"]) | (daily["count"] < daily["lower"])
        anom = daily[daily["anomaly"]]

        fig_vol = go.Figure()
        fig_vol.add_trace(go.Scatter(
            x=list(daily["date"])+list(daily["date"])[::-1],
            y=list(daily["upper"])+list(daily["lower"])[::-1],
            fill="toself", fillcolor="rgba(0,212,255,0.05)",
            line=dict(color="rgba(0,0,0,0)"), name="2σ band",
        ))
        fig_vol.add_trace(go.Bar(
            x=daily["date"], y=daily["count"],
            name="daily records", marker_color=ACCENT1, opacity=0.7,
        ))
        fig_vol.add_trace(go.Scatter(
            x=daily["date"], y=daily["avg7"],
            name="7d avg", line=dict(color=ACCENT5,width=2,dash="dot"),
        ))
        if not anom.empty:
            fig_vol.add_trace(go.Scatter(
                x=anom["date"], y=anom["count"],
                mode="markers", name=f"anomaly ({len(anom)})",
                marker=dict(color=ACCENT3,size=12,symbol="x-thin",line=dict(width=2,color=ACCENT3)),
            ))
        fig_vol.update_layout(
            **CHART_BASE,
            title=f"volume monitor · {len(anom)} anomaly detected",
            legend=dict(orientation="h",y=1.1,x=0,font=dict(size=10)),
            height=300,
        )
    else:
        fig_vol = empty_chart("volume monitor — no data")
        fig_vol.update_layout(height=300)
        anom = pd.DataFrame()

    # Null health
    key_cols = [c for c in ["booking_id","customer_id","car_id","payment_amount",
                             "pickup_location","drop_location","loyalty_tier","payment_method"]
                if c in df.columns]
    null_data = [{
        "col": c,
        "null_pct": round(df[c].isnull().sum()/len(df)*100, 2),
        "status": ("critical" if df[c].isnull().sum()/len(df)*100 > 5
                   else ("warning" if df[c].isnull().sum() > 0 else "healthy"))
    } for c in key_cols]
    null_df = pd.DataFrame(null_data)

    STATUS_COLOR = {"healthy": ACCENT2, "warning": ACCENT5, "critical": ACCENT3}
    fig_null = go.Figure(go.Bar(
        x=null_df["col"],
        y=null_df["null_pct"],
        marker_color=[STATUS_COLOR.get(s, ACCENT5) for s in null_df["status"]],
        text=[f"{v:.1f}%" for v in null_df["null_pct"]],
        textposition="outside",
        textfont=dict(color=TEXT, size=9, family=FONT_MONO),
    ))
    fig_null.update_layout(
        **CHART_BASE, title="null health · per column",
        yaxis_range=[0, max(null_df["null_pct"].max()*1.8, 5)],
        height=260,
    )
    fig_null.add_hline(y=5, line_dash="dot", line_color=ACCENT3,
                       annotation_text="5% threshold",
                       annotation_font=dict(color=ACCENT3, size=9))

    # Duplicates
    if "booking_id" in df.columns:
        dup_count = len(df) - df["booking_id"].nunique()
        dup_rate  = dup_count/len(df)*100
        dup_data  = [{"col":c, "dups":len(df)-df[c].nunique(),
                      "rate":round((len(df)-df[c].nunique())/len(df)*100,2)}
                     for c in ["booking_id","customer_id","car_id","payment_id"]
                     if c in df.columns]
        dup_df_chart = pd.DataFrame(dup_data)
        fig_dup = px.bar(dup_df_chart, x="col", y="dups",
                          title=f"duplicate rate · {dup_rate:.2f}% overall",
                          color="rate",
                          color_continuous_scale=[[0,ACCENT2],[0.05,ACCENT5],[0.2,ACCENT3]],
                          text="rate")
        fig_dup.update_traces(texttemplate="%{text:.1f}%", textposition="outside",
                               textfont=dict(color=TEXT,size=9))
        fig_dup.update_layout(**CHART_BASE, coloraxis_showscale=False, height=260)
    else:
        fig_dup   = empty_chart("duplicates — no data")
        fig_dup.update_layout(height=260)
        dup_count = 0
        dup_rate  = 0

    # Freshness
    if "booking_date" in df.columns:
        df_f = df.copy()
        df_f["booking_date"] = pd.to_datetime(df_f["booking_date"])
        latest = df_f["booking_date"].max()
        monthly = (df_f.groupby(df_f["booking_date"].dt.to_period("M").astype(str))
                   ["booking_id"].count().reset_index())
        monthly.columns = ["month","records"]
        fig_fresh = px.bar(monthly, x="month", y="records",
                            title="freshness · records by month",
                            color="records",
                            color_continuous_scale=[[0,"#0a1520"],[1,ACCENT2]])
        fig_fresh.update_layout(**CHART_BASE, coloraxis_showscale=False, height=250)
    else:
        fig_fresh = empty_chart("freshness — no data")
        fig_fresh.update_layout(height=250)
        latest    = "N/A"

    # Obs KPI cards
    obs_metrics = [
        ("volume_anomalies", str(len(anom)),
         ACCENT3 if len(anom)>0 else ACCENT2,
         "↑ detected" if len(anom)>0 else "✓ normal"),
        ("duplicate_rate", f"{dup_rate:.2f}%",
         ACCENT3 if dup_rate>5 else (ACCENT5 if dup_rate>0 else ACCENT2),
         f"{dup_count:,} records"),
        ("null_columns", str(len(null_df[null_df["null_pct"]>0])),
         ACCENT3 if len(null_df[null_df["null_pct"]>0])>0 else ACCENT2,
         "with missing values"),
        ("latest_record", str(latest)[:10],
         ACCENT2, "most recent date"),
    ]

    return html.Div([
        html.Div(style={"marginBottom":"24px","display":"flex",
                        "justifyContent":"space-between","alignItems":"center"},
                 children=[
                     section_header("observability","pipeline health · data quality signals"),
                     fbadge,
                 ]),
        html.Div(style={"display":"flex","gap":"10px","marginBottom":"16px","flexWrap":"wrap"},
                 children=[metric_card(l,v,c,i) for l,v,c,i in obs_metrics]),
        card([dcc.Graph(figure=fig_vol, config={"displayModeBar":False})],
             accent=ACCENT1, mb="12px"),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr",
                        "gap":"12px","marginBottom":"12px"},
                 children=[
                     card([dcc.Graph(figure=fig_null,  config={"displayModeBar":False})], accent=ACCENT5),
                     card([dcc.Graph(figure=fig_dup,   config={"displayModeBar":False})], accent=ACCENT3),
                 ]),
        card([dcc.Graph(figure=fig_fresh, config={"displayModeBar":False})], accent=ACCENT2),
    ])


# ============================================================
# TAB 4 — PIPELINE
# ============================================================
def tab_pipeline(sla_df):
    if sla_df.empty:
        return card([html.Div("[ no pipeline run data — run jenkins first ]",
                              style={"color":MUTED,"padding":"60px","textAlign":"center",
                                     "fontFamily":FONT_MONO})])

    runs   = sla_df["run_id"].nunique()
    avg_d  = sla_df["duration_seconds"].mean()
    recs   = sla_df["records_processed"].sum()
    s_rate = len(sla_df[sla_df["status"]=="SUCCESS"])/len(sla_df)*100

    # Stage duration
    sa = sla_df.groupby("stage_name")["duration_seconds"].agg(["mean","max"]).reset_index()
    sa.columns = ["stage","avg","max"]
    sa["stage"] = sa["stage"].str.replace("_"," ")
    fig_dur = go.Figure()
    fig_dur.add_trace(go.Bar(x=sa["stage"], y=sa["avg"], name="avg",
                              marker_color=ACCENT1))
    fig_dur.add_trace(go.Bar(x=sa["stage"], y=sa["max"], name="max",
                              marker_color=ACCENT3, opacity=0.5))
    fig_dur.update_layout(**CHART_BASE, title="stage duration · avg vs max (seconds)",
                          barmode="group", legend=dict(orientation="h",y=1.1,x=0),
                          height=280)

    # Status donut
    sc = sla_df["status"].value_counts().reset_index()
    sc.columns = ["status","count"]
    fig_st = px.pie(sc, names="status", values="count",
                     title="stage status distribution",
                     color="status",
                     color_discrete_map={"SUCCESS":ACCENT2,"FAILED":ACCENT3,"SKIPPED":MUTED},
                     hole=0.65)
    fig_st.update_traces(textfont=dict(family=FONT_MONO,size=10))
    fig_st.update_layout(**CHART_BASE, height=280)

    # Records per stage
    sr = (sla_df[sla_df["records_processed"]>0]
          .groupby("stage_name")["records_processed"].max()
          .reset_index())
    sr.columns = ["stage","records"]
    sr["stage"] = sr["stage"].str.replace("_"," ")
    fig_rec = px.bar(sr, x="stage", y="records",
                      title="records processed · per stage",
                      color="records",
                      color_continuous_scale=[[0,"#0a1520"],[1,ACCENT2]])
    fig_rec.update_layout(**CHART_BASE, coloraxis_showscale=False, height=260)

    # Latest run table
    latest_run = sla_df["run_id"].iloc[0]
    latest_df  = sla_df[sla_df["run_id"]==latest_run].copy()
    latest_df["stage_name"] = latest_df["stage_name"].str.replace("_"," ")
    show_cols  = [c for c in ["stage_name","records_processed","duration_seconds","status","started_at"]
                  if c in latest_df.columns]
    col_rename = {"stage_name":"stage","records_processed":"records",
                  "duration_seconds":"duration(s)","status":"status","started_at":"started"}

    return html.Div([
        section_header("pipeline health", "jenkins · spark · stage metrics"),
        html.Div(style={"display":"flex","gap":"10px","marginBottom":"16px"}, children=[
            metric_card("pipeline runs",  str(runs),         ACCENT1, "⚡"),
            metric_card("avg duration",   f"{avg_d:.0f}s",   ACCENT2, "⏱️"),
            metric_card("records total",  f"{recs:,}",       ACCENT5, "📦"),
            metric_card("success rate",   f"{s_rate:.0f}%",  ACCENT4, "✅"),
        ]),
        html.Div(style={"display":"grid","gridTemplateColumns":"2fr 1fr",
                        "gap":"12px","marginBottom":"12px"},
                 children=[
                     card([dcc.Graph(figure=fig_dur, config={"displayModeBar":False})], accent=ACCENT1),
                     card([dcc.Graph(figure=fig_st,  config={"displayModeBar":False})], accent=ACCENT4),
                 ]),
        card([dcc.Graph(figure=fig_rec, config={"displayModeBar":False})],
             accent=ACCENT2, mb="12px"),
        card(accent=ACCENT1, children=[
            html.Div(style={"display":"flex","justifyContent":"space-between",
                            "alignItems":"center","marginBottom":"14px"},
                     children=[
                html.Span("latest run · stage breakdown",
                          style={"color":ACCENT1,"fontFamily":FONT_MONO,
                                 "fontWeight":"700","fontSize":"12px"}),
                html.Span(f"run: {latest_run}",
                          style={"color":SUBTEXT,"fontSize":"10px","fontFamily":FONT_MONO,
                                 "border":f"1px solid {BORDER}","borderRadius":"4px",
                                 "padding":"2px 8px"}),
            ]),
            dash_table.DataTable(
                data=latest_df[show_cols].to_dict("records"),
                columns=[{"name":col_rename.get(c,c),"id":c} for c in show_cols],
                style_table={"overflowX":"auto"},
                style_cell={"backgroundColor":BG,"color":TEXT,
                             "border":f"1px solid {BORDER}",
                             "textAlign":"left","padding":"10px 14px",
                             "fontSize":"11px","fontFamily":FONT_MONO},
                style_header={"backgroundColor":SIDEBAR,"color":ACCENT1,
                               "fontWeight":"700","fontSize":"9px",
                               "border":f"1px solid {BORDER}",
                               "textTransform":"uppercase","letterSpacing":"1.5px"},
                style_data_conditional=[
                    {"if":{"row_index":"odd"},"backgroundColor":CARD_BG},
                    {"if":{"filter_query":"{status} = SUCCESS"},"color":ACCENT2},
                    {"if":{"filter_query":"{status} = FAILED"}, "color":ACCENT3},
                ],
                page_size=10, sort_action="native",
            ),
        ]),
    ])


# ============================================================
# TAB 5 — DATA QUALITY
# ============================================================
def tab_quality(val_df, exp_df, fail_df):
    if val_df.empty:
        return card([html.Div("[ no validation data — run great_expectations ]",
                              style={"color":MUTED,"padding":"60px","textAlign":"center",
                                     "fontFamily":FONT_MONO})])

    avg_rate    = float(val_df["success_rate"].mean())
    gauge_color = ACCENT2 if avg_rate>=90 else (ACCENT5 if avg_rate>=70 else ACCENT3)

    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number", value=avg_rate,
        title={"text":"data integrity score","font":{"color":SUBTEXT,"size":11,"family":FONT_MONO}},
        number={"suffix":"%","font":{"color":gauge_color,"size":42,"family":FONT_MONO}},
        gauge={
            "axis":{"range":[0,100],"tickcolor":MUTED,
                    "tickfont":{"color":MUTED,"size":9,"family":FONT_MONO}},
            "bar":{"color":gauge_color,"thickness":0.25},
            "bgcolor":CARD_BG,"borderwidth":0,
            "steps":[{"range":[0,70],"color":"#0d1520"},
                     {"range":[70,90],"color":"#0d1a14"},
                     {"range":[90,100],"color":"#0a1e10"}],
        }
    ))
    fig_gauge.update_layout(**CHART_BASE, height=260)

    # Rules
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
                            color_discrete_map={"passed":ACCENT2,"failed":ACCENT3})
        fig_rules.update_layout(**CHART_BASE,
                                 yaxis={"categoryorder":"total ascending"},
                                 height=300)
    else:
        fig_rules = empty_chart("validation rules — no data")
        fig_rules.update_layout(height=300)

    # Quality history
    if "created_at" in val_df.columns:
        fig_hist = px.line(val_df, x="created_at", y="success_rate",
                            title="quality score · history",
                            color_discrete_sequence=[ACCENT2], markers=True)
        fig_hist.add_hline(y=100, line_dash="dot", line_color=ACCENT3,
                           annotation_text="target: 100%",
                           annotation_font=dict(color=ACCENT3,size=9))
        fig_hist.update_traces(line_width=2, marker_size=7)
        fig_hist.update_layout(**CHART_BASE, yaxis_range=[0,105], height=260)
    else:
        fig_hist = empty_chart("quality history — no data")
        fig_hist.update_layout(height=260)

    val_passed   = int(exp_df["success"].sum())              if not exp_df.empty  else 0
    val_violated = int((~exp_df["success"].astype(bool)).sum()) if not exp_df.empty else 0
    val_flagged  = len(fail_df)                               if not fail_df.empty else 0

    return html.Div([
        section_header("data quality", "great expectations · validation results"),
        html.Div(style={"display":"flex","gap":"10px","marginBottom":"16px"}, children=[
            metric_card("rules passed",   str(val_passed),   ACCENT2, "✅"),
            metric_card("rules violated", str(val_violated), ACCENT3, "⚠️"),
            metric_card("flagged records",f"{val_flagged:,}", ACCENT5, "🚨"),
            metric_card("dq score",       f"{avg_rate:.1f}%",gauge_color,"🛡️"),
        ]),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 2fr",
                        "gap":"12px","marginBottom":"12px"},
                 children=[
                     card([dcc.Graph(figure=fig_gauge, config={"displayModeBar":False})], accent=gauge_color),
                     card([dcc.Graph(figure=fig_hist,  config={"displayModeBar":False})], accent=ACCENT2),
                 ]),
        card([dcc.Graph(figure=fig_rules, config={"displayModeBar":False})], accent=ACCENT1),
    ])


# ============================================================
# TAB 6 — DEBUG
# ============================================================
def tab_debug(df, fail_df, exp_df):
    # Schema grid
    schema_items = []
    if not df.empty:
        for col in df.columns:
            null_n   = df[col].isnull().sum()
            null_pct = null_n/len(df)*100
            uniq     = df[col].nunique()
            schema_items.append(html.Div(
                style={"backgroundColor":BG,"borderRadius":"6px",
                       "padding":"10px","border":f"1px solid {BORDER}"},
                children=[
                    html.Div(col, style={"color":ACCENT1,"fontSize":"10px",
                                          "fontFamily":FONT_MONO,"fontWeight":"700",
                                          "marginBottom":"6px","wordBreak":"break-all"}),
                    html.Div(str(df[col].dtype),
                             style={"color":ACCENT4,"fontSize":"9px","fontFamily":FONT_MONO,
                                    "marginBottom":"3px"}),
                    html.Div(f"null: {null_n} ({null_pct:.1f}%)",
                             style={"color":ACCENT3 if null_pct>0 else MUTED,
                                    "fontSize":"9px","fontFamily":FONT_MONO,
                                    "marginBottom":"3px"}),
                    html.Div(f"unique: {uniq:,}",
                             style={"color":SUBTEXT,"fontSize":"9px","fontFamily":FONT_MONO}),
                ]
            ))
    else:
        schema_items = [html.Div("[ no data ]", style={"color":MUTED,"fontFamily":FONT_MONO})]

    # Null bars
    null_bars = []
    if not df.empty:
        for col in df.columns:
            pct = df[col].isnull().sum()/len(df)*100
            null_bars.append(html.Div(
                style={"display":"flex","alignItems":"center","gap":"10px","marginBottom":"8px"},
                children=[
                    html.Span(col, style={"color":TEXT,"fontSize":"10px",
                                           "fontFamily":FONT_MONO,"width":"180px",
                                           "overflow":"hidden","textOverflow":"ellipsis",
                                           "whiteSpace":"nowrap"}),
                    html.Div(style={"flex":"1","backgroundColor":BORDER,
                                     "borderRadius":"2px","height":"4px"},
                             children=[html.Div(style={
                                 "backgroundColor": ACCENT3 if pct>5 else (ACCENT5 if pct>0 else ACCENT2),
                                 "width": f"{min(pct,100):.1f}%",
                                 "height":"100%","borderRadius":"2px",
                             })]),
                    html.Span(f"{pct:.1f}%", style={
                        "color": ACCENT3 if pct>5 else (ACCENT5 if pct>0 else MUTED),
                        "fontSize":"10px","fontFamily":FONT_MONO,"width":"38px","textAlign":"right",
                    }),
                ]
            ))

    # Flagged records table
    fail_table = html.Div("[ no flagged records ]",
                           style={"color":MUTED,"fontFamily":FONT_MONO,"fontSize":"11px"})
    if not fail_df.empty:
        show = [c for c in ["run_identifier","expectation_type","column_name",
                             "failed_reason","booking_id","customer_name",
                             "loyalty_tier","payment_method","payment_amount"]
                if c in fail_df.columns]
        col_map = {"run_identifier":"run","expectation_type":"rule",
                   "column_name":"col","failed_reason":"reason",
                   "booking_id":"booking","customer_name":"customer",
                   "loyalty_tier":"tier","payment_method":"payment","payment_amount":"amount"}
        fail_table = dash_table.DataTable(
            data=fail_df[show].head(300).to_dict("records"),
            columns=[{"name":col_map.get(c,c),"id":c} for c in show],
            style_table={"overflowX":"auto"},
            style_cell={"backgroundColor":BG,"color":TEXT,
                         "border":f"1px solid {BORDER}",
                         "textAlign":"left","padding":"8px 12px",
                         "fontSize":"10px","fontFamily":FONT_MONO,
                         "maxWidth":"160px","overflow":"hidden","textOverflow":"ellipsis"},
            style_header={"backgroundColor":SIDEBAR,"color":ACCENT1,
                           "fontWeight":"700","fontSize":"9px",
                           "border":f"1px solid {BORDER}",
                           "textTransform":"uppercase","letterSpacing":"1.5px"},
            style_data_conditional=[
                {"if":{"row_index":"odd"},"backgroundColor":CARD_BG},
            ],
            page_size=15, filter_action="native", sort_action="native",
        )

    return html.Div([
        section_header("debug console", "schema · nulls · flagged records"),
        card(accent=ACCENT1, mb="12px", children=[
            html.Div("schema inspector · customer_booking_staging",
                     style={"color":ACCENT1,"fontFamily":FONT_MONO,"fontWeight":"700",
                            "fontSize":"11px","marginBottom":"14px","letterSpacing":"0.5px"}),
            html.Div(style={"display":"grid","gridTemplateColumns":"repeat(auto-fill, minmax(140px,1fr))",
                            "gap":"8px"},
                     children=schema_items),
        ]),
        card(accent=ACCENT5, mb="12px", children=[
            html.Div("null analysis · all columns",
                     style={"color":ACCENT5,"fontFamily":FONT_MONO,"fontWeight":"700",
                            "fontSize":"11px","marginBottom":"14px","letterSpacing":"0.5px"}),
            *null_bars,
        ]),
        card(accent=ACCENT3, children=[
            html.Div(style={"display":"flex","justifyContent":"space-between",
                            "alignItems":"center","marginBottom":"14px"},
                     children=[
                html.Span("flagged records explorer",
                          style={"color":ACCENT3,"fontFamily":FONT_MONO,
                                 "fontWeight":"700","fontSize":"11px"}),
                html.Span(f"{len(fail_df):,} total",
                          style={"color":SUBTEXT,"fontSize":"10px","fontFamily":FONT_MONO,
                                 "border":f"1px solid {BORDER}","borderRadius":"4px",
                                 "padding":"2px 8px"}) if not fail_df.empty else None,
            ]),
            fail_table,
        ]),
    ])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)