import os
import dash
from sqlalchemy import create_engine
from dash import dcc, html, Input, Output, dash_table, ctx
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

# ============================================================
# DATA CONNECTIONS
# ============================================================
def get_booking_data():
    host = os.getenv("POSTGRES_HOST", "postgres")
    try:
        engine = create_engine(f"postgresql+psycopg2://admin:admin@{host}:5432/booking")
        df = pd.read_sql("SELECT * FROM customer_booking_staging", engine)
        engine.dispose()
        return df
    except Exception as e:
        print(f"PostgreSQL Error: {e}")
        return pd.DataFrame()

def get_monitoring_data():
    host = os.getenv("MYSQL_HOST", "mysql")
    try:
        engine = create_engine(f"mysql+mysqlconnector://admin:admin@{host}:3306/booking")
        validation_df  = pd.read_sql("SELECT * FROM validation_results", engine)
        expectation_df = pd.read_sql("SELECT * FROM validation_expectation_details", engine)
        failed_df      = pd.read_sql("SELECT * FROM validation_failed_records", engine)
        sla_df         = pd.read_sql("SELECT * FROM pipeline_run_stats ORDER BY started_at DESC", engine)
        engine.dispose()
        return validation_df, expectation_df, failed_df, sla_df
    except Exception as e:
        print(f"MySQL Error: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# ============================================================
# THEME
# ============================================================
BG       = "#0d1117"
CARD_BG  = "#161b22"
SIDEBAR  = "#010409"
BORDER   = "#30363d"
ACCENT1  = "#58a6ff"   # blue
ACCENT2  = "#3fb950"   # green
ACCENT3  = "#f78166"   # red
ACCENT4  = "#d2a8ff"   # purple
ACCENT5  = "#ffa657"   # orange
ACCENT6  = "#79c0ff"   # light blue
TEXT     = "#e6edf3"
SUBTEXT  = "#8b949e"
MUTED    = "#484f58"

CHART_BASE = dict(
    paper_bgcolor=CARD_BG,
    plot_bgcolor=CARD_BG,
    font=dict(color=TEXT, family="'Inter', 'Segoe UI', sans-serif", size=12),
    margin=dict(l=40, r=20, t=45, b=40),
    title_font=dict(color=TEXT, size=13, family="'Courier New', monospace"),
)

# ============================================================
# HELPERS
# ============================================================
def card(children, border_color=BORDER, style_extra=None):
    s = {"backgroundColor": CARD_BG, "borderRadius": "10px",
         "border": f"1px solid {border_color}", "padding": "18px"}
    if style_extra:
        s.update(style_extra)
    return html.Div(style=s, children=children)

def kpi(kpi_id, label, color, icon, subtitle_id=None):
    return html.Div(
        style={"backgroundColor": CARD_BG, "borderRadius": "10px",
               "border": f"1px solid {BORDER}", "borderTop": f"3px solid {color}",
               "padding": "18px 16px", "flex": "1", "minWidth": "130px"},
        children=[
            html.Div(style={"display": "flex", "justifyContent": "space-between",
                            "alignItems": "flex-start", "marginBottom": "10px"},
                     children=[
                         html.Span(icon, style={"fontSize": "22px"}),
                         html.Span(label, style={"color": SUBTEXT, "fontSize": "10px",
                                                  "textTransform": "uppercase",
                                                  "letterSpacing": "1px", "textAlign": "right",
                                                  "maxWidth": "80px"}),
                     ]),
            html.Div(id=kpi_id, style={"color": color, "fontSize": "28px",
                                        "fontWeight": "800",
                                        "fontFamily": "'Courier New', monospace",
                                        "lineHeight": "1"}),
            html.Div(id=subtitle_id, style={"color": MUTED, "fontSize": "11px",
                                             "marginTop": "4px"}) if subtitle_id else None,
        ]
    )

def sec(title, subtitle=None, right=None):
    return html.Div(
        style={"marginBottom": "16px", "display": "flex",
               "justifyContent": "space-between", "alignItems": "flex-end"},
        children=[
            html.Div([
                html.Div(title, style={"color": TEXT, "fontSize": "16px",
                                       "fontWeight": "700",
                                       "fontFamily": "'Courier New', monospace"}),
                html.Div(subtitle, style={"color": SUBTEXT, "fontSize": "11px",
                                          "marginTop": "3px"}) if subtitle else None,
            ]),
            right or html.Div()
        ]
    )

def empty_fig(msg="No data"):
    fig = go.Figure()
    fig.add_annotation(text=f"📭 {msg}", xref="paper", yref="paper",
                       x=0.5, y=0.5, showarrow=False,
                       font={"color": MUTED, "size": 13})
    fig.update_layout(**CHART_BASE)
    return fig

def tab_style(active=False):
    return {
        "backgroundColor": ACCENT1 if active else "transparent",
        "color": "#0d1117" if active else SUBTEXT,
        "border": "none",
        "padding": "10px 16px",
        "borderRadius": "8px",
        "cursor": "pointer",
        "fontSize": "12px",
        "fontWeight": "700",
        "fontFamily": "'Courier New', monospace",
        "width": "100%",
        "textAlign": "left",
        "marginBottom": "4px",
        "display": "flex",
        "alignItems": "center",
        "gap": "8px",
        "transition": "all 0.2s",
    }

# ============================================================
# APP
# ============================================================
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "🚗 Car Booking Analytics"

# ============================================================
# LAYOUT
# ============================================================
app.layout = html.Div(
    style={"backgroundColor": BG, "minHeight": "100vh", "display": "flex",
           "flexDirection": "column", "fontFamily": "'Inter', 'Segoe UI', sans-serif",
           "color": TEXT},
    children=[

        # ── TOP NAV ──────────────────────────────────────────
        html.Div(
            style={"backgroundColor": SIDEBAR, "borderBottom": f"1px solid {BORDER}",
                   "padding": "0 24px", "display": "flex", "alignItems": "center",
                   "justifyContent": "space-between", "height": "56px",
                   "position": "sticky", "top": "0", "zIndex": "200"},
            children=[
                html.Div(style={"display": "flex", "alignItems": "center", "gap": "10px"},
                         children=[
                             html.Span("🚗", style={"fontSize": "20px"}),
                             html.Span("Car Booking Analytics", style={
                                 "color": TEXT, "fontWeight": "700", "fontSize": "14px",
                                 "fontFamily": "'Courier New', monospace"
                             }),
                             html.Span("|", style={"color": BORDER, "margin": "0 8px"}),
                             html.Span("Kafka → Spark → Delta Lake → PostgreSQL", style={
                                 "color": MUTED, "fontSize": "11px",
                                 "fontFamily": "'Courier New', monospace"
                             }),
                         ]),
                html.Div(style={"display": "flex", "alignItems": "center", "gap": "12px"},
                         children=[
                             html.Span(id="last-updated", style={
                                 "color": MUTED, "fontSize": "11px",
                                 "fontFamily": "'Courier New', monospace"
                             }),
                             html.Button("⟳ Refresh", id="refresh-btn", style={
                                 "backgroundColor": "transparent",
                                 "color": ACCENT1, "border": f"1px solid {ACCENT1}",
                                 "padding": "5px 14px", "borderRadius": "6px",
                                 "cursor": "pointer", "fontWeight": "700", "fontSize": "12px",
                             }),
                         ]),
                dcc.Interval(id="auto-refresh", interval=30000, n_intervals=0),
            ]
        ),

        # ── MAIN BODY (Sidebar + Content) ─────────────────────
        html.Div(style={"display": "flex", "flex": "1", "overflow": "hidden"}, children=[

            # ── LEFT SIDEBAR ─────────────────────────────────
            html.Div(
                style={"width": "200px", "minWidth": "200px", "backgroundColor": SIDEBAR,
                       "borderRight": f"1px solid {BORDER}", "padding": "20px 12px",
                       "display": "flex", "flexDirection": "column", "gap": "2px",
                       "position": "sticky", "top": "56px", "height": "calc(100vh - 56px)",
                       "overflowY": "auto"},
                children=[
                    html.Div("NAVIGATION", style={
                        "color": MUTED, "fontSize": "10px", "fontWeight": "700",
                        "letterSpacing": "2px", "marginBottom": "12px",
                        "fontFamily": "'Courier New', monospace", "paddingLeft": "8px"
                    }),
                    html.Button("📊  Overview",       id="tab-overview",   n_clicks=0, style=tab_style(True)),
                    html.Button("📈  Analytics",      id="tab-analytics",  n_clicks=0, style=tab_style()),
                    html.Button("🔭  Observability",  id="tab-ml",         n_clicks=0, style=tab_style()),
                    html.Button("⚡  Pipeline Health",id="tab-pipeline",   n_clicks=0, style=tab_style()),
                    html.Button("🔍  Data Quality",   id="tab-quality",    n_clicks=0, style=tab_style()),
                    html.Button("🐛  Debug",          id="tab-debug",      n_clicks=0, style=tab_style()),

                    # Global Filters
                    html.Div("FILTERS", style={
                        "color": MUTED, "fontSize": "10px", "fontWeight": "700",
                        "letterSpacing": "2px", "marginTop": "24px", "marginBottom": "12px",
                        "fontFamily": "'Courier New', monospace", "paddingLeft": "8px"
                    }),
                    html.Div("Loyalty Tier", style={"color": SUBTEXT, "fontSize": "10px",
                                                     "marginBottom": "4px", "paddingLeft": "4px"}),
                    dcc.Dropdown(
                        id="filter-loyalty",
                        options=[
                            {"label": "⭐ Platinum", "value": "platinum"},
                            {"label": "🥇 Gold",     "value": "gold"},
                            {"label": "🥈 Silver",   "value": "silver"},
                            {"label": "🥉 Bronze",   "value": "bronze"},
                        ],
                        placeholder="All tiers",
                        clearable=True,
                        style={"fontSize": "11px", "color": "black", "marginBottom": "10px"}
                    ),
                    html.Div("Payment Method", style={"color": SUBTEXT, "fontSize": "10px",
                                                       "marginBottom": "4px", "paddingLeft": "4px"}),
                    dcc.Dropdown(
                        id="filter-payment",
                        placeholder="All methods",
                        clearable=True,
                        style={"fontSize": "11px", "color": "black", "marginBottom": "10px"}
                    ),
                    html.Div("Insurance", style={"color": SUBTEXT, "fontSize": "10px",
                                                  "marginBottom": "4px", "paddingLeft": "4px"}),
                    dcc.Dropdown(
                        id="filter-insurance",
                        placeholder="All coverage",
                        clearable=True,
                        style={"fontSize": "11px", "color": "black", "marginBottom": "10px"}
                    ),
                    html.Button("✕ Clear Filters", id="clear-filters", n_clicks=0, style={
                        "backgroundColor": "transparent", "color": ACCENT3,
                        "border": f"1px solid {ACCENT3}", "padding": "6px 8px",
                        "borderRadius": "6px", "cursor": "pointer",
                        "fontSize": "11px", "width": "100%", "marginTop": "4px"
                    }),
                ]
            ),

            # ── RIGHT CONTENT ─────────────────────────────────
            html.Div(
                style={"flex": "1", "overflowY": "auto", "padding": "24px",
                       "height": "calc(100vh - 56px)"},
                children=[html.Div(id="tab-content")]
            ),
        ]),
    ]
)


# ============================================================
# FILTER OPTIONS CALLBACK
# ============================================================
@app.callback(
    [Output("filter-payment",   "options"),
     Output("filter-insurance", "options")],
    [Input("auto-refresh", "n_intervals")]
)
def update_filter_options(n):
    df = get_booking_data()
    if df.empty:
        return [], []
    pay_opts = [{"label": p, "value": p} for p in sorted(df["payment_method"].dropna().unique())]
    ins_opts = [{"label": i, "value": i} for i in sorted(df["insurance_coverage"].dropna().unique())]
    return pay_opts, ins_opts


# ============================================================
# TAB NAVIGATION CALLBACK
# ============================================================
@app.callback(
    [Output("tab-overview",  "style"),
     Output("tab-analytics", "style"),
     Output("tab-ml",        "style"),
     Output("tab-pipeline",  "style"),
     Output("tab-quality",   "style"),
     Output("tab-debug",     "style"),
     Output("tab-content",   "children"),
     Output("last-updated",  "children")],
    [Input("tab-overview",   "n_clicks"),
     Input("tab-analytics",  "n_clicks"),
     Input("tab-ml",         "n_clicks"),
     Input("tab-pipeline",   "n_clicks"),
     Input("tab-quality",    "n_clicks"),
     Input("tab-debug",      "n_clicks"),
     Input("refresh-btn",    "n_clicks"),
     Input("auto-refresh",   "n_intervals"),
     Input("filter-loyalty", "value"),
     Input("filter-payment", "value"),
     Input("filter-insurance","value"),
     Input("clear-filters",  "n_clicks")],
)
def render_tab(*args):
    triggered = ctx.triggered_id or "tab-overview"
    now = f"↻  {pd.Timestamp.now().strftime('%d %b %Y  %H:%M:%S')}"

    # Determine active tab
    active = "tab-overview"
    if triggered in ["tab-overview","tab-analytics","tab-ml",
                     "tab-pipeline","tab-quality","tab-debug"]:
        active = triggered

    # Get filter values
    loyalty_filter  = args[8]
    payment_filter  = args[9]
    insurance_filter= args[10]
    clear_clicks    = args[11]

    # Sidebar styles
    tabs = ["tab-overview","tab-analytics","tab-ml","tab-pipeline","tab-quality","tab-debug"]
    styles = [tab_style(t == active) for t in tabs]

    # Load data
    df_raw = get_booking_data()
    validation_df, expectation_df, failed_df, sla_df = get_monitoring_data()

    # Apply filters
    df = df_raw.copy()
    if loyalty_filter:  df = df[df["loyalty_tier"]       == loyalty_filter]
    if payment_filter:  df = df[df["payment_method"]     == payment_filter]
    if insurance_filter:df = df[df["insurance_coverage"] == insurance_filter]

    filter_badge = html.Span(
        f"Filtered: {len(df):,} / {len(df_raw):,} records",
        style={"color": ACCENT5 if (loyalty_filter or payment_filter or insurance_filter) else MUTED,
               "fontSize": "11px", "fontFamily": "'Courier New', monospace"}
    )

    # ── RENDER TABS ──────────────────────────────────────────
    if active == "tab-overview":
        content = render_overview(df, validation_df, sla_df, filter_badge)
    elif active == "tab-analytics":
        content = render_analytics(df, filter_badge)
    elif active == "tab-ml":
        content = render_ml(df, failed_df, filter_badge)
    elif active == "tab-pipeline":
        content = render_pipeline(sla_df)
    elif active == "tab-quality":
        content = render_quality(validation_df, expectation_df, failed_df)
    elif active == "tab-debug":
        content = render_debug(df, failed_df, expectation_df)
    else:
        content = render_overview(df, validation_df, sla_df, filter_badge)

    return (*styles, content, now)


# ============================================================
# TAB 1: OVERVIEW
# ============================================================
def render_overview(df, validation_df, sla_df, filter_badge):
    total_bookings  = df["booking_id"].nunique()          if not df.empty else 0
    unique_customers= df["customer_id"].nunique()         if not df.empty else 0
    total_revenue   = df["payment_amount"].sum()          if not df.empty else 0
    unique_cars     = df["car_id"].nunique()              if not df.empty else 0
    avg_booking_val = df["payment_amount"].mean()         if not df.empty else 0
    integrity       = validation_df["success_rate"].mean()if not validation_df.empty else 0

    # Pipeline health summary
    pipeline_ok = not sla_df.empty and len(sla_df[sla_df["status"] == "FAILED"]) == 0

    # Revenue trend sparkline
    if not df.empty and "booking_date" in df.columns:
        rd = df.groupby("booking_date")["payment_amount"].sum().reset_index()
        fig_spark = px.area(rd, x="booking_date", y="payment_amount",
                            color_discrete_sequence=[ACCENT2])
        fig_spark.update_traces(line_color=ACCENT2, fillcolor="rgba(63,185,80,0.1)",
                                line_width=1.5)
        fig_spark.update_layout(**CHART_BASE, title="Revenue Trend",
                                xaxis={"visible": False}, yaxis={"visible": False},
                                margin=dict(l=0, r=0, t=30, b=0))
    else:
        fig_spark = empty_fig("Revenue Trend")

    # Booking heatmap (day of week vs hour)
    if not df.empty and "booking_date" in df.columns:
        df2 = df.copy()
        df2["booking_date"] = pd.to_datetime(df2["booking_date"])
        df2["day_of_week"] = df2["booking_date"].dt.day_name()
        df2["month"] = df2["booking_date"].dt.strftime("%b")
        heat = df2.groupby(["month", "day_of_week"])["booking_id"].count().reset_index()
        heat.columns = ["Month", "Day", "Bookings"]
        day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        heat["Day"] = pd.Categorical(heat["Day"], categories=day_order, ordered=True)
        heat = heat.sort_values("Day")
        fig_heat = px.density_heatmap(heat, x="Month", y="Day", z="Bookings",
                                       title="📅 Booking Heatmap — Month vs Day",
                                       color_continuous_scale=[[0,"#0d1117"],[0.5,ACCENT1],[1,ACCENT4]])
        fig_heat.update_layout(**CHART_BASE, coloraxis_showscale=False)
    else:
        fig_heat = empty_fig("Booking Heatmap")

    # Top revenue cars
    if not df.empty and "model" in df.columns:
        tc = df.groupby("model")["payment_amount"].sum().nlargest(6).reset_index()
        tc.columns = ["Model", "Revenue"]
        fig_cars = px.bar(tc, x="Revenue", y="Model", orientation="h",
                          title="🏆 Top Revenue Cars",
                          color="Revenue",
                          color_continuous_scale=[[0,"#1c2d3a"],[1,ACCENT5]])
        fig_cars.update_layout(**CHART_BASE, coloraxis_showscale=False,
                               yaxis={"categoryorder": "total ascending"})
    else:
        fig_cars = empty_fig("Top Revenue Cars")

    return html.Div([
        # Header
        html.Div(style={"marginBottom": "24px"}, children=[
            html.H1("Pipeline Overview", style={"color": TEXT, "fontSize": "22px",
                                                  "fontWeight": "800", "margin": "0 0 6px 0",
                                                  "fontFamily": "'Courier New', monospace"}),
            html.Div(style={"display": "flex", "gap": "12px", "alignItems": "center"}, children=[
                html.Span("🟢 Pipeline Healthy" if pipeline_ok else "🔴 Pipeline Issues",
                          style={"color": ACCENT2 if pipeline_ok else ACCENT3,
                                 "fontSize": "12px", "fontFamily": "'Courier New', monospace",
                                 "border": f"1px solid {ACCENT2 if pipeline_ok else ACCENT3}",
                                 "borderRadius": "4px", "padding": "2px 8px"}),
                filter_badge,
            ])
        ]),

        # KPI Row
        html.Div(
            style={"display": "flex", "gap": "12px", "marginBottom": "20px", "flexWrap": "wrap"},
            children=[
                kpi("ov-bookings",  "Total Bookings",    ACCENT1, "📋"),
                kpi("ov-customers", "Unique Customers",  ACCENT2, "👤"),
                kpi("ov-revenue",   "Total Revenue",     ACCENT3, "💰"),
                kpi("ov-cars",      "Unique Cars",       ACCENT5, "🚘"),
                kpi("ov-avg",       "Avg Booking Value", ACCENT6, "📊"),
                kpi("ov-integrity", "Data Integrity",    ACCENT4, "🛡️"),
            ]
        ),

        # Dummy outputs for KPIs
        html.Div(id="ov-bookings",  children=f"{total_bookings:,}",
                 style={"display":"none"}),

        # Charts Row 1
        html.Div(
            style={"display": "grid", "gridTemplateColumns": "2fr 1fr",
                   "gap": "14px", "marginBottom": "14px"},
            children=[
                card([dcc.Graph(figure=fig_spark, config={"displayModeBar": False})]),
                card([dcc.Graph(figure=fig_cars,  config={"displayModeBar": False})]),
            ]
        ),

        # Charts Row 2
        card([dcc.Graph(figure=fig_heat, config={"displayModeBar": False})],
             style_extra={"marginBottom": "14px"}),

        # Quick Stats Row
        html.Div(
            style={"display": "grid", "gridTemplateColumns": "1fr 1fr 1fr",
                   "gap": "14px"},
            children=[
                card([
                    html.Div("💳 Payment Split", style={
                        "color": SUBTEXT, "fontSize": "11px", "marginBottom": "12px",
                        "textTransform": "uppercase", "letterSpacing": "1px"
                    }),
                    *([html.Div(
                        style={"display": "flex", "justifyContent": "space-between",
                               "marginBottom": "8px", "alignItems": "center"},
                        children=[
                            html.Span(row["payment_method"], style={"color": TEXT, "fontSize": "12px"}),
                            html.Span(f"{row['payment_method_pct']:.1f}%",
                                      style={"color": ACCENT1, "fontFamily": "'Courier New', monospace",
                                             "fontSize": "12px", "fontWeight": "700"}),
                        ]
                    ) for _, row in (
                        df.groupby("payment_method")["booking_id"].count()
                        .reset_index()
                        .assign(payment_method_pct=lambda x: x["booking_id"] / x["booking_id"].sum() * 100)
                        .rename(columns={"booking_id": "count"})
                        .iterrows()
                    )] if not df.empty else [html.Span("No data", style={"color": MUTED})])
                ]),
                card([
                    html.Div("⭐ Loyalty Split", style={
                        "color": SUBTEXT, "fontSize": "11px", "marginBottom": "12px",
                        "textTransform": "uppercase", "letterSpacing": "1px"
                    }),
                    *([html.Div(
                        style={"display": "flex", "justifyContent": "space-between",
                               "marginBottom": "8px", "alignItems": "center"},
                        children=[
                            html.Span(row["loyalty_tier"].title(),
                                      style={"color": {"platinum":ACCENT4,"gold":ACCENT5,
                                                        "silver":"#8b949e","bronze":ACCENT3}.get(
                                                 row["loyalty_tier"], TEXT),
                                             "fontSize": "12px"}),
                            html.Span(f"{row['pct']:.1f}%",
                                      style={"color": TEXT, "fontFamily": "'Courier New', monospace",
                                             "fontSize": "12px"}),
                        ]
                    ) for _, row in (
                        df.groupby("loyalty_tier")["booking_id"].count()
                        .reset_index()
                        .assign(pct=lambda x: x["booking_id"] / x["booking_id"].sum() * 100)
                        .iterrows()
                    )] if not df.empty else [html.Span("No data", style={"color": MUTED})])
                ]),
                card([
                    html.Div("🛡️ Insurance Split", style={
                        "color": SUBTEXT, "fontSize": "11px", "marginBottom": "12px",
                        "textTransform": "uppercase", "letterSpacing": "1px"
                    }),
                    *([html.Div(
                        style={"display": "flex", "justifyContent": "space-between",
                               "marginBottom": "8px", "alignItems": "center"},
                        children=[
                            html.Span(row["insurance_coverage"],
                                      style={"color": TEXT, "fontSize": "12px"}),
                            html.Span(f"{row['pct']:.1f}%",
                                      style={"color": ACCENT2, "fontFamily": "'Courier New', monospace",
                                             "fontSize": "12px"}),
                        ]
                    ) for _, row in (
                        df.groupby("insurance_coverage")["booking_id"].count()
                        .reset_index()
                        .assign(pct=lambda x: x["booking_id"] / x["booking_id"].sum() * 100)
                        .iterrows()
                    )] if not df.empty else [html.Span("No data", style={"color": MUTED})])
                ]),
            ]
        ),

        # Hidden KPI updaters
        html.Script(f"""
            document.getElementById('ov-bookings') && (document.getElementById('ov-bookings').innerText = '{total_bookings:,}');
        """) if False else html.Div([
            html.Div(f"{total_bookings:,}",  id="ov-bookings",  style={"display":"none"}),
            html.Div(f"{unique_customers:,}", id="ov-customers", style={"display":"none"}),
            html.Div(f"₹{total_revenue:,.0f}",id="ov-revenue",  style={"display":"none"}),
            html.Div(f"{unique_cars:,}",      id="ov-cars",      style={"display":"none"}),
            html.Div(f"₹{avg_booking_val:,.0f}",id="ov-avg",    style={"display":"none"}),
            html.Div(f"{integrity:.1f}%",     id="ov-integrity", style={"display":"none"}),
        ]),
    ])


# ============================================================
# TAB 2: ANALYTICS
# ============================================================
def render_analytics(df, filter_badge):
    if df.empty:
        return card([html.Div("No data available", style={"color": MUTED})])

    # Daily bookings + revenue
    if "booking_date" in df.columns:
        daily = df.groupby("booking_date").agg(
            bookings=("booking_id", "nunique"),
            revenue=("payment_amount", "sum")
        ).reset_index()

        fig_daily = go.Figure()
        fig_daily.add_trace(go.Bar(x=daily["booking_date"], y=daily["bookings"],
                                   name="Bookings", marker_color=ACCENT1,
                                   yaxis="y"))
        fig_daily.add_trace(go.Scatter(x=daily["booking_date"], y=daily["revenue"],
                                       name="Revenue (₹)", line=dict(color=ACCENT5, width=2),
                                       yaxis="y2", mode="lines+markers",
                                       marker=dict(size=4)))
        fig_daily.update_layout(
            **CHART_BASE, title="📅 Daily Bookings & Revenue",
            yaxis=dict(title="Bookings", color=ACCENT1, showgrid=False),
            yaxis2=dict(title="Revenue (₹)", overlaying="y", side="right",
                        color=ACCENT5, showgrid=False),
            legend=dict(orientation="h", y=1.1, x=0),
            barmode="overlay"
        )
    else:
        fig_daily = empty_fig("Daily Bookings & Revenue")

    # Car models + revenue scatter
    if "model" in df.columns:
        car_stats = df.groupby("model").agg(
            bookings=("booking_id", "count"),
            revenue=("payment_amount", "sum"),
            avg_price=("price_per_day", "mean")
        ).reset_index()
        fig_scatter = px.scatter(car_stats, x="bookings", y="revenue",
                                  size="avg_price", text="model",
                                  title="🚗 Car Model — Bookings vs Revenue",
                                  color="avg_price",
                                  color_continuous_scale=[[0,CARD_BG],[1,ACCENT1]],
                                  hover_data=["avg_price"])
        fig_scatter.update_traces(textposition="top center",
                                   textfont=dict(color=SUBTEXT, size=9))
        fig_scatter.update_layout(**CHART_BASE, coloraxis_showscale=False)
    else:
        fig_scatter = empty_fig("Car Model Scatter")

    # Location flow
    if "pickup_location" in df.columns and "drop_location" in df.columns:
        top_pickup = df["pickup_location"].value_counts().head(8).reset_index()
        top_pickup.columns = ["location", "count"]
        top_drop = df["drop_location"].value_counts().head(8).reset_index()
        top_drop.columns = ["location", "count"]

        fig_loc = go.Figure()
        fig_loc.add_trace(go.Bar(y=top_pickup["location"], x=top_pickup["count"],
                                  name="Pickup", orientation="h",
                                  marker_color=ACCENT2))
        fig_loc.add_trace(go.Bar(y=top_drop["location"], x=top_drop["count"],
                                  name="Drop", orientation="h",
                                  marker_color=ACCENT4))
        fig_loc.update_layout(**CHART_BASE, title="📍 Pickup vs Drop Locations",
                               barmode="group",
                               yaxis={"categoryorder": "total ascending"})
    else:
        fig_loc = empty_fig("Location Analysis")

    # Revenue by loyalty tier + payment method
    if "loyalty_tier" in df.columns and "payment_method" in df.columns:
        lp = df.groupby(["loyalty_tier","payment_method"])["payment_amount"].sum().reset_index()
        lp.columns = ["Tier", "Payment", "Revenue"]
        fig_lp = px.bar(lp, x="Tier", y="Revenue", color="Payment",
                         title="💰 Revenue by Tier & Payment Method",
                         barmode="stack",
                         color_discrete_sequence=[ACCENT1,ACCENT2,ACCENT3,ACCENT5,ACCENT4])
        fig_lp.update_layout(**CHART_BASE)
    else:
        fig_lp = empty_fig("Revenue by Tier")

    # Loyalty points distribution
    if "loyalty_points" in df.columns and "loyalty_tier" in df.columns:
        fig_box = px.box(df, x="loyalty_tier", y="loyalty_points",
                          title="📦 Loyalty Points Distribution by Tier",
                          color="loyalty_tier",
                          color_discrete_map={"platinum":ACCENT4,"gold":ACCENT5,
                                              "silver":"#8b949e","bronze":ACCENT3})
        fig_box.update_layout(**CHART_BASE, showlegend=False)
    else:
        fig_box = empty_fig("Loyalty Points Distribution")

    # Price per day distribution
    if "price_per_day" in df.columns:
        fig_price = px.histogram(df, x="price_per_day",
                                  title="💵 Price Per Day Distribution",
                                  nbins=25, color_discrete_sequence=[ACCENT6])
        fig_price.update_traces(marker_line_width=0)
        fig_price.update_layout(**CHART_BASE)
    else:
        fig_price = empty_fig("Price Distribution")

    return html.Div([
        html.Div(style={"marginBottom": "20px"}, children=[
            html.H1("Booking Analytics", style={"color": TEXT, "fontSize": "22px",
                                                  "fontWeight": "800", "margin": "0 0 6px 0",
                                                  "fontFamily": "'Courier New', monospace"}),
            filter_badge,
        ]),
        card([dcc.Graph(figure=fig_daily,   config={"displayModeBar": False})],
             style_extra={"marginBottom": "14px"}),
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                        "gap": "14px", "marginBottom": "14px"},
                 children=[
                     card([dcc.Graph(figure=fig_scatter, config={"displayModeBar": False})]),
                     card([dcc.Graph(figure=fig_loc,     config={"displayModeBar": False})]),
                 ]),
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr 1fr",
                        "gap": "14px"},
                 children=[
                     card([dcc.Graph(figure=fig_lp,    config={"displayModeBar": False})]),
                     card([dcc.Graph(figure=fig_box,   config={"displayModeBar": False})]),
                     card([dcc.Graph(figure=fig_price, config={"displayModeBar": False})]),
                 ]),
    ])


# ============================================================
# TAB 3: PIPELINE OBSERVABILITY
# ============================================================
def render_ml(df, failed_df, filter_badge):
    """
    Pipeline Observability — Data Engineer mindset:
    1. Record Volume Anomaly  → "Aaj kam records kyun?"
    2. Data Drift Detection   → "Distribution shift hua?"
    3. Column Health          → "Null % badha?"
    4. Data Freshness         → "Pipeline kab last chali?"
    5. Duplicate Rate         → "Duplicates trend"
    """
    if df.empty:
        return card([html.Div("No data available — Run pipeline first.",
                              style={"color": MUTED, "padding": "40px", "textAlign": "center"})])

    # ── 1. RECORD VOLUME ANOMALY ─────────────────────────────
    if "booking_date" in df.columns:
        df_dated = df.copy()
        df_dated["booking_date"] = pd.to_datetime(df_dated["booking_date"])
        daily_counts = df_dated.groupby("booking_date")["booking_id"].count().reset_index()
        daily_counts.columns = ["date", "record_count"]
        daily_counts["rolling_avg"] = daily_counts["record_count"].rolling(7, min_periods=1).mean()
        daily_counts["rolling_std"] = daily_counts["record_count"].rolling(7, min_periods=1).std().fillna(0)
        daily_counts["upper"]       = daily_counts["rolling_avg"] + 2 * daily_counts["rolling_std"]
        daily_counts["lower"]       = (daily_counts["rolling_avg"] - 2 * daily_counts["rolling_std"]).clip(lower=0)
        daily_counts["is_anomaly"]  = (
            (daily_counts["record_count"] > daily_counts["upper"]) |
            (daily_counts["record_count"] < daily_counts["lower"])
        )
        vol_anomalies = daily_counts[daily_counts["is_anomaly"]]

        fig_vol = go.Figure()
        fig_vol.add_trace(go.Bar(
            x=daily_counts["date"], y=daily_counts["record_count"],
            name="Daily Records", marker_color=ACCENT1, opacity=0.8))
        fig_vol.add_trace(go.Scatter(
            x=daily_counts["date"], y=daily_counts["rolling_avg"],
            name="7-Day Avg", line=dict(color=ACCENT5, width=2, dash="dot")))
        fig_vol.add_trace(go.Scatter(
            x=list(daily_counts["date"]) + list(daily_counts["date"])[::-1],
            y=list(daily_counts["upper"]) + list(daily_counts["lower"])[::-1],
            fill="toself", fillcolor="rgba(88,166,255,0.06)",
            line=dict(color="rgba(0,0,0,0)"), name="Normal Band"))
        if not vol_anomalies.empty:
            fig_vol.add_trace(go.Scatter(
                x=vol_anomalies["date"], y=vol_anomalies["record_count"],
                mode="markers", name=f"Volume Spike/Drop ({len(vol_anomalies)})",
                marker=dict(color=ACCENT3, size=12, symbol="x", line=dict(width=2))))
        fig_vol.update_layout(
            **CHART_BASE,
            title=f"📊 Record Volume Monitor  —  {len(vol_anomalies)} anomalies detected",
            legend=dict(orientation="h", y=1.1, x=0))
    else:
        fig_vol = empty_fig("Record Volume Monitor")
        vol_anomalies = pd.DataFrame()

    # ── 2. DATA DRIFT — Payment Amount Distribution ──────────
    if "payment_amount" in df.columns and "booking_date" in df.columns:
        df_drift = df.copy()
        df_drift["booking_date"] = pd.to_datetime(df_drift["booking_date"])
        df_drift["month"] = df_drift["booking_date"].dt.to_period("M").astype(str)

        monthly_stats = df_drift.groupby("month")["payment_amount"].agg(
            mean="mean", std="std", p25=lambda x: x.quantile(0.25),
            p75=lambda x: x.quantile(0.75)
        ).reset_index()

        fig_drift = go.Figure()
        fig_drift.add_trace(go.Scatter(
            x=monthly_stats["month"], y=monthly_stats["mean"],
            name="Mean", line=dict(color=ACCENT2, width=2), mode="lines+markers",
            marker=dict(size=6)))
        fig_drift.add_trace(go.Scatter(
            x=list(monthly_stats["month"]) + list(monthly_stats["month"])[::-1],
            y=list(monthly_stats["p75"]) + list(monthly_stats["p25"])[::-1],
            fill="toself", fillcolor="rgba(63,185,80,0.1)",
            line=dict(color="rgba(0,0,0,0)"), name="IQR Band"))
        fig_drift.update_layout(
            **CHART_BASE,
            title="📉 Data Drift — Payment Amount Distribution Over Time")
    else:
        fig_drift = empty_fig("Data Drift Monitor")

    # ── 3. COLUMN NULL HEALTH OVER TIME ─────────────────────
    # Show null % per key column as a heatmap
    key_cols = ["booking_id","customer_id","car_id","payment_amount",
                "pickup_location","drop_location","loyalty_tier","payment_method"]
    key_cols = [c for c in key_cols if c in df.columns]

    null_data = []
    for col in key_cols:
        null_pct = df[col].isnull().sum() / len(df) * 100
        null_data.append({"Column": col, "Null %": round(null_pct, 2),
                          "Status": "🔴 Critical" if null_pct > 5
                                    else ("🟡 Warning" if null_pct > 0 else "🟢 Healthy")})
    null_df = pd.DataFrame(null_data)

    fig_null = px.bar(null_df, x="Column", y="Null %",
                       title="🩺 Column Null Health Check",
                       color="Null %",
                       color_continuous_scale=[[0, ACCENT2],[0.01, ACCENT5],[1, ACCENT3]],
                       text="Null %",
                       hover_data=["Status"])
    fig_null.update_traces(texttemplate="%{text:.2f}%", textposition="outside",
                            textfont_color=TEXT)
    fig_null.update_layout(**CHART_BASE, coloraxis_showscale=False,
                            yaxis_range=[0, max(null_df["Null %"].max() * 1.5, 1)])
    fig_null.add_hline(y=5, line_dash="dot", line_color=ACCENT3,
                       annotation_text="5% threshold",
                       annotation_font_color=ACCENT3)

    # ── 4. DUPLICATE RATE ANALYSIS ───────────────────────────
    if "booking_id" in df.columns:
        total_records   = len(df)
        unique_bookings = df["booking_id"].nunique()
        duplicate_count = total_records - unique_bookings
        dup_rate        = duplicate_count / total_records * 100

        # Duplicate distribution by column
        dup_by_col = []
        for col in ["booking_id", "customer_id", "car_id", "payment_id"]:
            if col in df.columns:
                dups = len(df) - df[col].nunique()
                dup_by_col.append({"Column": col, "Duplicates": dups,
                                   "Rate %": round(dups / len(df) * 100, 2)})
        dup_df = pd.DataFrame(dup_by_col)

        fig_dup = px.bar(dup_df, x="Column", y="Duplicates",
                          title=f"🔁 Duplicate Analysis  —  Overall rate: {dup_rate:.2f}%",
                          color="Rate %",
                          color_continuous_scale=[[0, ACCENT2],[0.1, ACCENT5],[1, ACCENT3]],
                          text="Rate %",
                          hover_data=["Rate %"])
        fig_dup.update_traces(texttemplate="%{text:.1f}%", textposition="outside",
                               textfont_color=TEXT)
        fig_dup.update_layout(**CHART_BASE, coloraxis_showscale=False)
    else:
        fig_dup = empty_fig("Duplicate Analysis")
        duplicate_count = 0
        dup_rate = 0

    # ── 5. DATA FRESHNESS ────────────────────────────────────
    if "booking_date" in df.columns:
        df_fresh = df.copy()
        df_fresh["booking_date"] = pd.to_datetime(df_fresh["booking_date"])
        latest_date  = df_fresh["booking_date"].max()
        earliest_date= df_fresh["booking_date"].min()
        date_range   = (latest_date - earliest_date).days
        freshness_ok = True  # pipeline just ran

        # Records per month bar
        monthly = df_fresh.groupby(
            df_fresh["booking_date"].dt.to_period("M").astype(str)
        )["booking_id"].count().reset_index()
        monthly.columns = ["Month", "Records"]
        fig_fresh = px.bar(monthly, x="Month", y="Records",
                            title="📅 Data Freshness — Records by Month",
                            color="Records",
                            color_continuous_scale=[[0,"#1c2d3a"],[1,ACCENT2]])
        fig_fresh.update_layout(**CHART_BASE, coloraxis_showscale=False)
    else:
        fig_fresh    = empty_fig("Data Freshness")
        latest_date  = "N/A"
        date_range   = 0
        freshness_ok = False

    # ── OBSERVABILITY SUMMARY CARDS ──────────────────────────
    obs_cards = [
        ("📊 Volume Anomalies", str(len(vol_anomalies)),
         ACCENT3 if len(vol_anomalies) > 0 else ACCENT2,
         "Unusual record counts detected" if len(vol_anomalies) > 0 else "Volume looks normal"),
        ("🔁 Duplicate Rate",   f"{dup_rate:.2f}%",
         ACCENT3 if dup_rate > 5 else (ACCENT5 if dup_rate > 0 else ACCENT2),
         f"{duplicate_count:,} duplicate records found"),
        ("🩺 Null Columns",     str(len(null_df[null_df["Null %"] > 0])),
         ACCENT3 if len(null_df[null_df["Null %"] > 0]) > 0 else ACCENT2,
         "Columns with missing values"),
        ("📅 Data Freshness",   str(latest_date)[:10] if latest_date != "N/A" else "N/A",
         ACCENT2 if freshness_ok else ACCENT3,
         f"Covering {date_range} days of data"),
    ]

    return html.Div([
        # Header
        html.Div(style={"marginBottom": "20px"}, children=[
            html.H1("Pipeline Observability", style={
                "color": TEXT, "fontSize": "22px", "fontWeight": "800",
                "margin": "0 0 6px 0", "fontFamily": "'Courier New', monospace"
            }),
            html.Div(style={"display": "flex", "gap": "8px", "flexWrap": "wrap"}, children=[
                filter_badge,
                html.Span("Real-time pipeline health monitoring",
                          style={"color": MUTED, "fontSize": "11px",
                                 "fontFamily": "'Courier New', monospace"}),
            ])
        ]),

        # Observability KPI cards
        html.Div(
            style={"display": "flex", "gap": "12px", "marginBottom": "20px", "flexWrap": "wrap"},
            children=[
                html.Div(style={
                    "backgroundColor": CARD_BG, "borderRadius": "10px",
                    "border": f"1px solid {BORDER}", "borderTop": f"3px solid {color}",
                    "padding": "16px", "flex": "1", "minWidth": "160px"
                }, children=[
                    html.Div(label, style={"color": SUBTEXT, "fontSize": "10px",
                                           "textTransform": "uppercase", "letterSpacing": "1px",
                                           "marginBottom": "8px"}),
                    html.Div(value, style={"color": color, "fontSize": "22px",
                                           "fontWeight": "800",
                                           "fontFamily": "'Courier New', monospace",
                                           "marginBottom": "4px"}),
                    html.Div(subtitle, style={"color": MUTED, "fontSize": "10px"}),
                ]) for label, value, color, subtitle in obs_cards
            ]
        ),

        # Row 1: Volume + Drift
        html.Div(style={"display": "grid", "gridTemplateColumns": "3fr 2fr",
                        "gap": "14px", "marginBottom": "14px"},
                 children=[
                     card([dcc.Graph(figure=fig_vol,   config={"displayModeBar": False})]),
                     card([dcc.Graph(figure=fig_drift, config={"displayModeBar": False})]),
                 ]),

        # Row 2: Null health + Duplicate
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                        "gap": "14px", "marginBottom": "14px"},
                 children=[
                     card([dcc.Graph(figure=fig_null, config={"displayModeBar": False})]),
                     card([dcc.Graph(figure=fig_dup,  config={"displayModeBar": False})]),
                 ]),

        # Row 3: Freshness + Column health table
        html.Div(style={"display": "grid", "gridTemplateColumns": "2fr 1fr",
                        "gap": "14px"},
                 children=[
                     card([dcc.Graph(figure=fig_fresh, config={"displayModeBar": False})]),
                     card(border_color=ACCENT2, children=[
                         html.Div("🩺 Column Health Summary", style={
                             "color": ACCENT2, "fontFamily": "'Courier New', monospace",
                             "fontWeight": "700", "fontSize": "12px", "marginBottom": "12px"
                         }),
                         *[html.Div(
                             style={"display": "flex", "justifyContent": "space-between",
                                    "alignItems": "center", "padding": "7px 0",
                                    "borderBottom": f"1px solid {BORDER}"},
                             children=[
                                 html.Span(row["Column"],
                                           style={"color": TEXT, "fontSize": "11px",
                                                  "fontFamily": "'Courier New', monospace"}),
                                 html.Span(row["Status"],
                                           style={"fontSize": "11px"}),
                             ]
                         ) for _, row in null_df.iterrows()]
                     ]),
                 ]),
    ])


# ============================================================
# TAB 4: PIPELINE HEALTH
# ============================================================
def render_pipeline(sla_df):
    if sla_df.empty:
        return card([html.Div("No pipeline run data yet. Run the pipeline first.",
                              style={"color": MUTED, "padding": "40px", "textAlign": "center"})])

    total_runs   = sla_df["run_id"].nunique()
    avg_dur      = sla_df["duration_seconds"].mean()
    total_rec    = sla_df["records_processed"].sum()
    success_rate = len(sla_df[sla_df["status"]=="SUCCESS"]) / len(sla_df) * 100

    # Stage duration
    sa = sla_df.groupby("stage_name")["duration_seconds"].agg(["mean","max","min"]).reset_index()
    sa.columns = ["Stage","Avg","Max","Min"]
    sa["Stage"] = sa["Stage"].str.replace("_"," ").str.title()
    fig_dur = go.Figure()
    fig_dur.add_trace(go.Bar(x=sa["Stage"], y=sa["Avg"], name="Avg",
                              marker_color=ACCENT1))
    fig_dur.add_trace(go.Bar(x=sa["Stage"], y=sa["Max"], name="Max",
                              marker_color=ACCENT3, opacity=0.6))
    fig_dur.update_layout(**CHART_BASE, title="⏱️ Stage Duration (seconds)",
                          barmode="group", legend=dict(orientation="h", y=1.1))

    # Records per stage
    sr = sla_df[sla_df["records_processed"] > 0].groupby(
        "stage_name")["records_processed"].max().reset_index()
    sr.columns = ["Stage","Records"]
    sr["Stage"] = sr["Stage"].str.replace("_"," ").str.title()
    fig_rec = px.bar(sr, x="Stage", y="Records",
                      title="📦 Records Processed per Stage",
                      color="Records",
                      color_continuous_scale=[[0,"#1c2d3a"],[1,ACCENT2]])
    fig_rec.update_layout(**CHART_BASE, coloraxis_showscale=False)

    # Timeline
    tl = sla_df.copy()
    tl["Stage"] = tl["stage_name"].str.replace("_"," ").str.title()
    fig_tl = px.bar(tl, x="started_at", y="duration_seconds", color="Stage",
                     title="📅 Pipeline Run Timeline", barmode="group",
                     color_discrete_sequence=[ACCENT1,ACCENT2,ACCENT3,ACCENT5,ACCENT4,ACCENT6])
    fig_tl.update_layout(**CHART_BASE)

    # Status donut
    sc = sla_df["status"].value_counts().reset_index()
    sc.columns = ["status","count"]
    fig_st = px.pie(sc, names="status", values="count",
                     title="✅ Stage Status",
                     color="status",
                     color_discrete_map={"SUCCESS":ACCENT2,"FAILED":ACCENT3,"SKIPPED":MUTED},
                     hole=0.6)
    fig_st.update_layout(**CHART_BASE)

    # Latest run table
    latest_run = sla_df["run_id"].iloc[0]
    latest_df  = sla_df[sla_df["run_id"] == latest_run].copy()
    latest_df["stage_name"] = latest_df["stage_name"].str.replace("_"," ").str.title()
    latest_df["duration"]   = latest_df["duration_seconds"].astype(str) + "s"

    show_cols  = [c for c in ["stage_name","records_processed","duration","status","started_at"]
                  if c in latest_df.columns]
    col_rename = {"stage_name":"Stage","records_processed":"Records",
                  "duration":"Duration","status":"Status","started_at":"Started At"}

    return html.Div([
        html.H1("Pipeline Health", style={"color": TEXT, "fontSize": "22px",
                                           "fontWeight": "800", "margin": "0 0 20px 0",
                                           "fontFamily": "'Courier New', monospace"}),
        # KPIs
        html.Div(style={"display": "flex", "gap": "12px", "marginBottom": "20px"}, children=[
            html.Div(style={"backgroundColor": CARD_BG, "borderRadius": "10px",
                            "border": f"1px solid {BORDER}", "borderTop": f"3px solid {ACCENT1}",
                            "padding": "16px", "flex": "1", "textAlign": "center"},
                     children=[
                         html.Div(f"{total_runs}", style={"color": ACCENT1, "fontSize": "28px",
                                                           "fontWeight": "800",
                                                           "fontFamily": "'Courier New', monospace"}),
                         html.Div("Pipeline Runs", style={"color": SUBTEXT, "fontSize": "11px",
                                                           "textTransform": "uppercase"})
                     ]),
            html.Div(style={"backgroundColor": CARD_BG, "borderRadius": "10px",
                            "border": f"1px solid {BORDER}", "borderTop": f"3px solid {ACCENT2}",
                            "padding": "16px", "flex": "1", "textAlign": "center"},
                     children=[
                         html.Div(f"{avg_dur:.0f}s", style={"color": ACCENT2, "fontSize": "28px",
                                                              "fontWeight": "800",
                                                              "fontFamily": "'Courier New', monospace"}),
                         html.Div("Avg Duration", style={"color": SUBTEXT, "fontSize": "11px",
                                                          "textTransform": "uppercase"})
                     ]),
            html.Div(style={"backgroundColor": CARD_BG, "borderRadius": "10px",
                            "border": f"1px solid {BORDER}", "borderTop": f"3px solid {ACCENT5}",
                            "padding": "16px", "flex": "1", "textAlign": "center"},
                     children=[
                         html.Div(f"{total_rec:,}", style={"color": ACCENT5, "fontSize": "28px",
                                                             "fontWeight": "800",
                                                             "fontFamily": "'Courier New', monospace"}),
                         html.Div("Records Processed", style={"color": SUBTEXT, "fontSize": "11px",
                                                               "textTransform": "uppercase"})
                     ]),
            html.Div(style={"backgroundColor": CARD_BG, "borderRadius": "10px",
                            "border": f"1px solid {BORDER}", "borderTop": f"3px solid {ACCENT4}",
                            "padding": "16px", "flex": "1", "textAlign": "center"},
                     children=[
                         html.Div(f"{success_rate:.0f}%", style={"color": ACCENT4, "fontSize": "28px",
                                                                   "fontWeight": "800",
                                                                   "fontFamily": "'Courier New', monospace"}),
                         html.Div("Stage Success Rate", style={"color": SUBTEXT, "fontSize": "11px",
                                                                "textTransform": "uppercase"})
                     ]),
        ]),
        html.Div(style={"display": "grid", "gridTemplateColumns": "2fr 1fr",
                        "gap": "14px", "marginBottom": "14px"},
                 children=[
                     card([dcc.Graph(figure=fig_dur, config={"displayModeBar": False})]),
                     card([dcc.Graph(figure=fig_st,  config={"displayModeBar": False})]),
                 ]),
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                        "gap": "14px", "marginBottom": "14px"},
                 children=[
                     card([dcc.Graph(figure=fig_rec, config={"displayModeBar": False})]),
                     card([dcc.Graph(figure=fig_tl,  config={"displayModeBar": False})]),
                 ]),
        card(border_color=ACCENT1, children=[
            html.Div(style={"display": "flex", "justifyContent": "space-between",
                            "alignItems": "center", "marginBottom": "12px"},
                     children=[
                         html.Span("🕐 Latest Run — Stage Breakdown",
                                   style={"color": TEXT, "fontFamily": "'Courier New', monospace",
                                          "fontWeight": "700", "fontSize": "13px"}),
                         html.Span(f"Run: {latest_run}",
                                   style={"color": ACCENT1, "fontSize": "11px",
                                          "fontFamily": "'Courier New', monospace",
                                          "border": f"1px solid {ACCENT1}",
                                          "borderRadius": "4px", "padding": "2px 8px"}),
                     ]),
            dash_table.DataTable(
                data=latest_df[show_cols].to_dict("records"),
                columns=[{"name": col_rename.get(c,c), "id": c} for c in show_cols],
                style_table={"overflowX": "auto"},
                style_cell={"backgroundColor": CARD_BG, "color": TEXT,
                             "border": f"1px solid {BORDER}",
                             "textAlign": "left", "padding": "10px 14px",
                             "fontSize": "12px", "fontFamily": "'Courier New', monospace"},
                style_header={"backgroundColor": "#0a0f1a", "color": ACCENT1,
                               "fontWeight": "700", "fontSize": "11px",
                               "border": f"1px solid {BORDER}",
                               "textTransform": "uppercase", "letterSpacing": "1px"},
                style_data_conditional=[
                    {"if": {"row_index": "odd"}, "backgroundColor": "#0d1117"},
                    {"if": {"filter_query": "{Status} = SUCCESS"}, "color": ACCENT2},
                    {"if": {"filter_query": "{Status} = FAILED"},  "color": ACCENT3},
                ],
                page_size=10, sort_action="native",
            ),
        ]),
    ])


# ============================================================
# TAB 5: DATA QUALITY
# ============================================================
def render_quality(validation_df, expectation_df, failed_df):
    avg_rate    = float(validation_df["success_rate"].mean()) if not validation_df.empty else 0
    gauge_color = ACCENT2 if avg_rate >= 90 else (ACCENT5 if avg_rate >= 70 else ACCENT3)

    # Integrity gauge
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number", value=avg_rate,
        title={"text": "Data Integrity Score", "font": {"color": TEXT, "size": 12}},
        number={"suffix": "%", "font": {"color": gauge_color, "size": 40,
                                         "family": "'Courier New', monospace"}},
        gauge={
            "axis": {"range": [0,100], "tickcolor": SUBTEXT,
                     "tickfont": {"color": SUBTEXT, "size": 10}},
            "bar": {"color": gauge_color, "thickness": 0.3},
            "bgcolor": CARD_BG, "borderwidth": 0,
            "steps": [
                {"range": [0,  70], "color": "#1c1f26"},
                {"range": [70, 90], "color": "#1c2416"},
                {"range": [90,100], "color": "#162414"},
            ],
        }
    ))
    fig_gauge.update_layout(**CHART_BASE)

    # Rules pass/fail
    if not expectation_df.empty:
        es = expectation_df.groupby("expectation_type")["success"].agg(
            Passed=lambda x: int(x.astype(bool).sum()),
            Failed=lambda x: int((~x.astype(bool)).sum())
        ).reset_index()
        es["Rule"] = (es["expectation_type"]
                      .str.replace("expect_column_values_to_be_","")
                      .str.replace("expect_column_","")
                      .str.replace("_"," ").str.title())
        em = es.melt(id_vars=["expectation_type","Rule"],
                     value_vars=["Passed","Failed"],
                     var_name="Status", value_name="Count")
        fig_rules = px.bar(em, x="Count", y="Rule", color="Status", orientation="h",
                            barmode="group", title="📋 Validation Rules — Pass vs Fail",
                            color_discrete_map={"Passed": ACCENT2, "Failed": ACCENT3})
        fig_rules.update_layout(**CHART_BASE, yaxis={"categoryorder": "total ascending"})

        # Anomaly by column
        cf = expectation_df[~expectation_df["success"].astype(bool)].groupby(
            "column_name")["unexpected_count"].sum().reset_index()
        cf.columns = ["Column", "Anomalies"]
        cf = cf.sort_values("Anomalies", ascending=True)
        fig_col = px.bar(cf, x="Anomalies", y="Column", orientation="h",
                          title="⚠️ Anomaly Count by Column",
                          color="Anomalies",
                          color_continuous_scale=[[0,"#1c2d3a"],[1,ACCENT3]])
        fig_col.update_layout(**CHART_BASE, coloraxis_showscale=False)
    else:
        fig_rules = empty_fig("Validation Rules")
        fig_col   = empty_fig("Anomalies by Column")

    # Run history
    if not validation_df.empty and "created_at" in validation_df.columns:
        fig_hist = px.line(validation_df, x="created_at", y="success_rate",
                            title="📈 Data Quality Score History",
                            color_discrete_sequence=[ACCENT2], markers=True)
        fig_hist.add_hline(y=100, line_dash="dot", line_color=ACCENT3,
                           annotation_text="Target: 100%",
                           annotation_font_color=ACCENT3)
        fig_hist.update_traces(line_width=2, marker_size=8)
        fig_hist.update_layout(**CHART_BASE, yaxis_range=[0,105])
    else:
        fig_hist = empty_fig("Quality Score History")

    # Failed records distribution
    if not failed_df.empty and "loyalty_tier" in failed_df.columns:
        fl = failed_df["loyalty_tier"].value_counts().reset_index()
        fl.columns = ["Tier", "Count"]
        fig_loy = px.pie(fl, names="Tier", values="Count",
                          title="👤 Failed Records by Customer Tier",
                          color="Tier",
                          color_discrete_map={"platinum":ACCENT4,"gold":ACCENT5,
                                              "silver":"#8b949e","bronze":ACCENT3},
                          hole=0.5)
        fig_loy.update_layout(**CHART_BASE)
    else:
        fig_loy = empty_fig("Failed Records by Tier")

    val_runs     = len(validation_df)       if not validation_df.empty else 0
    val_passed   = int(expectation_df["success"].sum()) if not expectation_df.empty else 0
    val_violated = int((~expectation_df["success"].astype(bool)).sum()) if not expectation_df.empty else 0
    val_anomalous= len(failed_df)           if not failed_df.empty else 0

    return html.Div([
        html.H1("Data Quality", style={"color": TEXT, "fontSize": "22px",
                                        "fontWeight": "800", "margin": "0 0 20px 0",
                                        "fontFamily": "'Courier New', monospace"}),
        html.Div(style={"display": "flex", "gap": "12px", "marginBottom": "20px"}, children=[
            html.Div(style={"backgroundColor": CARD_BG, "borderRadius": "10px",
                            "border": f"1px solid {BORDER}", "borderTop": f"3px solid {ACCENT2}",
                            "padding": "16px", "flex": "1", "textAlign": "center"},
                     children=[
                         html.Div(f"{val_runs}", style={"color": ACCENT2, "fontSize": "28px",
                                                         "fontWeight": "800",
                                                         "fontFamily": "'Courier New', monospace"}),
                         html.Div("Validation Runs", style={"color": SUBTEXT, "fontSize": "11px"})
                     ]),
            html.Div(style={"backgroundColor": CARD_BG, "borderRadius": "10px",
                            "border": f"1px solid {BORDER}", "borderTop": f"3px solid {ACCENT2}",
                            "padding": "16px", "flex": "1", "textAlign": "center"},
                     children=[
                         html.Div(f"{val_passed}", style={"color": ACCENT2, "fontSize": "28px",
                                                           "fontWeight": "800",
                                                           "fontFamily": "'Courier New', monospace"}),
                         html.Div("Rules Passed", style={"color": SUBTEXT, "fontSize": "11px"})
                     ]),
            html.Div(style={"backgroundColor": CARD_BG, "borderRadius": "10px",
                            "border": f"1px solid {BORDER}", "borderTop": f"3px solid {ACCENT3}",
                            "padding": "16px", "flex": "1", "textAlign": "center"},
                     children=[
                         html.Div(f"{val_violated}", style={"color": ACCENT3, "fontSize": "28px",
                                                             "fontWeight": "800",
                                                             "fontFamily": "'Courier New', monospace"}),
                         html.Div("Rules Violated", style={"color": SUBTEXT, "fontSize": "11px"})
                     ]),
            html.Div(style={"backgroundColor": CARD_BG, "borderRadius": "10px",
                            "border": f"1px solid {BORDER}", "borderTop": f"3px solid {ACCENT5}",
                            "padding": "16px", "flex": "1", "textAlign": "center"},
                     children=[
                         html.Div(f"{val_anomalous:,}", style={"color": ACCENT5, "fontSize": "28px",
                                                                "fontWeight": "800",
                                                                "fontFamily": "'Courier New', monospace"}),
                         html.Div("Flagged Records", style={"color": SUBTEXT, "fontSize": "11px"})
                     ]),
        ]),
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 2fr",
                        "gap": "14px", "marginBottom": "14px"},
                 children=[
                     card([dcc.Graph(figure=fig_gauge, config={"displayModeBar": False})]),
                     card([dcc.Graph(figure=fig_hist,  config={"displayModeBar": False})]),
                 ]),
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                        "gap": "14px", "marginBottom": "14px"},
                 children=[
                     card([dcc.Graph(figure=fig_rules, config={"displayModeBar": False})]),
                     card([dcc.Graph(figure=fig_col,   config={"displayModeBar": False})]),
                 ]),
        card([dcc.Graph(figure=fig_loy, config={"displayModeBar": False})]),
    ])


# ============================================================
# TAB 6: DEBUG
# ============================================================
def render_debug(df, failed_df, expectation_df):
    return html.Div([
        html.H1("Debug Console", style={"color": TEXT, "fontSize": "22px",
                                         "fontWeight": "800", "margin": "0 0 20px 0",
                                         "fontFamily": "'Courier New', monospace"}),

        # Schema Inspector
        card(border_color=ACCENT1, style_extra={"marginBottom": "14px"}, children=[
            html.Div("🔎 Schema Inspector — customer_booking_staging", style={
                "color": ACCENT1, "fontFamily": "'Courier New', monospace",
                "fontWeight": "700", "fontSize": "13px", "marginBottom": "14px"
            }),
            html.Div(style={"display": "grid", "gridTemplateColumns": "repeat(4, 1fr)",
                            "gap": "8px"},
                     children=[
                         html.Div(style={"backgroundColor": "#0d1117", "borderRadius": "6px",
                                         "padding": "10px 12px",
                                         "border": f"1px solid {BORDER}"},
                                  children=[
                                      html.Div(col, style={"color": ACCENT1, "fontSize": "11px",
                                                            "fontFamily": "'Courier New', monospace",
                                                            "fontWeight": "700"}),
                                      html.Div(str(df[col].dtype),
                                               style={"color": MUTED, "fontSize": "10px"}),
                                      html.Div(f"null: {df[col].isnull().sum()}",
                                               style={"color": ACCENT3 if df[col].isnull().sum() > 0
                                                      else MUTED, "fontSize": "10px"}),
                                  ]) for col in df.columns
                     ] if not df.empty else [html.Span("No data", style={"color": MUTED})])
        ]),

        # Null analysis
        card(border_color=ACCENT5, style_extra={"marginBottom": "14px"}, children=[
            html.Div("📊 Null Value Analysis", style={
                "color": ACCENT5, "fontFamily": "'Courier New', monospace",
                "fontWeight": "700", "fontSize": "13px", "marginBottom": "14px"
            }),
            html.Div(style={"display": "flex", "flexDirection": "column", "gap": "6px"},
                     children=[
                         html.Div(style={"display": "flex", "alignItems": "center", "gap": "10px"},
                                  children=[
                                      html.Span(col, style={"color": TEXT, "fontSize": "12px",
                                                             "fontFamily": "'Courier New', monospace",
                                                             "width": "200px"}),
                                      html.Div(style={"flex": "1", "backgroundColor": "#0d1117",
                                                       "borderRadius": "4px", "height": "6px"},
                                               children=[html.Div(style={
                                                   "backgroundColor": ACCENT3 if null_pct > 0 else ACCENT2,
                                                   "width": f"{null_pct:.1f}%",
                                                   "height": "100%", "borderRadius": "4px"
                                               })]),
                                      html.Span(f"{null_pct:.1f}%",
                                                style={"color": ACCENT3 if null_pct > 0 else MUTED,
                                                       "fontSize": "11px",
                                                       "fontFamily": "'Courier New', monospace",
                                                       "width": "40px", "textAlign": "right"}),
                                  ]) for col, null_pct in (
                             [(col, df[col].isnull().sum() / len(df) * 100)
                              for col in df.columns] if not df.empty else []
                         )
                     ])
        ]),

        # Flagged records explorer
        card(border_color=ACCENT3, children=[
            html.Div(style={"display": "flex", "justifyContent": "space-between",
                            "alignItems": "center", "marginBottom": "14px"},
                     children=[
                         html.Div("🚨 Flagged Records Explorer", style={
                             "color": ACCENT3, "fontFamily": "'Courier New', monospace",
                             "fontWeight": "700", "fontSize": "13px"
                         }),
                         html.Span(f"{len(failed_df):,} total flagged records",
                                   style={"color": ACCENT5, "fontSize": "11px",
                                          "fontFamily": "'Courier New', monospace",
                                          "border": f"1px solid {ACCENT5}",
                                          "borderRadius": "4px", "padding": "2px 8px"})
                         if not failed_df.empty else html.Span()
                     ]),
            html.Div(style={"display": "flex", "gap": "10px", "marginBottom": "12px",
                            "flexWrap": "wrap"},
                     children=[
                         dcc.Dropdown(id="dbg-run-filter", placeholder="Filter by Run...",
                                      clearable=True,
                                      options=[{"label": f"Run: {r}", "value": r}
                                               for r in failed_df["run_identifier"].unique()]
                                      if not failed_df.empty else [],
                                      style={"flex": "1", "minWidth": "200px",
                                             "color": "black", "fontSize": "12px"}),
                         dcc.Dropdown(id="dbg-rule-filter", placeholder="Filter by Rule...",
                                      clearable=True,
                                      options=[{"label": e.replace("expect_","").replace("_"," ").title(),
                                                "value": e}
                                               for e in failed_df["expectation_type"].unique()]
                                      if not failed_df.empty else [],
                                      style={"flex": "1", "minWidth": "200px",
                                             "color": "black", "fontSize": "12px"}),
                     ]),
            dash_table.DataTable(
                id="debug-table",
                data=(failed_df[[c for c in [
                    "run_identifier","expectation_type","column_name","failed_reason",
                    "booking_id","customer_name","email","loyalty_tier",
                    "payment_method","payment_amount"
                ] if c in failed_df.columns]].head(200).to_dict("records"))
                if not failed_df.empty else [],
                columns=[{"name": n, "id": c} for c, n in {
                    "run_identifier":   "Run ID",
                    "expectation_type": "Rule",
                    "column_name":      "Column",
                    "failed_reason":    "Reason",
                    "booking_id":       "Booking",
                    "customer_name":    "Customer",
                    "email":            "Email",
                    "loyalty_tier":     "Tier",
                    "payment_method":   "Payment",
                    "payment_amount":   "Amount ₹",
                }.items() if c in (failed_df.columns if not failed_df.empty else [])],
                style_table={"overflowX": "auto"},
                style_cell={"backgroundColor": CARD_BG, "color": TEXT,
                             "border": f"1px solid {BORDER}",
                             "textAlign": "left", "padding": "9px 12px",
                             "fontSize": "11px", "fontFamily": "'Courier New', monospace",
                             "maxWidth": "180px", "overflow": "hidden",
                             "textOverflow": "ellipsis"},
                style_header={"backgroundColor": "#0a0f1a", "color": ACCENT1,
                               "fontWeight": "700", "fontSize": "11px",
                               "border": f"1px solid {BORDER}",
                               "textTransform": "uppercase", "letterSpacing": "1px"},
                style_data_conditional=[
                    {"if": {"row_index": "odd"}, "backgroundColor": "#0d1117"},
                    {"if": {"filter_query": "{Reason} contains 'Duplicate'"}, "color": ACCENT3},
                    {"if": {"filter_query": "{Reason} contains 'Null'"},      "color": ACCENT5},
                ],
                page_size=15, filter_action="native", sort_action="native",
                tooltip_delay=0, tooltip_duration=None,
            ),
        ]),
    ])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)