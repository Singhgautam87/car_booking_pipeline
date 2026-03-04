import os
import dash
from sqlalchemy import create_engine
from dash import dcc, html, Input, Output, dash_table
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# ============================================================
# DATA CONNECTIONS
# PostgreSQL → booking data (customer_booking_staging)
# MySQL      → validation + SLA monitoring data
# ============================================================
def get_booking_data():
    """PostgreSQL se booking data"""
    host = os.getenv("POSTGRES_HOST", "postgres")
    try:
        engine = create_engine(f"postgresql+psycopg2://admin:admin@{host}:5432/booking")
        df = pd.read_sql("SELECT * FROM customer_booking_staging", engine)
        engine.dispose()
        return df
    except Exception as e:
        print(f"PostgreSQL Error: {e}")
        return pd.DataFrame()

def get_validation_data():
    """MySQL se validation + SLA data"""
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
BORDER   = "#30363d"
ACCENT1  = "#58a6ff"
ACCENT2  = "#3fb950"
ACCENT3  = "#f78166"
ACCENT4  = "#d2a8ff"
ACCENT5  = "#ffa657"
TEXT     = "#e6edf3"
SUBTEXT  = "#8b949e"

CHART_BASE = dict(
    paper_bgcolor=CARD_BG,
    plot_bgcolor=CARD_BG,
    font=dict(color=TEXT, family="'Courier New', monospace"),
    margin=dict(l=40, r=20, t=50, b=40),
    title_font=dict(color=TEXT, size=14),
)

# ============================================================
# HELPERS
# ============================================================
def card(children, border_color=BORDER, style_extra=None):
    s = {
        "backgroundColor": CARD_BG,
        "borderRadius": "12px",
        "border": f"1px solid {border_color}",
        "padding": "20px",
    }
    if style_extra:
        s.update(style_extra)
    return html.Div(style=s, children=children)

def kpi_card(kpi_id, label, color, icon):
    return html.Div(
        style={
            "backgroundColor": CARD_BG,
            "borderRadius": "12px",
            "border": f"1px solid {color}",
            "padding": "22px 18px",
            "flex": "1",
            "minWidth": "140px",
            "position": "relative",
            "overflow": "hidden",
        },
        children=[
            html.Div(icon, style={"fontSize": "28px", "marginBottom": "8px", "lineHeight": "1"}),
            html.Div(id=kpi_id, style={
                "color": color, "fontSize": "26px",
                "fontWeight": "900", "fontFamily": "'Courier New', monospace",
                "lineHeight": "1", "marginBottom": "6px"
            }),
            html.Div(label, style={
                "color": SUBTEXT, "fontSize": "11px",
                "textTransform": "uppercase", "letterSpacing": "1px"
            }),
            html.Div(style={
                "position": "absolute", "right": "-15px", "bottom": "-15px",
                "width": "70px", "height": "70px", "borderRadius": "50%",
                "backgroundColor": color, "opacity": "0.07"
            })
        ]
    )

def section_header(icon, title, subtitle=None):
    return html.Div(
        style={"marginBottom": "20px", "borderLeft": f"4px solid {ACCENT1}", "paddingLeft": "16px"},
        children=[
            html.H2(f"{icon}  {title}", style={
                "color": TEXT, "fontSize": "18px",
                "margin": "0 0 4px 0", "fontFamily": "'Courier New', monospace"
            }),
            html.P(subtitle, style={"color": SUBTEXT, "fontSize": "12px", "margin": "0"}) if subtitle else None
        ]
    )

def empty_fig(title="No data"):
    fig = go.Figure()
    fig.add_annotation(
        text="📭 No data available",
        xref="paper", yref="paper", x=0.5, y=0.5,
        showarrow=False, font={"color": SUBTEXT, "size": 14}
    )
    fig.update_layout(**CHART_BASE, title=title)
    return fig

# ============================================================
# APP
# ============================================================
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "🚗 Car Booking Pipeline Monitor"

# ============================================================
# LAYOUT
# ============================================================
app.layout = html.Div(
    style={"backgroundColor": BG, "minHeight": "100vh", "padding": "0",
           "fontFamily": "'Segoe UI', sans-serif", "color": TEXT},
    children=[

        # ── TOP NAV ──────────────────────────────────────────
        html.Div(
            style={
                "backgroundColor": CARD_BG,
                "borderBottom": f"1px solid {BORDER}",
                "padding": "0 32px",
                "display": "flex", "alignItems": "center",
                "justifyContent": "space-between",
                "height": "60px", "position": "sticky", "top": "0", "zIndex": "100",
            },
            children=[
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "12px"},
                    children=[
                        html.Span("🚗", style={"fontSize": "22px"}),
                        html.Span("Car Booking", style={
                            "color": TEXT, "fontWeight": "700", "fontSize": "15px",
                            "fontFamily": "'Courier New', monospace"
                        }),
                        html.Span("Pipeline Monitor", style={"color": SUBTEXT, "fontSize": "13px"}),
                        html.Span("·", style={"color": BORDER, "margin": "0 4px"}),
                        html.Span("PostgreSQL + MySQL", style={
                            "color": ACCENT1, "fontSize": "11px",
                            "fontFamily": "'Courier New', monospace",
                            "border": f"1px solid {ACCENT1}",
                            "borderRadius": "4px", "padding": "2px 8px"
                        }),
                    ]
                ),
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "14px"},
                    children=[
                        html.Span(id="last-updated", style={
                            "color": SUBTEXT, "fontSize": "11px",
                            "fontFamily": "'Courier New', monospace"
                        }),
                        html.Button("⟳ Refresh", id="refresh-btn", style={
                            "backgroundColor": ACCENT1, "color": "#0d1117",
                            "border": "none", "padding": "7px 18px",
                            "borderRadius": "6px", "cursor": "pointer",
                            "fontWeight": "700", "fontSize": "13px",
                        }),
                    ]
                ),
                dcc.Interval(id="auto-refresh", interval=30000, n_intervals=0),
            ]
        ),

        # ── PIPELINE FLOW BAR ─────────────────────────────────
        html.Div(
            style={
                "backgroundColor": "#0a0f1a",
                "borderBottom": f"1px solid {BORDER}",
                "padding": "10px 32px",
                "display": "flex", "alignItems": "center", "gap": "6px",
                "fontSize": "12px", "color": SUBTEXT,
                "fontFamily": "'Courier New', monospace",
                "overflowX": "auto", "whiteSpace": "nowrap",
            },
            children=[
                html.Span("FLOW:", style={"color": ACCENT1, "fontWeight": "700", "marginRight": "6px"}),
                *[html.Span(s, style={"color": ACCENT2 if i % 2 == 0 else SUBTEXT})
                  for i, s in enumerate([
                      "Kafka", " → ", "Spark Streaming", " → ",
                      "Delta Lake (MinIO)", " → ", "PostgreSQL Staging",
                      " → ", "Great Expectations", " → ",
                      "MySQL Monitoring", " → ", "Dashboard"
                  ])],
            ]
        ),

        # ── MAIN CONTENT ─────────────────────────────────────
        html.Div(style={"padding": "28px 32px"}, children=[

            # ══ SECTION 1: PIPELINE OVERVIEW ══════════════════
            section_header("📦", "Pipeline Overview",
                           "Live metrics from PostgreSQL → customer_booking_staging"),
            html.Div(
                style={"display": "flex", "gap": "14px", "marginBottom": "32px", "flexWrap": "wrap"},
                children=[
                    kpi_card("kpi-bookings",  "Total Bookings",   ACCENT1, "📋"),
                    kpi_card("kpi-customers", "Unique Customers", ACCENT2, "👤"),
                    kpi_card("kpi-revenue",   "Total Revenue",    ACCENT3, "💰"),
                    kpi_card("kpi-cars",      "Unique Cars",      ACCENT5, "🚘"),
                    kpi_card("kpi-integrity", "Data Integrity",   ACCENT4, "🛡️"),
                ]
            ),

            # ══ SECTION 2: BOOKING ANALYTICS ══════════════════
            section_header("📊", "Booking Analytics",
                           "Trends, patterns & customer insights from PostgreSQL"),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                       "gap": "16px", "marginBottom": "16px"},
                children=[
                    card([dcc.Graph(id="bookings-per-day", config={"displayModeBar": False})]),
                    card([dcc.Graph(id="revenue-per-day",  config={"displayModeBar": False})]),
                ]
            ),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr 1fr",
                       "gap": "16px", "marginBottom": "16px"},
                children=[
                    card([dcc.Graph(id="popular-cars",    config={"displayModeBar": False})]),
                    card([dcc.Graph(id="payment-methods", config={"displayModeBar": False})]),
                    card([dcc.Graph(id="loyalty-tiers",   config={"displayModeBar": False})]),
                ]
            ),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr 1fr",
                       "gap": "16px", "marginBottom": "32px"},
                children=[
                    card([dcc.Graph(id="insurance-coverage", config={"displayModeBar": False})]),
                    card([dcc.Graph(id="pickup-locations",   config={"displayModeBar": False})]),
                    card([dcc.Graph(id="drop-locations",     config={"displayModeBar": False})]),
                ]
            ),

            # ══ SECTION 3: SLA PIPELINE MONITORING ════════════
            section_header("⚡", "Pipeline SLA Monitor",
                           "Stage-wise duration & record tracking — MySQL → pipeline_run_stats"),
            html.Div(
                style={"display": "flex", "gap": "14px", "marginBottom": "20px", "flexWrap": "wrap"},
                children=[
                    kpi_card("sla-total-runs",    "Total Runs",        ACCENT1, "🔄"),
                    kpi_card("sla-avg-duration",  "Avg Duration",      ACCENT2, "⏱️"),
                    kpi_card("sla-total-records", "Records Processed", ACCENT5, "📦"),
                    kpi_card("sla-success-rate",  "Stage Success Rate",ACCENT4, "✅"),
                ]
            ),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                       "gap": "16px", "marginBottom": "16px"},
                children=[
                    card([dcc.Graph(id="sla-stage-duration", config={"displayModeBar": False})]),
                    card([dcc.Graph(id="sla-stage-records",  config={"displayModeBar": False})]),
                ]
            ),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "2fr 1fr",
                       "gap": "16px", "marginBottom": "16px"},
                children=[
                    card([dcc.Graph(id="sla-run-timeline", config={"displayModeBar": False})]),
                    card([dcc.Graph(id="sla-status-pie",   config={"displayModeBar": False})]),
                ]
            ),

            # SLA Latest Run Table
            card(
                border_color=ACCENT1,
                style_extra={"marginBottom": "32px"},
                children=[
                    html.Div(
                        style={"display": "flex", "justifyContent": "space-between",
                               "alignItems": "center", "marginBottom": "14px"},
                        children=[
                            html.Span("🕐 Latest Pipeline Run — Stage Breakdown", style={
                                "color": TEXT, "fontFamily": "'Courier New', monospace",
                                "fontWeight": "700", "fontSize": "14px"
                            }),
                            html.Span(id="sla-run-id-badge", style={
                                "color": ACCENT1, "fontSize": "12px",
                                "fontFamily": "'Courier New', monospace",
                                "border": f"1px solid {ACCENT1}",
                                "borderRadius": "4px", "padding": "3px 10px"
                            }),
                        ]
                    ),
                    dash_table.DataTable(
                        id="sla-table",
                        style_table={"overflowX": "auto"},
                        style_cell={
                            "backgroundColor": CARD_BG, "color": TEXT,
                            "border": f"1px solid {BORDER}",
                            "textAlign": "left", "padding": "10px 14px",
                            "fontSize": "12px", "fontFamily": "'Courier New', monospace",
                        },
                        style_header={
                            "backgroundColor": "#0a0f1a", "color": ACCENT1,
                            "fontWeight": "700", "fontSize": "12px",
                            "border": f"1px solid {BORDER}",
                            "textTransform": "uppercase", "letterSpacing": "1px",
                        },
                        style_data_conditional=[
                            {"if": {"row_index": "odd"}, "backgroundColor": "#0d1117"},
                            {"if": {"filter_query": "{Status} = SUCCESS"}, "color": ACCENT2},
                            {"if": {"filter_query": "{Status} = FAILED"},  "color": ACCENT3},
                        ],
                        page_size=10,
                        sort_action="native",
                    ),
                ]
            ),

            # ══ SECTION 4: DATA VALIDATION ════════════════════
            section_header("🔍", "Data Validation Report",
                           "Great Expectations quality checks — MySQL → validation tables"),
            html.Div(
                style={"display": "flex", "gap": "14px", "marginBottom": "20px", "flexWrap": "wrap"},
                children=[
                    kpi_card("val-runs",           "Validation Runs",  ACCENT2, "🔄"),
                    kpi_card("val-rules-passed",   "Rules Passed",     ACCENT2, "✅"),
                    kpi_card("val-rules-violated", "Rules Violated",   ACCENT3, "❌"),
                    kpi_card("val-anomalous",      "Anomalous Records",ACCENT5, "⚠️"),
                ]
            ),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 2fr",
                       "gap": "16px", "marginBottom": "16px"},
                children=[
                    card([dcc.Graph(id="val-gauge",       config={"displayModeBar": False})]),
                    card([dcc.Graph(id="val-run-history", config={"displayModeBar": False})]),
                ]
            ),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                       "gap": "16px", "marginBottom": "16px"},
                children=[
                    card([dcc.Graph(id="val-checks-passfail",  config={"displayModeBar": False})]),
                    card([dcc.Graph(id="val-column-anomalies", config={"displayModeBar": False})]),
                ]
            ),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                       "gap": "16px", "marginBottom": "32px"},
                children=[
                    card([dcc.Graph(id="val-anomalous-loyalty", config={"displayModeBar": False})]),
                    card([dcc.Graph(id="val-payment-dist",      config={"displayModeBar": False})]),
                ]
            ),

            # ══ SECTION 5: ANOMALOUS RECORDS EXPLORER ═════════
            section_header("🚨", "Anomalous Records Explorer",
                           "Filter & investigate failed validation records"),
            card(
                border_color=ACCENT3,
                children=[
                    html.Div(
                        style={"display": "flex", "gap": "14px", "marginBottom": "16px", "flexWrap": "wrap"},
                        children=[
                            html.Div(style={"flex": "1", "minWidth": "200px"}, children=[
                                html.Label("Filter by Pipeline Run", style={
                                    "color": SUBTEXT, "fontSize": "11px",
                                    "display": "block", "marginBottom": "6px",
                                    "textTransform": "uppercase", "letterSpacing": "1px"
                                }),
                                dcc.Dropdown(id="run-dropdown", placeholder="All runs...",
                                             clearable=True, style={"color": "black", "fontSize": "13px"}),
                            ]),
                            html.Div(style={"flex": "1", "minWidth": "200px"}, children=[
                                html.Label("Filter by Validation Rule", style={
                                    "color": SUBTEXT, "fontSize": "11px",
                                    "display": "block", "marginBottom": "6px",
                                    "textTransform": "uppercase", "letterSpacing": "1px"
                                }),
                                dcc.Dropdown(id="check-dropdown", placeholder="All rules...",
                                             clearable=True, style={"color": "black", "fontSize": "13px"}),
                            ]),
                            html.Div(style={"display": "flex", "alignItems": "flex-end"}, children=[
                                html.Div(id="record-count-badge", style={
                                    "backgroundColor": "#1c2d3a",
                                    "border": f"1px solid {ACCENT1}",
                                    "borderRadius": "20px", "padding": "6px 16px",
                                    "color": ACCENT1, "fontSize": "12px",
                                    "fontFamily": "'Courier New', monospace", "fontWeight": "700",
                                })
                            ]),
                        ]
                    ),
                    dash_table.DataTable(
                        id="anomalous-table",
                        style_table={"overflowX": "auto", "borderRadius": "8px"},
                        style_cell={
                            "backgroundColor": CARD_BG, "color": TEXT,
                            "border": f"1px solid {BORDER}",
                            "textAlign": "left", "padding": "10px 14px",
                            "fontSize": "12px", "fontFamily": "'Courier New', monospace",
                            "maxWidth": "200px", "overflow": "hidden", "textOverflow": "ellipsis",
                        },
                        style_header={
                            "backgroundColor": "#0a0f1a", "color": ACCENT1,
                            "fontWeight": "700", "fontSize": "12px",
                            "border": f"1px solid {BORDER}",
                            "textTransform": "uppercase", "letterSpacing": "1px",
                        },
                        style_data_conditional=[
                            {"if": {"row_index": "odd"}, "backgroundColor": "#0d1117"},
                            {"if": {"filter_query": "{failed_reason} contains 'Duplicate'"}, "color": ACCENT3},
                            {"if": {"filter_query": "{failed_reason} contains 'Null'"}, "color": ACCENT5},
                        ],
                        page_size=15, filter_action="native", sort_action="native",
                        tooltip_delay=0, tooltip_duration=None,
                    ),
                ]
            ),

            # ── FOOTER ───────────────────────────────────────
            html.Div(
                style={
                    "textAlign": "center", "marginTop": "40px",
                    "paddingTop": "20px", "borderTop": f"1px solid {BORDER}",
                    "color": SUBTEXT, "fontSize": "12px",
                    "fontFamily": "'Courier New', monospace",
                },
                children=[
                    html.Span("Car Booking Pipeline Monitor  ·  Auto-refresh every 30s  ·  "),
                    html.Span("Kafka → Spark → Delta Lake → PostgreSQL → MySQL",
                              style={"color": ACCENT1}),
                ]
            ),
        ]),
    ]
)


# ============================================================
# MAIN CALLBACK
# ============================================================
@app.callback(
    [
        Output("last-updated",          "children"),
        Output("kpi-bookings",          "children"),
        Output("kpi-customers",         "children"),
        Output("kpi-revenue",           "children"),
        Output("kpi-cars",              "children"),
        Output("kpi-integrity",         "children"),
        Output("bookings-per-day",      "figure"),
        Output("revenue-per-day",       "figure"),
        Output("popular-cars",          "figure"),
        Output("payment-methods",       "figure"),
        Output("loyalty-tiers",         "figure"),
        Output("insurance-coverage",    "figure"),
        Output("pickup-locations",      "figure"),
        Output("drop-locations",        "figure"),
        Output("sla-total-runs",        "children"),
        Output("sla-avg-duration",      "children"),
        Output("sla-total-records",     "children"),
        Output("sla-success-rate",      "children"),
        Output("sla-stage-duration",    "figure"),
        Output("sla-stage-records",     "figure"),
        Output("sla-run-timeline",      "figure"),
        Output("sla-status-pie",        "figure"),
        Output("val-runs",              "children"),
        Output("val-rules-passed",      "children"),
        Output("val-rules-violated",    "children"),
        Output("val-anomalous",         "children"),
        Output("val-gauge",             "figure"),
        Output("val-run-history",       "figure"),
        Output("val-checks-passfail",   "figure"),
        Output("val-column-anomalies",  "figure"),
        Output("val-anomalous-loyalty", "figure"),
        Output("val-payment-dist",      "figure"),
        Output("run-dropdown",          "options"),
        Output("check-dropdown",        "options"),
    ],
    [Input("refresh-btn",  "n_clicks"),
     Input("auto-refresh", "n_intervals")]
)
def update_all(n_clicks, n_intervals):
    booking_df = get_booking_data()
    validation_df, expectation_df, failed_df, sla_df = get_validation_data()

    last_updated = f"↻  {pd.Timestamp.now().strftime('%d %b %Y  %H:%M:%S')}"

    # ── PIPELINE KPIs ──
    kpi_bookings  = f"{booking_df['booking_id'].nunique():,}"     if not booking_df.empty else "—"
    kpi_customers = f"{booking_df['customer_id'].nunique():,}"    if not booking_df.empty else "—"
    kpi_revenue   = f"₹{booking_df['payment_amount'].sum():,.0f}" if not booking_df.empty else "—"
    kpi_cars      = f"{booking_df['car_id'].nunique():,}"         if not booking_df.empty else "—"
    kpi_integrity = f"{validation_df['success_rate'].mean():.1f}%" if not validation_df.empty else "—"

    # ── BOOKING CHARTS ──
    def booking_chart(col, groupby, agg, title, chart_type, color):
        if booking_df.empty or groupby not in booking_df.columns:
            return empty_fig(title)
        df = booking_df.groupby(groupby)[col].agg(agg).reset_index()
        df.columns = ["x", "y"]
        if chart_type == "bar":
            fig = px.bar(df, x="x", y="y", title=title, color_discrete_sequence=[color])
            fig.update_traces(marker_line_width=0)
        else:
            fig = px.area(df, x="x", y="y", title=title, color_discrete_sequence=[color])
            fig.update_traces(line_color=color, fillcolor=f"rgba(63,185,80,0.12)")
        fig.update_layout(**CHART_BASE)
        return fig

    fig1 = booking_chart("booking_id", "booking_date", "nunique", "📅 Daily Bookings",  "bar",  ACCENT1)
    fig2 = booking_chart("payment_amount", "booking_date", "sum",  "💰 Daily Revenue",   "area", ACCENT2)

    def hbar(col, title, color):
        if booking_df.empty or col not in booking_df.columns:
            return empty_fig(title)
        df = booking_df[col].value_counts().head(8).reset_index()
        df.columns = ["label", "count"]
        fig = px.bar(df, x="count", y="label", orientation="h", title=title,
                     color="count", color_continuous_scale=[[0,"#1c2d3a"],[1,color]])
        fig.update_layout(**CHART_BASE, coloraxis_showscale=False,
                          yaxis={"categoryorder": "total ascending"})
        return fig

    fig3 = hbar("model",         "🚗 Top Car Models",        ACCENT1)
    fig7 = hbar("pickup_location","📍 Top Pickup Locations",  ACCENT5)
    fig8 = hbar("drop_location",  "🏁 Top Drop Locations",    ACCENT4)

    def pie_chart(col, title, colors, hole=0.45):
        if booking_df.empty or col not in booking_df.columns:
            return empty_fig(title)
        df = booking_df[col].value_counts().reset_index()
        df.columns = ["label", "count"]
        fig = px.pie(df, names="label", values="count", title=title,
                     color_discrete_sequence=colors, hole=hole)
        fig.update_layout(**CHART_BASE)
        return fig

    fig4 = pie_chart("payment_method",    "💳 Payment Methods",   [ACCENT1,ACCENT2,ACCENT3,ACCENT5,ACCENT4])
    fig6 = pie_chart("insurance_coverage","🛡️ Insurance Coverage", [ACCENT2, ACCENT3])

    if not booking_df.empty and "loyalty_tier" in booking_df.columns:
        lt = booking_df["loyalty_tier"].value_counts().reset_index()
        lt.columns = ["tier", "count"]
        fig5 = px.bar(lt, x="tier", y="count", title="⭐ Loyalty Tiers", color="tier",
                      color_discrete_map={"platinum":ACCENT4,"gold":ACCENT5,"silver":"#8b949e","bronze":ACCENT3})
        fig5.update_layout(**CHART_BASE, showlegend=False)
    else:
        fig5 = empty_fig("⭐ Loyalty Tiers")

    # ── SLA KPIs ──
    if not sla_df.empty:
        sla_total_runs    = f"{sla_df['run_id'].nunique():,}"
        sla_avg_duration  = f"{sla_df['duration_seconds'].mean():.0f}s"
        sla_total_records = f"{sla_df['records_processed'].sum():,}"
        sla_success_rate  = f"{len(sla_df[sla_df['status']=='SUCCESS'])/len(sla_df)*100:.0f}%"
    else:
        sla_total_runs = sla_avg_duration = sla_total_records = sla_success_rate = "—"

    if not sla_df.empty:
        sa = sla_df.groupby("stage_name")["duration_seconds"].mean().reset_index()
        sa.columns = ["Stage", "Avg Duration (s)"]
        sa["Stage"] = sa["Stage"].str.replace("_"," ").str.title()
        fig_sla_dur = px.bar(sa, x="Stage", y="Avg Duration (s)",
                             title="⏱️ Avg Stage Duration (seconds)",
                             color="Avg Duration (s)",
                             color_continuous_scale=[[0,"#1c2d3a"],[1,ACCENT1]])
        fig_sla_dur.update_layout(**CHART_BASE, coloraxis_showscale=False)

        sr = sla_df.groupby("stage_name")["records_processed"].max().reset_index()
        sr.columns = ["Stage", "Records"]
        sr["Stage"] = sr["Stage"].str.replace("_"," ").str.title()
        sr = sr[sr["Records"] > 0]
        fig_sla_rec = px.bar(sr, x="Stage", y="Records",
                             title="📦 Records Processed per Stage",
                             color="Records",
                             color_continuous_scale=[[0,"#1c2d3a"],[1,ACCENT2]])
        fig_sla_rec.update_layout(**CHART_BASE, coloraxis_showscale=False)

        tl = sla_df.copy()
        tl["Stage"] = tl["stage_name"].str.replace("_"," ").str.title()
        fig_timeline = px.bar(tl, x="started_at", y="duration_seconds",
                              color="Stage", title="📅 Pipeline Run Timeline",
                              barmode="group",
                              color_discrete_sequence=[ACCENT1,ACCENT2,ACCENT3,ACCENT5,ACCENT4,"#79c0ff"])
        fig_timeline.update_layout(**CHART_BASE)

        sc = sla_df["status"].value_counts().reset_index()
        sc.columns = ["status", "count"]
        fig_status = px.pie(sc, names="status", values="count",
                            title="✅ Stage Status Distribution",
                            color="status",
                            color_discrete_map={"SUCCESS":ACCENT2,"FAILED":ACCENT3,"SKIPPED":SUBTEXT},
                            hole=0.5)
        fig_status.update_layout(**CHART_BASE)
    else:
        fig_sla_dur = empty_fig("⏱️ Avg Stage Duration")
        fig_sla_rec = empty_fig("📦 Records per Stage")
        fig_timeline = empty_fig("📅 Pipeline Run Timeline")
        fig_status   = empty_fig("✅ Stage Status")

    # ── VALIDATION KPIs ──
    val_runs      = f"{len(validation_df):,}"                                   if not validation_df.empty else "0"
    val_passed    = f"{int(expectation_df['success'].sum()):,}"                 if not expectation_df.empty else "0"
    val_violated  = f"{int((~expectation_df['success'].astype(bool)).sum()):,}" if not expectation_df.empty else "0"
    val_anomalous = f"{len(failed_df):,}"                                       if not failed_df.empty else "0"

    avg_rate    = float(validation_df["success_rate"].mean()) if not validation_df.empty else 0
    gauge_color = ACCENT2 if avg_rate >= 90 else (ACCENT5 if avg_rate >= 70 else ACCENT3)
    fig_gauge   = go.Figure(go.Indicator(
        mode="gauge+number", value=avg_rate,
        title={"text": "Data Integrity Score", "font": {"color": TEXT, "size": 13}},
        number={"suffix": "%", "font": {"color": gauge_color, "size": 36}},
        gauge={
            "axis": {"range": [0,100], "tickcolor": SUBTEXT, "tickfont": {"color": SUBTEXT, "size": 10}},
            "bar": {"color": gauge_color, "thickness": 0.25},
            "bgcolor": CARD_BG, "borderwidth": 0,
            "steps": [
                {"range": [0,  70], "color": "#1c1f26"},
                {"range": [70, 90], "color": "#1c2416"},
                {"range": [90,100], "color": "#162414"},
            ],
            "threshold": {"line": {"color": "white", "width": 2}, "thickness": 0.75, "value": 100}
        }
    ))
    fig_gauge.update_layout(**CHART_BASE)

    if not validation_df.empty and "created_at" in validation_df.columns:
        fig_hist = px.line(validation_df, x="created_at", y="success_rate",
                           title="📈 Validation Run History",
                           color_discrete_sequence=[ACCENT2], markers=True)
        fig_hist.add_hline(y=100, line_dash="dot", line_color=ACCENT3,
                           annotation_text="100% target", annotation_font_color=ACCENT3)
        fig_hist.update_traces(line_width=2, marker_size=8)
        fig_hist.update_layout(**CHART_BASE, yaxis_range=[0,105])
    else:
        fig_hist = empty_fig("📈 Validation Run History")

    if not expectation_df.empty:
        es = expectation_df.groupby("expectation_type")["success"].agg(
            Passed=lambda x: int(x.astype(bool).sum()),
            Violated=lambda x: int((~x.astype(bool)).sum())
        ).reset_index()
        em = es.melt(id_vars="expectation_type", value_vars=["Passed","Violated"],
                     var_name="Status", value_name="Count")
        em["Rule"] = em["expectation_type"].str.replace("expect_","").str.replace("_"," ").str.title()
        fig_checks = px.bar(em, x="Rule", y="Count", color="Status", barmode="group",
                            title="✅ Validation Rules — Pass vs Violated",
                            color_discrete_map={"Passed": ACCENT2, "Violated": ACCENT3})
        fig_checks.update_layout(**CHART_BASE, xaxis_tickangle=-30)

        cf = expectation_df[~expectation_df["success"].astype(bool)].groupby(
            "column_name")["unexpected_count"].sum().reset_index()
        cf.columns = ["column", "anomalies"]
        cf = cf.sort_values("anomalies", ascending=True)
        fig_col = px.bar(cf, x="anomalies", y="column", orientation="h",
                         title="⚠️ Anomalies by Column",
                         color="anomalies", color_continuous_scale=[[0,"#1c2d3a"],[1,ACCENT3]])
        fig_col.update_layout(**CHART_BASE, coloraxis_showscale=False)
    else:
        fig_checks = empty_fig("✅ Validation Rules")
        fig_col    = empty_fig("⚠️ Anomalies by Column")

    if not failed_df.empty and "loyalty_tier" in failed_df.columns:
        fl = failed_df["loyalty_tier"].value_counts().reset_index()
        fl.columns = ["tier", "count"]
        fig_loy = px.pie(fl, names="tier", values="count",
                         title="👤 Anomalous — Customer Segments", color="tier",
                         color_discrete_map={"platinum":ACCENT4,"gold":ACCENT5,"silver":"#8b949e","bronze":ACCENT3},
                         hole=0.45)
        fig_loy.update_layout(**CHART_BASE)
    else:
        fig_loy = empty_fig("👤 Anomalous — Customer Segments")

    if not failed_df.empty and "payment_amount" in failed_df.columns:
        fig_pay = px.histogram(failed_df, x="payment_amount",
                               title="💸 Anomalous — Payment Distribution",
                               nbins=30, color_discrete_sequence=[ACCENT3])
        fig_pay.update_traces(marker_line_width=0)
        fig_pay.update_layout(**CHART_BASE)
    else:
        fig_pay = empty_fig("💸 Anomalous — Payment Distribution")

    run_opts = [{"label": f"Run: {r}", "value": r}
                for r in failed_df["run_identifier"].unique()] if not failed_df.empty else []
    chk_opts = [{"label": e.replace("expect_","").replace("_"," ").title(), "value": e}
                for e in failed_df["expectation_type"].unique()] if not failed_df.empty else []

    return (
        last_updated,
        kpi_bookings, kpi_customers, kpi_revenue, kpi_cars, kpi_integrity,
        fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8,
        sla_total_runs, sla_avg_duration, sla_total_records, sla_success_rate,
        fig_sla_dur, fig_sla_rec, fig_timeline, fig_status,
        val_runs, val_passed, val_violated, val_anomalous,
        fig_gauge, fig_hist, fig_checks, fig_col, fig_loy, fig_pay,
        run_opts, chk_opts,
    )


# ============================================================
# SLA TABLE CALLBACK
# ============================================================
@app.callback(
    [Output("sla-table",        "data"),
     Output("sla-table",        "columns"),
     Output("sla-run-id-badge", "children")],
    [Input("refresh-btn",  "n_clicks"),
     Input("auto-refresh", "n_intervals")]
)
def update_sla_table(n_clicks, n_intervals):
    _, _, _, sla_df = get_validation_data()
    if sla_df.empty:
        return [], [], "No runs yet"

    latest_run = sla_df["run_id"].iloc[0]
    df = sla_df[sla_df["run_id"] == latest_run].copy()
    df["stage_name"] = df["stage_name"].str.replace("_"," ").str.title()
    df["duration"]   = df["duration_seconds"].astype(str) + "s"

    show_cols = [c for c in ["stage_name","records_processed","duration","status","started_at"] if c in df.columns]
    col_rename = {
        "stage_name":        "Stage",
        "records_processed": "Records",
        "duration":          "Duration",
        "status":            "Status",
        "started_at":        "Started At",
    }
    columns = [{"name": col_rename.get(c,c), "id": c} for c in show_cols]
    return df[show_cols].to_dict("records"), columns, f"Run ID: {latest_run}"


# ============================================================
# ANOMALOUS TABLE CALLBACK
# ============================================================
@app.callback(
    [Output("anomalous-table",    "data"),
     Output("anomalous-table",    "columns"),
     Output("anomalous-table",    "tooltip_data"),
     Output("record-count-badge", "children")],
    [Input("run-dropdown",   "value"),
     Input("check-dropdown", "value"),
     Input("refresh-btn",    "n_clicks"),
     Input("auto-refresh",   "n_intervals")]
)
def update_table(run_id, check_type, n_clicks, n_intervals):
    _, _, failed_df, _ = get_validation_data()
    if failed_df.empty:
        return [], [], [], "0 records"

    df = failed_df.copy()
    if run_id:
        df = df[df["run_identifier"] == run_id]
    if check_type:
        df = df[df["expectation_type"] == check_type]

    show_cols = ["run_identifier","expectation_type","column_name","failed_reason",
                 "booking_id","customer_name","email","loyalty_tier",
                 "pickup_location","drop_location","payment_method","payment_amount"]
    show_cols = [c for c in show_cols if c in df.columns]
    df_show   = df[show_cols].head(200)

    col_rename = {
        "run_identifier":   "Run ID",    "expectation_type": "Validation Rule",
        "column_name":      "Column",    "failed_reason":    "Reason",
        "booking_id":       "Booking ID","customer_name":    "Customer",
        "email":            "Email",     "loyalty_tier":     "Tier",
        "pickup_location":  "Pickup",    "drop_location":    "Drop",
        "payment_method":   "Payment",   "payment_amount":   "Amount (₹)",
    }
    columns      = [{"name": col_rename.get(c,c), "id": c} for c in show_cols]
    tooltip_data = [{c: {"value": str(row[c]), "type": "markdown"} for c in show_cols}
                    for row in df_show.to_dict("records")]

    return df_show.to_dict("records"), columns, tooltip_data, f"Showing {len(df_show):,} of {len(df):,} records"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)