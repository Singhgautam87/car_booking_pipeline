import os
import dash
from sqlalchemy import create_engine
from dash import dcc, html, Input, Output, dash_table
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# ============ MYSQL CONNECTION ============
def get_data():
    host = os.getenv("MYSQL_HOST", "localhost")
    engine = create_engine(f"mysql+mysqlconnector://admin:admin@{host}:3306/booking")
    try:
        booking_df    = pd.read_sql("SELECT * FROM customer_booking_final", engine)
        validation_df = pd.read_sql("SELECT * FROM validation_results", engine)
        expectation_df= pd.read_sql("SELECT * FROM validation_expectation_details", engine)
        failed_df     = pd.read_sql("SELECT * FROM validation_failed_records", engine)
    except Exception as e:
        print(f"DB Error: {e}")
        booking_df = validation_df = expectation_df = failed_df = pd.DataFrame()
    engine.dispose()
    return booking_df, validation_df, expectation_df, failed_df

# ============ APP ============
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "🚗 Car Booking Pipeline"

# ============ THEME ============
BG        = "#0d1117"
CARD_BG   = "#161b22"
BORDER    = "#30363d"
ACCENT1   = "#58a6ff"   # blue
ACCENT2   = "#3fb950"   # green
ACCENT3   = "#f78166"   # red/orange
ACCENT4   = "#d2a8ff"   # purple
ACCENT5   = "#ffa657"   # orange
TEXT      = "#e6edf3"
SUBTEXT   = "#8b949e"

CHART_BASE = dict(
    paper_bgcolor=CARD_BG,
    plot_bgcolor=CARD_BG,
    font=dict(color=TEXT, family="'Courier New', monospace"),
    margin=dict(l=40, r=20, t=50, b=40),
    title_font=dict(color=TEXT, size=14),
)

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
            html.Div(icon, style={
                "fontSize": "32px", "marginBottom": "10px",
                "lineHeight": "1"
            }),
            html.Div(id=kpi_id, style={
                "color": color, "fontSize": "28px",
                "fontWeight": "900", "fontFamily": "'Courier New', monospace",
                "lineHeight": "1", "marginBottom": "6px"
            }),
            html.Div(label, style={
                "color": SUBTEXT, "fontSize": "12px",
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
        style={"marginBottom": "20px", "borderLeft": f"4px solid {ACCENT1}",
               "paddingLeft": "16px"},
        children=[
            html.H2(f"{icon} {title}", style={
                "color": TEXT, "fontSize": "20px",
                "margin": "0 0 4px 0", "fontFamily": "'Courier New', monospace"
            }),
            html.P(subtitle, style={"color": SUBTEXT, "fontSize": "13px", "margin": "0"}) if subtitle else None
        ]
    )

def empty_fig(title="No data"):
    fig = go.Figure()
    fig.add_annotation(
        text="📭 No data available",
        xref="paper", yref="paper", x=0.5, y=0.5,
        showarrow=False, font={"color": SUBTEXT, "size": 15}
    )
    fig.update_layout(**CHART_BASE, title=title)
    return fig

# ============ LAYOUT ============
app.layout = html.Div(
    style={
        "backgroundColor": BG,
        "minHeight": "100vh",
        "padding": "0",
        "fontFamily": "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
        "color": TEXT,
    },
    children=[

        # ── TOP NAV ──────────────────────────────────────────
        html.Div(
            style={
                "backgroundColor": CARD_BG,
                "borderBottom": f"1px solid {BORDER}",
                "padding": "0 32px",
                "display": "flex",
                "alignItems": "center",
                "justifyContent": "space-between",
                "height": "60px",
                "position": "sticky",
                "top": "0",
                "zIndex": "100",
            },
            children=[
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "12px"},
                    children=[
                        html.Div("🚗", style={"fontSize": "24px"}),
                        html.Span("Car Booking", style={
                            "color": TEXT, "fontWeight": "700",
                            "fontSize": "16px", "fontFamily": "'Courier New', monospace"
                        }),
                        html.Span("Pipeline Monitor", style={
                            "color": SUBTEXT, "fontSize": "13px", "marginLeft": "4px"
                        }),
                    ]
                ),
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "16px"},
                    children=[
                        html.Span(id="last-updated", style={
                            "color": SUBTEXT, "fontSize": "12px",
                            "fontFamily": "'Courier New', monospace"
                        }),
                        html.Button(
                            "⟳ Refresh",
                            id="refresh-btn",
                            style={
                                "backgroundColor": ACCENT1,
                                "color": "#0d1117",
                                "border": "none",
                                "padding": "7px 18px",
                                "borderRadius": "6px",
                                "cursor": "pointer",
                                "fontWeight": "700",
                                "fontSize": "13px",
                            }
                        ),
                    ]
                ),
                dcc.Interval(id="auto-refresh", interval=30000, n_intervals=0),
            ]
        ),

        # ── PIPELINE STATUS BAR ───────────────────────────────
        html.Div(
            style={
                "backgroundColor": "#0d1f2d",
                "borderBottom": f"1px solid {BORDER}",
                "padding": "10px 32px",
                "display": "flex",
                "alignItems": "center",
                "gap": "8px",
                "fontSize": "12px",
                "color": SUBTEXT,
                "fontFamily": "'Courier New', monospace",
                "overflowX": "auto",
                "whiteSpace": "nowrap",
            },
            children=[
                html.Span("PIPELINE:", style={"color": ACCENT1, "fontWeight": "700", "marginRight": "4px"}),
                html.Span("Kafka", style={"color": ACCENT2}),
                html.Span(" → ", style={"color": SUBTEXT}),
                html.Span("Spark Streaming", style={"color": ACCENT2}),
                html.Span(" → ", style={"color": SUBTEXT}),
                html.Span("Delta Lake", style={"color": ACCENT2}),
                html.Span(" → ", style={"color": SUBTEXT}),
                html.Span("PostgreSQL Staging", style={"color": ACCENT2}),
                html.Span(" → ", style={"color": SUBTEXT}),
                html.Span("Data Validation", style={"color": ACCENT2}),
                html.Span(" → ", style={"color": SUBTEXT}),
                html.Span("MySQL Final", style={"color": ACCENT2}),
                html.Span(" → ", style={"color": SUBTEXT}),
                html.Span("Dashboard", style={"color": ACCENT1, "fontWeight": "700"}),
            ]
        ),

        # ── MAIN CONTENT ──────────────────────────────────────
        html.Div(
            style={"padding": "28px 32px"},
            children=[

                # ── SECTION 1: PIPELINE KPIs ──────────────────
                section_header("📦", "Pipeline Overview", "Live metrics from customer_booking_final"),

                html.Div(
                    style={"display": "flex", "gap": "14px", "marginBottom": "32px", "flexWrap": "wrap"},
                    children=[
                        kpi_card("kpi-bookings",  "Total Bookings",    ACCENT1, "📋"),
                        kpi_card("kpi-customers", "Unique Customers",  ACCENT2, "👤"),
                        kpi_card("kpi-revenue",   "Total Revenue",     ACCENT3, "💰"),
                        kpi_card("kpi-cars",      "Unique Cars",       ACCENT5, "🚘"),
                        kpi_card("kpi-integrity", "Data Integrity",    ACCENT4, "🛡️"),
                    ]
                ),

                # ── SECTION 2: BOOKING ANALYTICS ─────────────
                section_header("📊", "Booking Analytics", "Trends, patterns & customer insights"),

                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px", "marginBottom": "16px"},
                    children=[
                        card([dcc.Graph(id="bookings-per-day", config={"displayModeBar": False})]),
                        card([dcc.Graph(id="revenue-per-day",  config={"displayModeBar": False})]),
                    ]
                ),
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "1fr 1fr 1fr", "gap": "16px", "marginBottom": "16px"},
                    children=[
                        card([dcc.Graph(id="popular-cars",      config={"displayModeBar": False})]),
                        card([dcc.Graph(id="payment-methods",   config={"displayModeBar": False})]),
                        card([dcc.Graph(id="loyalty-tiers",     config={"displayModeBar": False})]),
                    ]
                ),
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "1fr 1fr 1fr", "gap": "16px", "marginBottom": "32px"},
                    children=[
                        card([dcc.Graph(id="insurance-coverage", config={"displayModeBar": False})]),
                        card([dcc.Graph(id="pickup-locations",   config={"displayModeBar": False})]),
                        card([dcc.Graph(id="drop-locations",     config={"displayModeBar": False})]),
                    ]
                ),

                # ── SECTION 3: DATA VALIDATION ────────────────
                section_header("🔍", "Data Validation Report", "Quality checks on pipeline data"),

                html.Div(
                    style={"display": "flex", "gap": "14px", "marginBottom": "20px", "flexWrap": "wrap"},
                    children=[
                        kpi_card("val-runs",          "Pipeline Runs",    ACCENT2, "🔄"),
                        kpi_card("val-rules-passed",  "Rules Passed",     ACCENT2, "✅"),
                        kpi_card("val-rules-violated","Rules Violated",   ACCENT3, "❌"),
                        kpi_card("val-anomalous",     "Anomalous Records",ACCENT5, "⚠️"),
                    ]
                ),

                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "1fr 2fr", "gap": "16px", "marginBottom": "16px"},
                    children=[
                        card([dcc.Graph(id="val-gauge",       config={"displayModeBar": False})]),
                        card([dcc.Graph(id="val-run-history", config={"displayModeBar": False})]),
                    ]
                ),
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px", "marginBottom": "16px"},
                    children=[
                        card([dcc.Graph(id="val-checks-passfail",  config={"displayModeBar": False})]),
                        card([dcc.Graph(id="val-column-anomalies", config={"displayModeBar": False})]),
                    ]
                ),
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px", "marginBottom": "32px"},
                    children=[
                        card([dcc.Graph(id="val-anomalous-loyalty", config={"displayModeBar": False})]),
                        card([dcc.Graph(id="val-payment-dist",      config={"displayModeBar": False})]),
                    ]
                ),

                # ── SECTION 4: ANOMALOUS RECORDS TABLE ────────
                section_header("🚨", "Anomalous Records Explorer", "Filter & investigate data quality issues"),

                card(
                    border_color=ACCENT3,
                    children=[
                        # Filter Row
                        html.Div(
                            style={"display": "flex", "gap": "14px", "marginBottom": "16px", "flexWrap": "wrap"},
                            children=[
                                html.Div(
                                    style={"flex": "1", "minWidth": "220px"},
                                    children=[
                                        html.Label("Filter by Pipeline Run", style={
                                            "color": SUBTEXT, "fontSize": "12px",
                                            "display": "block", "marginBottom": "6px",
                                            "textTransform": "uppercase", "letterSpacing": "1px"
                                        }),
                                        dcc.Dropdown(
                                            id="run-dropdown",
                                            placeholder="All runs...",
                                            clearable=True,
                                            style={"color": "black", "fontSize": "13px"}
                                        ),
                                    ]
                                ),
                                html.Div(
                                    style={"flex": "1", "minWidth": "220px"},
                                    children=[
                                        html.Label("Filter by Validation Rule", style={
                                            "color": SUBTEXT, "fontSize": "12px",
                                            "display": "block", "marginBottom": "6px",
                                            "textTransform": "uppercase", "letterSpacing": "1px"
                                        }),
                                        dcc.Dropdown(
                                            id="check-dropdown",
                                            placeholder="All rules...",
                                            clearable=True,
                                            style={"color": "black", "fontSize": "13px"}
                                        ),
                                    ]
                                ),
                                html.Div(
                                    style={"display": "flex", "alignItems": "flex-end"},
                                    children=[
                                        html.Div(
                                            id="record-count-badge",
                                            style={
                                                "backgroundColor": "#1c2d3a",
                                                "border": f"1px solid {ACCENT1}",
                                                "borderRadius": "20px",
                                                "padding": "6px 16px",
                                                "color": ACCENT1,
                                                "fontSize": "13px",
                                                "fontFamily": "'Courier New', monospace",
                                                "fontWeight": "700",
                                            }
                                        )
                                    ]
                                )
                            ]
                        ),

                        dash_table.DataTable(
                            id="anomalous-table",
                            style_table={"overflowX": "auto", "borderRadius": "8px"},
                            style_cell={
                                "backgroundColor": CARD_BG,
                                "color": TEXT,
                                "border": f"1px solid {BORDER}",
                                "textAlign": "left",
                                "padding": "10px 14px",
                                "fontSize": "12px",
                                "fontFamily": "'Courier New', monospace",
                                "maxWidth": "200px",
                                "overflow": "hidden",
                                "textOverflow": "ellipsis",
                            },
                            style_header={
                                "backgroundColor": "#1c2d3a",
                                "color": ACCENT1,
                                "fontWeight": "700",
                                "fontSize": "12px",
                                "border": f"1px solid {BORDER}",
                                "textTransform": "uppercase",
                                "letterSpacing": "1px",
                            },
                            style_data_conditional=[
                                {"if": {"row_index": "odd"},
                                 "backgroundColor": "#0d1117"},
                                {"if": {"filter_query": "{failed_reason} contains 'Duplicate'"},
                                 "color": ACCENT3},
                                {"if": {"filter_query": "{failed_reason} contains 'Null'"},
                                 "color": ACCENT5},
                            ],
                            page_size=15,
                            filter_action="native",
                            sort_action="native",
                            tooltip_delay=0,
                            tooltip_duration=None,
                        ),
                    ]
                ),

                # ── FOOTER ────────────────────────────────────
                html.Div(
                    style={
                        "textAlign": "center",
                        "marginTop": "40px",
                        "paddingTop": "20px",
                        "borderTop": f"1px solid {BORDER}",
                        "color": SUBTEXT,
                        "fontSize": "12px",
                        "fontFamily": "'Courier New', monospace",
                    },
                    children=[
                        html.Span("Car Booking Data Pipeline Monitor  ·  Auto-refresh every 30s  ·  "),
                        html.Span("Kafka → Spark → Delta Lake → PostgreSQL → MySQL",
                                  style={"color": ACCENT1}),
                    ]
                ),
            ]
        ),
    ]
)


# ============ MAIN CALLBACK ============
@app.callback(
    [
        Output("last-updated", "children"),
        Output("kpi-bookings",  "children"),
        Output("kpi-customers", "children"),
        Output("kpi-revenue",   "children"),
        Output("kpi-cars",      "children"),
        Output("kpi-integrity", "children"),
        Output("bookings-per-day",   "figure"),
        Output("revenue-per-day",    "figure"),
        Output("popular-cars",       "figure"),
        Output("payment-methods",    "figure"),
        Output("loyalty-tiers",      "figure"),
        Output("insurance-coverage", "figure"),
        Output("pickup-locations",   "figure"),
        Output("drop-locations",     "figure"),
        Output("val-runs",           "children"),
        Output("val-rules-passed",   "children"),
        Output("val-rules-violated", "children"),
        Output("val-anomalous",      "children"),
        Output("val-gauge",          "figure"),
        Output("val-run-history",    "figure"),
        Output("val-checks-passfail","figure"),
        Output("val-column-anomalies","figure"),
        Output("val-anomalous-loyalty","figure"),
        Output("val-payment-dist",   "figure"),
        Output("run-dropdown",   "options"),
        Output("check-dropdown", "options"),
    ],
    [Input("refresh-btn",  "n_clicks"),
     Input("auto-refresh", "n_intervals")]
)
def update_all(n_clicks, n_intervals):
    booking_df, validation_df, expectation_df, failed_df = get_data()

    now = pd.Timestamp.now().strftime("%d %b %Y  %H:%M:%S")
    last_updated = f"↻  {now}"

    # ── PIPELINE KPIs ──
    kpi_bookings  = f"{booking_df['booking_id'].nunique():,}"          if not booking_df.empty else "—"
    kpi_customers = f"{booking_df['customer_id'].nunique():,}"         if not booking_df.empty else "—"
    kpi_revenue   = f"₹{booking_df['payment_amount'].sum():,.0f}"      if not booking_df.empty else "—"
    kpi_cars      = f"{booking_df['car_id'].nunique():,}"              if not booking_df.empty else "—"
    kpi_integrity = f"{validation_df['success_rate'].mean():.1f}%"    if not validation_df.empty else "—"

    # ── BOOKINGS PER DAY ──
    if not booking_df.empty and "booking_date" in booking_df.columns:
        bd = booking_df.groupby("booking_date")["booking_id"].nunique().reset_index()
        bd.columns = ["date", "bookings"]
        fig1 = px.bar(bd, x="date", y="bookings", title="📅 Daily Bookings",
                      color_discrete_sequence=[ACCENT1])
        fig1.update_traces(marker_line_width=0)
        fig1.update_layout(**CHART_BASE)
    else:
        fig1 = empty_fig("📅 Daily Bookings")

    # ── REVENUE PER DAY ──
    if not booking_df.empty and "booking_date" in booking_df.columns:
        rd = booking_df.groupby("booking_date")["payment_amount"].sum().reset_index()
        rd.columns = ["date", "revenue"]
        fig2 = px.area(rd, x="date", y="revenue", title="💰 Daily Revenue",
                       color_discrete_sequence=[ACCENT2])
        fig2.update_traces(line_color=ACCENT2, fillcolor=f"rgba(63,185,80,0.15)")
        fig2.update_layout(**CHART_BASE)
    else:
        fig2 = empty_fig("💰 Daily Revenue")

    # ── POPULAR CARS ──
    if not booking_df.empty and "model" in booking_df.columns:
        pc = booking_df["model"].value_counts().head(8).reset_index()
        pc.columns = ["model", "count"]
        fig3 = px.bar(pc, x="count", y="model", orientation="h",
                      title="🚗 Top Car Models",
                      color="count", color_continuous_scale=[[0, "#1c2d3a"], [1, ACCENT1]])
        fig3.update_layout(**CHART_BASE, showlegend=False,
                           coloraxis_showscale=False,
                           yaxis={"categoryorder": "total ascending"})
    else:
        fig3 = empty_fig("🚗 Top Car Models")

    # ── PAYMENT METHODS ──
    if not booking_df.empty and "payment_method" in booking_df.columns:
        pm = booking_df["payment_method"].value_counts().reset_index()
        pm.columns = ["method", "count"]
        fig4 = px.pie(pm, names="method", values="count",
                      title="💳 Payment Methods",
                      color_discrete_sequence=[ACCENT1, ACCENT2, ACCENT3, ACCENT5, ACCENT4],
                      hole=0.45)
        fig4.update_traces(textposition="outside", textfont_size=11)
        fig4.update_layout(**CHART_BASE)
    else:
        fig4 = empty_fig("💳 Payment Methods")

    # ── LOYALTY TIERS ──
    if not booking_df.empty and "loyalty_tier" in booking_df.columns:
        lt = booking_df["loyalty_tier"].value_counts().reset_index()
        lt.columns = ["tier", "count"]
        color_map = {"platinum": ACCENT4, "gold": ACCENT5,
                     "silver": "#8b949e", "bronze": "#f78166"}
        fig5 = px.bar(lt, x="tier", y="count", title="⭐ Loyalty Tiers",
                      color="tier", color_discrete_map=color_map)
        fig5.update_layout(**CHART_BASE, showlegend=False)
    else:
        fig5 = empty_fig("⭐ Loyalty Tiers")

    # ── INSURANCE COVERAGE ──
    if not booking_df.empty and "insurance_coverage" in booking_df.columns:
        ic = booking_df["insurance_coverage"].value_counts().reset_index()
        ic.columns = ["coverage", "count"]
        fig6 = px.pie(ic, names="coverage", values="count",
                      title="🛡️ Insurance Coverage",
                      color_discrete_sequence=[ACCENT2, ACCENT3],
                      hole=0.45)
        fig6.update_layout(**CHART_BASE)
    else:
        fig6 = empty_fig("🛡️ Insurance Coverage")

    # ── PICKUP LOCATIONS ──
    if not booking_df.empty and "pickup_location" in booking_df.columns:
        pu = booking_df["pickup_location"].value_counts().head(8).reset_index()
        pu.columns = ["location", "count"]
        fig7 = px.bar(pu, x="count", y="location", orientation="h",
                      title="📍 Top Pickup Locations",
                      color="count", color_continuous_scale=[[0, "#1c2d3a"], [1, ACCENT5]])
        fig7.update_layout(**CHART_BASE, coloraxis_showscale=False,
                           yaxis={"categoryorder": "total ascending"})
    else:
        fig7 = empty_fig("📍 Top Pickup Locations")

    # ── DROP LOCATIONS ──
    if not booking_df.empty and "drop_location" in booking_df.columns:
        dl = booking_df["drop_location"].value_counts().head(8).reset_index()
        dl.columns = ["location", "count"]
        fig8 = px.bar(dl, x="count", y="location", orientation="h",
                      title="🏁 Top Drop Locations",
                      color="count", color_continuous_scale=[[0, "#1c2d3a"], [1, ACCENT4]])
        fig8.update_layout(**CHART_BASE, coloraxis_showscale=False,
                           yaxis={"categoryorder": "total ascending"})
    else:
        fig8 = empty_fig("🏁 Top Drop Locations")

    # ── VALIDATION KPIs ──
    val_runs      = f"{len(validation_df):,}"                                    if not validation_df.empty else "0"
    val_passed    = f"{int(expectation_df['success'].sum()):,}"                  if not expectation_df.empty else "0"
    val_violated  = f"{int((~expectation_df['success'].astype(bool)).sum()):,}"  if not expectation_df.empty else "0"
    val_anomalous = f"{len(failed_df):,}"                                        if not failed_df.empty else "0"

    # ── GAUGE ──
    avg_rate = float(validation_df["success_rate"].mean()) if not validation_df.empty else 0
    gauge_color = ACCENT2 if avg_rate >= 90 else (ACCENT5 if avg_rate >= 70 else ACCENT3)
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=avg_rate,
        title={"text": "Data Integrity Score", "font": {"color": TEXT, "size": 13}},
        number={"suffix": "%", "font": {"color": gauge_color, "size": 36}},
        gauge={
            "axis": {"range": [0, 100], "tickcolor": SUBTEXT,
                     "tickfont": {"color": SUBTEXT, "size": 10}},
            "bar": {"color": gauge_color, "thickness": 0.25},
            "bgcolor": CARD_BG,
            "borderwidth": 0,
            "steps": [
                {"range": [0,  70], "color": "#1c1f26"},
                {"range": [70, 90], "color": "#1c2416"},
                {"range": [90,100], "color": "#162414"},
            ],
            "threshold": {
                "line": {"color": "white", "width": 2},
                "thickness": 0.75, "value": 100
            }
        }
    ))
    fig_gauge.update_layout(**CHART_BASE)

    # ── RUN HISTORY ──
    if not validation_df.empty and "created_at" in validation_df.columns:
        fig_hist = px.line(validation_df, x="created_at", y="success_rate",
                           title="📈 Validation Run History",
                           color_discrete_sequence=[ACCENT2], markers=True)
        fig_hist.add_hline(y=100, line_dash="dot", line_color=ACCENT3,
                           annotation_text="100% target",
                           annotation_font_color=ACCENT3)
        fig_hist.update_traces(line_width=2, marker_size=8)
        fig_hist.update_layout(**CHART_BASE, yaxis_range=[0, 105])
    else:
        fig_hist = empty_fig("📈 Validation Run History")

    # ── CHECKS PASS / FAIL ──
    if not expectation_df.empty:
        es = expectation_df.groupby("expectation_type")["success"].agg(
            Passed=lambda x: int(x.astype(bool).sum()),
            Violated=lambda x: int((~x.astype(bool)).sum())
        ).reset_index()
        em = es.melt(id_vars="expectation_type", value_vars=["Passed","Violated"],
                     var_name="Status", value_name="Count")
        em["Rule"] = em["expectation_type"].str.replace("expect_","").str.replace("_"," ").str.title()
        fig_checks = px.bar(em, x="Rule", y="Count", color="Status",
                            barmode="group",
                            title="✅ Validation Rules — Pass vs Violated",
                            color_discrete_map={"Passed": ACCENT2, "Violated": ACCENT3})
        fig_checks.update_layout(**CHART_BASE, xaxis_tickangle=-30)
    else:
        fig_checks = empty_fig("✅ Validation Rules — Pass vs Violated")

    # ── COLUMN ANOMALIES ──
    if not expectation_df.empty:
        cf = expectation_df[~expectation_df["success"].astype(bool)].groupby(
            "column_name")["unexpected_count"].sum().reset_index()
        cf.columns = ["column", "anomalies"]
        cf = cf.sort_values("anomalies", ascending=True)
        fig_col = px.bar(cf, x="anomalies", y="column", orientation="h",
                         title="⚠️ Anomalies by Column",
                         color="anomalies",
                         color_continuous_scale=[[0,"#1c2d3a"],[1, ACCENT3]])
        fig_col.update_layout(**CHART_BASE, coloraxis_showscale=False)
    else:
        fig_col = empty_fig("⚠️ Anomalies by Column")

    # ── ANOMALOUS BY LOYALTY ──
    if not failed_df.empty and "loyalty_tier" in failed_df.columns:
        fl = failed_df["loyalty_tier"].value_counts().reset_index()
        fl.columns = ["tier", "count"]
        fig_loy = px.pie(fl, names="tier", values="count",
                         title="👤 Anomalous — Customer Segments",
                         color="tier",
                         color_discrete_map={"platinum": ACCENT4, "gold": ACCENT5,
                                             "silver": "#8b949e", "bronze": "#f78166"},
                         hole=0.45)
        fig_loy.update_layout(**CHART_BASE)
    else:
        fig_loy = empty_fig("👤 Anomalous — Customer Segments")

    # ── PAYMENT DIST ──
    if not failed_df.empty and "payment_amount" in failed_df.columns:
        fig_pay = px.histogram(failed_df, x="payment_amount",
                               title="💸 Anomalous — Payment Distribution",
                               nbins=30,
                               color_discrete_sequence=[ACCENT3])
        fig_pay.update_traces(marker_line_width=0)
        fig_pay.update_layout(**CHART_BASE)
    else:
        fig_pay = empty_fig("💸 Anomalous — Payment Distribution")

    # ── DROPDOWNS ──
    run_opts = [{"label": f"Run: {r}", "value": r}
                for r in failed_df["run_identifier"].unique()] if not failed_df.empty else []
    chk_opts = [{"label": e.replace("expect_","").replace("_"," ").title(), "value": e}
                for e in failed_df["expectation_type"].unique()] if not failed_df.empty else []

    return (
        last_updated,
        kpi_bookings, kpi_customers, kpi_revenue, kpi_cars, kpi_integrity,
        fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8,
        val_runs, val_passed, val_violated, val_anomalous,
        fig_gauge, fig_hist, fig_checks, fig_col, fig_loy, fig_pay,
        run_opts, chk_opts,
    )


# ============ TABLE CALLBACK ============
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
    _, _, _, failed_df = get_data()

    if failed_df.empty:
        return [], [], [], "0 records"

    df = failed_df.copy()
    if run_id:
        df = df[df["run_identifier"] == run_id]
    if check_type:
        df = df[df["expectation_type"] == check_type]

    show_cols = ["run_identifier", "expectation_type", "column_name", "failed_reason",
                 "booking_id", "customer_name", "email", "loyalty_tier",
                 "pickup_location", "drop_location", "payment_method", "payment_amount"]
    show_cols = [c for c in show_cols if c in df.columns]
    df_show = df[show_cols].head(200)

    col_rename = {
        "run_identifier":   "Run ID",
        "expectation_type": "Validation Rule",
        "column_name":      "Column",
        "failed_reason":    "Reason",
        "booking_id":       "Booking ID",
        "customer_name":    "Customer",
        "email":            "Email",
        "loyalty_tier":     "Tier",
        "pickup_location":  "Pickup",
        "drop_location":    "Drop",
        "payment_method":   "Payment",
        "payment_amount":   "Amount (₹)",
    }
    columns = [{"name": col_rename.get(c, c), "id": c} for c in show_cols]

    tooltip_data = [
        {c: {"value": str(row[c]), "type": "markdown"} for c in show_cols}
        for row in df_show.to_dict("records")
    ]

    badge = f"Showing {len(df_show):,} of {len(df):,} records"

    return df_show.to_dict("records"), columns, tooltip_data, badge


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)