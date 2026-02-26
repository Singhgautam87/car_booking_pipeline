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
    booking_df = pd.read_sql("SELECT * FROM customer_booking_final", engine)
    validation_df = pd.read_sql("SELECT * FROM validation_results", engine)
    expectation_df = pd.read_sql("SELECT * FROM validation_expectation_details", engine)
    failed_df = pd.read_sql("SELECT * FROM validation_failed_records", engine)
    engine.dispose()
    return booking_df, validation_df, expectation_df, failed_df

# ============ APP ============
app = dash.Dash(__name__)
app.title = "Car Booking Data Pipeline Monitor"

booking_df, validation_df, expectation_df, failed_df = get_data()

# ============ CHART STYLE ============
CHART_STYLE = {
    "paper_bgcolor": "#16213e",
    "plot_bgcolor": "#16213e",
    "font": {"color": "#a8a8b3"},
    "title_font": {"color": "white", "size": 16}
}

def empty_fig(title):
    fig = go.Figure()
    fig.add_annotation(text="No data available", xref="paper", yref="paper",
                       x=0.5, y=0.5, showarrow=False, font={"color": "white", "size": 18})
    fig.update_layout(**CHART_STYLE, title=title)
    return fig

# ============ LAYOUT ============
app.layout = html.Div(
    style={"backgroundColor": "#1a1a2e", "minHeight": "100vh", "padding": "20px", "fontFamily": "Arial"},
    children=[

        # ============ HEADER ============
        html.Div(
            style={"textAlign": "center", "padding": "20px", "marginBottom": "10px"},
            children=[
                html.H1("🚗 Car Booking Data Pipeline Monitor",
                        style={"color": "#e94560", "fontSize": "36px", "margin": "0"}),
                html.P("Kafka → Spark Streaming → Delta Lake → PostgreSQL → MySQL",
                       style={"color": "#a8a8b3", "fontSize": "14px", "margin": "8px 0"}),
                html.Div(
                    style={"display": "flex", "justifyContent": "center", "gap": "15px", "marginTop": "15px"},
                    children=[
                        html.Button("🔄 Refresh", id="refresh-btn",
                                    style={"backgroundColor": "#e94560", "color": "white",
                                           "border": "none", "padding": "8px 25px",
                                           "borderRadius": "5px", "cursor": "pointer", "fontSize": "14px"}),
                        html.Span(id="last-updated",
                                  style={"color": "#a8a8b3", "lineHeight": "35px", "fontSize": "13px"})
                    ]
                ),
                dcc.Interval(id="auto-refresh", interval=30000, n_intervals=0)
            ]
        ),

        # ============ SECTION 1: PIPELINE OVERVIEW ============
        html.Hr(style={"borderColor": "#0f3460", "margin": "20px 0"}),
        html.H2("📦 Pipeline Overview",
                style={"color": "#4fc3f7", "marginBottom": "15px", "fontSize": "22px"}),

        html.Div(
            style={"display": "flex", "gap": "15px", "marginBottom": "30px"},
            children=[
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "flex": "1", "border": "1px solid #e94560"},
                    children=[
                        html.H2(id="kpi-bookings", style={"color": "#e94560", "fontSize": "30px", "margin": "0"}),
                        html.P("Total Bookings", style={"color": "#a8a8b3", "margin": "5px 0 0 0"})
                    ]
                ),
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "flex": "1", "border": "1px solid #4fc3f7"},
                    children=[
                        html.H2(id="kpi-customers", style={"color": "#4fc3f7", "fontSize": "30px", "margin": "0"}),
                        html.P("Unique Customers", style={"color": "#a8a8b3", "margin": "5px 0 0 0"})
                    ]
                ),
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "flex": "1", "border": "1px solid #81c784"},
                    children=[
                        html.H2(id="kpi-revenue", style={"color": "#81c784", "fontSize": "30px", "margin": "0"}),
                        html.P("Total Revenue", style={"color": "#a8a8b3", "margin": "5px 0 0 0"})
                    ]
                ),
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "flex": "1", "border": "1px solid #ffb74d"},
                    children=[
                        html.H2(id="kpi-cars", style={"color": "#ffb74d", "fontSize": "30px", "margin": "0"}),
                        html.P("Unique Cars", style={"color": "#a8a8b3", "margin": "5px 0 0 0"})
                    ]
                ),
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "flex": "1", "border": "1px solid #ce93d8"},
                    children=[
                        html.H2(id="kpi-integrity", style={"color": "#ce93d8", "fontSize": "30px", "margin": "0"}),
                        html.P("Data Integrity Score", style={"color": "#a8a8b3", "margin": "5px 0 0 0"})
                    ]
                ),
            ]
        ),

        # ============ SECTION 2: BOOKING ANALYTICS ============
        html.Hr(style={"borderColor": "#0f3460", "margin": "20px 0"}),
        html.H2("📊 Booking Analytics",
                style={"color": "#4fc3f7", "marginBottom": "15px", "fontSize": "22px"}),

        html.Div(style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
                 children=[
                     html.Div(style={"width": "50%"}, children=[dcc.Graph(id="bookings-per-day")]),
                     html.Div(style={"width": "50%"}, children=[dcc.Graph(id="revenue-per-day")])
                 ]),
        html.Div(style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
                 children=[
                     html.Div(style={"width": "50%"}, children=[dcc.Graph(id="popular-cars")]),
                     html.Div(style={"width": "50%"}, children=[dcc.Graph(id="payment-methods")])
                 ]),
        html.Div(style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
                 children=[
                     html.Div(style={"width": "50%"}, children=[dcc.Graph(id="loyalty-tiers")]),
                     html.Div(style={"width": "50%"}, children=[dcc.Graph(id="insurance-coverage")])
                 ]),
        html.Div(style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
                 children=[
                     html.Div(style={"width": "50%"}, children=[dcc.Graph(id="pickup-locations")]),
                     html.Div(style={"width": "50%"}, children=[dcc.Graph(id="drop-locations")])
                 ]),

        # ============ SECTION 3: DATA VALIDATION REPORT ============
        html.Hr(style={"borderColor": "#0f3460", "margin": "20px 0"}),
        html.H2("🔍 Data Validation Report",
                style={"color": "#4fc3f7", "marginBottom": "15px", "fontSize": "22px"}),

        # Validation KPI Cards
        html.Div(
            style={"display": "flex", "gap": "15px", "marginBottom": "30px"},
            children=[
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "flex": "1", "border": "1px solid #81c784"},
                    children=[
                        html.H2(id="val-runs", style={"color": "#81c784", "fontSize": "30px", "margin": "0"}),
                        html.P("Pipeline Runs", style={"color": "#a8a8b3", "margin": "5px 0 0 0"})
                    ]
                ),
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "flex": "1", "border": "1px solid #4fc3f7"},
                    children=[
                        html.H2(id="val-rules-passed", style={"color": "#4fc3f7", "fontSize": "30px", "margin": "0"}),
                        html.P("Rules Passed", style={"color": "#a8a8b3", "margin": "5px 0 0 0"})
                    ]
                ),
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "flex": "1", "border": "1px solid #e94560"},
                    children=[
                        html.H2(id="val-rules-violated", style={"color": "#e94560", "fontSize": "30px", "margin": "0"}),
                        html.P("Rules Violated", style={"color": "#a8a8b3", "margin": "5px 0 0 0"})
                    ]
                ),
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "flex": "1", "border": "1px solid #ffb74d"},
                    children=[
                        html.H2(id="val-anomalous", style={"color": "#ffb74d", "fontSize": "30px", "margin": "0"}),
                        html.P("Anomalous Records", style={"color": "#a8a8b3", "margin": "5px 0 0 0"})
                    ]
                ),
            ]
        ),

        html.Div(style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
                 children=[
                     html.Div(style={"width": "50%"}, children=[dcc.Graph(id="val-gauge")]),
                     html.Div(style={"width": "50%"}, children=[dcc.Graph(id="val-run-history")])
                 ]),
        html.Div(style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
                 children=[
                     html.Div(style={"width": "50%"}, children=[dcc.Graph(id="val-checks-passfail")]),
                     html.Div(style={"width": "50%"}, children=[dcc.Graph(id="val-column-anomalies")])
                 ]),
        html.Div(style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
                 children=[
                     html.Div(style={"width": "50%"}, children=[dcc.Graph(id="val-anomalous-loyalty")]),
                     html.Div(style={"width": "50%"}, children=[dcc.Graph(id="val-payment-dist")])
                 ]),

        # ============ SECTION 4: ANOMALOUS RECORDS EXPLORER ============
        html.Hr(style={"borderColor": "#0f3460", "margin": "20px 0"}),
        html.H2("🚨 Anomalous Records Explorer",
                style={"color": "#4fc3f7", "marginBottom": "15px", "fontSize": "22px"}),

        html.Div(
            style={"display": "flex", "gap": "20px", "marginBottom": "15px"},
            children=[
                dcc.Dropdown(id="run-dropdown", placeholder="🔍 Filter by Pipeline Run...",
                             style={"width": "350px", "color": "black"}),
                dcc.Dropdown(id="check-dropdown", placeholder="🔍 Filter by Validation Rule...",
                             style={"width": "350px", "color": "black"}),
            ]
        ),

        dash_table.DataTable(
            id="anomalous-table",
            style_table={"overflowX": "auto"},
            style_cell={
                "backgroundColor": "#16213e", "color": "#a8a8b3",
                "border": "1px solid #0f3460", "textAlign": "left",
                "padding": "10px", "fontSize": "12px"
            },
            style_header={
                "backgroundColor": "#0f3460", "color": "white",
                "fontWeight": "bold", "fontSize": "13px"
            },
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "#1a1a2e"}
            ],
            page_size=15,
            filter_action="native",
            sort_action="native"
        ),

        dcc.Store(id="store-data")
    ]
)


# ============ MAIN CALLBACK ============
@app.callback(
    [
        Output("last-updated", "children"),
        # Pipeline KPIs
        Output("kpi-bookings", "children"),
        Output("kpi-customers", "children"),
        Output("kpi-revenue", "children"),
        Output("kpi-cars", "children"),
        Output("kpi-integrity", "children"),
        # Booking charts
        Output("bookings-per-day", "figure"),
        Output("revenue-per-day", "figure"),
        Output("popular-cars", "figure"),
        Output("payment-methods", "figure"),
        Output("loyalty-tiers", "figure"),
        Output("insurance-coverage", "figure"),
        Output("pickup-locations", "figure"),
        Output("drop-locations", "figure"),
        # Validation KPIs
        Output("val-runs", "children"),
        Output("val-rules-passed", "children"),
        Output("val-rules-violated", "children"),
        Output("val-anomalous", "children"),
        # Validation charts
        Output("val-gauge", "figure"),
        Output("val-run-history", "figure"),
        Output("val-checks-passfail", "figure"),
        Output("val-column-anomalies", "figure"),
        Output("val-anomalous-loyalty", "figure"),
        Output("val-payment-dist", "figure"),
        # Dropdowns
        Output("run-dropdown", "options"),
        Output("check-dropdown", "options"),
    ],
    [Input("refresh-btn", "n_clicks"),
     Input("auto-refresh", "n_intervals")]
)
def update_all(n_clicks, n_intervals):
    booking_df, validation_df, expectation_df, failed_df = get_data()

    last_updated = f"🕐 Last updated: {pd.Timestamp.now().strftime('%d %b %Y %H:%M:%S')}"

    # ===== PIPELINE KPIs =====
    kpi_bookings  = f"{booking_df['booking_id'].nunique():,}"
    kpi_customers = f"{booking_df['customer_id'].nunique():,}"
    kpi_revenue   = f"₹{booking_df['payment_amount'].sum():,.0f}"
    kpi_cars      = f"{booking_df['car_id'].nunique():,}"
    kpi_integrity = f"{validation_df['success_rate'].mean():.1f}%" if not validation_df.empty else "N/A"

    # ===== BOOKING CHARTS =====
    bookings_day = booking_df.groupby("booking_date")["booking_id"].nunique().reset_index()
    fig1 = px.line(bookings_day, x="booking_date", y="booking_id",
                   title="📅 Bookings Per Day", color_discrete_sequence=["#e94560"])
    fig1.update_layout(**CHART_STYLE)

    revenue_day = booking_df.groupby("booking_date")["payment_amount"].sum().reset_index()
    fig2 = px.area(revenue_day, x="booking_date", y="payment_amount",
                   title="💰 Revenue Per Day", color_discrete_sequence=["#81c784"])
    fig2.update_layout(**CHART_STYLE)

    popular_cars = booking_df["model"].value_counts().reset_index()
    popular_cars.columns = ["model", "count"]
    fig3 = px.bar(popular_cars, x="model", y="count", title="🚗 Popular Car Models",
                  color="count", color_continuous_scale="reds")
    fig3.update_layout(**CHART_STYLE)

    payment = booking_df["payment_method"].value_counts().reset_index()
    payment.columns = ["method", "count"]
    fig4 = px.pie(payment, names="method", values="count", title="💳 Payment Methods",
                  color_discrete_sequence=px.colors.sequential.RdBu)
    fig4.update_layout(**CHART_STYLE)

    loyalty = booking_df["loyalty_tier"].value_counts().reset_index()
    loyalty.columns = ["tier", "count"]
    fig5 = px.bar(loyalty, x="tier", y="count", title="⭐ Loyalty Tiers", color="tier",
                  color_discrete_map={"platinum": "#ce93d8", "gold": "#ffb74d",
                                      "silver": "#a8a8b3", "bronze": "#ff8a65"})
    fig5.update_layout(**CHART_STYLE)

    insurance = booking_df["insurance_coverage"].value_counts().reset_index()
    insurance.columns = ["coverage", "count"]
    fig6 = px.pie(insurance, names="coverage", values="count", title="🛡️ Insurance Coverage",
                  color_discrete_sequence=["#4fc3f7", "#e94560"])
    fig6.update_layout(**CHART_STYLE)

    pickup = booking_df["pickup_location"].value_counts().head(10).reset_index()
    pickup.columns = ["location", "count"]
    fig7 = px.bar(pickup, x="count", y="location", orientation="h",
                  title="📍 Top 10 Pickup Locations",
                  color="count", color_continuous_scale="blues")
    fig7.update_layout(**CHART_STYLE)

    drop = booking_df["drop_location"].value_counts().head(10).reset_index()
    drop.columns = ["location", "count"]
    fig_drop = px.bar(drop, x="count", y="location", orientation="h",
                      title="📍 Top 10 Drop Locations",
                      color="count", color_continuous_scale="greens")
    fig_drop.update_layout(**CHART_STYLE)

    # ===== VALIDATION KPIs =====
    val_runs     = f"{len(validation_df):,}" if not validation_df.empty else "0"
    val_passed   = f"{expectation_df['success'].sum():,}" if not expectation_df.empty else "0"
    val_violated = f"{(~expectation_df['success'].astype(bool)).sum():,}" if not expectation_df.empty else "0"
    val_anomalous = f"{len(failed_df):,}" if not failed_df.empty else "0"

    # ===== GAUGE =====
    avg_rate = validation_df["success_rate"].mean() if not validation_df.empty else 0
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=avg_rate,
        title={"text": "🎯 Data Integrity Score", "font": {"color": "white", "size": 16}},
        delta={"reference": 100},
        number={"suffix": "%", "font": {"color": "white"}},
        gauge={
            "axis": {"range": [0, 100], "tickcolor": "#a8a8b3"},
            "bar": {"color": "#e94560"},
            "bgcolor": "#16213e",
            "bordercolor": "#0f3460",
            "steps": [
                {"range": [0, 50], "color": "#ff5252"},
                {"range": [50, 80], "color": "#ffb74d"},
                {"range": [80, 100], "color": "#81c784"}
            ],
            "threshold": {"line": {"color": "white", "width": 4}, "thickness": 0.75, "value": 100}
        }
    ))
    fig_gauge.update_layout(**CHART_STYLE)

    # ===== RUN HISTORY =====
    if not validation_df.empty:
        fig_history = px.line(validation_df, x="created_at", y="success_rate",
                              title="📈 Validation Run History",
                              color_discrete_sequence=["#81c784"], markers=True)
        fig_history.add_hline(y=100, line_dash="dash", line_color="#e94560",
                              annotation_text="100% Target")
        fig_history.update_layout(**CHART_STYLE)
    else:
        fig_history = empty_fig("📈 Validation Run History")

    # ===== CHECKS PASS/FAIL =====
    if not expectation_df.empty:
        exp_summary = expectation_df.groupby("expectation_type")["success"].agg(
            passed=lambda x: x.astype(bool).sum(),
            violated=lambda x: (~x.astype(bool)).sum()
        ).reset_index()
        exp_melted = exp_summary.melt(id_vars="expectation_type",
                                      value_vars=["passed", "violated"],
                                      var_name="status", value_name="count")
        # Clean names
        exp_melted["expectation_type"] = exp_melted["expectation_type"].str.replace(
            "expect_", "").str.replace("_", " ").str.title()
        fig_checks = px.bar(exp_melted, x="expectation_type", y="count",
                            color="status", barmode="group",
                            title="✅ Validation Rules — Pass vs Violated",
                            color_discrete_map={"passed": "#81c784", "violated": "#e94560"})
        fig_checks.update_layout(**CHART_STYLE, xaxis_tickangle=-45)
    else:
        fig_checks = empty_fig("✅ Validation Rules — Pass vs Violated")

    # ===== COLUMN ANOMALIES =====
    if not expectation_df.empty:
        col_fail = expectation_df[~expectation_df["success"].astype(bool)].groupby(
            "column_name")["unexpected_count"].sum().reset_index()
        col_fail.columns = ["column_name", "anomaly_count"]
        col_fail = col_fail.sort_values("anomaly_count", ascending=True)
        fig_col = px.bar(col_fail, x="anomaly_count", y="column_name",
                         orientation="h", title="⚠️ Anomalies Detected by Column",
                         color="anomaly_count", color_continuous_scale="reds")
        fig_col.update_layout(**CHART_STYLE)
    else:
        fig_col = empty_fig("⚠️ Anomalies Detected by Column")

    # ===== ANOMALOUS BY LOYALTY =====
    if not failed_df.empty:
        failed_loyalty = failed_df["loyalty_tier"].value_counts().reset_index()
        failed_loyalty.columns = ["tier", "count"]
        fig_loyalty = px.pie(failed_loyalty, names="tier", values="count",
                             title="⭐ Anomalous Records by Customer Segment",
                             color="tier",
                             color_discrete_map={"platinum": "#ce93d8", "gold": "#ffb74d",
                                                 "silver": "#a8a8b3", "bronze": "#ff8a65"})
        fig_loyalty.update_layout(**CHART_STYLE)
    else:
        fig_loyalty = empty_fig("⭐ Anomalous Records by Customer Segment")

    # ===== PAYMENT DISTRIBUTION =====
    if not failed_df.empty:
        fig_payment = px.histogram(failed_df, x="payment_amount",
                                   title="💰 Anomalous Records — Payment Distribution",
                                   color_discrete_sequence=["#e94560"], nbins=30)
        fig_payment.update_layout(**CHART_STYLE)
    else:
        fig_payment = empty_fig("💰 Anomalous Records — Payment Distribution")

    # ===== DROPDOWNS =====
    run_options = [{"label": f"Run: {r}", "value": r}
                   for r in failed_df["run_identifier"].unique()] if not failed_df.empty else []
    check_options = [{"label": e.replace("expect_", "").replace("_", " ").title(), "value": e}
                     for e in failed_df["expectation_type"].unique()] if not failed_df.empty else []

    return (
        last_updated,
        kpi_bookings, kpi_customers, kpi_revenue, kpi_cars, kpi_integrity,
        fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig_drop,
        val_runs, val_passed, val_violated, val_anomalous,
        fig_gauge, fig_history, fig_checks, fig_col, fig_loyalty, fig_payment,
        run_options, check_options
    )


# ============ ANOMALOUS RECORDS TABLE ============
@app.callback(
    [Output("anomalous-table", "data"),
     Output("anomalous-table", "columns")],
    [Input("run-dropdown", "value"),
     Input("check-dropdown", "value"),
     Input("refresh-btn", "n_clicks"),
     Input("auto-refresh", "n_intervals")]
)
def update_table(run_id, check_type, n_clicks, n_intervals):
    _, _, _, failed_df = get_data()

    if failed_df.empty:
        return [], []

    if run_id:
        failed_df = failed_df[failed_df["run_identifier"] == run_id]
    if check_type:
        failed_df = failed_df[failed_df["expectation_type"] == check_type]

    show_cols = ["run_identifier", "expectation_type", "column_name", "failed_reason",
                 "booking_id", "customer_name", "email", "loyalty_tier", "payment_amount"]
    show_cols = [c for c in show_cols if c in failed_df.columns]
    df_show = failed_df[show_cols].head(100)

    # Clean column names
    col_rename = {
        "run_identifier": "Run ID",
        "expectation_type": "Validation Rule",
        "column_name": "Column",
        "failed_reason": "Reason",
        "booking_id": "Booking ID",
        "customer_name": "Customer",
        "email": "Email",
        "loyalty_tier": "Tier",
        "payment_amount": "Amount"
    }
    columns = [{"name": col_rename.get(c, c), "id": c} for c in show_cols]
    return df_show.to_dict("records"), columns


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
