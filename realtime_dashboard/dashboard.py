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
app.title = "Car Booking Pipeline Dashboard"

booking_df, validation_df, expectation_df, failed_df = get_data()

# ============ CHART STYLE ============
CHART_STYLE = {
    "paper_bgcolor": "#16213e",
    "plot_bgcolor": "#16213e",
    "font": {"color": "#a8a8b3"},
    "title_font": {"color": "white", "size": 16}
}

# ============ LAYOUT ============
app.layout = html.Div(
    style={"backgroundColor": "#1a1a2e", "minHeight": "100vh", "padding": "20px", "fontFamily": "Arial"},
    children=[

        # Header
        html.Div(
            style={"textAlign": "center", "padding": "20px", "marginBottom": "30px"},
            children=[
                html.H1("🚗 Car Booking Pipeline Dashboard",
                        style={"color": "#e94560", "fontSize": "36px"}),
                html.P("Real-time Data Engineering Pipeline Monitor",
                       style={"color": "#a8a8b3", "fontSize": "16px"}),
                html.Button("🔄 Refresh Data", id="refresh-btn",
                            style={"backgroundColor": "#e94560", "color": "white",
                                   "border": "none", "padding": "10px 20px",
                                   "borderRadius": "5px", "cursor": "pointer",
                                   "fontSize": "14px"}),
                dcc.Interval(id="auto-refresh", interval=30000, n_intervals=0)
            ]
        ),

        # ============ BOOKING KPI CARDS ============
        html.H2("📊 Booking Overview", style={"color": "white", "marginBottom": "15px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-around", "marginBottom": "30px"},
            children=[
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "width": "18%", "border": "1px solid #e94560"},
                    children=[
                        html.H2(id="kpi-bookings", style={"color": "#e94560", "fontSize": "32px", "margin": "0"}),
                        html.P("Total Bookings", style={"color": "#a8a8b3"})
                    ]
                ),
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "width": "18%", "border": "1px solid #4fc3f7"},
                    children=[
                        html.H2(id="kpi-customers", style={"color": "#4fc3f7", "fontSize": "32px", "margin": "0"}),
                        html.P("Total Customers", style={"color": "#a8a8b3"})
                    ]
                ),
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "width": "18%", "border": "1px solid #81c784"},
                    children=[
                        html.H2(id="kpi-revenue", style={"color": "#81c784", "fontSize": "32px", "margin": "0"}),
                        html.P("Total Revenue", style={"color": "#a8a8b3"})
                    ]
                ),
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "width": "18%", "border": "1px solid #ffb74d"},
                    children=[
                        html.H2(id="kpi-cars", style={"color": "#ffb74d", "fontSize": "32px", "margin": "0"}),
                        html.P("Total Cars", style={"color": "#a8a8b3"})
                    ]
                ),
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "width": "18%", "border": "1px solid #ce93d8"},
                    children=[
                        html.H2(id="kpi-success-rate", style={"color": "#ce93d8", "fontSize": "32px", "margin": "0"}),
                        html.P("Avg GE Success Rate", style={"color": "#a8a8b3"})
                    ]
                ),
            ]
        ),

        # Row 1 — Bookings per day + Revenue per day
        html.Div(
            style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
            children=[
                html.Div(style={"width": "50%"}, children=[dcc.Graph(id="bookings-per-day")]),
                html.Div(style={"width": "50%"}, children=[dcc.Graph(id="revenue-per-day")])
            ]
        ),

        # Row 2 — Popular cars + Payment methods
        html.Div(
            style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
            children=[
                html.Div(style={"width": "50%"}, children=[dcc.Graph(id="popular-cars")]),
                html.Div(style={"width": "50%"}, children=[dcc.Graph(id="payment-methods")])
            ]
        ),

        # Row 3 — Loyalty tiers + Insurance coverage
        html.Div(
            style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
            children=[
                html.Div(style={"width": "50%"}, children=[dcc.Graph(id="loyalty-tiers")]),
                html.Div(style={"width": "50%"}, children=[dcc.Graph(id="insurance-coverage")])
            ]
        ),

        # Row 4 — Pickup locations + GE Validation history
        html.Div(
            style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
            children=[
                html.Div(style={"width": "50%"}, children=[dcc.Graph(id="pickup-locations")]),
                html.Div(style={"width": "50%"}, children=[dcc.Graph(id="ge-validation")])
            ]
        ),

        # ============ GE VALIDATION SECTION ============
        html.Hr(style={"borderColor": "#e94560", "margin": "30px 0"}),
        html.H2("🔍 Data Quality - Great Expectations",
                style={"color": "#e94560", "marginBottom": "20px"}),

        # GE KPI Cards
        html.Div(
            style={"display": "flex", "justifyContent": "space-around", "marginBottom": "30px"},
            children=[
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "width": "22%", "border": "1px solid #81c784"},
                    children=[
                        html.H2(id="ge-total-runs", style={"color": "#81c784", "fontSize": "32px", "margin": "0"}),
                        html.P("Total Validation Runs", style={"color": "#a8a8b3"})
                    ]
                ),
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "width": "22%", "border": "1px solid #4fc3f7"},
                    children=[
                        html.H2(id="ge-passed", style={"color": "#4fc3f7", "fontSize": "32px", "margin": "0"}),
                        html.P("Total Passed Expectations", style={"color": "#a8a8b3"})
                    ]
                ),
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "width": "22%", "border": "1px solid #e94560"},
                    children=[
                        html.H2(id="ge-failed", style={"color": "#e94560", "fontSize": "32px", "margin": "0"}),
                        html.P("Total Failed Expectations", style={"color": "#a8a8b3"})
                    ]
                ),
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "width": "22%", "border": "1px solid #ffb74d"},
                    children=[
                        html.H2(id="ge-failed-records", style={"color": "#ffb74d", "fontSize": "32px", "margin": "0"}),
                        html.P("Total Failed Records", style={"color": "#a8a8b3"})
                    ]
                ),
            ]
        ),

        # GE Row 1 — Expectation Pass/Fail + Column wise failures
        html.Div(
            style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
            children=[
                html.Div(style={"width": "50%"}, children=[dcc.Graph(id="ge-expectation-passfail")]),
                html.Div(style={"width": "50%"}, children=[dcc.Graph(id="ge-column-failures")])
            ]
        ),

        # GE Row 2 — Gauge chart + Failed loyalty tier
        html.Div(
            style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
            children=[
                html.Div(style={"width": "50%"}, children=[dcc.Graph(id="ge-gauge")]),
                html.Div(style={"width": "50%"}, children=[dcc.Graph(id="ge-failed-loyalty")])
            ]
        ),

        # GE Row 3 — Run dropdown + Failed records table
        html.Div(
            style={"marginBottom": "20px"},
            children=[
                html.H3("🔎 Failed Records Detail", style={"color": "white", "marginBottom": "10px"}),
                html.Div(
                    style={"display": "flex", "gap": "20px", "marginBottom": "10px"},
                    children=[
                        dcc.Dropdown(
                            id="run-dropdown",
                            placeholder="Select Run...",
                            style={"width": "300px", "backgroundColor": "#16213e", "color": "black"}
                        ),
                        dcc.Dropdown(
                            id="expectation-dropdown",
                            placeholder="Filter by Expectation...",
                            style={"width": "300px", "backgroundColor": "#16213e", "color": "black"}
                        ),
                    ]
                ),
                dash_table.DataTable(
                    id="failed-records-table",
                    style_table={"overflowX": "auto"},
                    style_cell={
                        "backgroundColor": "#16213e",
                        "color": "#a8a8b3",
                        "border": "1px solid #0f3460",
                        "textAlign": "left",
                        "padding": "8px",
                        "fontSize": "12px"
                    },
                    style_header={
                        "backgroundColor": "#0f3460",
                        "color": "white",
                        "fontWeight": "bold"
                    },
                    style_data_conditional=[
                        {
                            "if": {"row_index": "odd"},
                            "backgroundColor": "#1a1a2e"
                        }
                    ],
                    page_size=10,
                    filter_action="native",
                    sort_action="native"
                )
            ]
        ),

        dcc.Store(id="store-data")
    ]
)


# ============ CALLBACKS ============
@app.callback(
    [
        # Booking KPIs
        Output("kpi-bookings", "children"),
        Output("kpi-customers", "children"),
        Output("kpi-revenue", "children"),
        Output("kpi-cars", "children"),
        Output("kpi-success-rate", "children"),
        # Booking charts
        Output("bookings-per-day", "figure"),
        Output("revenue-per-day", "figure"),
        Output("popular-cars", "figure"),
        Output("payment-methods", "figure"),
        Output("loyalty-tiers", "figure"),
        Output("insurance-coverage", "figure"),
        Output("pickup-locations", "figure"),
        Output("ge-validation", "figure"),
        # GE KPIs
        Output("ge-total-runs", "children"),
        Output("ge-passed", "children"),
        Output("ge-failed", "children"),
        Output("ge-failed-records", "children"),
        # GE charts
        Output("ge-expectation-passfail", "figure"),
        Output("ge-column-failures", "figure"),
        Output("ge-gauge", "figure"),
        Output("ge-failed-loyalty", "figure"),
        # Dropdowns
        Output("run-dropdown", "options"),
        Output("expectation-dropdown", "options"),
    ],
    [Input("refresh-btn", "n_clicks"),
     Input("auto-refresh", "n_intervals")]
)
def update_all(n_clicks, n_intervals):
    booking_df, validation_df, expectation_df, failed_df = get_data()

    # ===== BOOKING KPIs =====
    kpi_bookings = f"{booking_df['booking_id'].nunique():,}"
    kpi_customers = f"{booking_df['customer_id'].nunique():,}"
    kpi_revenue = f"₹{booking_df['payment_amount'].sum():,.0f}"
    kpi_cars = f"{booking_df['car_id'].nunique():,}"
    kpi_success = f"{validation_df['success_rate'].mean():.1f}%" if not validation_df.empty else "N/A"

    # ===== BOOKING CHARTS =====

    # 1. Bookings per day
    bookings_day = booking_df.groupby("booking_date")["booking_id"].nunique().reset_index()
    fig1 = px.line(bookings_day, x="booking_date", y="booking_id",
                   title="📅 Bookings Per Day",
                   color_discrete_sequence=["#e94560"])
    fig1.update_layout(**CHART_STYLE)

    # 2. Revenue per day
    revenue_day = booking_df.groupby("booking_date")["payment_amount"].sum().reset_index()
    fig2 = px.area(revenue_day, x="booking_date", y="payment_amount",
                   title="💰 Revenue Per Day",
                   color_discrete_sequence=["#81c784"])
    fig2.update_layout(**CHART_STYLE)

    # 3. Popular cars
    popular_cars = booking_df["model"].value_counts().reset_index()
    popular_cars.columns = ["model", "count"]
    fig3 = px.bar(popular_cars, x="model", y="count",
                  title="🚗 Popular Car Models",
                  color="count", color_continuous_scale="reds")
    fig3.update_layout(**CHART_STYLE)

    # 4. Payment methods
    payment = booking_df["payment_method"].value_counts().reset_index()
    payment.columns = ["method", "count"]
    fig4 = px.pie(payment, names="method", values="count",
                  title="💳 Payment Methods",
                  color_discrete_sequence=px.colors.sequential.RdBu)
    fig4.update_layout(**CHART_STYLE)

    # 5. Loyalty tiers
    loyalty = booking_df["loyalty_tier"].value_counts().reset_index()
    loyalty.columns = ["tier", "count"]
    fig5 = px.bar(loyalty, x="tier", y="count",
                  title="⭐ Loyalty Tiers",
                  color="tier",
                  color_discrete_map={
                      "platinum": "#ce93d8", "gold": "#ffb74d",
                      "silver": "#a8a8b3", "bronze": "#ff8a65"
                  })
    fig5.update_layout(**CHART_STYLE)

    # 6. Insurance coverage
    insurance = booking_df["insurance_coverage"].value_counts().reset_index()
    insurance.columns = ["coverage", "count"]
    fig6 = px.pie(insurance, names="coverage", values="count",
                  title="🛡️ Insurance Coverage",
                  color_discrete_sequence=["#4fc3f7", "#e94560"])
    fig6.update_layout(**CHART_STYLE)

    # 7. Pickup locations
    pickup = booking_df["pickup_location"].value_counts().head(10).reset_index()
    pickup.columns = ["location", "count"]
    fig7 = px.bar(pickup, x="count", y="location",
                  title="📍 Top 10 Pickup Locations",
                  orientation="h", color="count",
                  color_continuous_scale="blues")
    fig7.update_layout(**CHART_STYLE)

    # 8. GE Validation history
    if not validation_df.empty:
        fig8 = px.line(validation_df, x="created_at", y="success_rate",
                       title="✅ GE Validation Success Rate History",
                       color_discrete_sequence=["#81c784"], markers=True)
        fig8.add_hline(y=100, line_dash="dash", line_color="#e94560",
                       annotation_text="100% Target")
    else:
        fig8 = go.Figure()
        fig8.add_annotation(text="No validation data yet",
                            xref="paper", yref="paper",
                            x=0.5, y=0.5, showarrow=False,
                            font={"color": "white", "size": 20})
    fig8.update_layout(**CHART_STYLE, title="✅ GE Validation Success Rate History")

    # ===== GE KPIs =====
    ge_total_runs = f"{len(validation_df):,}" if not validation_df.empty else "0"
    ge_passed = f"{expectation_df['success'].sum():,}" if not expectation_df.empty else "0"
    ge_failed = f"{(~expectation_df['success'].astype(bool)).sum():,}" if not expectation_df.empty else "0"
    ge_failed_records = f"{len(failed_df):,}" if not failed_df.empty else "0"

    # ===== GE CHARTS =====

    # 9. Expectation Pass/Fail Bar Chart
    if not expectation_df.empty:
        exp_summary = expectation_df.groupby("expectation_type")["success"].agg(
            passed=lambda x: x.astype(bool).sum(),
            failed=lambda x: (~x.astype(bool)).sum()
        ).reset_index()
        exp_melted = exp_summary.melt(id_vars="expectation_type",
                                      value_vars=["passed", "failed"],
                                      var_name="status", value_name="count")
        fig9 = px.bar(exp_melted, x="expectation_type", y="count",
                      color="status", barmode="group",
                      title="📊 Expectation Pass/Fail",
                      color_discrete_map={"passed": "#81c784", "failed": "#e94560"})
        fig9.update_layout(**CHART_STYLE, xaxis_tickangle=-45)
    else:
        fig9 = go.Figure()
        fig9.add_annotation(text="No expectation data", xref="paper", yref="paper",
                            x=0.5, y=0.5, showarrow=False, font={"color": "white", "size": 20})
        fig9.update_layout(**CHART_STYLE, title="📊 Expectation Pass/Fail")

    # 10. Column wise failures
    if not expectation_df.empty:
        col_failures = expectation_df[~expectation_df["success"].astype(bool)].groupby(
            "column_name")["unexpected_count"].sum().reset_index()
        col_failures.columns = ["column_name", "failed_count"]
        col_failures = col_failures.sort_values("failed_count", ascending=True)
        fig10 = px.bar(col_failures, x="failed_count", y="column_name",
                       orientation="h",
                       title="⚠️ Failed Records by Column",
                       color="failed_count",
                       color_continuous_scale="reds")
        fig10.update_layout(**CHART_STYLE)
    else:
        fig10 = go.Figure()
        fig10.add_annotation(text="No failures found", xref="paper", yref="paper",
                             x=0.5, y=0.5, showarrow=False, font={"color": "white", "size": 20})
        fig10.update_layout(**CHART_STYLE, title="⚠️ Failed Records by Column")

    # 11. Gauge Chart — Data Quality Score
    avg_rate = validation_df["success_rate"].mean() if not validation_df.empty else 0
    fig11 = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=avg_rate,
        title={"text": "🎯 Data Quality Score", "font": {"color": "white"}},
        delta={"reference": 100, "increasing": {"color": "#81c784"}},
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
            "threshold": {
                "line": {"color": "white", "width": 4},
                "thickness": 0.75,
                "value": 100
            }
        }
    ))
    fig11.update_layout(**CHART_STYLE, title="🎯 Data Quality Score")

    # 12. Failed records by loyalty tier
    if not failed_df.empty:
        failed_loyalty = failed_df["loyalty_tier"].value_counts().reset_index()
        failed_loyalty.columns = ["tier", "count"]
        fig12 = px.pie(failed_loyalty, names="tier", values="count",
                       title="⭐ Failed Records by Loyalty Tier",
                       color="tier",
                       color_discrete_map={
                           "platinum": "#ce93d8", "gold": "#ffb74d",
                           "silver": "#a8a8b3", "bronze": "#ff8a65"
                       })
        fig12.update_layout(**CHART_STYLE)
    else:
        fig12 = go.Figure()
        fig12.add_annotation(text="No failed records", xref="paper", yref="paper",
                             x=0.5, y=0.5, showarrow=False, font={"color": "white", "size": 20})
        fig12.update_layout(**CHART_STYLE, title="⭐ Failed Records by Loyalty Tier")

    # ===== DROPDOWNS =====
    run_options = [{"label": r, "value": r} for r in failed_df["run_identifier"].unique()] if not failed_df.empty else []
    exp_options = [{"label": e, "value": e} for e in failed_df["expectation_type"].unique()] if not failed_df.empty else []

    return (
        kpi_bookings, kpi_customers, kpi_revenue, kpi_cars, kpi_success,
        fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8,
        ge_total_runs, ge_passed, ge_failed, ge_failed_records,
        fig9, fig10, fig11, fig12,
        run_options, exp_options
    )


# ===== FAILED RECORDS TABLE CALLBACK =====
@app.callback(
    [Output("failed-records-table", "data"),
     Output("failed-records-table", "columns")],
    [Input("run-dropdown", "value"),
     Input("expectation-dropdown", "value"),
     Input("refresh-btn", "n_clicks"),
     Input("auto-refresh", "n_intervals")]
)
def update_failed_table(run_id, exp_type, n_clicks, n_intervals):
    _, _, _, failed_df = get_data()

    if failed_df.empty:
        return [], []

    # Filter karo
    if run_id:
        failed_df = failed_df[failed_df["run_identifier"] == run_id]
    if exp_type:
        failed_df = failed_df[failed_df["expectation_type"] == exp_type]

    # Columns select karo
    show_cols = ["run_identifier", "expectation_type", "column_name",
                 "failed_reason", "booking_id", "customer_name",
                 "email", "loyalty_tier", "payment_amount"]

    # Sirf jo columns exist karte hain
    show_cols = [c for c in show_cols if c in failed_df.columns]
    df_show = failed_df[show_cols].head(100)

    columns = [{"name": c.replace("_", " ").title(), "id": c} for c in show_cols]
    data = df_show.to_dict("records")

    return data, columns


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)