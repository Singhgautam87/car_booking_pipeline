import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import plotly.graph_objects as go
import mysql.connector
import pandas as pd

# ============ MYSQL CONNECTION ============
def get_data():
    conn = mysql.connector.connect(
        host="mysql",
        user="admin",
        password="admin",
        database="booking"
    )
    booking_df = pd.read_sql("SELECT * FROM customer_booking_final", conn)
    validation_df = pd.read_sql("SELECT * FROM validation_results", conn)
    conn.close()
    return booking_df, validation_df

booking_df, validation_df = get_data()

# ============ APP ============
app = dash.Dash(__name__)
app.title = "Car Booking Pipeline Dashboard"

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

                # Refresh button
                html.Button("🔄 Refresh Data", id="refresh-btn",
                            style={"backgroundColor": "#e94560", "color": "white",
                                   "border": "none", "padding": "10px 20px",
                                   "borderRadius": "5px", "cursor": "pointer",
                                   "fontSize": "14px"})
            ]
        ),

        # KPI Cards
        html.Div(
            style={"display": "flex", "justifyContent": "space-around", "marginBottom": "30px"},
            children=[
                # Total Bookings
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "width": "18%", "border": "1px solid #e94560"},
                    children=[
                        html.H2(f"{booking_df['booking_id'].nunique():,}",
                                style={"color": "#e94560", "fontSize": "32px", "margin": "0"}),
                        html.P("Total Bookings", style={"color": "#a8a8b3"})
                    ]
                ),
                # Total Customers
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "width": "18%", "border": "1px solid #0f3460"},
                    children=[
                        html.H2(f"{booking_df['customer_id'].nunique():,}",
                                style={"color": "#4fc3f7", "fontSize": "32px", "margin": "0"}),
                        html.P("Total Customers", style={"color": "#a8a8b3"})
                    ]
                ),
                # Total Revenue
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "width": "18%", "border": "1px solid #0f3460"},
                    children=[
                        html.H2(f"₹{booking_df['payment_amount'].sum():,.0f}",
                                style={"color": "#81c784", "fontSize": "32px", "margin": "0"}),
                        html.P("Total Revenue", style={"color": "#a8a8b3"})
                    ]
                ),
                # Total Cars
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "width": "18%", "border": "1px solid #0f3460"},
                    children=[
                        html.H2(f"{booking_df['car_id'].nunique():,}",
                                style={"color": "#ffb74d", "fontSize": "32px", "margin": "0"}),
                        html.P("Total Cars", style={"color": "#a8a8b3"})
                    ]
                ),
                # GE Success Rate
                html.Div(
                    style={"backgroundColor": "#16213e", "padding": "20px", "borderRadius": "10px",
                           "textAlign": "center", "width": "18%", "border": "1px solid #0f3460"},
                    children=[
                        html.H2(
                            f"{validation_df['success_rate'].mean():.1f}%" if not validation_df.empty else "N/A",
                            style={"color": "#ce93d8", "fontSize": "32px", "margin": "0"}),
                        html.P("Avg GE Success Rate", style={"color": "#a8a8b3"})
                    ]
                ),
            ]
        ),

        # Row 1 — Bookings per day + Revenue per day
        html.Div(
            style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
            children=[
                html.Div(style={"width": "50%"}, children=[
                    dcc.Graph(id="bookings-per-day")
                ]),
                html.Div(style={"width": "50%"}, children=[
                    dcc.Graph(id="revenue-per-day")
                ])
            ]
        ),

        # Row 2 — Popular cars + Payment methods
        html.Div(
            style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
            children=[
                html.Div(style={"width": "50%"}, children=[
                    dcc.Graph(id="popular-cars")
                ]),
                html.Div(style={"width": "50%"}, children=[
                    dcc.Graph(id="payment-methods")
                ])
            ]
        ),

        # Row 3 — Loyalty tiers + Insurance coverage
        html.Div(
            style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
            children=[
                html.Div(style={"width": "50%"}, children=[
                    dcc.Graph(id="loyalty-tiers")
                ]),
                html.Div(style={"width": "50%"}, children=[
                    dcc.Graph(id="insurance-coverage")
                ])
            ]
        ),

        # Row 4 — Pickup locations + GE Validation history
        html.Div(
            style={"display": "flex", "gap": "20px", "marginBottom": "20px"},
            children=[
                html.Div(style={"width": "50%"}, children=[
                    dcc.Graph(id="pickup-locations")
                ]),
                html.Div(style={"width": "50%"}, children=[
                    dcc.Graph(id="ge-validation")
                ])
            ]
        ),

        # Hidden div for refresh
        dcc.Store(id="store-data")
    ]
)

# ============ CHART STYLE ============
CHART_STYLE = {
    "paper_bgcolor": "#16213e",
    "plot_bgcolor": "#16213e",
    "font": {"color": "#a8a8b3"},
    "title_font": {"color": "white", "size": 16}
}

# ============ CALLBACKS ============
@app.callback(
    [Output("bookings-per-day", "figure"),
     Output("revenue-per-day", "figure"),
     Output("popular-cars", "figure"),
     Output("payment-methods", "figure"),
     Output("loyalty-tiers", "figure"),
     Output("insurance-coverage", "figure"),
     Output("pickup-locations", "figure"),
     Output("ge-validation", "figure")],
    Input("refresh-btn", "n_clicks")
)
def update_charts(n_clicks):
    booking_df, validation_df = get_data()

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
                  color="count",
                  color_continuous_scale="reds")
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
                      "platinum": "#ce93d8",
                      "gold": "#ffb74d",
                      "silver": "#a8a8b3",
                      "bronze": "#ff8a65"
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
                  orientation="h",
                  color="count",
                  color_continuous_scale="blues")
    fig7.update_layout(**CHART_STYLE)

    # 8. GE Validation history
    if not validation_df.empty:
        fig8 = px.line(validation_df, x="created_at", y="success_rate",
                       title="✅ GE Validation Success Rate History",
                       color_discrete_sequence=["#81c784"],
                       markers=True)
        fig8.add_hline(y=100, line_dash="dash", line_color="#e94560",
                       annotation_text="100% Target")
    else:
        fig8 = go.Figure()
        fig8.add_annotation(text="No validation data yet",
                           xref="paper", yref="paper",
                           x=0.5, y=0.5, showarrow=False,
                           font={"color": "white", "size": 20})
    fig8.update_layout(**CHART_STYLE, title="✅ GE Validation Success Rate History")

    return fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=False)