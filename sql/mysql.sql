CREATE TABLE IF NOT EXISTS customer_booking_final (
    booking_id VARCHAR(20),
    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    booking_status VARCHAR(30),
    car_type VARCHAR(30),
    pickup_city VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    signup_date DATE
);
