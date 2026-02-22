CREATE TABLE IF NOT EXISTS customer_booking_final (
    booking_id          VARCHAR(20),
    customer_id         VARCHAR(20),
    customer_name       VARCHAR(100),
    email               VARCHAR(100),
    loyalty_tier        VARCHAR(20),
    loyalty_points      INT,
    booking_date        DATE,
    pickup_location     VARCHAR(100),
    pickup_time         VARCHAR(20),
    drop_location       VARCHAR(100),
    drop_time           VARCHAR(20),
    car_id              VARCHAR(20),
    model               VARCHAR(50),
    price_per_day       DOUBLE,
    insurance_provider  VARCHAR(50),
    insurance_coverage  VARCHAR(20),
    payment_id          VARCHAR(20),
    payment_method      VARCHAR(30),
    payment_amount      DOUBLE
);

CREATE TABLE IF NOT EXISTS validation_results (
    result_id           INT AUTO_INCREMENT PRIMARY KEY,
    run_identifier      VARCHAR(255),
    checkpoint_name     VARCHAR(100),
    total_expectations  INT,
    passed_expectations INT,
    failed_expectations INT,
    success_rate        DECIMAL(5,2),
    validation_status   VARCHAR(50),
    validation_details  JSON,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_run_identifier (run_identifier),
    INDEX idx_created (created_at)
);