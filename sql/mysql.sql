-- ── 1. VALIDATION RESULTS (run summary) ──────────────
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
    INDEX idx_created   (created_at)
);

-- ── 2. VALIDATION EXPECTATION DETAILS (flat GE JSON) ─
CREATE TABLE IF NOT EXISTS validation_expectation_details (
    id                              INT AUTO_INCREMENT PRIMARY KEY,
    run_identifier                  VARCHAR(255),
    expectation_type                VARCHAR(100),
    column_name                     VARCHAR(100),
    batch_id                        VARCHAR(255),
    success                         BOOLEAN,
    observed_value                  VARCHAR(255),
    element_count                   INT          DEFAULT 0,
    unexpected_count                INT          DEFAULT 0,
    unexpected_percent              DECIMAL(8,4) DEFAULT 0.0000,
    missing_count                   INT          DEFAULT 0,
    missing_percent                 DECIMAL(8,4) DEFAULT 0.0000,
    unexpected_percent_total        DECIMAL(8,4) DEFAULT 0.0000,
    unexpected_percent_nonmissing   DECIMAL(8,4) DEFAULT 0.0000,
    partial_unexpected_list         JSON,
    partial_unexpected_counts       JSON,
    min_value                       VARCHAR(50),
    max_value                       VARCHAR(50),
    regex                           VARCHAR(255),
    value_set                       JSON,
    created_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_run         (run_identifier),
    INDEX idx_success     (success),
    INDEX idx_column      (column_name),
    INDEX idx_expectation (expectation_type)
);

-- ── 3. VALIDATION FAILED RECORDS ─────────────────────
CREATE TABLE IF NOT EXISTS validation_failed_records (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    run_identifier      VARCHAR(255),
    expectation_type    VARCHAR(100),
    column_name         VARCHAR(100),
    failed_reason       VARCHAR(255),
    failed_value        VARCHAR(255),
    booking_id          VARCHAR(20),
    customer_id         VARCHAR(20),
    customer_name       VARCHAR(100),
    email               VARCHAR(100),
    loyalty_tier        VARCHAR(20),
    loyalty_points      INT,
    booking_date        DATE,
    pickup_location     VARCHAR(100),
    drop_location       VARCHAR(100),
    car_id              VARCHAR(20),
    model               VARCHAR(50),
    price_per_day       DOUBLE,
    insurance_provider  VARCHAR(50),
    insurance_coverage  VARCHAR(20),
    payment_id          VARCHAR(20),
    payment_method      VARCHAR(30),
    payment_amount      DOUBLE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_run       (run_identifier),
    INDEX idx_booking   (booking_id),
    INDEX idx_type      (expectation_type)
);

-- ── 4. PIPELINE RUN STATS (SLA monitoring) ───────────
CREATE TABLE IF NOT EXISTS pipeline_run_stats (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    run_id              VARCHAR(100)  NOT NULL,
    stage_name          VARCHAR(50)   NOT NULL,
    -- e.g. kafka_producer, spark_ingest, spark_transform,
    --      spark_merge, write_postgres, data_validation
    records_processed   INT           DEFAULT 0,
    duration_seconds    INT           DEFAULT 0,
    status              VARCHAR(20)   DEFAULT 'SUCCESS',
    -- SUCCESS | FAILED | SKIPPED
    error_message       TEXT,
    started_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    completed_at        TIMESTAMP     NULL,
    INDEX idx_run_id    (run_id),
    INDEX idx_stage     (stage_name),
    INDEX idx_status    (status),
    INDEX idx_started   (started_at)
);
