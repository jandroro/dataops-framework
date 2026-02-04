CREATE TABLE IF NOT EXISTS dataops.silver_customers (
    ingest_ts TIMESTAMP,
    customer_id STRING,
    customer_name STRING,
    email STRING,
    status STRING,
    payload STRING,
    dq_is_name_null BOOLEAN,
    dq_is_underage BOOLEAN,
    dq_status STRING
);