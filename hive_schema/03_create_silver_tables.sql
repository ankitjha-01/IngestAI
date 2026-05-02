-- Silver layer tables + Pipeline Lineage

-- All Silver tables are EXTERNAL — Spark own files, Hive owns metadata.
-- Partitioned by (processing_date, source_id) for minimal scans.
-- US + India card txns both land in card_transactions_silver, separated by source_id.

USE mif_db;


-- Unified card transactions (US + IN)
-- amount_usd always in USD — INR converted by Spark engine
CREATE EXTERNAL TABLE IF NOT EXISTS card_transactions_silver (

  transaction_id    STRING,
  card_identifier   STRING    COMMENT 'Card number or token, varies by region',
  merchant_name     STRING,
  amount_usd        DOUBLE    COMMENT 'Always USD, INR converted at runtime',
  transaction_ts    TIMESTAMP COMMENT 'UTC, normalized from region-specific formats',
  status            STRING    COMMENT 'APPROVED | DECLINED | PENDING',
  is_international  INT,
  source_currency   STRING    COMMENT 'Original currency: USD, INR',
  original_amount   DOUBLE    COMMENT 'Amount in source_currency, null for USD sources',
  dq_passed         BOOLEAN,
  run_id            STRING,
  ingested_at       TIMESTAMP
)
PARTITIONED BY (processing_date STRING, source_id STRING)
STORED AS PARQUET
LOCATION 'hdfs://master-node:9000/mif/silver/card_transactions/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');


-- Merchant reference data — full snapshot overwrite each run
CREATE EXTERNAL TABLE IF NOT EXISTS merchant_silver (

  merchant_id       STRING,
  business_name     STRING,
  merchant_category STRING,
  country_code      STRING    COMMENT 'ISO 3166-1 alpha-2, DQ validates ^[A-Z]{2}$',
  city              STRING,
  risk_tier         STRING    COMMENT 'LOW | MEDIUM | HIGH',
  onboarded_date    DATE,
  is_active         INT,
  dq_passed         BOOLEAN,
  run_id            STRING,
  ingested_at       TIMESTAMP
)
PARTITIONED BY (processing_date STRING, source_id STRING)
STORED AS PARQUET
LOCATION 'hdfs://master-node:9000/mif/silver/merchant_updates/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');


-- Singapore ATM withdrawals
-- DQ rule: amount_sgd must be multiple of 50
CREATE EXTERNAL TABLE IF NOT EXISTS atm_silver (

  withdrawal_id     STRING,
  atm_id            STRING,
  card_hash         STRING    COMMENT 'Truncated SHA-256, PII masked at source',
  amount_sgd        DOUBLE    COMMENT 'Must be multiple of 50',
  currency          STRING    COMMENT 'Always SGD',
  withdrawal_ts     TIMESTAMP,
  dispense_status   STRING    COMMENT 'DISPENSED | FAILED | PARTIAL',
  location          STRING,
  dq_passed         BOOLEAN,
  run_id            STRING,
  ingested_at       TIMESTAMP
)
PARTITIONED BY (processing_date STRING, source_id STRING)
STORED AS PARQUET
LOCATION 'hdfs://master-node:9000/mif/silver/atm_withdrawals/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');


-- Fraud signals from risk model
-- Joins to card_transactions_silver on transaction_ref = transaction_id
CREATE EXTERNAL TABLE IF NOT EXISTS fraud_flags_silver (

  flag_id           STRING,
  transaction_ref   STRING    COMMENT 'FK → card_transactions_silver.transaction_id',
  fraud_score       DOUBLE    COMMENT '0.5 (low) to 1.0 (definite fraud)',
  flag_type         STRING    COMMENT 'VELOCITY | GEO_ANOMALY | AMOUNT_SPIKE | DEVICE_MISMATCH | NEW_MERCHANT',
  flagged_at        TIMESTAMP,
  reviewed          INT       COMMENT '1 = analyst reviewed, 0 = pending',
  dq_passed         BOOLEAN,
  run_id            STRING,
  ingested_at       TIMESTAMP
)
PARTITIONED BY (processing_date STRING, source_id STRING)
STORED AS PARQUET
LOCATION 'hdfs://master-node:9000/mif/silver/fraud_flags/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');


-- Row-count reconciliation: rows_read = rows_written + rows_rejected
CREATE EXTERNAL TABLE IF NOT EXISTS pipeline_lineage (

  lineage_id          STRING,
  run_id              STRING,
  source_id           STRING,

  source_path         STRING    COMMENT 'HDFS path of the raw file read',
  target_table        STRING    COMMENT 'Silver table written to',
  target_path         STRING    COMMENT 'Exact HDFS partition path written',

  rows_read           BIGINT,
  rows_written        BIGINT,
  rows_rejected       BIGINT,

  transformation_log  STRING    COMMENT 'JSON: {column_mapping_applied, dq_rules_applied, dedup_column}',
  spark_app_id        STRING    COMMENT 'Look up in Spark UI for stage-level debugging',

  started_at          TIMESTAMP,
  completed_at        TIMESTAMP,
  status              STRING    COMMENT 'SUCCESS | PARTIAL | FAILED'
)
PARTITIONED BY (processing_date STRING)
STORED AS PARQUET
LOCATION 'hdfs://master-node:9000/mif/hive/lineage/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');


SHOW TABLES IN mif_db;
