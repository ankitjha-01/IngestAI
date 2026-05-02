-- Data Quality tracking tables

USE mif_db;

-- One row per DQ rule evaluation per run
-- Granular audit log, never deleted
CREATE EXTERNAL TABLE IF NOT EXISTS dq_results (
    run_id               STRING     COMMENT 'UUID for this ingestion run',
    source_id            STRING,
    source_name          STRING     COMMENT 'Denormalized to avoid joins in dashboards',

    rule_id              STRING     COMMENT 'Stable ID from dq_rules_json: r001, r_auto_003',
    rule_name            STRING     COMMENT 'amount_not_null, status_valid',
    rule_type            STRING     COMMENT 'not_null | range | regex | allowed_values | custom',
    rule_expression      STRING     COMMENT 'Evaluated expression as string, for audit',
    column_name          STRING,

    total_rows           BIGINT,
    passed_rows          BIGINT,
    failed_rows          BIGINT,
    pass_rate            DOUBLE     COMMENT '0.0 to 1.0',
    threshold            DOUBLE     COMMENT 'Min acceptable pass_rate from dq_rules_json',
    rule_passed          BOOLEAN    COMMENT 'True if pass_rate >= threshold',

    sample_failures      STRING     COMMENT 'JSON array of up to 3 failing rows',
    checked_at           TIMESTAMP
)
PARTITIONED BY (
    processing_date      STRING,
    source_id_part       STRING
)
STORED AS PARQUET
LOCATION 'hdfs://master-node:9000/mif/hive/dq_results/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');


-- One row per source per run (aggregated from dq_results)
-- Read by dashboard — avoids GROUP BY at query time
CREATE EXTERNAL TABLE IF NOT EXISTS dq_summary (
    run_id                 STRING,
    source_id              STRING,
    source_name            STRING,
    processing_date        STRING,

    total_rows_ingested    BIGINT,
    total_rows_passed      BIGINT,
    total_rows_rejected    BIGINT,

    overall_pass_rate      DOUBLE,
    rules_checked          INT,
    rules_passed           INT,
    rules_failed           INT,

    ingestion_status       STRING     COMMENT 'SUCCESS | PARTIAL | FAILED',
    duration_seconds       BIGINT,

    silver_path            STRING,
    rejected_path          STRING,
    run_completed_at       TIMESTAMP
)
PARTITIONED BY (
    processing_date_part   STRING
)
STORED AS PARQUET
LOCATION 'hdfs://master-node:9000/mif/hive/dq_results/summary/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');


DESCRIBE FORMATTED mif_db.dq_results;