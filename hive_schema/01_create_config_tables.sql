-- Config + Schema Registry tables

CREATE DATABASE IF NOT EXISTS mif_db
  COMMENT 'Metadata Ingestion Framework';

USE mif_db;


-- Master source registry — one row per data source
CREATE TABLE IF NOT EXISTS ingestion_config (

  source_id           STRING    COMMENT 'Unique key: src_us_card_txns',
  source_name         STRING    COMMENT 'Label for dashboards',
  source_type         STRING    COMMENT 'card_transactions | merchant | atm | fraud_flags',

  hdfs_raw_path       STRING    COMMENT 'HDFS path to raw file or directory',
  file_format         STRING    COMMENT 'csv | parquet | json',
  delimiter           STRING    COMMENT 'CSV separator',
  has_header          BOOLEAN,

  date_column         STRING    COMMENT 'Column carrying the event timestamp',
  date_format         STRING    COMMENT 'Timestamp format: yyyy-MM-dd HH:mm:ss',

  primary_key_column  STRING,

  target_table        STRING    COMMENT 'Silver table name: card_transactions_silver',
  hdfs_silver_path    STRING,
  hdfs_rejected_path  STRING    COMMENT 'DQ-failed rows land here, never discarded',

  -- JSON so new rule types need zero DDL changes
  dq_rules_json       STRING    COMMENT '[{rule_id, rule_name, column, type, expression?, values?, threshold}]',
  column_mapping_json STRING    COMMENT '{"raw_col": "silver_col"}',

  is_active           BOOLEAN   COMMENT 'False = skip on all pipeline runs',
  created_at          TIMESTAMP,
  created_by          STRING    COMMENT 'human | llm_agent',
  last_modified_at    TIMESTAMP,
  schema_version      INT       COMMENT 'Increment on column_mapping or dq_rules change'
)
COMMENT 'Pipeline behaviour is 100% driven by this table'
STORED AS ORC
TBLPROPERTIES (
  'transactional' = 'true',
  'orc.compress'  = 'SNAPPY'
);


-- Column-level metadata for schema drift detection
-- Append-only, partitioned by source for fast lookups
-- For each (source_id, schema_version), rows = total columns in that schema snapshot
CREATE EXTERNAL TABLE IF NOT EXISTS schema_registry (

  source_id       STRING    COMMENT 'FK → ingestion_config.source_id',
  column_name     STRING,
  column_type     STRING    COMMENT 'string | integer | double | timestamp | boolean',
  is_nullable     BOOLEAN,
  sample_values   STRING    COMMENT 'Up to 10 representative values',
  null_rate       DOUBLE    COMMENT '0.0 to 1.0',
  min_value       STRING,
  max_value       STRING,
  schema_version  INT       COMMENT 'Matches ingestion_config.schema_version',
  analysed_at     TIMESTAMP
)
PARTITIONED BY (source_id_part STRING)
STORED AS PARQUET
LOCATION 'hdfs://master-node:9000/mif/hive/config/schema_registry/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');


SHOW TABLES IN mif_db;
