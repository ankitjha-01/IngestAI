-- Seed 4 source configs into ingestion_config

USE mif_db;

-- Reset config table, will use MERGE/UPSERT after migrating to Spark + Iceberg
TRUNCATE TABLE ingestion_config;

-- USE TEZ ON YARN
SET hive.execution.engine=tez;
-- ACID settings required for INSERT into transactional ORC table
SET hive.support.concurrency         = true;
SET hive.txn.manager                 = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.exec.dynamic.partition      = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


-- US Card Transactions (30k rows, 3% null amounts, 2% invalid status)
INSERT INTO TABLE ingestion_config VALUES (
  'src_us_card_txns',
  'US Card Transactions',
  'card_transactions',
  'hdfs://master-node:9000/mif/raw/card_transactions/processing_date={YYYY-MM-DD}/',
  'csv',
  ',',
  true,
  'transaction_time',
  'yyyy-MM-dd HH:mm:ss',
  'transaction_id',
  'card_transactions_silver',
  'hdfs://master-node:9000/mif/silver/card_transactions/',
  'hdfs://master-node:9000/mif/rejected/us_card_txns/',
  '[
    {"rule_id":"r001","rule_name":"amount_not_null",        "column":"amount_usd",      "type":"not_null",       "threshold":0.97},
    {"rule_id":"r002","rule_name":"amount_positive",        "column":"amount_usd",      "type":"range",          "expression":"amount_usd > 0 AND amount_usd < 50000","threshold":0.99},
    {"rule_id":"r003","rule_name":"status_valid",           "column":"status",          "type":"allowed_values", "values":["APPROVED","DECLINED","PENDING"],          "threshold":0.98},
    {"rule_id":"r004","rule_name":"transaction_id_not_null","column":"transaction_id",  "type":"not_null",       "threshold":1.0}
  ]',
  '{
    "transaction_id":   "transaction_id",
    "card_number":      "card_identifier",
    "merchant_name":    "merchant_name",
    "amount_usd":       "amount_usd",
    "transaction_time": "transaction_ts",
    "status":           "status",
    "is_international": "is_international"
  }',
  true,
  CURRENT_TIMESTAMP,
  'human',
  CURRENT_TIMESTAMP,
  1
);


-- India Card Transactions (20k rows, 5% null card_id, different col names)
INSERT INTO TABLE ingestion_config VALUES (
  'src_in_card_txns',
  'India Card Transactions',
  'card_transactions',
  'hdfs://master-node:9000/mif/raw/card_transactions/processing_date={YYYY-MM-DD}/',
  'csv',
  ',',
  true,
  'txn_datetime',
  'dd/MM/yyyy HH:mm',
  'txn_ref',
  'card_transactions_silver',
  'hdfs://master-node:9000/mif/silver/card_transactions/',
  'hdfs://master-node:9000/mif/rejected/in_card_txns/',
  '[
    {"rule_id":"r005","rule_name":"card_id_not_null",   "column":"card_identifier","type":"not_null",       "threshold":0.94},
    {"rule_id":"r006","rule_name":"amount_inr_positive","column":"original_amount","type":"range",          "expression":"original_amount > 0 AND original_amount < 10000000","threshold":0.99},
    {"rule_id":"r007","rule_name":"txn_ref_not_null",   "column":"transaction_id", "type":"not_null",       "threshold":1.0},
    {"rule_id":"r008","rule_name":"status_valid",       "column":"status",         "type":"allowed_values", "values":["success","failed","pending"],                          "threshold":0.97}
  ]',
  '{
    "txn_ref":      "transaction_id",
    "card_id":      "card_identifier",
    "vendor":       "merchant_name",
    "amount_inr":   "original_amount",
    "txn_datetime": "transaction_ts",
    "txn_status":   "status"
  }',
  true,
  CURRENT_TIMESTAMP,
  'human',
  CURRENT_TIMESTAMP,
  1
);


-- Merchant Reference Data (5k rows, 2% null category, 2% invalid country codes)
INSERT INTO TABLE ingestion_config VALUES (
  'src_merchant_updates',
  'Merchant Reference Data',
  'merchant',
  'hdfs://master-node:9000/mif/raw/merchant_updates/processing_date={YYYY-MM-DD}/',
  'csv',
  ',',
  true,
  'onboarded_date',
  'yyyy-MM-dd',
  'merchant_id',
  'merchant_silver',
  'hdfs://master-node:9000/mif/silver/merchant_updates/',
  'hdfs://master-node:9000/mif/rejected/merchants/',
  '[
    {"rule_id":"r009","rule_name":"merchant_id_not_null",   "column":"merchant_id",      "type":"not_null",       "threshold":1.0},
    {"rule_id":"r010","rule_name":"category_not_null",      "column":"merchant_category", "type":"not_null",       "threshold":0.97},
    {"rule_id":"r011","rule_name":"country_code_format",    "column":"country_code",      "type":"regex",          "expression":"^[A-Z]{2}$","threshold":0.99},
    {"rule_id":"r012","rule_name":"risk_tier_valid",        "column":"risk_tier",         "type":"allowed_values", "values":["LOW","MEDIUM","HIGH"],"threshold":1.0},
    {"rule_id":"r013","rule_name":"onboarded_date_not_null","column":"onboarded_date",    "type":"not_null",       "threshold":1.0}
  ]',
  '{
    "merchant_id":       "merchant_id",
    "business_name":     "business_name",
    "merchant_category": "merchant_category",
    "country_code":      "country_code",
    "city":              "city",
    "risk_tier":         "risk_tier",
    "onboarded_date":    "onboarded_date",
    "is_active":         "is_active"
  }',
  true,
  CURRENT_TIMESTAMP,
  'human',
  CURRENT_TIMESTAMP,
  1
);


-- Singapore ATM Withdrawals (10k rows, ~3% amounts not multiples of 50)
INSERT INTO TABLE ingestion_config VALUES (
  'src_atm_sg',
  'Singapore ATM Withdrawals',
  'atm',
  'hdfs://master-node:9000/mif/raw/atm_withdrawals/processing_date={YYYY-MM-DD}/',
  'csv',
  ',',
  true,
  'withdrawal_ts',
  "yyyy-MM-dd'T'HH:mm:ss",
  'withdrawal_id',
  'atm_silver',
  'hdfs://master-node:9000/mif/silver/atm_withdrawals/',
  'hdfs://master-node:9000/mif/rejected/atm_sg/',
  '[
    {"rule_id":"r014","rule_name":"withdrawal_id_not_null","column":"withdrawal_id",  "type":"not_null",       "threshold":1.0},
    {"rule_id":"r015","rule_name":"amount_multiple_of_50", "column":"amount_sgd",     "type":"custom",         "expression":"amount_sgd % 50 = 0","threshold":0.96},
    {"rule_id":"r016","rule_name":"amount_positive",       "column":"amount_sgd",     "type":"range",          "expression":"amount_sgd > 0 AND amount_sgd <= 20000","threshold":0.99},
    {"rule_id":"r017","rule_name":"dispense_status_valid", "column":"dispense_status","type":"allowed_values", "values":["DISPENSED","FAILED","PARTIAL"],"threshold":1.0},
    {"rule_id":"r018","rule_name":"currency_sgd",          "column":"currency",       "type":"allowed_values", "values":["SGD"],"threshold":1.0}
  ]',
  '{
    "withdrawal_id":   "withdrawal_id",
    "atm_id":          "atm_id",
    "card_hash":       "card_hash",
    "amount_sgd":      "amount_sgd",
    "currency":        "currency",
    "withdrawal_ts":   "withdrawal_ts",
    "dispense_status": "dispense_status",
    "location":        "location"
  }',
  true,
  CURRENT_TIMESTAMP,
  'human',
  CURRENT_TIMESTAMP,
  1
);


-- Verify: expect 4 rows, all active, schema_version=1
SELECT
  source_id,
  source_name,
  source_type,
  is_active,
  created_by,
  schema_version
FROM ingestion_config
ORDER BY source_type, source_id;
