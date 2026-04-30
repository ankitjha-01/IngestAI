-- 1. Setup Environment
CREATE DATABASE IF NOT EXISTS mif_test;
USE mif_test;

-- 2. Create Staging Table (Matches the CSV structure)
CREATE EXTERNAL TABLE IF NOT EXISTS test_users_raw (
    id INT,
    name STRING,
    city STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/mif/test/raw/';

-- 3. Create Final ACID Table
CREATE TABLE IF NOT EXISTS test_users_final (
    id INT,
    name STRING,
    city STRING
)
STORED AS ORC
TBLPROPERTIES("transactional"="true");  -- only tez supports acid table

-- 4. Enable Tez and Load Data
SET hive.execution.engine=tez; -- tez/mr
INSERT OVERWRITE TABLE test_users_final SELECT * FROM test_users_raw;

-- 5. Show Results for Verification
SELECT * FROM test_users_final;

-- 6. Cleanup Metadata
DROP TABLE test_users_raw;
DROP TABLE test_users_final;