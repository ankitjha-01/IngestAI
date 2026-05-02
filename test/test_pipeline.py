import os
import subprocess
import sys
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = f"{BASE_DIR}/test_data.csv"
SQL_FILE = f"{BASE_DIR}/test_hive.sql"
HDFS_DIR = "/mif/unified_test/raw"
DB_NAME = "mif_pipeline_test"

def run_cmd(cmd, step_name, show_out=False):
    print(f"[{step_name}]...")
    try:
        res = subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True)
        if show_out and res.stdout: print(res.stdout.strip())
        return res.stdout
    except subprocess.CalledProcessError as e:
        print(f"\nFAILED: {step_name}\n{e.stderr.strip() or e.stdout.strip()}")
        sys.exit(1)

def cleanup():
    print("\n[4/4] Cleaning up test artifacts...")
    devnull = subprocess.DEVNULL
    cmds = [
        f"docker exec master-node hdfs dfs -rm -r -skipTrash /mif/unified_test",
        f"docker exec hiveserver2 beeline -u jdbc:hive2://localhost:10000 -n hive -e 'DROP DATABASE IF EXISTS {DB_NAME} CASCADE;'",
        "docker exec master-node rm -f /tmp/test_data.csv",
        "docker exec hiveserver2 rm -f /tmp/test_hive.sql"
    ]
    for cmd in cmds:
        subprocess.run(cmd, shell=True, stderr=devnull, stdout=devnull)
    
    for f in [CSV_FILE, SQL_FILE]:
        if os.path.exists(f): os.remove(f)
    print("Cleanup complete.")

def main():
    try:
        print("Starting Pipeline Test\n")

        # 1. Create Data (Fixed: Removed leading spaces and extra newlines)
        print("[1/4] Creating local CSV...")
        with open(CSV_FILE, "w") as f:
            f.write("101,ALICE,Data Engineering,75000.0\n102,BOB,Analytics,68000.0\n103,CHARLIE,Operations,55000.0\n")

        # 2. Upload to HDFS
        print("[2/4] Uploading to HDFS...")
        run_cmd(f"docker exec master-node hdfs dfs -mkdir -p {HDFS_DIR}", "HDFS mkdir")
        run_cmd(f"docker cp {CSV_FILE} master-node:/tmp/test_data.csv", "Copy CSV")
        run_cmd(f"docker exec master-node hdfs dfs -put -f /tmp/test_data.csv {HDFS_DIR}/", "Put to HDFS")

        # 3. Hive Tez & ACID Jobs
        print("[3/4] Running Hive SQL...")
        sql_content = f"""
        SET hive.execution.engine=tez;
        SET hive.support.concurrency=true;
        SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

        CREATE DATABASE IF NOT EXISTS {DB_NAME};
        USE {DB_NAME};

        CREATE TABLE IF NOT EXISTS emp_acid (
            emp_id INT, emp_name STRING, dept STRING, salary DOUBLE
        ) STORED AS ORC TBLPROPERTIES ('transactional'='true');

        CREATE TEMPORARY EXTERNAL TABLE IF NOT EXISTS raw_csv (
            emp_id INT, emp_name STRING, dept STRING, salary DOUBLE
        ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '{HDFS_DIR}/'; 

        INSERT INTO emp_acid SELECT * FROM raw_csv;
        
        -- Use AS status to fix the _c0 header
        SELECT '>>> 1: AFTER LOAD <<<' AS status;
        SELECT * FROM emp_acid ORDER BY emp_id;

        UPDATE emp_acid SET salary = 85000.0 WHERE dept = 'Data Engineering';
        SELECT '>>> 2: AFTER UPDATE <<<' AS status;
        SELECT * FROM emp_acid ORDER BY emp_id;

        DELETE FROM emp_acid WHERE emp_id = 103;
        SELECT '>>> 3: AFTER DELETE <<<' AS status;
        SELECT * FROM emp_acid ORDER BY emp_id;
        """
        
        with open(SQL_FILE, "w") as f: f.write(sql_content)
        run_cmd(f"docker cp {SQL_FILE} hiveserver2:/tmp/test_hive.sql", "Copy SQL")
        run_cmd("docker exec hiveserver2 beeline -u jdbc:hive2://localhost:10000 -n hive -f /tmp/test_hive.sql --showHeader=true --outputformat=dsv --delimiterForDSV=' | '", "Hive Jobs", True)

    finally:
        cleanup()
if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print(f"Total time taken: {end - start:.2f}s")