import argparse
import subprocess
import sys
import time
from pathlib import Path

from hive_query_runner import log, run_beeline

STEPS = [
    (1, "Schema Registry", "hive_schema/01_create_config_tables.sql"),
    (2, "DQ tracking tables", "hive_schema/02_create_dq_tables.sql"),
    (3, "Silver + Lineage tables", "hive_schema/03_create_silver_tables.sql"),
    (4, "Seed initial configs", "hive_schema/04_seed_initial_config.sql"),
]


def verify():
    print("\n\033[1m--- Verification ---\033[0m")
    failed = 0
    checks = [
        (
            "Active sources",
            "4",
            "SELECT COUNT(*) FROM mif_db.ingestion_config WHERE is_active=true",
        ),
        (
            "ACID Check",
            "MANAGED_TABLE",
            "DESCRIBE FORMATTED mif_db.ingestion_config",
        ),
        (
            "External Table Check",
            "EXTERNAL_TABLE",
            "DESCRIBE FORMATTED mif_db.dq_results",
        ),
        (
            "ACID Update test",
            "1",
            """ UPDATE mif_db.ingestion_config SET last_modified_at=CURRENT_TIMESTAMP
            WHERE source_id='src_us_card_txns';
            SELECT COUNT(*) FROM mif_db.ingestion_config WHERE source_id='src_us_card_txns' """,
        ),
        (
            "DQ Seed verify",
            "r014",
            "SELECT dq_rules_json FROM mif_db.ingestion_config WHERE source_id='src_atm_sg'",
        ),
    ]

    for name, expected, sql in checks:
        ok, out = run_beeline(["-e", sql], silent=True)
        if ok and expected in out:
            log("G", f"OK {name}")
        else:
            log("R", f"FAIL {name} (Expected {expected}, got {out[:50]}...)")
            failed += 1
    return failed == 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true", help="Skip DDL, run verification only")
    parser.add_argument("--step", type=int)
    args = parser.parse_args()

    log("C", "INFO Waiting for HiveServer2...")
    for _ in range(20):
        if run_beeline(["-e", "SELECT 1"], silent=True)[0]:
            break
        time.sleep(5)
    else:
        log("R", "FAIL Timeout")
        sys.exit(1)

    if not args.verify_only:
        to_run = [s for s in STEPS if s[0] == args.step] if args.step else STEPS
        for num, label, fpath in to_run:
            print(f"\n\033[1mStep {num}: {label}\033[0m")
            full_path = Path(__file__).parent.parent / fpath
            tmp = f"/tmp/step_{num}.sql"
            subprocess.run(["docker", "cp", str(full_path), f"hiveserver2:{tmp}"], check=True)
            ok, _ = run_beeline(["-f", tmp])
            if not ok:
                log("R", f"FAIL Step {num} failed")
                sys.exit(1)
            log("G", f"OK Step {num} complete")

    sys.exit(0 if verify() else 1)


if __name__ == "__main__":
    main()
