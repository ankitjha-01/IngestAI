"""
Uploads all raw CSV files from data/raw/ into the HDFS namespace.

Creates the full HDFS directory tree before uploading:
  /mif/raw/          — landing zone for incoming CSV files
  /mif/silver/       — cleaned output
  /mif/hive/         — Hive metastore directories
  /mif/rejected/     — rows that fail DQ checks

Each file is copied into the master-node container first (docker cp),
then moved into HDFS (hdfs dfs -put). This avoids needing the
HDFS client installed locally.
"""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Config -> will move to a .env file later
MASTERNODE_CONTAINER = "master-node"

PROCESSING_DATE = datetime.now()

# Allow passing a date argument (e.g., python upload_to_hdfs.py 2026-01-01)
if len(sys.argv) > 1:
    DATE_STR = sys.argv[1]
else:
    PROCESSING_DATE = datetime.now()
    DATE_STR = PROCESSING_DATE.strftime("%Y-%m-%d")

print(f"Running pipeline for date: {DATE_STR}")


# All HDFS directories to create upfront
HDFS_DIRS = [
    # Raw (landing zone for incoming CSV files)
    "/mif/raw/card_transactions",
    "/mif/raw/merchant_updates",
    "/mif/raw/atm_withdrawals",
    "/mif/raw/fraud_flags",
    # Silver (cleaned Parquet)
    "/mif/silver/card_transactions",
    "/mif/silver/merchant_updates",
    "/mif/silver/atm_withdrawals",
    "/mif/silver/fraud_flags",
    # Hive warehouse
    "/mif/hive/config",
    "/mif/hive/dq_results",
    "/mif/hive/lineage",
    # Rejected (DQ failures land here, never discarded)
    "/mif/rejected",
]


""" HELPERS """


def hdfs_cmd(*args: str, check: bool = False) -> subprocess.CompletedProcess:
    """Run an HDFS command inside the master-node container."""
    cmd = ["docker", "exec", MASTERNODE_CONTAINER, "hdfs", "dfs", *args]
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def hdfs_mkdir(path: str) -> None:
    """Create directory in HDFS, ignore if it already exists."""
    result = hdfs_cmd("-mkdir", "-p", path)
    if result.returncode != 0 and "exists" not in result.stderr:
        print(f"  WARN mkdir {path}: {result.stderr.strip()}")


def hdfs_put(container_tmp: str, hdfs_dir: str) -> None:
    """Move a file already inside the container into HDFS."""
    hdfs_cmd("-put", container_tmp, hdfs_dir)


def hdfs_size(hdfs_path: str) -> Optional[str]:
    """Return human-readable size of an HDFS file, or None if not found."""
    result = hdfs_cmd("-du", "-h", hdfs_path)
    parts = result.stdout.strip().split()
    return f"{parts[0]} {parts[1]}" if len(parts) >= 2 else None  # e.g. "2.6 MB"


def docker_cp(local_path: str, container_tmp: str) -> None:
    """Copy a local file into the master-node container."""
    subprocess.run(
        ["docker", "cp", local_path, f"{MASTERNODE_CONTAINER}:{container_tmp}"],
        check=True,
        capture_output=True,
    )


""" ACTIONS """


def create_hdfs_namespace() -> None:
    print("Creating HDFS namespace...")
    for path in HDFS_DIRS:
        hdfs_mkdir(path)
        print(f"  mkdir -p {path}")
    print()


def upload_partitioned_folder(local_base: str, hdfs_base: str) -> None:
    """
    Upload all files under:
    data/raw/<source>/processing_date=YYYY-MM-DD/
    """
    # 1. Dynamically find the project root (one folder up from where this script lives)
    project_root = Path(__file__).resolve().parent.parent

    # 2. Build the absolute path to the data folder
    local_path = project_root / local_base / f"processing_date={DATE_STR}"

    # 3. Check if the absolute path exists
    if not local_path.exists():
        print(f"  SKIP {local_base} — no data for {DATE_STR}")
        return

    hdfs_dir = f"{hdfs_base}/processing_date={DATE_STR}/"
    hdfs_mkdir(hdfs_dir)

    for filename in os.listdir(local_path):
        full_local_path = str(local_path / filename)  # Convert back to string for Docker
        container_tmp = f"/tmp/{filename}"

        # Step 1: copy from host into master-node container
        docker_cp(full_local_path, container_tmp)

        # Step 2: move from container into HDFS
        hdfs_put(container_tmp, hdfs_dir)

        # Step 3: confirm with file size from HDFS
        hdfs_file = hdfs_dir + filename
        size = hdfs_size(hdfs_file)

        print(f"  {filename:<50} {size or '?':>6}")


def upload_all() -> None:
    print("Uploading raw files to HDFS...")

    sources = [
        ("data/raw/card_transactions", "/mif/raw/card_transactions"),
        ("data/raw/merchant_updates", "/mif/raw/merchant_updates"),
        ("data/raw/atm_withdrawals", "/mif/raw/atm_withdrawals"),
        ("data/raw/fraud_flags", "/mif/raw/fraud_flags"),
    ]

    for local_base, hdfs_base in sources:
        upload_partitioned_folder(local_base, hdfs_base)

    print()


def verify() -> None:
    print("Verifying upload...")

    result = hdfs_cmd("-ls", "-R", "/mif/raw")
    files = [line for line in result.stdout.splitlines() if line.startswith("-")]
    print(f"  Files in /mif/raw: {len(files)}")

    total = hdfs_cmd("-du", "-s", "-h", "/mif")
    parts = total.stdout.strip().split()
    size = f"{parts[0]} {parts[1]}" if len(parts) >= 2 else None

    print(f"  Total HDFS usage:  {size}")
    print()


""" MAIN """


def main():
    create_hdfs_namespace()
    upload_all()
    verify()


if __name__ == "__main__":
    main()
