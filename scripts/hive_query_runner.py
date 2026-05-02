#!/usr/bin/env python3
import argparse
import subprocess
import sys
import time
from pathlib import Path

# UI Helpers
C = {"G": "32", "R": "31", "Y": "33", "C": "36", "B": "1"}


def log(code, msg):
    color = f"\033[{C[code]}m" if sys.stdout.isatty() else ""
    print(f"{color}[{msg.split()[0]}]{color and '\033[0m'} {' '.join(msg.split()[1:])}")


def run_beeline(cmd_args, silent=False):
    container = "hiveserver2"
    jdbc = "jdbc:hive2://localhost:10000"
    base = ["docker", "exec", container, "beeline", "-u", jdbc]
    if silent:
        base += ["--silent=true", "--outputformat=csv2"]

    res = subprocess.run(base + cmd_args, capture_output=True, text=True)

    # Filter noise for non-silent runs
    if not silent:
        noise = {"SLF4J", "log4j", "WARNING", "Connecting", "Connected", "Transaction", "Driver", "Beeline"}
        for line in (res.stdout + res.stderr).splitlines():
            if line.strip() and not any(n in line for n in noise):
                print(f"  {line}")
    return res.returncode == 0, res.stdout.strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.file.exists():
        log("R", f"FAIL File not found: {args.file}")
        sys.exit(1)

    if args.dry_run:
        log("Y", f"INFO Dry run for {args.file.name}:")
        print(args.file.read_text())
        return

    log("C", f"INFO Executing {args.file.name}...")
    tmp = f"/tmp/run_{int(time.time())}.sql"
    subprocess.run(["docker", "cp", str(args.file), f"hiveserver2:{tmp}"], check=True)

    start = time.time()
    ok, _ = run_beeline(["-f", tmp])
    subprocess.run(["docker", "exec", "hiveserver2", "rm", "-f", tmp])

    if ok:
        log("G", f"OK Completed in {time.time() - start:.1f}s")
    else:
        log("R", "FAIL Execution failed")
        sys.exit(3)


if __name__ == "__main__":
    main()
