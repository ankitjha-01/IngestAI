"""
Generates synthetic raw CSV files for 5 financial data sources.
Each source has a deliberately different schema and embedded DQ issues
to simulate real-world heterogeneous ingestion scenarios.

Sources:
  1. US Card Transactions   — 30,000 rows
  2. India Card Transactions — 20,000 rows
  3. Merchant Updates        —  5,000 rows
  4. ATM Withdrawals (SG)   — 10,000 rows
  5. Fraud Flags             —  2,000 rows
"""

import os
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

# Config -> will move to a .env file later***

SEED = 42
OUTPUT_DIR = "data/raw"

PROCESSING_DATE = datetime.now()
DATE_STR = PROCESSING_DATE.strftime("%Y-%m-%d")

random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

""" HELPERS """


def random_timestamp(base: datetime = PROCESSING_DATE, window_seconds: int = 86400) -> datetime:
    return base + timedelta(seconds=random.randint(0, window_seconds))


def get_next_suffix(path: str, base_filename: str) -> str:
    """
    Returns filename with incremental suffix:
    file.csv → file_001.csv, file_002.csv
    """
    name, ext = os.path.splitext(base_filename)

    existing = [f for f in os.listdir(path) if f.startswith(name)] if os.path.exists(path) else []

    count = len(existing) + 1
    return f"{name}_{count:03d}{ext}"


def save(df: pd.DataFrame, subfolder: str, filename: str) -> str:
    path = os.path.join(
        OUTPUT_DIR,
        subfolder,
        f"processing_date={DATE_STR}",
    )
    os.makedirs(path, exist_ok=True)

    filename = get_next_suffix(path, filename)

    full_path = os.path.join(path, filename)
    df.to_csv(full_path, index=False)

    print(f"  Saved {len(df):>7,} rows → {full_path}")
    return full_path


""" GENERATORS """


# 1. DQ issues: 3% null amounts | 2% invalid status values
def generate_us_transactions(n: int = 30_000) -> pd.DataFrame:
    VALID_STATUSES = ["APPROVED", "DECLINED", "PENDING"]

    def generate_row(i: int):
        return {
            "transaction_id": f"US_TXN_{i:>08d}",
            "card_number": f"4{random.randint(10**14, 10**15 - 1)}",
            "merchant_name": fake.company(),
            "amount_usd": (round(random.uniform(1, 5_000), 2) if random.random() > 0.03 else None),
            "transaction_time": random_timestamp().strftime("%Y-%m-%d %H:%M:%S"),
            "status": (random.choice(VALID_STATUSES) if random.random() > 0.02 else "UNKNOWN_STATUS"),
            "is_international": random.choice([0, 1]),
        }

    return pd.DataFrame([generate_row(i) for i in range(n)])


# 2. DQ issues: 5% null card_id | different column names from US source
def generate_in_transactions(n: int = 20_000) -> pd.DataFrame:
    STATUSES = ["success", "failed", "pending"]

    def generate_row(i: int):
        return {
            "txn_ref": f"IN_TXN_{i:08d}",
            "card_id": (None if random.random() < 0.05 else f"IN_CARD_{i:08d}"),
            "vendor": fake.company(),
            "amount_inr": round(random.uniform(10, 500_000), 2),
            "txn_datetime": random_timestamp().strftime("%d/%m/%Y %H:%M"),
            "txn_status": random.choice(STATUSES),
            "upi_flag": random.choice(["Y", "N"]),
        }

    return pd.DataFrame([generate_row(i) for i in range(n)])


# 3. DQ issues: 2% null merchant_category | ~2% invalid country codes
def generate_merchant_updates(n: int = 5_000) -> pd.DataFrame:
    CATEGORIES = [
        "grocery",
        "electronics",
        "restaurant",
        "travel",
        "healthcare",
        "entertainment",
        "fuel",
        "retail",
        "online",
    ]
    VALID_COUNTRIES = ["US", "IN"]

    def generate_row(i: int):
        return {
            "merchant_id": f"MERCH_{i:06d}",
            "business_name": fake.company(),
            "merchant_category": (None if random.random() < 0.02 else random.choice(CATEGORIES)),
            "country_code": ("INVALID" if random.random() < 0.02 else random.choice(VALID_COUNTRIES)),
            "city": fake.city(),
            "risk_tier": random.choice(["LOW", "MEDIUM", "HIGH"]),
            "onboarded_date": (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1_400))).strftime("%Y-%m-%d"),
            "is_active": random.choice([0, 1]),
        }

    return pd.DataFrame([generate_row(i) for i in range(n)])


# 4. DQ issues: ~3% amounts are not multiples of 50 (should always be)
def generate_atm_withdrawals(n: int = 10_000) -> pd.DataFrame:
    VALID_AMOUNTS = [100, 200, 500, 1_000, 2_000, 5_000, 10_000]
    STATUSES = ["DISPENSED", "FAILED", "PARTIAL"]

    def generate_row(i: int):
        base_amount = random.choice(VALID_AMOUNTS)

        return {
            "withdrawal_id": f"ATM_{i:08d}",
            "atm_id": f"ATM_SG_{random.randint(1000, 9999)}",
            "card_hash": fake.sha256()[:16],
            "amount_sgd": (base_amount if random.random() > 0.03 else base_amount + random.randint(1, 49)),
            "currency": "SGD",
            "withdrawal_ts": random_timestamp().isoformat(),
            "dispense_status": random.choice(STATUSES),
            "location": fake.address().replace("\n", " "),
        }

    return pd.DataFrame([generate_row(i) for i in range(n)])


# 5. Small reference table — fraud signals from a risk model
def generate_fraud_flags(n: int = 2_000) -> pd.DataFrame:
    FLAG_TYPES = [
        "VELOCITY",
        "GEO_ANOMALY",
        "AMOUNT_SPIKE",
        "DEVICE_MISMATCH",
        "NEW_MERCHANT",
    ]
    rows = []
    for i in range(n):
        rows.append(
            {
                "flag_id": f"FLAG_{i:06d}",
                "transaction_ref": f"US_TXN_{random.randint(0, 30_000):08d}",
                "fraud_score": round(random.uniform(0.5, 1.0), 4),
                "flag_type": random.choice(FLAG_TYPES),
                "flagged_at": random_timestamp().isoformat(),
                "reviewed": random.choice([0, 1]),
            }
        )
    return pd.DataFrame(rows)


""" MAIN """


def main():
    print("Generating sample data...\n")

    sources = [
        (generate_us_transactions, "card_transactions", f"us_transactions_{DATE_STR}.csv"),
        (generate_in_transactions, "card_transactions", f"in_transactions_{DATE_STR}.csv"),
        (generate_merchant_updates, "merchant_updates", f"merchants_{DATE_STR}.csv"),
        (generate_atm_withdrawals, "atm_withdrawals", f"atm_sg_{DATE_STR}.csv"),
        (generate_fraud_flags, "fraud_flags", f"fraud_{DATE_STR}.csv"),
    ]

    total_rows = 0
    for generator, subfolder, filename in sources:
        df = generator()
        save(df, subfolder, filename)
        total_rows += len(df)

    print(f"\nDone. Total rows generated: {total_rows:,}")


if __name__ == "__main__":
    main()
