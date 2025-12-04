try:
    import pandas as pd
    import numpy as np
except Exception as e:
    raise ImportError(
        "Required packages are missing. This project uses a virtual environment (.venv).\n"
        "Option 1 — Run using the project's venv (recommended):\n"
        "    & ./.venv/Scripts/python.exe prep_data.py\n"
        "Option 2 — Activate venv then run: (PowerShell)\n"
        "    & ./.venv/Scripts/Activate.ps1\n"
        "    python prep_data.py\n"
        "Option 3 — Install requirements into the current Python: (only if you know what you're doing)\n"
        "    py -m pip install -r requirements.txt\n"
        f"Original error: {e}"
    )
from pathlib import Path
import argparse



# -----------------------------
# Config
# -----------------------------
# Path to the raw CSV you downloaded from Kaggle
RAW_PATH = Path("data_raw/nyc_311_2019.csv")

# Output path for cleaned unified data
OUT_PATH = Path("data/issues_unified.csv")

# SLA threshold: consider a complaint "breached" if it took longer than this
SLA_HOURS_TARGET = 72  # 3 days

# How many rows to read from the raw CSV (to avoid out-of-memory)
# Adjust this based on your machine: 100_000 or 200_000 is usually plenty
N_ROWS = 200_000

# Columns we actually care about (from the list you shared)
USECOLS = [
    "Unique Key",
    "Created Date",
    "Closed Date",
    "Complaint Type",
    "Descriptor",
    "Borough",
    "Status",
    "City",
    "Open Data Channel Type",
]


def main():
    print(f"Loading raw data from {RAW_PATH}...")
    print(f"Reading up to {N_ROWS} rows and only selected columns...")
    df = pd.read_csv(
        RAW_PATH,
        low_memory=False,
        usecols=USECOLS,
        nrows=N_ROWS,
    )
    print("Raw columns:")
    print(df.columns.tolist())
    print(f"Raw row count (limited by N_ROWS): {len(df)}")

    # -----------------------------
    # 1) Select & rename columns
    # -----------------------------
    col_map = {
        "Unique Key": "issue_id",
        "Created Date": "created_at",
        "Closed Date": "resolved_at",
        "Complaint Type": "raw_category",
        "Descriptor": "raw_subcategory",
        "Borough": "region",
        "Status": "status",
        "City": "city",
        "Open Data Channel Type": "raw_channel",
    }

    # Ensure required columns are present
    required_raw = ["Unique Key", "Created Date", "Complaint Type"]
    missing = [c for c in required_raw if c not in df.columns]
    if missing:
        raise ValueError(
            f"Required columns missing from CSV: {missing}. "
            f"Columns present: {df.columns.tolist()}"
        )

    df = df.rename(columns=col_map)

    # Ensure optional columns exist
    for col in ["resolved_at", "raw_subcategory", "region", "status", "city", "raw_channel"]:
        if col not in df.columns:
            df[col] = np.nan

    # -----------------------------
    # 2) Parse dates
    # -----------------------------
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["resolved_at"] = pd.to_datetime(df["resolved_at"], errors="coerce")

    # Drop rows with no created_at
    before = len(df)
    df = df[~df["created_at"].isna()]
    print(f"Dropped {before - len(df)} rows with null created_at. Remaining: {len(df)}")

    # -----------------------------
    # 3) Build issue_text
    # -----------------------------
    df["raw_category"] = df["raw_category"].fillna("")
    df["raw_subcategory"] = df["raw_subcategory"].fillna("")

    df["issue_text"] = (
        df["raw_category"].astype(str).str.strip()
        + " - "
        + df["raw_subcategory"].astype(str).str.strip()
    )

    # Drop rows with empty issue_text
    before = len(df)
    df = df[df["issue_text"].str.strip() != ""]
    print(f"Dropped {before - len(df)} rows with empty issue_text. Remaining: {len(df)}")

    # -----------------------------
    # 4) Region, channel, source
    # -----------------------------
    # Region: use Borough, fall back to City, then UNKNOWN
    df["region"] = df["region"].fillna(df["city"]).fillna("UNKNOWN")

    # Channel: use Open Data Channel Type if present, else default '311'
    df["channel"] = df["raw_channel"].fillna("311")
    df["source_system"] = "nyc_311_2019"

    # -----------------------------
    # 5) SLA + resolution_hours
    # -----------------------------
    df["sla_hours_target"] = SLA_HOURS_TARGET

    df["resolution_hours"] = (
        (df["resolved_at"] - df["created_at"]).dt.total_seconds() / 3600.0
    )

    # SLA breached: only if we have a resolved_at and it took longer than SLA_HOURS_TARGET
    df["sla_breached"] = (
        df["resolution_hours"].notna()
        & (df["resolution_hours"] > df["sla_hours_target"])
    )

    # -----------------------------
    # 6) Save unified CSV
    # -----------------------------
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)

    print(f"\nSaved unified data to: {OUT_PATH.resolve()}")
    print("Final columns:")
    print(df.columns.tolist())
    print("Final row count:", len(df))


if __name__ == "__main__":
    main()
