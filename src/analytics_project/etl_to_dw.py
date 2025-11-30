"""
P7 - Create and populate the Data Warehouse (DW) for insurance-cost-analytics.

This module implements a star schema for business intelligence (BI)
using SQLite as the data warehouse engine.

Workflow:

1. Create the DW folder and SQLite database file.
2. Load the prepared CSV file from data/prepared/:
   - insurance_prepared.csv
3. Build dimension tables:
   - dim_demographics  (age_group, sex, children)
   - dim_region        (region)
   - dim_risk          (smoker, smoker_flag, bmi_category)
4. Build the fact table:
   - fact_insurance_charges
5. Insert all rows into the DW tables.

Star schema structure:

- Dimension: dim_demographics
- Dimension: dim_region
- Dimension: dim_risk
- Fact:      fact_insurance_charges

Source file (prepared):

- data/prepared/insurance_prepared.csv

This DW supports analysis such as:
- Comparing average charges by age group, sex, and region
- Understanding the impact of smoking and BMI category on charges
- Identifying high-risk, high-cost patient profiles
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


# -------------------------------------------------------------------
# Paths & constants
# -------------------------------------------------------------------

# Script is here: src/analytics_project/etl_to_dw_insurance.py
# PROJECT_ROOT = .../insurance-cost-analytics
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
PREPARED_DATA_DIR = DATA_DIR / "prepared"

# Data warehouse directory and database file
DW_DIR = DATA_DIR / "dw"
DW_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DW_DIR / "insurance_dw.db"


# -------------------------------------------------------------------
# Schema creation
# -------------------------------------------------------------------


def create_schema(cursor: sqlite3.Cursor) -> None:
    """
    Create the Data Warehouse tables if they do not already exist.

    Tables:

    - dim_demographics        (dimension)
    - dim_region              (dimension)
    - dim_risk                (dimension)
    - fact_insurance_charges  (fact)
    """

    # Demographics dimension: age group, sex, children
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_demographics (
            demographics_key   INTEGER PRIMARY KEY,
            age_group          TEXT,
            sex                TEXT,
            children           INTEGER
        )
        """
    )

    # Region dimension: one row per region
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_region (
            region_key   INTEGER PRIMARY KEY,
            region       TEXT NOT NULL
        )
        """
    )

    # Risk dimension: smoking & BMI category
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_risk (
            risk_key       INTEGER PRIMARY KEY,
            smoker         TEXT,
            smoker_flag    INTEGER,
            bmi_category   TEXT
        )
        """
    )

    # Fact table: one row per observation in the prepared dataset
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_insurance_charges (
            fact_key          INTEGER PRIMARY KEY,
            demographics_key  INTEGER NOT NULL,
            region_key        INTEGER NOT NULL,
            risk_key          INTEGER NOT NULL,
            charges           REAL NOT NULL,
            age               INTEGER,
            bmi               REAL,
            children          INTEGER,
            FOREIGN KEY (demographics_key) REFERENCES dim_demographics(demographics_key),
            FOREIGN KEY (region_key)       REFERENCES dim_region(region_key),
            FOREIGN KEY (risk_key)         REFERENCES dim_risk(risk_key)
        )
        """
    )


def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """
    Delete all existing records from the DW tables.

    This makes the ETL process idempotent:
    we can run it multiple times without duplicating data.
    """
    # Delete from fact table first to avoid foreign key issues
    cursor.execute("DELETE FROM fact_insurance_charges")
    cursor.execute("DELETE FROM dim_risk")
    cursor.execute("DELETE FROM dim_region")
    cursor.execute("DELETE FROM dim_demographics")


# -------------------------------------------------------------------
# Load analytic dataset
# -------------------------------------------------------------------


def load_analytic_dataset() -> pd.DataFrame:
    """
    Load the prepared insurance dataset into a single analytic DataFrame.

    Expected columns in insurance_prepared.csv:

    - age (int)
    - sex (string)
    - bmi (float)
    - children (int)
    - smoker (string: yes/no)
    - region (string)
    - charges (float)
    - age_group (categorical)
    - bmi_category (categorical)
    - smoker_flag (0/1)
    """
    prepared_path = PREPARED_DATA_DIR / "insurance_prepared.csv"

    print(f"📥 Reading insurance_prepared from: {prepared_path}")
    df = pd.read_csv(prepared_path)

    print(f"✅ Analytic dataset shape: {df.shape}")
    print(f"✅ Columns: {', '.join(df.columns.astype(str).tolist())}")
    return df


# -------------------------------------------------------------------
# Build dimensional dataframes from the analytic dataset
# -------------------------------------------------------------------


def build_dim_demographics(analytic_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the dim_demographics dataframe.

    Uses:
    - age_group
    - sex
    - children
    """
    cols = []
    for col in ["age_group", "sex", "children"]:
        if col in analytic_df.columns:
            cols.append(col)

    if not cols:
        raise ValueError("No columns found for dim_demographics.")

    demo_df = analytic_df[cols].drop_duplicates().copy()

    # Surrogate key
    demo_df.insert(0, "demographics_key", range(1, len(demo_df) + 1))
    return demo_df


def build_dim_region(analytic_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the dim_region dataframe.

    Uses:
    - region
    """
    if "region" not in analytic_df.columns:
        raise ValueError("Column 'region' not found for dim_region.")

    region_df = analytic_df[["region"]].drop_duplicates().copy()
    region_df.insert(0, "region_key", range(1, len(region_df) + 1))
    return region_df


def build_dim_risk(analytic_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the dim_risk dataframe.

    Uses:
    - smoker
    - smoker_flag
    - bmi_category
    """
    cols = []
    for col in ["smoker", "smoker_flag", "bmi_category"]:
        if col in analytic_df.columns:
            cols.append(col)

    if not cols:
        raise ValueError("No columns found for dim_risk.")

    risk_df = analytic_df[cols].drop_duplicates().copy()

    risk_df.insert(0, "risk_key", range(1, len(risk_df) + 1))
    return risk_df


# -------------------------------------------------------------------
# Insert helpers
# -------------------------------------------------------------------


def insert_dim_table(
    df: pd.DataFrame, cursor: sqlite3.Cursor, table_name: str
) -> None:
    """
    Generic helper to insert all rows from a dimension dataframe into the DW.
    """
    cols = list(df.columns)
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join(cols)

    cursor.executemany(
        f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})",
        df.itertuples(index=False, name=None),
    )


def build_and_insert_fact(
    analytic_df: pd.DataFrame,
    demo_df: pd.DataFrame,
    region_df: pd.DataFrame,
    risk_df: pd.DataFrame,
    cursor: sqlite3.Cursor,
) -> None:
    """
    Build the fact_insurance_charges table and insert rows.

    Joins analytic_df with dimension tables on:

    - demographic profile:
        age_group, sex, children
    - region:
        region
    - risk:
        smoker, smoker_flag, bmi_category
    """
    fact = analytic_df.copy()

    # -----------------------------
    # Join demographics dimension
    # -----------------------------
    demo_keys = ["age_group", "sex", "children"]
    demo_keys = [c for c in demo_keys if c in fact.columns and c in demo_df.columns]

    fact = fact.merge(
        demo_df,
        on=demo_keys,
        how="left",
    )

    # -----------------------------
    # Join region dimension
    # -----------------------------
    if "region" in fact.columns and "region" in region_df.columns:
        fact = fact.merge(
            region_df,
            on="region",
            how="left",
        )

    # -----------------------------
    # Join risk dimension
    # -----------------------------
    risk_keys = ["smoker", "smoker_flag", "bmi_category"]
    risk_keys = [c for c in risk_keys if c in fact.columns and c in risk_df.columns]

    fact = fact.merge(
        risk_df,
        on=risk_keys,
        how="left",
    )

    # Basic data quality checks
    if fact["demographics_key"].isna().any():
        print("⚠ Warning: some rows have no matching demographics_key.")
    if "region_key" in fact.columns and fact["region_key"].isna().any():
        print("⚠ Warning: some rows have no matching region_key.")
    if fact["risk_key"].isna().any():
        print("⚠ Warning: some rows have no matching risk_key.")

    # Select core fact columns
    fact_cols = [
        "demographics_key",
        "region_key",
        "risk_key",
        "charges",
    ]

    # Optional numeric context
    optional_features = ["age", "bmi", "children"]
    for c in optional_features:
        if c in fact.columns:
            fact_cols.append(c)

    fact_df = fact[fact_cols].copy()

    cols = list(fact_df.columns)
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join(cols)

    cursor.executemany(
        f"""
        INSERT INTO fact_insurance_charges ({col_list})
        VALUES ({placeholders})
        """,
        fact_df.itertuples(index=False, name=None),
    )


# -------------------------------------------------------------------
# Main ETL function
# -------------------------------------------------------------------


def load_data_to_db() -> None:
    """
    Run the full ETL process for the Insurance Data Warehouse.

    Steps:
    1. Connect to the SQLite DW database (creates the file if needed).
    2. Create the DW schema (dimension + fact tables).
    3. Clear existing records from the DW tables.
    4. Load prepared CSV file into an analytic DataFrame.
    5. Build and insert dimension tables (demographics, region, risk).
    6. Build and insert the fact_insurance_charges table.

    This function is safe to run multiple times:
    each run replaces the existing DW data with a fresh load
    from the prepared CSV file.
    """
    print(f"📁 Using DW database at: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()

        print("🧱 Creating schema...")
        create_schema(cursor)

        print("🧹 Clearing existing records...")
        delete_existing_records(cursor)

        print("📥 Building analytic dataset from prepared CSV...")
        analytic_df = load_analytic_dataset()

        print("📌 Building dimension tables...")
        dim_demo_df = build_dim_demographics(analytic_df)
        dim_region_df = build_dim_region(analytic_df)
        dim_risk_df = build_dim_risk(analytic_df)

        print("📌 Inserting dimension tables...")
        insert_dim_table(dim_demo_df, cursor, "dim_demographics")
        insert_dim_table(dim_region_df, cursor, "dim_region")
        insert_dim_table(dim_risk_df, cursor, "dim_risk")

        print("📌 Building and inserting fact_insurance_charges...")
        build_and_insert_fact(
            analytic_df, dim_demo_df, dim_region_df, dim_risk_df, cursor
        )

        conn.commit()
        print("✅ ETL complete: Insurance Data Warehouse populated successfully.")

    finally:
        conn.close()


# -------------------------------------------------------------------
# Script entry point
# -------------------------------------------------------------------

if __name__ == "__main__":
    load_data_to_db()
