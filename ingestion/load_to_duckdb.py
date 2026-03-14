"""
load_to_duckdb.py
-----------------
Warehouse loader script for the Broadband Access Pipeline.

Reads raw CSV files from data/raw/ and loads them into a local
DuckDB database file (broadband.db). This simulates what a cloud
data engineer would do loading data from S3 into Redshift or BigQuery.

Data flow:
  data/raw/census_state_*.csv   -> DuckDB table: raw.census_state
  data/raw/census_county_*.csv  -> DuckDB table: raw.census_county

Author: Joey | Broadband Pipeline Portfolio Project
"""

import os
import glob
import duckdb
import pandas as pd
from datetime import datetime
from loguru import logger

# ── Configuration ─────────────────────────────────────────────────────────────

# Paths relative to this script's location
BASE_DIR  = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR   = os.path.join(BASE_DIR, "data", "raw")
DB_PATH   = os.path.join(BASE_DIR, "broadband.db")

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_latest_csv(prefix: str) -> str | None:
    """
    Find the most recently created CSV in data/raw/ matching a prefix.

    Each ingestion run creates a timestamped file like:
      census_state_20260314_162234.csv

    This function finds the latest one so we always load fresh data
    without hardcoding a filename.
    """
    pattern = os.path.join(RAW_DIR, f"{prefix}_*.csv")
    matches = glob.glob(pattern)

    if not matches:
        logger.warning(f"No CSV files found matching pattern: {pattern}")
        return None

    # Sort by filename which includes the timestamp, newest last
    latest = sorted(matches)[-1]
    logger.info(f"Found latest [{prefix}]: {os.path.basename(latest)}")
    return latest


def create_schema(conn: duckdb.DuckDBPyConnection):
    """
    Create the 'raw' schema inside DuckDB if it does not exist.

    In a real warehouse like Redshift or BigQuery, schemas separate
    layers of data: raw, staging, marts. We follow that same pattern
    here so the structure mirrors production.
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    logger.info("Schema 'raw' ready")


def load_table(
    conn: duckdb.DuckDBPyConnection,
    csv_path: str,
    table_name: str,
):
    """
    Load a CSV file into a DuckDB table using the raw schema.

    Steps:
      1. Read CSV into a pandas DataFrame
      2. Add a load timestamp column for auditability
      3. Drop the table if it already exists (full refresh pattern)
      4. Register the DataFrame with DuckDB
      5. Create the table from the registered DataFrame

    The full refresh pattern means every load replaces the previous
    data. This is appropriate for raw layers where we always want
    the latest ingestion run reflected.
    """
    logger.info(f"Loading {os.path.basename(csv_path)} -> raw.{table_name}")

    # Step 1: Read CSV
    df = pd.read_csv(csv_path)
    logger.info(f"Read {len(df)} rows, {df.shape[1]} columns from CSV")

    # Step 2: Add load timestamp
    df["loaded_at"] = datetime.now().isoformat()

    # Step 3: Drop existing table for full refresh
    conn.execute(f"DROP TABLE IF EXISTS raw.{table_name}")

    # Step 4: Register DataFrame so DuckDB can see it
    conn.register("df_temp", df)

    # Step 5: Create table from the registered DataFrame
    conn.execute(f"""
        CREATE TABLE raw.{table_name} AS
        SELECT * FROM df_temp
    """)

    # Verify row count matches
    result = conn.execute(
        f"SELECT COUNT(*) FROM raw.{table_name}"
    ).fetchone()[0]

    logger.success(
        f"Loaded raw.{table_name}: {result} rows confirmed in DuckDB"
    )


def verify_tables(conn: duckdb.DuckDBPyConnection):
    """
    Run verification queries after loading to confirm data integrity.

    These are the same kinds of checks a data engineer would run
    after a load job completes in production.
    """
    logger.info("Running post-load verification queries...")

    checks = [
        (
            "State count",
            "SELECT COUNT(*) FROM raw.census_state",
        ),
        (
            "County count",
            "SELECT COUNT(*) FROM raw.census_county",
        ),
        (
            "Top 5 states by broadband penetration",
            """
            SELECT geography_name,
                   broadband_penetration_pct
            FROM   raw.census_state
            WHERE  broadband_penetration_pct IS NOT NULL
            ORDER  BY broadband_penetration_pct DESC
            LIMIT  5
            """,
        ),
        (
            "Bottom 5 states by broadband penetration",
            """
            SELECT geography_name,
                   broadband_penetration_pct
            FROM   raw.census_state
            WHERE  broadband_penetration_pct IS NOT NULL
            ORDER  BY broadband_penetration_pct ASC
            LIMIT  5
            """,
        ),
        (
            "Counties with zero internet access",
            """
            SELECT COUNT(*) as county_count
            FROM   raw.census_county
            WHERE  households_no_internet > total_households * 0.5
            """,
        ),
        (
            "National broadband penetration average",
            """
            SELECT ROUND(AVG(broadband_penetration_pct), 2) as avg_pct
            FROM   raw.census_state
            WHERE  broadband_penetration_pct IS NOT NULL
            """,
        ),
    ]

    for label, query in checks:
        try:
            result = conn.execute(query).fetchdf()
            logger.success(f"\n{label}:\n{result.to_string(index=False)}")
        except Exception as e:
            logger.warning(f"Verification query [{label}] failed: {e}")


def print_schema(conn: duckdb.DuckDBPyConnection):
    """
    Print the column names and types for each loaded table.
    This confirms the schema landed correctly in DuckDB.
    """
    for table in ["census_state", "census_county"]:
        try:
            schema = conn.execute(
                f"DESCRIBE raw.{table}"
            ).fetchdf()
            logger.info(f"\nSchema for raw.{table}:\n{schema.to_string(index=False)}")
        except Exception as e:
            logger.warning(f"Could not describe raw.{table}: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_load():
    """
    Main loader function. Orchestrates the full extract-load process:

      1. Connect to DuckDB (creates the .db file if it does not exist)
      2. Create the raw schema
      3. Find the latest CSV files from ingestion
      4. Load each CSV into its DuckDB table
      5. Print the schema of each table
      6. Run verification queries to confirm data integrity
    """
    logger.info("=" * 60)
    logger.info("Broadband Pipeline: DuckDB Warehouse Load Starting")
    logger.info(f"Database path  : {DB_PATH}")
    logger.info(f"Raw data dir   : {RAW_DIR}")
    logger.info("=" * 60)

    # Step 1: Connect to DuckDB
    # If broadband.db does not exist, DuckDB creates it automatically
    conn = duckdb.connect(DB_PATH)
    logger.info(f"Connected to DuckDB: {DB_PATH}")

    # Step 2: Create raw schema
    create_schema(conn)

    # Step 3: Find latest CSVs
    state_csv  = get_latest_csv("census_state")
    county_csv = get_latest_csv("census_county")

    if not state_csv and not county_csv:
        logger.error(
            "No CSV files found in data/raw/. "
            "Run ingestion/fetch_broadband_data.py first."
        )
        conn.close()
        return

    # Step 4: Load tables
    if state_csv:
        load_table(conn, state_csv, "census_state")

    if county_csv:
        load_table(conn, county_csv, "census_county")

    # Step 5: Print schema
    print_schema(conn)

    # Step 6: Verify
    verify_tables(conn)

    conn.close()

    logger.info("=" * 60)
    logger.success("DuckDB Load Complete")
    logger.info(f"Database file  : {DB_PATH}")
    logger.info(
        "Next step      : run dbt to transform raw -> staging -> marts"
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    run_load()
