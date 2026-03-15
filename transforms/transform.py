"""
transform.py
------------
Transformation layer for the Broadband Access Pipeline.

Runs SQL transformations directly against DuckDB in two layers:

  Layer 1 - Staging (views):
    Cleans raw tables, casts types, adds derived columns
    - staging.stg_census_state
    - staging.stg_census_county

  Layer 2 - Marts (tables):
    Final analytics-ready tables built on staging
    - marts.mart_state_broadband_summary
    - marts.mart_underserved_counties

This mirrors exactly what dbt would do but runs natively in DuckDB
with zero dependency issues.

Author: Joey | Broadband Pipeline Portfolio Project
"""

import os
import duckdb
import pandas as pd
from loguru import logger

# ── Configuration ─────────────────────────────────────────────────────────────

BASE_DIR    = os.path.join(os.path.dirname(__file__), "..")
DB_PATH     = os.path.join(BASE_DIR, "broadband.db")
OUTPUT_DIR  = os.path.join(BASE_DIR, "data", "output")

# ── SQL Definitions ───────────────────────────────────────────────────────────

STAGING_MODELS = {

    "stg_census_state": """
        CREATE OR REPLACE VIEW staging.stg_census_state AS
        WITH source AS (
            SELECT * FROM raw.census_state
        ),
        cleaned AS (
            SELECT
                geography_name                              AS state_name,
                state                                       AS state_fips,
                CAST(total_households AS INTEGER)           AS total_households,
                CAST(households_with_internet AS INTEGER)   AS households_with_internet,
                CAST(households_broadband_any AS INTEGER)   AS households_broadband_any,
                CAST(households_fiber_optic AS INTEGER)     AS households_fiber_optic,
                CAST(households_cable_dsl_satellite AS INTEGER)
                                                            AS households_cable_dsl_satellite,
                CAST(households_no_internet AS INTEGER)     AS households_no_internet,
                CAST(broadband_penetration_pct AS DOUBLE)   AS broadband_penetration_pct,
                data_source,
                ingested_at,
                loaded_at
            FROM source
            WHERE total_households IS NOT NULL
              AND CAST(total_households AS INTEGER) > 0
        )
        SELECT * FROM cleaned
    """,

    "stg_census_county": """
        CREATE OR REPLACE VIEW staging.stg_census_county AS
        WITH source AS (
            SELECT * FROM raw.census_county
        ),
        cleaned AS (
            SELECT
                geography_name                              AS county_name,
                county                                      AS county_fips,
                state                                       AS state_fips,
                CAST(total_households AS INTEGER)           AS total_households,
                CAST(households_with_internet AS INTEGER)   AS households_with_internet,
                CAST(households_broadband_any AS INTEGER)   AS households_broadband_any,
                CAST(households_fiber_optic AS INTEGER)     AS households_fiber_optic,
                CAST(households_cable_dsl_satellite AS INTEGER)
                                                            AS households_cable_dsl_satellite,
                CAST(households_no_internet AS INTEGER)     AS households_no_internet,
                CAST(broadband_penetration_pct AS DOUBLE)   AS broadband_penetration_pct,
                CASE
                    WHEN CAST(households_no_internet AS INTEGER) * 1.0
                         / NULLIF(CAST(total_households AS INTEGER), 0) > 0.20
                    THEN TRUE
                    ELSE FALSE
                END                                         AS is_underserved,
                data_source,
                ingested_at,
                loaded_at
            FROM source
            WHERE total_households IS NOT NULL
              AND CAST(total_households AS INTEGER) > 0
        )
        SELECT * FROM cleaned
    """,
}

MART_MODELS = {

    "mart_state_broadband_summary": """
        CREATE OR REPLACE TABLE marts.mart_state_broadband_summary AS
        WITH staging AS (
            SELECT * FROM staging.stg_census_state
        ),
        ranked AS (
            SELECT
                state_name,
                state_fips,
                total_households,
                households_with_internet,
                households_broadband_any,
                households_fiber_optic,
                households_cable_dsl_satellite,
                households_no_internet,
                broadband_penetration_pct,
                RANK() OVER (
                    ORDER BY broadband_penetration_pct DESC
                )                                           AS broadband_rank,
                ROUND(
                    households_fiber_optic * 100.0
                    / NULLIF(total_households, 0), 2
                )                                           AS fiber_penetration_pct,
                ROUND(
                    households_no_internet * 100.0
                    / NULLIF(total_households, 0), 2
                )                                           AS no_internet_pct,
                CASE
                    WHEN broadband_penetration_pct >= 90 THEN 'High Access'
                    WHEN broadband_penetration_pct >= 85 THEN 'Medium Access'
                    WHEN broadband_penetration_pct >= 80 THEN 'Low Access'
                    ELSE 'Critical Gap'
                END                                         AS access_tier,
                data_source,
                loaded_at
            FROM staging
        )
        SELECT * FROM ranked
        ORDER BY broadband_rank
    """,

    "mart_underserved_counties": """
        CREATE OR REPLACE TABLE marts.mart_underserved_counties AS
        WITH counties AS (
            SELECT * FROM staging.stg_census_county
        ),
        states AS (
            SELECT
                state_fips,
                state_name,
                broadband_penetration_pct AS state_broadband_pct
            FROM staging.stg_census_state
        ),
        joined AS (
            SELECT
                c.county_name,
                c.county_fips,
                c.state_fips,
                s.state_name,
                c.total_households,
                c.households_broadband_any,
                c.households_no_internet,
                c.broadband_penetration_pct         AS county_broadband_pct,
                s.state_broadband_pct,
                ROUND(
                    s.state_broadband_pct - c.broadband_penetration_pct, 2
                )                                   AS pct_below_state_avg,
                ROUND(
                    c.households_no_internet * 100.0
                    / NULLIF(c.total_households, 0), 2
                )                                   AS no_internet_pct,
                c.is_underserved,
                CASE
                    WHEN c.broadband_penetration_pct < 70 THEN 'Critical'
                    WHEN c.broadband_penetration_pct < 80 THEN 'Severe'
                    WHEN c.broadband_penetration_pct < 85 THEN 'Moderate'
                    ELSE 'Marginal'
                END                                 AS underserved_severity,
                c.loaded_at
            FROM counties c
            LEFT JOIN states s ON c.state_fips = s.state_fips
            WHERE c.is_underserved = TRUE
        )
        SELECT * FROM joined
        ORDER BY county_broadband_pct ASC
    """,
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def create_schemas(conn: duckdb.DuckDBPyConnection):
    """Create staging and marts schemas if they do not exist."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute("CREATE SCHEMA IF NOT EXISTS marts")
    logger.info("Schemas ready: staging, marts")


def run_model(
    conn: duckdb.DuckDBPyConnection,
    name: str,
    sql: str,
    layer: str,
):
    """Run a single SQL model and log the result."""
    try:
        conn.execute(sql)
        logger.success(f"[{layer}] {name} OK")
    except Exception as e:
        logger.error(f"[{layer}] {name} FAILED: {e}")
        raise


def export_marts(conn: duckdb.DuckDBPyConnection):
    """
    Export final mart tables to CSV in data/output/.
    This simulates what a pipeline would deliver to analysts
    or load into a BI tool like Tableau or Metabase.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    exports = [
        ("mart_state_broadband_summary",  "state_broadband_summary.csv"),
        ("mart_underserved_counties",     "underserved_counties.csv"),
    ]

    for table, filename in exports:
        try:
            df = conn.execute(
                f"SELECT * FROM marts.{table}"
            ).fetchdf()
            path = os.path.join(OUTPUT_DIR, filename)
            df.to_csv(path, index=False)
            logger.success(
                f"Exported {len(df)} rows -> data/output/{filename}"
            )
        except Exception as e:
            logger.error(f"Export failed for {table}: {e}")


def preview_results(conn: duckdb.DuckDBPyConnection):
    """Print a quick preview of each mart table after transformation."""

    previews = [
        (
            "State Broadband Rankings (Top 10)",
            """
            SELECT state_name, broadband_penetration_pct,
                   broadband_rank, access_tier
            FROM   marts.mart_state_broadband_summary
            ORDER  BY broadband_rank
            LIMIT  10
            """,
        ),
        (
            "Most Underserved Counties (Top 10)",
            """
            SELECT county_name, state_name,
                   county_broadband_pct, underserved_severity
            FROM   marts.mart_underserved_counties
            ORDER  BY county_broadband_pct ASC
            LIMIT  10
            """,
        ),
        (
            "Underserved Count by Severity",
            """
            SELECT underserved_severity,
                   COUNT(*) AS county_count
            FROM   marts.mart_underserved_counties
            GROUP  BY underserved_severity
            ORDER  BY county_count DESC
            """,
        ),
    ]

    for label, query in previews:
        result = conn.execute(query).fetchdf()
        logger.success(f"\n{label}:\n{result.to_string(index=False)}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_transforms():
    """
    Main transformation orchestrator.

      1. Connect to DuckDB
      2. Create staging and marts schemas
      3. Run staging models (views that clean raw data)
      4. Run mart models (tables with analytics logic)
      5. Export marts to CSV in data/output/
      6. Preview results
    """
    logger.info("=" * 60)
    logger.info("Broadband Pipeline: Transformations Starting")
    logger.info(f"Database: {DB_PATH}")
    logger.info("=" * 60)

    conn = duckdb.connect(DB_PATH)

    # Step 1: Schemas
    create_schemas(conn)

    # Step 2: Staging models
    logger.info("Running staging layer...")
    for name, sql in STAGING_MODELS.items():
        run_model(conn, name, sql, "staging")

    # Step 3: Mart models
    logger.info("Running marts layer...")
    for name, sql in MART_MODELS.items():
        run_model(conn, name, sql, "marts")

    # Step 4: Export to CSV
    logger.info("Exporting mart tables to data/output/...")
    export_marts(conn)

    # Step 5: Preview
    preview_results(conn)

    conn.close()

    logger.info("=" * 60)
    logger.success("Transformations Complete")
    logger.info("Schemas created : staging, marts")
    logger.info("Models run      : 2 staging views, 2 mart tables")
    logger.info("Exports         : data/output/state_broadband_summary.csv")
    logger.info("                  data/output/underserved_counties.csv")
    logger.info("Next step       : run orchestration/pipeline.py")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_transforms()
