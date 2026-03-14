"""
fetch_broadband_data.py
-----------------------
Ingestion script for US Broadband Access data from two public sources:

  1. US Census Bureau ACS (American Community Survey)
     - Internet and broadband access by state and county
     - Variable B28002: Presence and types of internet subscriptions
     - No API key required for small queries
     - Endpoint: https://api.census.gov/data/2022/acs/acs5

  2. FCC Fixed Broadband Deployment Summary (Form 477 - legacy public data)
     - Available as direct public CSV download from data.gov
     - No authentication required

Together these paint a complete picture: WHO has broadband (Census)
and WHERE it is deployed (FCC), which is exactly what analytics teams
at telecoms like Cox care about.

Author: Joey | Broadband Pipeline Portfolio Project
"""

import os
import json
import time
import requests
import pandas as pd
from datetime import datetime
from loguru import logger

# ── Configuration ────────────────────────────────────────────────────────────

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

CENSUS_BASE_URL = "https://api.census.gov/data/2022/acs/acs5"

# ACS variables for internet/broadband access (Table B28002)
# These measure household internet subscription types
CENSUS_VARIABLES = {
    "B28002_001E": "total_households",
    "B28002_002E": "households_with_internet",
    "B28002_004E": "households_broadband_any",
    "B28002_006E": "households_fiber_optic",
    "B28002_008E": "households_cable_dsl_satellite",
    "B28002_013E": "households_no_internet",
    "NAME":        "geography_name",
}

# FCC Form 477 - public broadband deployment summary
# Available on data.gov as a direct public download, no auth required
FCC_477_URL = (
    "https://opendata.fcc.gov/api/views/sgz3-kiqt/rows.csv"
    "?accessType=DOWNLOAD"
)

HEADERS = {
    "User-Agent": "broadband-pipeline-portfolio/1.0",
    "Accept": "application/json",
}

# ── Helpers ──────────────────────────────────────────────────────────────────

def ensure_raw_dir():
    """Create data/raw directory if it does not exist."""
    os.makedirs(RAW_DIR, exist_ok=True)
    logger.info(f"Raw data directory ready: {RAW_DIR}")


def fetch_census_state_data() -> pd.DataFrame | None:
    """
    Pull broadband/internet access data for all 50 US states + DC
    from the Census Bureau ACS 5-Year Estimates (2022).

    Uses geography wildcard 'for=state:*' to get all states in one call.
    No API key required.

    Returns a cleaned DataFrame or None on failure.
    """
    variable_list = ",".join(CENSUS_VARIABLES.keys())
    params = {
        "get": variable_list,
        "for": "state:*",
    }

    logger.info("Fetching Census ACS broadband data at state level...")

    try:
        response = requests.get(
            CENSUS_BASE_URL,
            params=params,
            headers=HEADERS,
            timeout=30,
        )
        response.raise_for_status()
        raw = response.json()

        # First row is the header, rest is data
        headers = raw[0]
        rows = raw[1:]
        df = pd.DataFrame(rows, columns=headers)

        # Rename columns to human-readable names
        df = df.rename(columns=CENSUS_VARIABLES)

        # Add metadata columns
        df["data_source"] = "Census ACS 5-Year 2022"
        df["geography_level"] = "state"
        df["ingested_at"] = datetime.now().isoformat()

        # Convert numeric columns from strings
        numeric_cols = [v for k, v in CENSUS_VARIABLES.items() if k != "NAME"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Derive a broadband penetration rate column
        df["broadband_penetration_pct"] = (
            df["households_broadband_any"] / df["total_households"] * 100
        ).round(2)

        logger.success(
            f"Census state data fetched: {len(df)} states, "
            f"{df.shape[1]} columns"
        )
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Census API request failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing Census state data: {e}")
        return None


def fetch_census_county_data() -> pd.DataFrame | None:
    """
    Pull broadband/internet access data for all US counties
    from the Census Bureau ACS 5-Year Estimates (2022).

    Uses geography 'for=county:*&in=state:*' for all counties nationwide.
    This is a larger call (~3000 rows) and may take longer.

    Returns a cleaned DataFrame or None on failure.
    """
    variable_list = ",".join(CENSUS_VARIABLES.keys())
    params = {
        "get": variable_list,
        "for": "county:*",
        "in":  "state:*",
    }

    logger.info("Fetching Census ACS broadband data at county level...")
    logger.info("This call covers ~3100 US counties and may take 15-30 seconds...")

    try:
        response = requests.get(
            CENSUS_BASE_URL,
            params=params,
            headers=HEADERS,
            timeout=60,
        )
        response.raise_for_status()
        raw = response.json()

        headers = raw[0]
        rows = raw[1:]
        df = pd.DataFrame(rows, columns=headers)

        df = df.rename(columns=CENSUS_VARIABLES)

        df["data_source"] = "Census ACS 5-Year 2022"
        df["geography_level"] = "county"
        df["ingested_at"] = datetime.now().isoformat()

        numeric_cols = [v for k, v in CENSUS_VARIABLES.items() if k != "NAME"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df["broadband_penetration_pct"] = (
            df["households_broadband_any"] / df["total_households"] * 100
        ).round(2)

        logger.success(
            f"Census county data fetched: {len(df)} counties, "
            f"{df.shape[1]} columns"
        )
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Census county API request failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing Census county data: {e}")
        return None


def fetch_fcc_477_summary() -> pd.DataFrame | None:
    """
    Download FCC Form 477 fixed broadband deployment summary.
    This is a public dataset hosted on the FCC's open data portal
    with no authentication required.

    Returns a cleaned DataFrame or None on failure.
    """
    logger.info("Fetching FCC Form 477 broadband deployment summary...")
    logger.info("Downloading from FCC Open Data portal...")

    try:
        response = requests.get(
            FCC_477_URL,
            headers={"User-Agent": "broadband-pipeline-portfolio/1.0"},
            timeout=60,
            stream=True,
        )
        response.raise_for_status()

        from io import StringIO
        content = response.content.decode("utf-8")
        df = pd.read_csv(StringIO(content))

        df["data_source"] = "FCC Form 477 Open Data"
        df["ingested_at"] = datetime.now().isoformat()

        logger.success(
            f"FCC 477 data fetched: {len(df)} rows, "
            f"{df.shape[1]} columns"
        )
        return df

    except requests.exceptions.RequestException as e:
        logger.warning(f"FCC 477 download failed: {e}")
        logger.info("This dataset is optional. Continuing without it.")
        return None
    except Exception as e:
        logger.warning(f"Error parsing FCC 477 data: {e}")
        return None


def validate_dataframe(df: pd.DataFrame, label: str):
    """
    Run basic data quality checks on a DataFrame.
    Logs warnings for high null rates and reports shape.
    """
    logger.info(f"Validating [{label}]...")

    null_pct = (df.isnull().sum() / len(df) * 100).round(1)
    high_null = null_pct[null_pct > 50]
    if not high_null.empty:
        logger.warning(f"High null columns in [{label}]: {high_null.to_dict()}")
    else:
        logger.success(f"[{label}] null check passed")

    logger.success(
        f"[{label}] shape: {df.shape[0]} rows x {df.shape[1]} cols"
    )


def save_csv(df: pd.DataFrame, name: str, run_timestamp: str) -> str:
    """Save a DataFrame to a timestamped CSV in data/raw/."""
    filename = f"{name}_{run_timestamp}.csv"
    filepath = os.path.join(RAW_DIR, filename)
    df.to_csv(filepath, index=False)
    logger.success(f"Saved: {filepath}")
    return filepath


def save_manifest(run_timestamp: str, datasets: list[dict]):
    """Write a JSON audit manifest for this pipeline run."""
    manifest = {
        "run_timestamp": run_timestamp,
        "pipeline_version": "1.0.0",
        "datasets": datasets,
    }
    path = os.path.join(RAW_DIR, f"manifest_{run_timestamp}.json")
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)
    logger.info(f"Manifest written: {path}")


# ── Main Pipeline ─────────────────────────────────────────────────────────────

def run_ingestion():
    """
    Main ingestion orchestrator. Runs all data fetches and saves
    raw CSV files to data/raw/ with a manifest for auditability.

    Data flow:
      Census API (state level)  -> data/raw/census_state_TIMESTAMP.csv
      Census API (county level) -> data/raw/census_county_TIMESTAMP.csv
      FCC 477 Open Data         -> data/raw/fcc_477_TIMESTAMP.csv  (optional)
      Audit log                 -> data/raw/manifest_TIMESTAMP.json
    """
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    datasets = []

    logger.info("=" * 60)
    logger.info("Broadband Pipeline: Ingestion Starting")
    logger.info(f"Run timestamp  : {run_timestamp}")
    logger.info(f"Sources        : Census ACS 2022 + FCC Form 477")
    logger.info("=" * 60)

    ensure_raw_dir()

    # 1. Census state level
    df_state = fetch_census_state_data()
    if df_state is not None:
        validate_dataframe(df_state, "census_state")
        filepath = save_csv(df_state, "census_state", run_timestamp)
        datasets.append({
            "name": "census_state",
            "source": "Census ACS 5-Year 2022 - State Level",
            "rows": len(df_state),
            "columns": list(df_state.columns),
            "file": filepath,
        })
    else:
        logger.error("State-level Census fetch failed. Check connectivity.")

    time.sleep(1)

    # 2. Census county level
    df_county = fetch_census_county_data()
    if df_county is not None:
        validate_dataframe(df_county, "census_county")
        filepath = save_csv(df_county, "census_county", run_timestamp)
        datasets.append({
            "name": "census_county",
            "source": "Census ACS 5-Year 2022 - County Level",
            "rows": len(df_county),
            "columns": list(df_county.columns),
            "file": filepath,
        })
    else:
        logger.warning("County-level Census fetch failed. Continuing.")

    time.sleep(1)

    # 3. FCC Form 477 (optional, may time out on slow connections)
    df_fcc = fetch_fcc_477_summary()
    if df_fcc is not None:
        validate_dataframe(df_fcc, "fcc_477")
        filepath = save_csv(df_fcc, "fcc_477", run_timestamp)
        datasets.append({
            "name": "fcc_477",
            "source": "FCC Form 477 Open Data Portal",
            "rows": len(df_fcc),
            "columns": list(df_fcc.columns),
            "file": filepath,
        })

    save_manifest(run_timestamp, datasets)

    logger.info("=" * 60)
    logger.success("Ingestion Complete")
    logger.info(f"Datasets ingested : {len(datasets)}")
    for d in datasets:
        logger.info(f"  {d['name']:20s} -> {d['rows']} rows")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_ingestion()
