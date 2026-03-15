"""
pipeline.py
-----------
Prefect orchestration flow for the Broadband Access Pipeline.

Ties all pipeline steps together into a single command:
  1. Ingest raw data from Census Bureau ACS
  2. Load raw CSVs into DuckDB warehouse
  3. Run SQL transformations (staging -> marts)

Prefect adds:
  - Task-level logging and status tracking
  - Automatic retries on failure
  - Run history so you can see past executions
  - A local dashboard to monitor runs

Author: Joey | Broadband Pipeline Portfolio Project
"""

import os
import sys
from prefect import flow, task
from prefect.logging import get_run_logger

# Add project root to path so we can import from ingestion and transforms
ROOT_DIR = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT_DIR)

from ingestion.fetch_broadband_data import run_ingestion
from ingestion.load_to_duckdb import run_load
from transforms.transform import run_transforms

# ── Tasks ─────────────────────────────────────────────────────────────────────
# Each pipeline step is wrapped in a @task decorator.
# This gives Prefect visibility into each step individually so if
# one fails you know exactly where it broke.

@task(
    name="Ingest Broadband Data",
    description="Fetch Census ACS broadband data and save to data/raw/",
    retries=2,
    retry_delay_seconds=30,
)
def ingest_task():
    """
    Task 1: Pull raw data from Census Bureau ACS API.
    Retries up to 2 times with a 30 second delay if it fails.
    This handles temporary network issues gracefully.
    """
    logger = get_run_logger()
    logger.info("Starting ingestion task...")
    run_ingestion()
    logger.info("Ingestion task complete")


@task(
    name="Load to DuckDB",
    description="Load raw CSVs from data/raw/ into DuckDB warehouse",
    retries=1,
    retry_delay_seconds=10,
)
def load_task():
    """
    Task 2: Load raw CSV files into DuckDB.
    Reads the latest timestamped files from data/raw/
    and creates tables in the raw schema.
    """
    logger = get_run_logger()
    logger.info("Starting DuckDB load task...")
    run_load()
    logger.info("Load task complete")


@task(
    name="Run Transformations",
    description="Run staging and mart SQL models in DuckDB",
    retries=1,
    retry_delay_seconds=10,
)
def transform_task():
    """
    Task 3: Run the full transformation layer.
    Creates staging views and mart tables,
    then exports results to data/output/.
    """
    logger = get_run_logger()
    logger.info("Starting transformation task...")
    run_transforms()
    logger.info("Transformation task complete")


# ── Flow ──────────────────────────────────────────────────────────────────────
# A @flow is the top-level Prefect unit that groups tasks together.
# It tracks the overall pipeline run, logs timing, and reports success/failure.

@flow(
    name="Broadband Access Pipeline",
    description=(
        "End-to-end pipeline: ingest Census broadband data, "
        "load to DuckDB, run SQL transformations"
    ),
)
def broadband_pipeline():
    """
    Main Prefect flow. Runs all three pipeline tasks in sequence.

    If any task fails it will retry according to its retry config.
    If retries are exhausted the flow is marked as failed and
    all subsequent tasks are skipped automatically.
    """
    logger = get_run_logger()

    logger.info("=" * 50)
    logger.info("Broadband Access Pipeline Starting")
    logger.info("=" * 50)

    # Tasks run sequentially. Each one must complete before the next starts.
    ingest_task()
    load_task()
    transform_task()

    logger.info("=" * 50)
    logger.info("Broadband Access Pipeline Complete")
    logger.info("=" * 50)


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    broadband_pipeline()
