# Broadband Access Pipeline - CLAUDE.md

## Project Overview
End-to-end local data engineering pipeline using FCC public broadband data.
Built for resume demonstration of real-world DE skills.

## Tech Stack
- Python 3.11+ - ingestion and scripting
- DuckDB - local analytical warehouse (replaces cloud storage)
- SQL - transformations run natively inside DuckDB
- Prefect - pipeline orchestration and scheduling
- GitHub Actions - CI/CD and scheduled runs

## Directory Map
- /ingestion - Python scripts that pull Census and FCC data
- /data/raw - raw CSV files land here (simulates S3 raw layer)
- /data/staging - intermediate cleaned files
- /data/output - final analytics-ready exports
- /transforms - SQL transformation layer (pure DuckDB SQL via Python)
- /orchestration - Prefect flow definitions
- /queries - standalone SQL analytics showcasing skills
- /.claude/skills/pipeline - Claude skill for this pipeline

## Architecture
Census Bureau ACS API + FCC Open Data
  -> ingestion/fetch_broadband_data.py
  -> data/raw/ (raw layer)
  -> ingestion/load_to_duckdb.py
  -> DuckDB warehouse (raw schema)
  -> transforms/transform.py (staging views -> mart tables)
  -> data/output/ (analytics layer)
  -> Prefect orchestrates all steps

## Commands
pip install pandas requests loguru dbt-duckdb prefect python-dotenv
python ingestion/fetch_broadband_data.py   # ingest raw data
python ingestion/load_to_duckdb.py         # load raw CSVs into DuckDB
python transforms/transform.py             # run SQL transformations
python orchestration/pipeline.py           # run full pipeline via Prefect

## Design Decisions
- DuckDB chosen over PostgreSQL for zero-infrastructure local dev
- Transformations run as native DuckDB SQL via Python instead of dbt
  to avoid Windows C++ build tool dependency issues
- Same layered architecture as dbt: raw -> staging -> marts
- Prefect local runner to avoid managed service costs
- Data partitioned by state for realistic warehouse simulation

## Gotchas
- Run ingestion before transforms or load will fail on missing data
- DuckDB .db file is gitignored, only schema/code is committed
- Raw and output CSV files are gitignored, regenerate by running pipeline
- Staging models are views, mart models are physical tables in DuckDB

## Workflow Rules
- Always run scripts in order: fetch -> load -> transform -> orchestrate
- Keep raw data untouched, transformations happen in DuckDB only
- Commit code changes frequently, never commit raw data or .db files