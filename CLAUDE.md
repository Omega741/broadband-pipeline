# Broadband Access Pipeline - CLAUDE.md

## Project Overview
End-to-end local data engineering pipeline using FCC public broadband data.
Built for resume demonstration of real-world DE skills.

## Tech Stack
- Python 3.11+ - ingestion and scripting
- DuckDB - local analytical warehouse (replaces cloud storage)
- dbt Core - SQL transformations and data modeling
- Prefect - pipeline orchestration and scheduling
- GitHub Actions - CI/CD and scheduled runs

## Directory Map
- /ingestion - Python scripts that pull raw FCC data
- /data/raw - raw CSV files land here (simulates S3 raw layer)
- /data/staging - intermediate cleaned files
- /data/output - final analytics-ready exports
- /transforms - dbt project for SQL modeling
- /orchestration - Prefect flow definitions
- /queries - standalone SQL analytics showcasing skills
- /.claude/skills/pipeline - Claude skill for this pipeline

## Architecture
FCC Public API / CSV Download
  -> ingestion/fetch_fcc_data.py
  -> data/raw/ (raw layer)
  -> DuckDB (warehouse)
  -> dbt Core transforms (staging -> marts)
  -> data/output/ (analytics layer)
  -> Prefect orchestrates all steps

## Commands
pip install -r requirements.txt     # install all dependencies
python ingestion/fetch_fcc_data.py  # run ingestion
dbt run --project-dir transforms    # run transformations
python orchestration/pipeline.py    # run full pipeline via Prefect

## Design Decisions
- DuckDB chosen over PostgreSQL for zero-infrastructure local dev
- dbt Core (not Cloud) to stay fully free and open source
- Prefect local runner to avoid managed service costs
- Data partitioned by state for realistic warehouse simulation

## Gotchas
- FCC data files can be large, raw/ folder is gitignored
- DuckDB .db file is gitignored, only schema/code is committed
- dbt needs profiles.yml configured to point at local DuckDB file
- Run ingestion before dbt or transforms will fail on missing data

## Workflow Rules
- Always run ingestion first, then dbt, then analytics queries
- Keep raw data untouched, transformations happen in DuckDB only
- Commit code changes frequently, never commit raw data files