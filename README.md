# Broadband Access ETL Pipeline

A fully local, end-to-end data engineering pipeline built on free open-source tools.
Ingests FCC public broadband availability data, transforms it using dbt, and surfaces
analytics on coverage gaps across the United States.

---

## Architecture

```
FCC Public Data (CSV)
        |
        v
  [Python Ingestion]
  ingestion/fetch_fcc_data.py
        |
        v
  data/raw/          <-- Raw layer (simulates S3 landing zone)
        |
        v
  [DuckDB Warehouse]
        |
        v
  [dbt Core Models]
  staging -> marts
        |
        v
  data/output/       <-- Analytics-ready layer
        |
        v
  [Prefect Orchestration]
  Schedules and monitors all steps
```

---

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python + Requests | Pulls FCC public data |
| Raw Storage | Local filesystem | Simulates S3 raw bucket |
| Warehouse | DuckDB | Zero-infra analytical DB |
| Transformation | dbt Core | SQL modeling best practices |
| Orchestration | Prefect | Pipeline scheduling and monitoring |
| Version Control | GitHub | Code and lineage tracking |

---

## Setup

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/broadband-pipeline.git
cd broadband-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the full pipeline
python ingestion/fetch_fcc_data.py
dbt run --project-dir transforms
python orchestration/pipeline.py
```

---

## Data Source

FCC Broadband Map public data: https://broadbandmap.fcc.gov/
Updated annually. No API key required for public datasets.

---

## Key Analytics

- Broadband coverage percentage by state
- Provider count and market concentration per county
- Underserved areas (below 25 Mbps download threshold)
- Speed tier distribution across rural vs urban areas

---

## Project Structure

```
broadband-pipeline/
├── CLAUDE.md              # Project memory and context
├── README.md
├── requirements.txt
├── .gitignore
├── data/
│   ├── raw/               # Raw FCC CSVs (gitignored)
│   ├── staging/           # Intermediate files (gitignored)
│   └── output/            # Final exports (gitignored)
├── ingestion/
│   └── fetch_fcc_data.py  # Data ingestion script
├── transforms/            # dbt project
├── orchestration/
│   └── pipeline.py        # Prefect flow
└── queries/
    └── analytics.sql      # Showcase analytical queries
```

---

## Author

Joey | Data Engineering Portfolio Project
