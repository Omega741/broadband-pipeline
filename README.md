# Broadband Access ETL Pipeline

A fully local, end-to-end data engineering pipeline built on free open-source tools.
Ingests US broadband and internet access data from the Census Bureau ACS and FCC open
data, transforms it through a layered SQL architecture in DuckDB, and surfaces
analytics on coverage gaps across the United States.

---

## Architecture

```
Census Bureau ACS API          FCC Form 477 Open Data
(State + County Level)         (Deployment Summary)
        |                               |
        └──────────────┬────────────────┘
                       v
             [Python Ingestion]
        ingestion/fetch_broadband_data.py
                       |
                       v
             data/raw/              <-- Raw layer (simulates S3 landing zone)
             census_state_*.csv
             census_county_*.csv
             manifest_*.json
                       |
                       v
             [DuckDB Warehouse]
                       |
                       v
             [dbt Core Models]
             staging -> marts
                       |
                       v
             data/output/           <-- Analytics-ready layer
                       |
                       v
             [Prefect Orchestration]
             Schedules and monitors all steps
```

---

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python + Requests | Pulls Census ACS and FCC public data |
| Raw Storage | Local filesystem | Simulates S3 raw bucket |
| Warehouse | DuckDB | Zero-infra analytical DB |
| Transformation | Python + DuckDB SQL | Layered staging and mart models |
| Orchestration | Prefect | Pipeline scheduling and monitoring |
| Version Control | GitHub | Code and lineage tracking |

---

## Setup

```bash
# Clone the repo
git clone https://github.com/Omega741/broadband-pipeline.git
cd broadband-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install pandas requests loguru duckdb prefect python-dotenv

# Run the full pipeline
python ingestion/fetch_broadband_data.py
python ingestion/load_to_duckdb.py
python transforms/transform.py
python orchestration/pipeline.py
```

---

## Data Sources

**Census Bureau ACS 5-Year Estimates (2022)**
Table B28002: Presence and types of internet subscriptions by household.
Covers all 50 states, DC, and 3200+ US counties.
No API key required.
https://api.census.gov/data/2022/acs/acs5

**FCC Form 477 Open Data (optional)**
Fixed broadband deployment summary by geography.
Publicly hosted on the FCC open data portal.
https://opendata.fcc.gov

---

## Key Analytics

- Broadband penetration rate by state (households with broadband / total households)
- Counties with lowest internet access rates (underserved identification)
- Fiber optic vs cable vs no-internet distribution across states
- State ranking by broadband adoption

---

## Project Structure

```
broadband-pipeline/
├── CLAUDE.md                           # Project memory and context
├── README.md
├── requirements.txt
├── .gitignore
├── data/
│   ├── raw/                            # Raw CSVs + manifest (gitignored)
│   ├── staging/                        # Intermediate files (gitignored)
│   └── output/                         # Final exports (gitignored)
├── ingestion/
│   ├── fetch_broadband_data.py         # Data ingestion script
│   └── load_to_duckdb.py              # Warehouse loader
├── transforms/
│   └── transform.py                    # SQL transformation layer
├── orchestration/
│   └── pipeline.py                     # Prefect flow
└── queries/
    └── analytics.sql                   # Showcase analytical queries
```

---

## Author

Joey | Data Engineering Portfolio Project
