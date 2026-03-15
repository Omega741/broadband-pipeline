-- analytics.sql
-- ============================================================
-- Broadband Access Pipeline - Showcase Analytical Queries
-- ============================================================
-- These queries run directly against the DuckDB mart tables
-- and demonstrate SQL skills relevant to data engineering
-- and analytics roles in the telecom industry.
--
-- To run these queries:
--   python -c "import duckdb; conn = duckdb.connect('broadband.db'); print(conn.execute(open('queries/analytics.sql').read()).fetchdf())"
--
-- Or open broadband.db in DBeaver and run each block manually.
--
-- Author: Joey | Broadband Pipeline Portfolio Project
-- ============================================================


-- ── Query 1: National Broadband Scorecard ─────────────────────────────────
-- High level summary of broadband access across the entire US.
-- This is the kind of executive dashboard query a director would ask for.

SELECT
    COUNT(*)                                    AS total_states,
    ROUND(AVG(broadband_penetration_pct), 2)    AS avg_broadband_pct,
    ROUND(MIN(broadband_penetration_pct), 2)    AS min_broadband_pct,
    ROUND(MAX(broadband_penetration_pct), 2)    AS max_broadband_pct,
    ROUND(AVG(fiber_penetration_pct), 2)        AS avg_fiber_pct,
    ROUND(AVG(no_internet_pct), 2)              AS avg_no_internet_pct,
    COUNT(CASE WHEN access_tier = 'High Access'   THEN 1 END) AS high_access_states,
    COUNT(CASE WHEN access_tier = 'Medium Access' THEN 1 END) AS medium_access_states,
    COUNT(CASE WHEN access_tier = 'Low Access'    THEN 1 END) AS low_access_states,
    COUNT(CASE WHEN access_tier = 'Critical Gap'  THEN 1 END) AS critical_gap_states
FROM marts.mart_state_broadband_summary;


-- ── Query 2: State Broadband Rankings with Tier ────────────────────────────
-- Full ranking of all states by broadband penetration.
-- Shows rank, penetration rate, fiber rate, and access tier side by side.

SELECT
    broadband_rank                              AS rank,
    state_name,
    broadband_penetration_pct                   AS broadband_pct,
    fiber_penetration_pct                       AS fiber_pct,
    no_internet_pct,
    access_tier
FROM marts.mart_state_broadband_summary
ORDER BY broadband_rank;


-- ── Query 3: Fiber Leaders vs Laggards ────────────────────────────────────
-- States with the highest and lowest fiber optic adoption.
-- Fiber is the gold standard for broadband infrastructure.
-- Useful for identifying markets with the most room for expansion.

WITH fiber_ranked AS (
    SELECT
        state_name,
        fiber_penetration_pct,
        broadband_penetration_pct,
        RANK() OVER (ORDER BY fiber_penetration_pct DESC) AS fiber_rank
    FROM marts.mart_state_broadband_summary
    WHERE fiber_penetration_pct IS NOT NULL
)
SELECT
    fiber_rank,
    state_name,
    fiber_penetration_pct,
    broadband_penetration_pct,
    CASE
        WHEN fiber_rank <= 10 THEN 'Fiber Leader'
        WHEN fiber_rank > (SELECT COUNT(*) FROM fiber_ranked) - 10
        THEN 'Fiber Laggard'
        ELSE 'Mid Tier'
    END AS fiber_category
FROM fiber_ranked
ORDER BY fiber_rank;


-- ── Query 4: Underserved County Summary by State ───────────────────────────
-- How many underserved counties does each state have?
-- States with many underserved counties are priority markets
-- for broadband expansion funding and infrastructure investment.

SELECT
    state_name,
    COUNT(*)                                    AS underserved_county_count,
    ROUND(AVG(county_broadband_pct), 2)         AS avg_county_broadband_pct,
    ROUND(AVG(no_internet_pct), 2)              AS avg_no_internet_pct,
    COUNT(CASE WHEN underserved_severity = 'Critical' THEN 1 END)
                                                AS critical_counties,
    COUNT(CASE WHEN underserved_severity = 'Severe'   THEN 1 END)
                                                AS severe_counties
FROM marts.mart_underserved_counties
GROUP BY state_name
ORDER BY underserved_county_count DESC
LIMIT 20;


-- ── Query 5: Critical Gap Counties ────────────────────────────────────────
-- Counties where broadband penetration is below 70%.
-- These are the most urgent targets for infrastructure investment.
-- Mirrors FCC and NTIA definitions of unserved communities.

SELECT
    county_name,
    state_name,
    county_broadband_pct,
    no_internet_pct,
    pct_below_state_avg,
    total_households,
    CAST(
        total_households * (1 - county_broadband_pct / 100.0)
    AS INTEGER)                                 AS households_without_broadband
FROM marts.mart_underserved_counties
WHERE underserved_severity = 'Critical'
ORDER BY county_broadband_pct ASC;


-- ── Query 6: Broadband Gap Index ──────────────────────────────────────────
-- A composite score measuring how far each underserved county
-- falls behind its state average. Higher score = larger gap.
-- This kind of derived metric is what separates junior from
-- senior data work on a resume.

SELECT
    county_name,
    state_name,
    county_broadband_pct,
    state_broadband_pct,
    pct_below_state_avg,
    no_internet_pct,
    total_households,
    ROUND(
        (pct_below_state_avg * 0.6) + (no_internet_pct * 0.4), 2
    )                                           AS broadband_gap_index,
    underserved_severity
FROM marts.mart_underserved_counties
ORDER BY broadband_gap_index DESC
LIMIT 25;


-- ── Query 7: Household Impact Analysis ────────────────────────────────────
-- Total number of households without broadband by state,
-- across all underserved counties. This translates data into
-- real human impact which is what policy makers and executives care about.

SELECT
    state_name,
    COUNT(county_name)                          AS underserved_counties,
    SUM(total_households)                       AS total_households_in_underserved,
    SUM(households_broadband_any)               AS households_with_broadband,
    SUM(total_households) - SUM(households_broadband_any)
                                                AS households_without_broadband,
    ROUND(
        (SUM(total_households) - SUM(households_broadband_any)) * 100.0
        / NULLIF(SUM(total_households), 0), 2
    )                                           AS pct_without_broadband
FROM marts.mart_underserved_counties
GROUP BY state_name
ORDER BY households_without_broadband DESC
LIMIT 15;


-- ── Query 8: Access Tier Distribution ─────────────────────────────────────
-- How many states fall into each access tier category?
-- Useful for a quick status report on national broadband health.

SELECT
    access_tier,
    COUNT(*)                                    AS state_count,
    ROUND(AVG(broadband_penetration_pct), 2)    AS avg_penetration,
    STRING_AGG(state_name, ', ' ORDER BY broadband_penetration_pct DESC)
                                                AS states
FROM marts.mart_state_broadband_summary
GROUP BY access_tier
ORDER BY avg_penetration DESC;
