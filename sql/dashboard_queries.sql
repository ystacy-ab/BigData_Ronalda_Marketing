-- Queries for a Databricks AI/BI dashboard "Marketing — TPC-H".
-- Replace workspace.dev_ with your <catalog>.<env>_ prefix.

-- [Counter] Q1 · BUILDING activation rate
SELECT ROUND(activation_rate * 100, 2) AS activation_rate_pct
FROM workspace.dev_tpch_mkt_gold.segment_summary WHERE market_segment = 'BUILDING';

-- [Bar] Activation & repeat rate by segment (Q1, Q3)
SELECT market_segment, activation_rate, repeat_rate, repeat_rate_among_active
FROM workspace.dev_tpch_mkt_gold.segment_summary ORDER BY market_segment;

-- [Counter] Q2 · New customers in 1996-Q1
SELECT SUM(new_customers) AS new_customers
FROM workspace.dev_tpch_mkt_gold.segment_kpi_monthly WHERE quarter = '1996-Q1';

-- [Stacked bar] New customers per quarter by segment (monitoring)
SELECT quarter, market_segment, SUM(new_customers) AS new_customers
FROM workspace.dev_tpch_mkt_gold.segment_kpi_monthly GROUP BY quarter, market_segment;

-- [Line] Activation rate over time by segment (monitoring)
SELECT month, market_segment, activation_rate_to_date
FROM workspace.dev_tpch_mkt_gold.segment_kpi_monthly;

-- [Bar] Q4 · cohort 3-month retention
SELECT cohort_month, cohort_size, retention_rate_3m, window_complete
FROM workspace.dev_tpch_mkt_gold.cohort_retention_3m ORDER BY cohort_month;

-- [Table] Data-quality status of the last run
SELECT rule_id, layer, description, actual, expected, passed
FROM workspace.dev_tpch_mkt_ops.validation_results
WHERE run_ts = (SELECT MAX(run_ts) FROM workspace.dev_tpch_mkt_ops.validation_results)
ORDER BY passed, rule_id;
