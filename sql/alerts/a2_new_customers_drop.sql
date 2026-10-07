-- Databricks SQL Alert A2: new customers fell > 30% quarter over quarter (previous quarter >= 100).
-- Only complete quarters are compared (the last data quarter is partial).
WITH bounds AS (SELECT MAX(month) AS last_month FROM workspace.dev_tpch_mkt_gold.segment_kpi_monthly),
q AS (
    SELECT quarter, SUM(new_customers) AS new_customers, COUNT(DISTINCT month) AS months
    FROM workspace.dev_tpch_mkt_gold.segment_kpi_monthly GROUP BY quarter
),
w AS (
    SELECT quarter, new_customers, LAG(new_customers) OVER (ORDER BY quarter) AS prev
    FROM q WHERE months = 3
)
SELECT quarter, prev, new_customers,
       prev >= 100 AND new_customers < prev * 0.7 AS alert
FROM w ORDER BY quarter DESC LIMIT 1;
