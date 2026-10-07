-- Databricks SQL Alert A1: activation rate of a segment dropped by > 2 pp vs previous pipeline run.
-- Replace workspace.dev_ with your <catalog>.<env>_ ; configure the alert to trigger when MAX(alert) = true.
WITH runs AS (
    SELECT DISTINCT run_ts FROM workspace.dev_tpch_mkt_ops.kpi_snapshots ORDER BY run_ts DESC LIMIT 2
),
r AS (SELECT MAX(run_ts) AS curr_ts, MIN(run_ts) AS prev_ts FROM runs)
SELECT c.market_segment,
       p.activation_rate AS prev_rate,
       c.activation_rate AS curr_rate,
       ROUND((p.activation_rate - c.activation_rate) * 100, 2) AS drop_pp,
       (p.activation_rate - c.activation_rate) * 100 > 2 AS alert
FROM workspace.dev_tpch_mkt_ops.kpi_snapshots c
JOIN r ON c.run_ts = r.curr_ts
JOIN workspace.dev_tpch_mkt_ops.kpi_snapshots p
  ON p.run_ts = r.prev_ts AND p.market_segment = c.market_segment;
