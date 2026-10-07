# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — Marketing data products
# MAGIC
# MAGIC | table | grain | answers |
# MAGIC |---|---|---|
# MAGIC | `customer_activity` | 1 row per customer (**incl. customers with 0 orders**) | base for everything |
# MAGIC | `segment_summary` | 1 row per market segment | Q1 activation, Q3 repeat rate |
# MAGIC | `segment_kpi_monthly` | month × segment | Q2 new customers, monitoring |
# MAGIC | `cohort_retention_3m` | first-order month (cohort) | Q4 |
# MAGIC | `activity_retention_3m` | activity month | Q4 alternative view (see README) |
# MAGIC
# MAGIC `customer_activity` is built **from `silver.customer` with a LEFT JOIN to orders**, so customers
# MAGIC without orders survive as zero-activity rows instead of being dropped by an inner join.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

W = RETENTION_WINDOW_MONTHS

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.customer_activity
COMMENT 'One row per customer, including customers with zero orders.'
AS
WITH ranked AS (
    SELECT customer_key, order_key, order_date, total_price,
           ROW_NUMBER() OVER (PARTITION BY customer_key ORDER BY order_date, order_key) AS rn
    FROM {SILVER}.orders
),
agg AS (
    SELECT customer_key,
           COUNT(*)                                   AS order_count,
           SUM(total_price)                           AS lifetime_order_value,
           MIN(order_date)                            AS first_order_date,
           MAX(CASE WHEN rn = 2 THEN order_date END)  AS second_order_date,
           MAX(order_date)                            AS last_order_date
    FROM ranked
    GROUP BY customer_key
),
bounds AS (SELECT MAX(order_date) AS data_end FROM {SILVER}.orders)
SELECT
    c.customer_key,
    c.market_segment,
    n.nation_name,
    r.region_name,
    COALESCE(a.order_count, 0)                          AS order_count,
    COALESCE(a.lifetime_order_value, 0)                 AS lifetime_order_value,
    a.first_order_date,
    a.second_order_date,
    a.last_order_date,
    CAST(DATE_TRUNC('MONTH', a.first_order_date) AS DATE) AS cohort_month,
    CASE WHEN a.first_order_date IS NOT NULL
         THEN CONCAT(YEAR(a.first_order_date), '-Q', QUARTER(a.first_order_date)) END AS cohort_quarter,
    COALESCE(a.order_count, 0) >= 1                     AS is_activated,
    COALESCE(a.order_count, 0) >= 2                     AS is_repeat_buyer,
    COALESCE(a.second_order_date <= ADD_MONTHS(a.first_order_date, {W}), FALSE) AS reordered_within_{W}m,
    COALESCE(ADD_MONTHS(a.first_order_date, {W}) <= b.data_end, FALSE)          AS has_full_{W}m_window
FROM {SILVER}.customer c
LEFT JOIN agg a           ON a.customer_key = c.customer_key
LEFT JOIN {SILVER}.nation n ON n.nation_key = c.nation_key
LEFT JOIN {SILVER}.region r ON r.region_key = n.region_key
CROSS JOIN bounds b
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.segment_summary
COMMENT 'Activation and repeat-purchase KPIs per market segment.'
AS
SELECT
    market_segment,
    COUNT(*)                                                 AS customers,
    COUNT_IF(is_activated)                                   AS activated_customers,
    COUNT_IF(is_activated) / COUNT(*)                        AS activation_rate,
    COUNT_IF(is_repeat_buyer)                                AS repeat_customers,
    COUNT_IF(is_repeat_buyer) / COUNT(*)                     AS repeat_rate,
    COUNT_IF(is_repeat_buyer) / NULLIF(COUNT_IF(is_activated), 0) AS repeat_rate_among_active,
    AVG(CASE WHEN is_activated THEN order_count END)         AS avg_orders_per_active_customer
FROM {GOLD}.customer_activity
GROUP BY market_segment
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.segment_kpi_monthly
COMMENT 'Month x segment: new customers and cumulative activation rate. Months with 0 new customers are kept.'
AS
WITH months AS (
    SELECT EXPLODE(SEQUENCE(CAST(DATE_TRUNC('MONTH', MIN(order_date)) AS DATE),
                            CAST(DATE_TRUNC('MONTH', MAX(order_date)) AS DATE),
                            INTERVAL 1 MONTH)) AS month
    FROM {SILVER}.orders
),
segments AS (
    SELECT market_segment, COUNT(*) AS total_customers
    FROM {GOLD}.customer_activity GROUP BY market_segment
),
new_by_month AS (
    SELECT cohort_month AS month, market_segment, COUNT(*) AS new_customers
    FROM {GOLD}.customer_activity WHERE is_activated
    GROUP BY cohort_month, market_segment
),
grid AS (
    SELECT m.month, s.market_segment, s.total_customers, COALESCE(n.new_customers, 0) AS new_customers
    FROM months m
    CROSS JOIN segments s
    LEFT JOIN new_by_month n ON n.month = m.month AND n.market_segment = s.market_segment
)
SELECT
    month,
    CONCAT(YEAR(month), '-Q', QUARTER(month)) AS quarter,
    market_segment,
    total_customers,
    new_customers,
    SUM(new_customers) OVER (PARTITION BY market_segment ORDER BY month) AS activated_to_date,
    SUM(new_customers) OVER (PARTITION BY market_segment ORDER BY month) / total_customers
        AS activation_rate_to_date
FROM grid
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.cohort_retention_{W}m
COMMENT 'Cohort = month of first order. Retained = second order within {W} months of the first.'
AS
SELECT
    cohort_month,
    COUNT(*)                                        AS cohort_size,
    COUNT_IF(reordered_within_{W}m)                 AS reordered_customers,
    COUNT_IF(reordered_within_{W}m) / COUNT(*)      AS retention_rate_{W}m,
    BOOL_AND(has_full_{W}m_window)                  AS window_complete
FROM {GOLD}.customer_activity
WHERE is_activated
GROUP BY cohort_month
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.activity_retention_{W}m
COMMENT 'Activity month = any month with an order. Returned = next order within {W} months.'
AS
WITH cm AS (
    SELECT DISTINCT customer_key, CAST(DATE_TRUNC('MONTH', order_date) AS DATE) AS month
    FROM {SILVER}.orders
),
nxt AS (
    SELECT customer_key, month,
           LEAD(month) OVER (PARTITION BY customer_key ORDER BY month) AS next_month
    FROM cm
),
bounds AS (SELECT CAST(DATE_TRUNC('MONTH', MAX(order_date)) AS DATE) AS last_month FROM {SILVER}.orders)
SELECT
    n.month                                                         AS activity_month,
    COUNT(*)                                                        AS active_customers,
    COUNT_IF(n.next_month <= ADD_MONTHS(n.month, {W}))              AS returned_within_{W}m,
    COUNT_IF(n.next_month <= ADD_MONTHS(n.month, {W})) / COUNT(*)   AS return_rate_{W}m,
    ADD_MONTHS(n.month, {W}) < MAX(b.last_month)                    AS window_complete
FROM nxt n CROSS JOIN bounds b
GROUP BY n.month
""")

# COMMAND ----------

for t in ["customer_activity", "segment_summary", "segment_kpi_monthly",
          f"cohort_retention_{W}m", f"activity_retention_{W}m"]:
    print(f"{GOLD}.{t:<26} {spark.table(f'{GOLD}.{t}').count():>12,} rows")
