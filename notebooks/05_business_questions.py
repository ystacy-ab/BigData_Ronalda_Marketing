# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Marketing business questions
# MAGIC All answers are computed **from gold only**. Use the chart button under each `display()` to build
# MAGIC the visualisation (suggested chart type is written above each cell), or reuse the queries in
# MAGIC `sql/dashboard_queries.sql` for a Databricks AI/BI dashboard.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

W = RETENTION_WINDOW_MONTHS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. Activation rate for the BUILDING segment
# MAGIC *Activation rate = customers with ≥ 1 order / all customers in the segment.*
# MAGIC Chart: bar, `market_segment` × `activation_rate` (BUILDING highlighted).

# COMMAND ----------

display(spark.sql(f"""
SELECT market_segment, customers, activated_customers, ROUND(activation_rate * 100, 2) AS activation_rate_pct
FROM {GOLD}.segment_summary
ORDER BY market_segment = 'BUILDING' DESC, market_segment
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. New customers (by first order date) in 1996-Q1
# MAGIC Chart: line, `quarter` × `new_customers` for the whole history (shows why 1996 is so low).

# COMMAND ----------

display(spark.sql(f"""
SELECT market_segment, SUM(new_customers) AS new_customers_1996_q1
FROM {GOLD}.segment_kpi_monthly
WHERE quarter = '1996-Q1'
GROUP BY ROLLUP (market_segment)
ORDER BY market_segment NULLS LAST
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT quarter, SUM(new_customers) AS new_customers
FROM {GOLD}.segment_kpi_monthly
GROUP BY quarter ORDER BY quarter
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Segment with the highest repeat purchase rate
# MAGIC *Repeat rate = customers with > 1 order / all customers in the segment* (as in the brief).
# MAGIC We also show it among **active** customers, because 1/3 of customers never ordered and that
# MAGIC denominator would otherwise dominate the comparison.
# MAGIC Chart: bar, `market_segment` × `repeat_rate_pct`.

# COMMAND ----------

display(spark.sql(f"""
SELECT market_segment,
       repeat_customers,
       ROUND(repeat_rate * 100, 3)              AS repeat_rate_pct,
       ROUND(repeat_rate_among_active * 100, 3) AS repeat_rate_among_active_pct,
       ROUND(avg_orders_per_active_customer, 2) AS avg_orders_per_active_customer,
       RANK() OVER (ORDER BY repeat_rate DESC)  AS rank
FROM {GOLD}.segment_summary
ORDER BY rank
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4. 1996 cohorts: share who ordered again within 3 months
# MAGIC *Cohort = month of the customer's first order. Retained = second order ≤ first order date + 3 months.*
# MAGIC Chart: bar, `cohort_month` × `retention_rate_pct`.

# COMMAND ----------

q4 = spark.sql(f"""
SELECT cohort_month, cohort_size, reordered_customers,
       ROUND(retention_rate_{W}m * 100, 2) AS retention_rate_pct, window_complete
FROM {GOLD}.cohort_retention_{W}m
WHERE YEAR(cohort_month) = 1996
ORDER BY cohort_month
""")
display(q4)
if q4.count() == 0:
    print("No customer placed their FIRST order in 1996 -> every 1996 cohort is empty. See the cells below.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Context: all cohorts (why 1996 is empty / tiny)
# MAGIC Chart: combo — bars `cohort_size`, line `retention_rate_pct`.

# COMMAND ----------

display(spark.sql(f"""
SELECT cohort_month, cohort_size, ROUND(retention_rate_{W}m * 100, 2) AS retention_rate_pct, window_complete
FROM {GOLD}.cohort_retention_{W}m ORDER BY cohort_month
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4 (alternative reading): customers *active* in each 1996 month who ordered again within 3 months
# MAGIC Useful when first-order cohorts are empty: the business question "do our 1996 buyers come back?"
# MAGIC is still answerable.

# COMMAND ----------

display(spark.sql(f"""
SELECT activity_month, active_customers, returned_within_{W}m,
       ROUND(return_rate_{W}m * 100, 2) AS return_rate_pct, window_complete
FROM {GOLD}.activity_retention_{W}m
WHERE YEAR(activity_month) = 1996
ORDER BY activity_month
"""))
