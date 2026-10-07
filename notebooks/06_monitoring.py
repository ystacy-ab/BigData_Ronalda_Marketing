# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Monitoring & alerting
# MAGIC Two notions of "over time":
# MAGIC * **business time** — how KPIs evolve month by month in the data (`gold.segment_kpi_monthly`);
# MAGIC * **run time** — each pipeline run appends a KPI snapshot to `<ops>.kpi_snapshots`, so if a new data
# MAGIC   load breaks something (e.g. orders lost), the next run sees the drop and alerts.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F

RUN_TS = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md ## 1. Business-time metrics
# MAGIC Chart: line, x=`month`, y=`activation_rate_to_date`, series=`market_segment`.

# COMMAND ----------

display(spark.sql(f"""
SELECT month, market_segment, ROUND(activation_rate_to_date * 100, 2) AS activation_rate_to_date_pct
FROM {GOLD}.segment_kpi_monthly ORDER BY month, market_segment
"""))

# COMMAND ----------

# MAGIC %md Chart: stacked bar, x=`quarter`, y=`new_customers`, series=`market_segment`.

# COMMAND ----------

display(spark.sql(f"""
SELECT quarter, market_segment, SUM(new_customers) AS new_customers
FROM {GOLD}.segment_kpi_monthly GROUP BY quarter, market_segment ORDER BY quarter
"""))

# COMMAND ----------

# MAGIC %md ## 2. Snapshot of today's KPIs (run time)

# COMMAND ----------

snapshot = spark.sql(f"""
SELECT market_segment, customers, activated_customers, activation_rate, repeat_rate
FROM {GOLD}.segment_summary
""").withColumn("run_ts", F.lit(RUN_TS))
snapshot.write.mode("append").saveAsTable(f"{OPS}.kpi_snapshots")
display(spark.table(f"{OPS}.kpi_snapshots").orderBy(F.desc("run_ts"), "market_segment"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Alert rules
# MAGIC | id | rule | why this threshold |
# MAGIC |---|---|---|
# MAGIC | A1 | activation rate of any segment drops by **> 2 pp** vs previous run | activation is cumulative and should never fall; > 2 pp means lost orders/customers, not noise |
# MAGIC | A2 | new customers in a quarter drop by **> 30 %** vs previous quarter, *if* previous quarter ≥ 100 | volume guard avoids alerting on tiny numbers (new customers naturally fade to ~0 in late TPC-H years) |
# MAGIC
# MAGIC The same SQL lives in `sql/alerts/` to be scheduled as **Databricks SQL Alerts** (trigger when `alert = true`).

# COMMAND ----------


def activation_alert(previous_df, current_df, threshold_pp=ALERT_ACTIVATION_DROP_PP):
    """Compare two (market_segment, activation_rate) frames; flag drops larger than threshold_pp."""
    return (
        previous_df.select("market_segment", F.col("activation_rate").alias("prev_rate"))
        .join(current_df.select("market_segment", F.col("activation_rate").alias("curr_rate")),
              "market_segment")
        .withColumn("drop_pp", F.round((F.col("prev_rate") - F.col("curr_rate")) * 100, 2))
        .withColumn("alert", F.col("drop_pp") > threshold_pp)
    )


# A1 on real snapshots: last run vs the one before
last_two = spark.sql(f"""
SELECT * FROM {OPS}.kpi_snapshots
WHERE run_ts IN (SELECT DISTINCT run_ts FROM {OPS}.kpi_snapshots ORDER BY run_ts DESC LIMIT 2)
""")
runs = [r.run_ts for r in last_two.select("run_ts").distinct().orderBy("run_ts").collect()]
if len(runs) == 2:
    display(activation_alert(last_two.filter(F.col("run_ts") == runs[0]),
                             last_two.filter(F.col("run_ts") == runs[1])))
else:
    print("Only one snapshot so far — A1 needs two runs. See the demo below.")

# COMMAND ----------

# MAGIC %md ### Demo: A1 fires on artificially degraded data
# MAGIC We pretend 30 % of activated BUILDING customers lost their orders (deterministic hash sample).

# COMMAND ----------

degraded = spark.sql(f"""
SELECT market_segment,
       COUNT_IF(is_activated AND NOT (market_segment = 'BUILDING' AND PMOD(HASH(customer_key), 10) < 3))
         / COUNT(*) AS activation_rate
FROM {GOLD}.customer_activity
GROUP BY market_segment
""")
display(activation_alert(spark.table(f"{GOLD}.segment_summary"), degraded).orderBy(F.desc("drop_pp")))

# COMMAND ----------

# MAGIC %md ### A2: quarter-over-quarter drop in new customers

# COMMAND ----------

display(spark.sql(f"""
WITH q AS (
    SELECT quarter, SUM(new_customers) AS new_customers
    FROM {GOLD}.segment_kpi_monthly GROUP BY quarter
),
w AS (SELECT quarter, new_customers, LAG(new_customers) OVER (ORDER BY quarter) AS prev FROM q)
SELECT quarter, prev AS prev_quarter_new, new_customers,
       ROUND(1 - new_customers / prev, 3) AS drop_share,
       prev >= {ALERT_NEW_CUSTOMERS_MIN_BASE}
         AND new_customers < prev * (1 - {ALERT_NEW_CUSTOMERS_DROP}) AS alert
FROM w ORDER BY quarter
"""))
