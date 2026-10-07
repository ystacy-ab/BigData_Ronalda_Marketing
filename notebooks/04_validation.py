# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Validation — can the Marketing numbers be trusted?
# MAGIC Rules use **all layers** (source → bronze → silver/quarantine → gold).
# MAGIC Every run is appended to `<ops>.validation_results`; the notebook **fails** if any rule fails,
# MAGIC so a scheduled job turns red and downstream tasks do not run.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md ## Profiling — how the allowed lists were derived
# MAGIC We profiled bronze (distinct values + counts) and took the observed closed set as the allowed list.
# MAGIC The TPC-H specification defines the same 5 segments, which confirms the list.

# COMMAND ----------

display(spark.sql(f"""
SELECT c_mktsegment AS market_segment, COUNT(*) AS customers,
       ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS share
FROM {BRONZE}.customer GROUP BY c_mktsegment ORDER BY c_mktsegment
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT 'discount' AS col, MIN(l_discount) AS min_v, MAX(l_discount) AS max_v FROM {BRONZE}.lineitem
UNION ALL
SELECT 'tax', MIN(l_tax), MAX(l_tax) FROM {BRONZE}.lineitem
"""))

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F

RUN_TS = datetime.now(timezone.utc)
results = []


def scalar(sql: str):
    return spark.sql(sql).first()[0]


def check(rule_id: str, layer: str, description: str, actual, expected, op: str = "=="):
    passed = {"==": actual == expected, ">": actual > expected, "<=": actual <= expected}[op]
    results.append((rule_id, layer, description, str(actual), f"{op} {expected}", bool(passed)))


SEG = sql_in_list(ALLOWED_MARKET_SEGMENTS)

# COMMAND ----------

# MAGIC %md ## L · Layer reconciliation (nothing lost, nothing invented)

# COMMAND ----------

for t in SOURCE_TABLES:
    src = spark.table(f"{SOURCE}.{t}").count()
    brz = spark.table(f"{BRONZE}.{t}").count()
    slv = spark.table(f"{SILVER}.{t}").count()
    qrt = spark.table(f"{OPS}.quarantine_{t}").count()
    check(f"L1_{t}", "bronze", f"{t}: bronze rows = source rows", brz, src)
    check(f"L2_{t}", "silver", f"{t}: silver + quarantine = bronze", slv + qrt, brz)

# COMMAND ----------

# MAGIC %md ## M1 · Every customer has a valid, non-null market segment

# COMMAND ----------

bronze_bad_seg = scalar(f"""
    SELECT COUNT(*) FROM {BRONZE}.customer
    WHERE c_mktsegment IS NULL OR TRIM(c_mktsegment) NOT IN ({SEG})""")
check("M1_silver", "silver", "customers with NULL / unknown segment in silver",
      scalar(f"SELECT COUNT(*) FROM {SILVER}.customer "
             f"WHERE market_segment IS NULL OR market_segment NOT IN ({SEG})"), 0)
check("M1_gold", "gold", "customers with NULL / unknown segment in gold",
      scalar(f"SELECT COUNT(*) FROM {GOLD}.customer_activity "
             f"WHERE market_segment IS NULL OR market_segment NOT IN ({SEG})"), 0)
check("M1_quarantine", "silver", "bad-segment rows in bronze are all in quarantine (not lost)",
      scalar(f"SELECT COUNT(*) FROM {OPS}.quarantine_customer "
             f"WHERE array_contains(_failed_rules, 'segment_valid')"), bronze_bad_seg)

# COMMAND ----------

# MAGIC %md ## M2 · Every order has a valid customer reference

# COMMAND ----------

bronze_orphans = scalar(f"""
    SELECT COUNT(*) FROM {BRONZE}.orders o
    LEFT ANTI JOIN {BRONZE}.customer c ON c.c_custkey = o.o_custkey""")
check("M2_silver", "silver", "orders whose customer does not exist in silver",
      scalar(f"SELECT COUNT(*) FROM {SILVER}.orders o "
             f"LEFT ANTI JOIN {SILVER}.customer c ON c.customer_key = o.customer_key"), 0)
check("M2_quarantine", "silver", "orphan orders found in bronze are quarantined with fk_customer",
      scalar(f"SELECT COUNT(*) FROM {OPS}.quarantine_orders "
             f"WHERE array_contains(_failed_rules, 'fk_customer')"), bronze_orphans)

# COMMAND ----------

# MAGIC %md ## M3 · Customers with no orders survive into silver and gold as zero-activity

# COMMAND ----------

silver_customers = spark.table(f"{SILVER}.customer").count()
silver_zero = scalar(f"""
    SELECT COUNT(*) FROM {SILVER}.customer c
    LEFT ANTI JOIN {SILVER}.orders o ON o.customer_key = c.customer_key""")
bronze_zero = scalar(f"""
    SELECT COUNT(*) FROM {BRONZE}.customer c
    LEFT ANTI JOIN {BRONZE}.orders o ON o.o_custkey = c.c_custkey""")

check("M3_exists", "bronze", "source really contains customers with zero orders", bronze_zero, 0, ">")
check("M3_silver", "silver", "zero-order customers in silver = in bronze", silver_zero, bronze_zero)
check("M3_gold_rows", "gold", "gold.customer_activity rows = silver customers",
      spark.table(f"{GOLD}.customer_activity").count(), silver_customers)
check("M3_gold_zero", "gold", "gold zero-activity customers = silver customers without orders",
      scalar(f"SELECT COUNT(*) FROM {GOLD}.customer_activity WHERE order_count = 0"), silver_zero)
check("M3_summary", "gold", "segment_summary.customers sums to silver customers",
      scalar(f"SELECT SUM(customers) FROM {GOLD}.segment_summary"), silver_customers)

# COMMAND ----------

# MAGIC %md ## G · Gold internal consistency

# COMMAND ----------

check("G1_unique", "gold", "customer_key is unique in customer_activity",
      scalar(f"SELECT COUNT(*) - COUNT(DISTINCT customer_key) FROM {GOLD}.customer_activity"), 0)
check("G2_orders", "gold", "sum(order_count) in gold = silver orders",
      scalar(f"SELECT SUM(order_count) FROM {GOLD}.customer_activity"),
      spark.table(f"{SILVER}.orders").count())
check("G3_new", "gold", "sum of monthly new customers = activated customers",
      scalar(f"SELECT SUM(new_customers) FROM {GOLD}.segment_kpi_monthly"),
      scalar(f"SELECT COUNT_IF(is_activated) FROM {GOLD}.customer_activity"))
check("G4_rate", "gold", "activation_rate within [0, 1]",
      scalar(f"SELECT COUNT(*) FROM {GOLD}.segment_summary WHERE activation_rate NOT BETWEEN 0 AND 1"), 0)

# COMMAND ----------

res = spark.createDataFrame(
    results, "rule_id string, layer string, description string, actual string, expected string, passed boolean"
)
(res.withColumn("run_ts", F.lit(RUN_TS))
    .write.mode("append").saveAsTable(f"{OPS}.validation_results"))
display(res.orderBy("passed", "rule_id"))

failed = [r for r in results if not r[-1]]
assert not failed, f"{len(failed)} validation rule(s) failed: {[r[0] for r in failed]}"
print(f"All {len(results)} validation rules passed ✅")
