# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — raw copy of TPC-H
# MAGIC Data is stored **as is**: same columns, same types, same values, no filtering.
# MAGIC Only three technical columns are added: `_source_table`, `_ingested_at`, `_batch_id`.
# MAGIC
# MAGIC The source is a static snapshot, so every run is a full refresh (idempotent).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import uuid

from pyspark.sql import functions as F

batch_id = str(uuid.uuid4())

for table in SOURCE_TABLES:
    src = f"{SOURCE}.{table}"
    (
        spark.table(src)
        .withColumn("_source_table", F.lit(src))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{BRONZE}.{table}")
    )
    print(f"{src:<30} -> {BRONZE}.{table}")

# COMMAND ----------

# MAGIC %md ### Row-count reconciliation: source vs bronze

# COMMAND ----------

rows = [
    (t, spark.table(f"{SOURCE}.{t}").count(), spark.table(f"{BRONZE}.{t}").count())
    for t in SOURCE_TABLES
]
display(spark.createDataFrame(rows, "table string, source_rows long, bronze_rows long"))
assert all(s == b for _, s, b in rows), "Bronze row counts differ from source!"
