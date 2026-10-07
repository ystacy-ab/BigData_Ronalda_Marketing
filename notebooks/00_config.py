# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Config
# MAGIC Included by every other notebook with `%run ./00_config`.
# MAGIC
# MAGIC Everything environment-specific is a **widget / job parameter**, so the same code runs in any workspace:
# MAGIC
# MAGIC | parameter | default | meaning |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `workspace` | Unity Catalog catalog we are allowed to write to |
# MAGIC | `env` | `dev` | environment prefix (`dev`, `preprod`, ...) |
# MAGIC | `source` | `samples.tpch` | where raw TPC-H lives |
# MAGIC
# MAGIC Resulting schemas: `<catalog>.<env>_tpch_mkt_{bronze|silver|gold|ops}`.

# COMMAND ----------

import os
import sys

_src = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if _src not in sys.path:
    sys.path.append(_src)

from tpch_marketing.config import *

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Target catalog")
dbutils.widgets.text("env", "dev", "Environment prefix")
dbutils.widgets.text("source", "samples.tpch", "Source schema")

NAMES = LayerNames(dbutils.widgets.get("catalog").strip(), dbutils.widgets.get("env").strip())
SOURCE = validate_source(dbutils.widgets.get("source").strip())
BRONZE, SILVER, GOLD, OPS = NAMES.bronze, NAMES.silver, NAMES.gold, NAMES.ops

for _schema in (BRONZE, SILVER, GOLD, OPS):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_schema}")

print(f"source={SOURCE}\nbronze={BRONZE}\nsilver={SILVER}\ngold={GOLD}\nops={OPS}")
