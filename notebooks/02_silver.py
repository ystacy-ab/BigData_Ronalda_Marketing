# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — 3NF model with enforced data quality
# MAGIC
# MAGIC **Model.** TPC-H is already in 3NF (every non-key attribute depends on the whole key and only on it;
# MAGIC nation/region are separate entities, partsupp is the associative entity between part and supplier).
# MAGIC In silver we keep that structure, give columns readable names, cast to explicit types and declare keys.
# MAGIC
# MAGIC **How data quality is enforced (3 levels):**
# MAGIC 1. **Row-level rules** (below, per table) — a row that breaks any rule goes to
# MAGIC    `<ops>.quarantine_<table>` together with the list of rules it failed. Nothing is dropped silently.
# MAGIC 2. **Delta constraints** — `NOT NULL` and `CHECK` constraints are enforced by Delta on every write,
# MAGIC    so even a bug in step 1 cannot put a bad row into silver.
# MAGIC 3. **Keys** — `PRIMARY KEY` / `FOREIGN KEY` are declared in Unity Catalog (informational: they build the
# MAGIC    ER diagram but are not enforced by Delta), so uniqueness and referential integrity are enforced by
# MAGIC    rules `pk_unique` and `fk_*` in step 1 and re-checked in `04_validation`.
# MAGIC
# MAGIC Duplicate keys: *all* copies are quarantined — we do not guess which copy is correct.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

SEG = sql_in_list(ALLOWED_MARKET_SEGMENTS)
OST = sql_in_list(ALLOWED_ORDER_STATUS)
OPR = sql_in_list(ALLOWED_ORDER_PRIORITY)
SHM = sql_in_list(ALLOWED_SHIP_MODES)
RFL = sql_in_list(ALLOWED_RETURN_FLAGS)
LST = sql_in_list(ALLOWED_LINE_STATUS)
D_LO, D_HI = DISCOUNT_RANGE
T_LO, T_HI = TAX_RANGE

# COMMAND ----------

# MAGIC %md ## Table specifications
# MAGIC `select` reads bronze; helper columns starting with `_` (FK lookups) are used by rules and then dropped.

# COMMAND ----------

SPECS = [
    dict(
        name="region",
        pk=["region_key"],
        ddl="""region_key INT NOT NULL, region_name STRING NOT NULL, region_comment STRING,
               CONSTRAINT region_pk PRIMARY KEY (region_key)""",
        select=f"""
            SELECT CAST(r_regionkey AS INT) AS region_key, TRIM(r_name) AS region_name,
                   r_comment AS region_comment
            FROM {BRONZE}.region""",
        rules={"region_name_not_empty": "region_name <> ''"},
        checks={},
    ),
    dict(
        name="nation",
        pk=["nation_key"],
        ddl=f"""nation_key INT NOT NULL, nation_name STRING NOT NULL, region_key INT NOT NULL,
                nation_comment STRING,
                CONSTRAINT nation_pk PRIMARY KEY (nation_key),
                CONSTRAINT nation_region_fk FOREIGN KEY (region_key) REFERENCES {SILVER}.region""",
        select=f"""
            SELECT CAST(n.n_nationkey AS INT) AS nation_key, TRIM(n.n_name) AS nation_name,
                   CAST(n.n_regionkey AS INT) AS region_key, n.n_comment AS nation_comment,
                   r.region_key IS NOT NULL AS _fk_region
            FROM {BRONZE}.nation n
            LEFT JOIN {SILVER}.region r ON r.region_key = CAST(n.n_regionkey AS INT)""",
        rules={"nation_name_not_empty": "nation_name <> ''", "fk_region": "_fk_region"},
        checks={},
    ),
    dict(
        name="customer",
        pk=["customer_key"],
        ddl=f"""customer_key BIGINT NOT NULL, customer_name STRING NOT NULL, address STRING,
                nation_key INT NOT NULL, phone STRING, account_balance DECIMAL(18,2),
                market_segment STRING NOT NULL, customer_comment STRING,
                CONSTRAINT customer_pk PRIMARY KEY (customer_key),
                CONSTRAINT customer_nation_fk FOREIGN KEY (nation_key) REFERENCES {SILVER}.nation""",
        select=f"""
            SELECT CAST(c.c_custkey AS BIGINT) AS customer_key, c.c_name AS customer_name,
                   c.c_address AS address, CAST(c.c_nationkey AS INT) AS nation_key, c.c_phone AS phone,
                   CAST(c.c_acctbal AS DECIMAL(18,2)) AS account_balance,
                   TRIM(c.c_mktsegment) AS market_segment, c.c_comment AS customer_comment,
                   n.nation_key IS NOT NULL AS _fk_nation
            FROM {BRONZE}.customer c
            LEFT JOIN {SILVER}.nation n ON n.nation_key = CAST(c.c_nationkey AS INT)""",
        rules={
            "customer_name_not_null": "customer_name IS NOT NULL",
            "segment_valid": f"market_segment IN ({SEG})",
            "fk_nation": "_fk_nation",
        },
        checks={"customer_segment_valid": f"market_segment IN ({SEG})"},
    ),
    dict(
        name="supplier",
        pk=["supplier_key"],
        ddl=f"""supplier_key BIGINT NOT NULL, supplier_name STRING NOT NULL, address STRING,
                nation_key INT NOT NULL, phone STRING, account_balance DECIMAL(18,2), supplier_comment STRING,
                CONSTRAINT supplier_pk PRIMARY KEY (supplier_key),
                CONSTRAINT supplier_nation_fk FOREIGN KEY (nation_key) REFERENCES {SILVER}.nation""",
        select=f"""
            SELECT CAST(s.s_suppkey AS BIGINT) AS supplier_key, s.s_name AS supplier_name,
                   s.s_address AS address, CAST(s.s_nationkey AS INT) AS nation_key, s.s_phone AS phone,
                   CAST(s.s_acctbal AS DECIMAL(18,2)) AS account_balance, s.s_comment AS supplier_comment,
                   n.nation_key IS NOT NULL AS _fk_nation
            FROM {BRONZE}.supplier s
            LEFT JOIN {SILVER}.nation n ON n.nation_key = CAST(s.s_nationkey AS INT)""",
        rules={"supplier_name_not_null": "supplier_name IS NOT NULL", "fk_nation": "_fk_nation"},
        checks={},
    ),
    dict(
        name="part",
        pk=["part_key"],
        ddl="""part_key BIGINT NOT NULL, part_name STRING, manufacturer STRING NOT NULL, brand STRING NOT NULL,
               part_type STRING, size INT, container STRING, retail_price DECIMAL(18,2) NOT NULL,
               part_comment STRING,
               CONSTRAINT part_pk PRIMARY KEY (part_key)""",
        select=f"""
            SELECT CAST(p_partkey AS BIGINT) AS part_key, p_name AS part_name,
                   TRIM(p_mfgr) AS manufacturer, TRIM(p_brand) AS brand, p_type AS part_type,
                   CAST(p_size AS INT) AS size, p_container AS container,
                   CAST(p_retailprice AS DECIMAL(18,2)) AS retail_price, p_comment AS part_comment
            FROM {BRONZE}.part""",
        rules={
            "brand_not_empty": "brand <> ''",
            "manufacturer_not_empty": "manufacturer <> ''",
            "retail_price_positive": "retail_price > 0",
        },
        checks={"part_retail_price_positive": "retail_price > 0"},
    ),
    dict(
        name="partsupp",
        pk=["part_key", "supplier_key"],
        ddl=f"""part_key BIGINT NOT NULL, supplier_key BIGINT NOT NULL, available_qty INT,
                supply_cost DECIMAL(18,2) NOT NULL, partsupp_comment STRING,
                CONSTRAINT partsupp_pk PRIMARY KEY (part_key, supplier_key),
                CONSTRAINT partsupp_part_fk FOREIGN KEY (part_key) REFERENCES {SILVER}.part,
                CONSTRAINT partsupp_supplier_fk FOREIGN KEY (supplier_key) REFERENCES {SILVER}.supplier""",
        select=f"""
            SELECT CAST(ps.ps_partkey AS BIGINT) AS part_key, CAST(ps.ps_suppkey AS BIGINT) AS supplier_key,
                   CAST(ps.ps_availqty AS INT) AS available_qty,
                   CAST(ps.ps_supplycost AS DECIMAL(18,2)) AS supply_cost, ps.ps_comment AS partsupp_comment,
                   p.part_key IS NOT NULL AS _fk_part, s.supplier_key IS NOT NULL AS _fk_supplier
            FROM {BRONZE}.partsupp ps
            LEFT JOIN {SILVER}.part p ON p.part_key = CAST(ps.ps_partkey AS BIGINT)
            LEFT JOIN {SILVER}.supplier s ON s.supplier_key = CAST(ps.ps_suppkey AS BIGINT)""",
        rules={
            "supply_cost_positive": "supply_cost > 0",
            "available_qty_non_negative": "available_qty >= 0",
            "fk_part": "_fk_part",
            "fk_supplier": "_fk_supplier",
        },
        checks={"partsupp_supply_cost_positive": "supply_cost > 0"},
    ),
    dict(
        name="orders",
        pk=["order_key"],
        ddl=f"""order_key BIGINT NOT NULL, customer_key BIGINT NOT NULL, order_status STRING,
                total_price DECIMAL(18,2) NOT NULL, order_date DATE NOT NULL, order_priority STRING,
                clerk STRING, ship_priority INT, order_comment STRING,
                CONSTRAINT orders_pk PRIMARY KEY (order_key),
                CONSTRAINT orders_customer_fk FOREIGN KEY (customer_key) REFERENCES {SILVER}.customer""",
        select=f"""
            SELECT CAST(o.o_orderkey AS BIGINT) AS order_key, CAST(o.o_custkey AS BIGINT) AS customer_key,
                   o.o_orderstatus AS order_status, CAST(o.o_totalprice AS DECIMAL(18,2)) AS total_price,
                   CAST(o.o_orderdate AS DATE) AS order_date, o.o_orderpriority AS order_priority,
                   o.o_clerk AS clerk, CAST(o.o_shippriority AS INT) AS ship_priority,
                   o.o_comment AS order_comment,
                   c.customer_key IS NOT NULL AS _fk_customer
            FROM {BRONZE}.orders o
            LEFT JOIN {SILVER}.customer c ON c.customer_key = CAST(o.o_custkey AS BIGINT)""",
        rules={
            "fk_customer": "_fk_customer",
            "order_date_not_null": "order_date IS NOT NULL",
            "total_price_positive": "total_price > 0",
            "order_status_valid": f"order_status IN ({OST})",
            "order_priority_valid": f"order_priority IN ({OPR})",
        },
        checks={
            "orders_total_price_positive": "total_price > 0",
            "orders_status_valid": f"order_status IN ({OST})",
        },
    ),
    dict(
        name="lineitem",
        pk=["order_key", "line_number"],
        ddl=f"""order_key BIGINT NOT NULL, line_number INT NOT NULL, part_key BIGINT NOT NULL,
                supplier_key BIGINT NOT NULL, quantity DECIMAL(18,2) NOT NULL,
                extended_price DECIMAL(18,2) NOT NULL, discount DECIMAL(18,2) NOT NULL,
                tax DECIMAL(18,2) NOT NULL, return_flag STRING, line_status STRING,
                ship_date DATE, commit_date DATE, receipt_date DATE, ship_instruct STRING,
                ship_mode STRING, line_comment STRING,
                CONSTRAINT lineitem_pk PRIMARY KEY (order_key, line_number),
                CONSTRAINT lineitem_order_fk FOREIGN KEY (order_key) REFERENCES {SILVER}.orders,
                CONSTRAINT lineitem_partsupp_fk FOREIGN KEY (part_key, supplier_key)
                    REFERENCES {SILVER}.partsupp""",
        select=f"""
            SELECT CAST(l.l_orderkey AS BIGINT) AS order_key, CAST(l.l_linenumber AS INT) AS line_number,
                   CAST(l.l_partkey AS BIGINT) AS part_key, CAST(l.l_suppkey AS BIGINT) AS supplier_key,
                   CAST(l.l_quantity AS DECIMAL(18,2)) AS quantity,
                   CAST(l.l_extendedprice AS DECIMAL(18,2)) AS extended_price,
                   CAST(l.l_discount AS DECIMAL(18,2)) AS discount, CAST(l.l_tax AS DECIMAL(18,2)) AS tax,
                   l.l_returnflag AS return_flag, l.l_linestatus AS line_status,
                   CAST(l.l_shipdate AS DATE) AS ship_date, CAST(l.l_commitdate AS DATE) AS commit_date,
                   CAST(l.l_receiptdate AS DATE) AS receipt_date, l.l_shipinstruct AS ship_instruct,
                   l.l_shipmode AS ship_mode, l.l_comment AS line_comment,
                   o.order_key IS NOT NULL AS _fk_order, o.order_date AS _order_date,
                   ps.part_key IS NOT NULL AS _fk_partsupp
            FROM {BRONZE}.lineitem l
            LEFT JOIN {SILVER}.orders o ON o.order_key = CAST(l.l_orderkey AS BIGINT)
            LEFT JOIN {SILVER}.partsupp ps
                   ON ps.part_key = CAST(l.l_partkey AS BIGINT) AND ps.supplier_key = CAST(l.l_suppkey AS BIGINT)""",
        rules={
            "fk_order": "_fk_order",
            "fk_partsupp": "_fk_partsupp",  # two-column FK: (part_key, supplier_key) must exist in partsupp
            "quantity_positive": "quantity > 0",
            "extended_price_positive": "extended_price > 0",
            "discount_in_range": f"discount BETWEEN {D_LO} AND {D_HI}",
            "tax_in_range": f"tax BETWEEN {T_LO} AND {T_HI}",
            "ship_mode_valid": f"ship_mode IN ({SHM})",
            "return_flag_valid": f"return_flag IN ({RFL})",
            "line_status_valid": f"line_status IN ({LST})",
            "ship_not_before_order": "ship_date >= _order_date",
            "receipt_not_before_ship": "receipt_date >= ship_date",
        },
        checks={
            "lineitem_quantity_positive": "quantity > 0",
            "lineitem_discount_in_range": f"discount BETWEEN {D_LO} AND {D_HI}",
            "lineitem_receipt_after_ship": "receipt_date >= ship_date",
        },
    ),
]

# COMMAND ----------

# MAGIC %md ## Rebuild silver
# MAGIC Tables are dropped children-first (FKs), then created parents-first.

# COMMAND ----------

for spec in reversed(SPECS):
    spark.sql(f"DROP TABLE IF EXISTS {SILVER}.{spec['name']}")


def load_silver(spec: dict) -> tuple[int, int]:
    name, pk = spec["name"], spec["pk"]
    target = f"{SILVER}.{name}"

    rules = {f"{c}_not_null": f"{c} IS NOT NULL" for c in pk}
    rules.update(spec["rules"])
    rules["pk_unique"] = "_pk_count = 1"

    df = spark.sql(spec["select"]).withColumn("_pk_count", F.count(F.lit(1)).over(Window.partitionBy(*pk)))
    # A rule passes only if it evaluates to TRUE; NULL counts as a failure.
    failed = [F.when(~F.coalesce(F.expr(expr), F.lit(False)), F.lit(rule)) for rule, expr in rules.items()]
    df = df.withColumn("_failed_rules", F.filter(F.array(*failed), lambda x: x.isNotNull()))

    tech_cols = [c for c in df.columns if c.startswith("_")]
    valid = df.filter(F.size("_failed_rules") == 0).drop(*tech_cols)
    invalid = df.filter(F.size("_failed_rules") > 0).withColumn("_quarantined_at", F.current_timestamp())

    spark.sql(f"CREATE TABLE {target} ({spec['ddl']})")
    for cname, expr in spec["checks"].items():
        spark.sql(f"ALTER TABLE {target} ADD CONSTRAINT {cname} CHECK ({expr})")

    valid.write.mode("append").saveAsTable(target)
    (invalid.write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{OPS}.quarantine_{name}"))

    return spark.table(target).count(), spark.table(f"{OPS}.quarantine_{name}").count()


summary = []
for spec in SPECS:
    n_ok, n_bad = load_silver(spec)
    summary.append((spec["name"], n_ok, n_bad))
    print(f"{spec['name']:<10} silver={n_ok:>12,}  quarantined={n_bad:>8,}")

display(spark.createDataFrame(summary, "table string, silver_rows long, quarantined_rows long"))
