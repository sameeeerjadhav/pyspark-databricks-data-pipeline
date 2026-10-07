# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Spark optimization
# MAGIC
# MAGIC The sample is small. Spark may broadcast it automatically, so this notebook
# MAGIC turns automatic broadcast off and compares an explicit broadcast join with a
# MAGIC sort-merge join. The useful output is the physical plan, not a claim that
# MAGIC one side is faster on a laptop.
# MAGIC
# MAGIC Techniques shown:
# MAGIC column pruning, predicate pushdown, broadcast join, avoiding `collect()`,
# MAGIC and caching only when a DataFrame is reused.

# COMMAND ----------

import os
import sys
import time
from pathlib import Path

try:
    REPO_ROOT = Path(__file__).resolve().parents[1]
except NameError:
    REPO_ROOT = Path(os.environ.get("PIPELINE_BASE_PATH", "/Workspace/Repos/sameeeerjadhav/pyspark-databricks-data-pipeline"))

os.environ.setdefault("PIPELINE_BASE_PATH", str(REPO_ROOT))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# COMMAND ----------

from pyspark.sql.functions import broadcast, col

from src.config import load_config
from src.spark_session import get_spark_session
from src.storage import read_table, table_exists, table_path

spark = get_spark_session()
config = load_config()
orders_path = table_path(config.silver_dir, "orders")
customers_path = table_path(config.silver_dir, "customers")

if table_exists(spark, orders_path, config.storage_format) and table_exists(spark, customers_path, config.storage_format):
    orders = read_table(spark, orders_path, config.storage_format)
    customers = read_table(spark, customers_path, config.storage_format)
else:
    orders = spark.range(0, 20000).select(
        col("id").cast("string").alias("order_id"),
        (col("id") % 500).cast("string").alias("customer_id"),
    )
    customers = spark.range(0, 500).select(
        col("id").cast("string").alias("customer_id"),
        col("id").alias("signup_year"),
    )

# Column pruning: the join does not need every source column.
orders_slim = orders.select("order_id", "customer_id")
customers_slim = customers.select("customer_id")

# Predicate pushdown: filter before the join so less data is shuffled.
filtered_orders = orders_slim.filter(col("customer_id").isNotNull())

previous_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

shuffle_join = filtered_orders.join(customers_slim, "customer_id", "inner")
broadcast_join = filtered_orders.join(broadcast(customers_slim), "customer_id", "inner")

print("Sort-merge style plan with automatic broadcast disabled:")
shuffle_join.explain()
print("Explicit broadcast plan:")
broadcast_join.explain()


def _time_action(frame, label: str) -> None:
    start = time.perf_counter()
    row_count = frame.count()
    elapsed = time.perf_counter() - start
    print(f"{label}: {row_count} rows in {elapsed:.3f}s across {frame.rdd.getNumPartitions()} partitions")


_time_action(shuffle_join, "join without broadcast hint")
_time_action(broadcast_join, "broadcast join")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", previous_threshold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to read the result
# MAGIC
# MAGIC - `BroadcastHashJoin` means the customer side was copied to every executor.
# MAGIC - `SortMergeJoin` means both sides were shuffled and sorted.
# MAGIC - On this sample the broadcast plan can be slower, because building the broadcast is extra work for a tiny table.
# MAGIC - Broadcast is appropriate when one side is small enough to fit in executor memory and the other side is large.
# MAGIC - `repartition(n)` reshuffles. `coalesce(n)` reduces partitions without a full shuffle and is the better choice before writing a small result.
# MAGIC - Cache a DataFrame only when several actions reuse it. The Gold build caches the sales fact for that reason, then unpersists it.

# COMMAND ----------

filtered_orders.groupBy("customer_id").count().coalesce(1).explain()
