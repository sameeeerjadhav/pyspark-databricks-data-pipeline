# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver transformations
# MAGIC
# MAGIC Silver standardizes types, names, statuses, and geography, then keeps one row per business key.
# MAGIC Rebuilds the Silver and Quarantine tables from Bronze.

# COMMAND ----------

import os
import sys
from pathlib import Path

try:
    REPO_ROOT = Path(__file__).resolve().parents[1]
except NameError:
    REPO_ROOT = Path(os.environ.get("PIPELINE_BASE_PATH", "/Workspace/Repos/sameeeerjadhav/pyspark-databricks-data-pipeline"))

os.environ.setdefault("PIPELINE_BASE_PATH", str(REPO_ROOT))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# COMMAND ----------

from src.config import load_config
from src.silver.build_silver import build_silver
from src.spark_session import get_spark_session
from src.storage import read_table, table_path

spark = get_spark_session()
config = load_config()
metrics = build_silver(spark, config)
for metric in metrics:
    print(metric["dataset"], metric["valid_records"], "/", metric["total_records"])

# COMMAND ----------

silver_orders = read_table(spark, table_path(config.silver_dir, "orders"))
silver_orders.groupBy("order_status").count().orderBy("count", ascending=False).show()
silver_orders.select("order_id", "customer_id", "order_date", "order_status", "shipping_country").show(5, truncate=False)
