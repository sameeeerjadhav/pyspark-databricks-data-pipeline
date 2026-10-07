# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Data quality
# MAGIC
# MAGIC Field rules, duplicate detection, and referential checks run before Silver is published.
# MAGIC Invalid rows are written to Quarantine with every failed rule, not deleted.

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
from src.quality.validators import evaluate_customers, evaluate_orders
from src.spark_session import get_spark_session
from src.storage import read_table, table_path

spark = get_spark_session()
config = load_config()
customers = evaluate_customers(read_table(spark, table_path(config.bronze_dir, "customers")))
orders = evaluate_orders(read_table(spark, table_path(config.bronze_dir, "orders")))

# COMMAND ----------

from pyspark.sql.functions import explode, size

print("Customers failing at least one field rule:", customers.filter(size("validation_errors") > 0).count())
customers.filter(size("validation_errors") > 0).select(explode("validation_errors").alias("validation_error")).groupBy("validation_error").count().orderBy("count", ascending=False).show(truncate=False)
orders.filter(size("validation_errors") > 0).select(explode("validation_errors").alias("validation_error")).groupBy("validation_error").count().orderBy("count", ascending=False).show(truncate=False)

# COMMAND ----------

from src.storage import table_exists

quarantine_path = table_path(config.quarantine_dir, "customers")
if table_exists(spark, quarantine_path, config.storage_format):
    read_table(spark, quarantine_path).select(
        "source_dataset", "validation_errors", "validation_timestamp", "source_file"
    ).show(10, truncate=False)
else:
    print("Quarantine is written when the Silver build runs.")
