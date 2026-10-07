# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Gold transformations
# MAGIC
# MAGIC Gold keeps analytics tables for recognized demand:
# MAGIC `CONFIRMED`, `SHIPPED`, and `DELIVERED`.
# MAGIC `PENDING` and `CANCELLED` remain available in Silver for operational reporting.

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
from src.gold.build_gold import build_gold
from src.spark_session import get_spark_session
from src.storage import read_table, table_path

spark = get_spark_session()
config = load_config()
build_gold(spark, config)

# COMMAND ----------

daily = read_table(spark, table_path(config.gold_dir, "daily_sales"))
daily.orderBy("date").show(10, truncate=False)
read_table(spark, table_path(config.gold_dir, "category_sales_summary")).show(truncate=False)
