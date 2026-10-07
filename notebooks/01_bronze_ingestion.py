# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze ingestion
# MAGIC
# MAGIC Land ShopSphere CSV and JSON files in Bronze.
# MAGIC Local runs write Parquet. On Databricks, set `PIPELINE_STORAGE_FORMAT=delta`
# MAGIC and point `PIPELINE_BASE_PATH` at the ADLS container root.
# MAGIC
# MAGIC This notebook calls the same ingestion code as `python -m src.pipeline`.

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
from src.ingestion.ingest import ingest_bronze
from src.spark_session import get_spark_session

spark = get_spark_session()
config = load_config()
counts = ingest_bronze(spark, config)
print(counts)

# COMMAND ----------

from src.storage import read_table, table_path

orders = read_table(spark, table_path(config.bronze_dir, "orders"))
orders.printSchema()
orders.select("order_id", "order_status", "order_date", "order_year", "source_file", "ingestion_timestamp").show(5, truncate=False)
