# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Databricks SQL
# MAGIC
# MAGIC Registers Silver and Gold datasets as temporary views and runs the SQL in `sql/`.
# MAGIC On a Databricks SQL warehouse, save the Gold tables with `saveAsTable` and run the same statements there.

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

from src.analytics import register_analytics_views, run_sql_directory, sql_statements
from src.config import load_config
from src.spark_session import get_spark_session

spark = get_spark_session()
config = load_config()
register_analytics_views(spark, config)

# COMMAND ----------

sql_dir = REPO_ROOT / "sql"
for path in sorted(sql_dir.glob("*.sql")):
    print(f"\n===== {path.name} =====")
    for index, statement in enumerate(sql_statements(path.read_text(encoding="utf-8")), start=1):
        print(f"\n-- statement {index}")
        spark.sql(statement).show(10, truncate=False)

# COMMAND ----------

results = run_sql_directory(spark, sql_dir)
print(results)
