"""Register curated tables and run the SQL analytics scripts."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from src.config import GOLD_DATASETS, SILVER_DATASETS, PipelineConfig
from src.logger import get_logger
from src.storage import read_table, table_path

logger = get_logger("shopsphere.analytics")


def sql_statements(script: str) -> list[str]:
    kept_lines = []
    for line in script.splitlines():
        if line.strip().startswith("--"):
            continue
        kept_lines.append(line)
    return [part.strip() for part in "\n".join(kept_lines).split(";") if part.strip()]


def register_analytics_views(spark: SparkSession, config: PipelineConfig) -> None:
    for dataset in SILVER_DATASETS:
        read_table(spark, table_path(config.silver_dir, dataset), config.storage_format).createOrReplaceTempView(
            f"silver_{dataset}"
        )
    for dataset in GOLD_DATASETS:
        read_table(spark, table_path(config.gold_dir, dataset), config.storage_format).createOrReplaceTempView(dataset)
    if config.local and Path(config.quality_report_csv).exists():
        spark.read.option("header", True).csv(config.quality_report_csv).createOrReplaceTempView("data_quality_report")
    logger.info("Analytics views registered")


def run_sql_directory(spark: SparkSession, sql_dir: Path) -> dict[str, int]:
    results: dict[str, int] = {}
    if not sql_dir.exists():
        raise FileNotFoundError(f"SQL directory not found: {sql_dir}")
    for path in sorted(sql_dir.glob("*.sql")):
        statements = sql_statements(path.read_text(encoding="utf-8"))
        for index, statement in enumerate(statements, start=1):
            row_count = spark.sql(statement).count()
            key = f"{path.name}#{index}"
            results[key] = row_count
            logger.info("SQL %s returned %s rows", key, row_count)
    return results
