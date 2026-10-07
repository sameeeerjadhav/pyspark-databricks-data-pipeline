"""ShopSphere medallion pipeline entry point.

Run from the repository root:

    python -m src.pipeline
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.analytics import register_analytics_views, run_sql_directory
from src.config import GOLD_DATASETS, PipelineConfig, load_config
from src.gold.build_gold import build_gold
from src.incremental import run_incremental
from src.ingestion.ingest import ingest_bronze
from src.logger import get_logger
from src.monitoring.pipeline_metrics import write_pipeline_metrics
from src.quality.quality_report import write_quality_report
from src.silver.build_silver import build_silver
from src.spark_session import get_spark_session
from src.storage import read_table, table_path

logger = get_logger("shopsphere.pipeline")


def validate_outputs(spark: SparkSession, config: PipelineConfig) -> None:
    """Fail the run when a curated table is missing or revenue is negative."""
    for name in GOLD_DATASETS:
        frame = read_table(spark, table_path(config.gold_dir, name), config.storage_format)
        row_count = frame.count()
        if row_count == 0:
            raise RuntimeError(f"Gold table {name} is empty")
        logger.info("Validated %s rows in %s", row_count, name)
    negative_sales = (
        read_table(spark, table_path(config.gold_dir, "daily_sales"), config.storage_format)
        .filter(col("net_sales") < 0)
        .count()
    )
    if negative_sales:
        raise RuntimeError("daily_sales contains negative net_sales")
    null_customers = (
        read_table(spark, table_path(config.silver_dir, "customers"), config.storage_format)
        .filter(col("customer_id").isNull())
        .count()
    )
    if null_customers:
        raise RuntimeError("silver_customers contains null customer_id values")
    logger.info("Final validation completed")


def run_pipeline(spark: SparkSession, config: PipelineConfig) -> None:
    started_at = datetime.now(timezone.utc)
    metrics: list[dict[str, object]] = []
    status = "FAILED"
    try:
        logger.info("Starting pipeline")
        ingest_bronze(spark, config)
        metrics.extend(build_silver(spark, config))
        build_gold(spark, config)
        incremental_metrics = run_incremental(spark, config)
        metrics.extend(incremental_metrics)
        if incremental_metrics:
            build_gold(spark, config)
        write_quality_report(spark, config, metrics)
        validate_outputs(spark, config)
        register_analytics_views(spark, config)
        run_sql_directory(spark, Path(__file__).resolve().parents[1] / "sql")
        status = "SUCCESS"
        logger.info("Pipeline completed successfully")
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        raise
    finally:
        try:
            write_pipeline_metrics(config, started_at, metrics, status)
        except Exception:
            logger.exception("Failed to write pipeline metrics")
            if status == "SUCCESS":
                raise


def main() -> None:
    spark = None
    try:
        config = load_config()
        spark = get_spark_session(config)
        run_pipeline(spark, config)
    except Exception as exc:
        raise SystemExit(1) from exc
    finally:
        if spark is not None and not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
            spark.stop()


if __name__ == "__main__":
    main()
