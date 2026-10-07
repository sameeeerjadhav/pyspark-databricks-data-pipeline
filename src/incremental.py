"""Incremental order load using a watermark and an upsert.

The watermark is the latest order_date already published to Silver.
A later batch is filtered to newer order dates, validated with the same
rules as the full load, and merged by primary key.

Delta Lake uses MERGE. Parquet, used for local runs, rewrites the table
after keeping the newest version of each key. Both paths are idempotent
for a repeated key.
"""

from __future__ import annotations

import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, lit, max as spark_max, row_number, to_date

from src.config import PipelineConfig
from src.ingestion.ingest import _add_order_year, _read_source, _with_metadata
from src.logger import get_logger
from src.quality.validators import (
    count_duplicate_extras,
    count_null_rows,
    evaluate_order_items,
    evaluate_orders,
    flag_duplicate_extras,
    flag_orders_before_signup,
    flag_orphans,
    split_by_validation,
    to_quarantine,
)
from src.schemas import ORDER_COLUMNS, ORDER_ITEM_COLUMNS, ORDER_ITEM_SCHEMA, ORDER_SCHEMA
from src.storage import read_table, table_exists, table_path, write_table
from src.transformations.common import completeness
from src.transformations.order_items import to_silver_order_items
from src.transformations.orders import to_silver_orders

logger = get_logger("shopsphere.incremental")


def filter_newer_than(df: DataFrame, column: str, watermark: str | None) -> DataFrame:
    """Keep rows strictly newer than the previous watermark."""
    if not watermark:
        return df
    return df.filter(to_date(col(column), "yyyy-MM-dd") > to_date(lit(watermark), "yyyy-MM-dd"))


def read_watermark(spark: SparkSession, config: PipelineConfig) -> str | None:
    path = table_path(config.checkpoint_dir, "orders_watermark")
    if not table_exists(spark, path, config.storage_format):
        return None
    row = read_table(spark, path, config.storage_format).select(
        spark_max("last_processed_timestamp").alias("ts")
    ).first()
    if row is None or row["ts"] is None:
        return None
    return str(row["ts"])[:10]


def write_watermark(spark: SparkSession, config: PipelineConfig, value: str) -> None:
    frame = spark.createDataFrame([{"dataset": "orders", "last_processed_timestamp": value}])
    write_table(frame, table_path(config.checkpoint_dir, "orders_watermark"), storage_format=config.storage_format)
    logger.info("Updated orders watermark to %s", value)


def latest_order_date(orders: DataFrame) -> str | None:
    row = orders.select(spark_max("order_date").alias("ts")).first()
    if row is None or row["ts"] is None:
        return None
    value = row["ts"]
    return value.isoformat() if hasattr(value, "isoformat") else str(value)[:10]


def upsert_by_key(
    spark: SparkSession,
    path: str,
    incoming: DataFrame,
    key: str,
    partition_by: list[str] | None = None,
    storage_format: str = "parquet",
) -> None:
    """Insert new keys and replace existing keys with the incoming row."""
    if not table_exists(spark, path, storage_format):
        write_table(incoming, path, partition_by=partition_by, storage_format=storage_format)
        return
    if storage_format == "delta":
        from delta.tables import DeltaTable

        DeltaTable.forPath(spark, path).alias("target").merge(
            incoming.alias("source"),
            f"target.{key} = source.{key}",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info("Delta MERGE completed for %s", path)
        return

    existing = read_table(spark, path, storage_format)
    order_by = [col("ingestion_timestamp").desc_nulls_last()]
    if "order_date" in incoming.columns:
        order_by.append(col("order_date").desc_nulls_last())
    combined = existing.unionByName(incoming, allowMissingColumns=True)
    window = Window.partitionBy(key).orderBy(*order_by)
    deduped = combined.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
    write_table(deduped, path, partition_by=partition_by, storage_format=storage_format)
    logger.info("Parquet upsert completed for %s", path)


def _source_path(directory: str, filename: str) -> str:
    if directory.startswith(("abfss://", "dbfs:", "s3://")):
        return f"{directory.rstrip('/')}/{filename}"
    return str(Path(directory) / filename)


def _classify_orders(df: DataFrame, customers: DataFrame) -> tuple[DataFrame, DataFrame]:
    evaluated = evaluate_orders(df)
    unique_rows, invalid = _dedupe(evaluated, "order_id", "DUPLICATE_ORDER_ID", ORDER_COLUMNS, "order_date")
    checked = flag_orders_before_signup(
        flag_orphans(unique_rows, "customer_id", customers, "customer_id", "ORPHAN_CUSTOMER_ID"),
        customers,
    )
    survivors, rejected = split_by_validation(checked)
    return to_silver_orders(survivors), invalid.unionByName(rejected)


def _classify_items(df: DataFrame, orders: DataFrame, products: DataFrame) -> tuple[DataFrame, DataFrame]:
    evaluated = evaluate_order_items(df)
    unique_rows, invalid = _dedupe(
        evaluated, "order_item_id", "DUPLICATE_ORDER_ITEM_ID", ORDER_ITEM_COLUMNS, "quantity"
    )
    checked = flag_orphans(unique_rows, "order_id", orders, "order_id", "ORPHAN_ORDER_ID")
    checked = flag_orphans(checked, "product_id", products, "product_id", "ORPHAN_PRODUCT_ID")
    survivors, rejected = split_by_validation(checked)
    return to_silver_order_items(survivors), invalid.unionByName(rejected)


def _dedupe(df: DataFrame, key: str, error_code: str, columns: tuple[str, ...], tie_column: str) -> tuple[DataFrame, DataFrame]:
    candidates, field_invalid = split_by_validation(df)
    ranked = flag_duplicate_extras(
        candidates,
        key,
        error_code,
        [completeness(*columns).desc(), col(tie_column).desc_nulls_last()],
    )
    unique_rows, duplicates = split_by_validation(ranked)
    return unique_rows, field_invalid.unionByName(duplicates)


def _metric(dataset: str, total: int, valid: DataFrame, invalid: DataFrame, duplicates: int, nulls: int, started: float) -> dict[str, object]:
    valid_count = valid.count()
    invalid_count = invalid.count()
    if total != valid_count + invalid_count:
        raise RuntimeError(
            f"{dataset} reconciliation failed: input={total} valid={valid_count} invalid={invalid_count}"
        )
    return {
        "dataset": dataset,
        "total_records": total,
        "valid_records": valid_count,
        "invalid_records": invalid_count,
        "duplicate_records": duplicates,
        "null_records": nulls,
        "duration_seconds": round(time.perf_counter() - started, 3),
    }


def run_incremental(spark: SparkSession, config: PipelineConfig) -> list[dict[str, object]]:
    """Apply the late-arriving sample batch if it is present."""
    late_orders = _source_path(config.sample_dir, "late_orders.csv")
    late_items = _source_path(config.sample_dir, "late_order_items.csv")
    if config.local and not Path(late_orders).exists():
        logger.info("No incremental sample found; skipping incremental load")
        return []

    current_orders = read_table(spark, table_path(config.silver_dir, "orders"), config.storage_format)
    watermark = read_watermark(spark, config) or latest_order_date(current_orders)
    if watermark and read_watermark(spark, config) is None:
        write_watermark(spark, config, watermark)
    logger.info("Incremental orders watermark: %s", watermark)

    started = time.perf_counter()
    raw_orders = _add_order_year(
        _with_metadata(_read_source(spark, late_orders, ORDER_SCHEMA, "csv"), "shopsphere_oms_incremental")
    )
    new_orders = filter_newer_than(raw_orders, "order_date", watermark)
    incoming_orders = new_orders.count()
    if incoming_orders == 0:
        logger.info("Incremental batch has no orders newer than the watermark")
        return []

    customers = read_table(spark, table_path(config.silver_dir, "customers"), config.storage_format)
    products = read_table(spark, table_path(config.silver_dir, "products"), config.storage_format)
    evaluated_orders = evaluate_orders(new_orders)
    valid_orders, invalid_orders = _classify_orders(new_orders, customers)
    write_table(
        to_quarantine(invalid_orders, "incremental_orders"),
        table_path(config.quarantine_dir, "incremental_orders"),
        storage_format=config.storage_format,
    )
    upsert_by_key(
        spark,
        table_path(config.silver_dir, "orders"),
        valid_orders,
        "order_id",
        partition_by=["order_year"],
        storage_format=config.storage_format,
    )
    order_metric = _metric(
        "incremental_orders",
        incoming_orders,
        valid_orders,
        invalid_orders,
        count_duplicate_extras(evaluated_orders, "order_id"),
        count_null_rows(evaluated_orders, ["order_id", "customer_id", "order_date", "order_status"]),
        started,
    )
    logger.info("Incremental orders accepted: %s", order_metric["valid_records"])

    item_started = time.perf_counter()
    raw_items = _with_metadata(_read_source(spark, late_items, ORDER_ITEM_SCHEMA, "csv"), "shopsphere_oms_incremental")
    refreshed_orders = read_table(spark, table_path(config.silver_dir, "orders"), config.storage_format)
    scoped_items = raw_items
    valid_items, invalid_items = _classify_items(scoped_items, refreshed_orders, products)
    write_table(
        to_quarantine(invalid_items, "incremental_order_items"),
        table_path(config.quarantine_dir, "incremental_order_items"),
        storage_format=config.storage_format,
    )
    upsert_by_key(
        spark,
        table_path(config.silver_dir, "order_items"),
        valid_items,
        "order_item_id",
        storage_format=config.storage_format,
    )
    evaluated_items = evaluate_order_items(scoped_items)
    item_metric = _metric(
        "incremental_order_items",
        scoped_items.count(),
        valid_items,
        invalid_items,
        count_duplicate_extras(evaluated_items, "order_item_id"),
        count_null_rows(evaluated_items, ["order_item_id", "order_id", "product_id", "quantity", "unit_price"]),
        item_started,
    )

    new_watermark = latest_order_date(valid_orders)
    if new_watermark and (watermark is None or new_watermark > watermark):
        write_watermark(spark, config, new_watermark)
    logger.info("Incremental processing completed")
    return [order_metric, item_metric]

