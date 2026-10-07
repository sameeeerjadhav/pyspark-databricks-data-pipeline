"""Validate Bronze data, quarantine failures, and publish Silver tables."""

from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from src.config import PipelineConfig
from src.logger import get_logger
from src.quality.validators import (
    count_duplicate_extras,
    count_null_rows,
    evaluate_customers,
    evaluate_order_items,
    evaluate_orders,
    evaluate_payments,
    evaluate_products,
    flag_duplicate_extras,
    flag_orders_before_signup,
    flag_orphans,
    split_by_validation,
    to_quarantine,
)
from src.schemas import (
    CUSTOMER_COLUMNS,
    ORDER_COLUMNS,
    ORDER_ITEM_COLUMNS,
    PAYMENT_COLUMNS,
    PRODUCT_COLUMNS,
)
from src.storage import read_table, table_path, write_table
from src.transformations.common import completeness
from src.transformations.customers import to_silver_customers
from src.transformations.order_items import to_silver_order_items
from src.transformations.orders import to_silver_orders
from src.transformations.payments import to_silver_payments
from src.transformations.products import to_silver_products

logger = get_logger("shopsphere.silver")


def _publish(
    dataset: str,
    valid: DataFrame,
    invalid: DataFrame,
    config: PipelineConfig,
    partition_by: list[str] | None = None,
) -> tuple[DataFrame, DataFrame]:
    # Cached because each frame is counted and then written.
    valid = valid.cache()
    invalid = invalid.cache()
    valid_count = valid.count()
    invalid_count = invalid.count()
    write_table(valid, table_path(config.silver_dir, dataset), partition_by=partition_by)
    write_table(to_quarantine(invalid, dataset), table_path(config.quarantine_dir, dataset))
    logger.info("Silver %s written", dataset.replace("_", " "))
    logger.info("%s valid records: %s", dataset, valid_count)
    logger.info("%s invalid records: %s", dataset, invalid_count)
    valid.unpersist()
    invalid.unpersist()
    return valid, invalid


def _metric(
    dataset: str,
    total: int,
    valid: int,
    invalid: int,
    duplicates: int,
    nulls: int,
    started: float,
) -> dict[str, object]:
    if total != valid + invalid:
        raise RuntimeError(
            f"{dataset} reconciliation failed: bronze={total} silver={valid} quarantine={invalid}"
        )
    return {
        "dataset": dataset,
        "total_records": total,
        "valid_records": valid,
        "invalid_records": invalid,
        "duplicate_records": duplicates,
        "null_records": nulls,
        "duration_seconds": round(time.perf_counter() - started, 3),
    }


def _dedupe(df: DataFrame, key: str, error_code: str, rank_columns: list) -> tuple[DataFrame, DataFrame]:
    candidates, field_invalid = split_by_validation(df)
    ranked = flag_duplicate_extras(candidates, key, error_code, rank_columns)
    unique_rows, duplicates = split_by_validation(ranked)
    invalid = field_invalid.unionByName(duplicates)
    return unique_rows, invalid


def build_customers(df: DataFrame, config: PipelineConfig) -> tuple[DataFrame, dict[str, object]]:
    started = time.perf_counter()
    total = df.count()
    evaluated = evaluate_customers(df)
    duplicates = count_duplicate_extras(evaluated, "customer_id")
    nulls = count_null_rows(evaluated, ["customer_id", "email", "city", "signup_date"])
    unique_rows, invalid = _dedupe(
        evaluated,
        "customer_id",
        "DUPLICATE_CUSTOMER_ID",
        [completeness(*CUSTOMER_COLUMNS).desc(), col("signup_date").desc_nulls_last()],
    )
    silver = to_silver_customers(unique_rows)
    published, quarantined = _publish("customers", silver, invalid, config)
    valid_count = published.count()
    return published, _metric("customers", total, valid_count, quarantined.count(), duplicates, nulls, started)


def build_products(df: DataFrame, config: PipelineConfig) -> tuple[DataFrame, dict[str, object]]:
    started = time.perf_counter()
    total = df.count()
    evaluated = evaluate_products(df)
    duplicates = count_duplicate_extras(evaluated, "product_id")
    nulls = count_null_rows(evaluated, ["product_id", "price", "category", "cost", "stock_quantity"])
    unique_rows, invalid = _dedupe(
        evaluated,
        "product_id",
        "DUPLICATE_PRODUCT_ID",
        [completeness(*PRODUCT_COLUMNS).desc(), col("price").desc_nulls_last()],
    )
    silver = to_silver_products(unique_rows)
    published, quarantined = _publish("products", silver, invalid, config)
    return published, _metric("products", total, published.count(), quarantined.count(), duplicates, nulls, started)


def build_orders(
    df: DataFrame, customers: DataFrame, config: PipelineConfig
) -> tuple[DataFrame, dict[str, object]]:
    started = time.perf_counter()
    total = df.count()
    evaluated = evaluate_orders(df)
    duplicates = count_duplicate_extras(evaluated, "order_id")
    nulls = count_null_rows(evaluated, ["order_id", "customer_id", "order_date", "order_status"])
    unique_rows, invalid = _dedupe(
        evaluated,
        "order_id",
        "DUPLICATE_ORDER_ID",
        [completeness(*ORDER_COLUMNS).desc(), col("order_date").desc_nulls_last()],
    )
    checked = flag_orphans(unique_rows, "customer_id", customers, "customer_id", "ORPHAN_CUSTOMER_ID")
    checked = flag_orders_before_signup(checked, customers)
    survivors, rejected = split_by_validation(checked)
    invalid = invalid.unionByName(rejected)
    silver = to_silver_orders(survivors)
    published, quarantined = _publish("orders", silver, invalid, config, partition_by=["order_year"])
    return published, _metric("orders", total, published.count(), quarantined.count(), duplicates, nulls, started)


def build_order_items(
    df: DataFrame, orders: DataFrame, products: DataFrame, config: PipelineConfig
) -> tuple[DataFrame, dict[str, object]]:
    started = time.perf_counter()
    total = df.count()
    evaluated = evaluate_order_items(df)
    duplicates = count_duplicate_extras(evaluated, "order_item_id")
    nulls = count_null_rows(evaluated, ["order_item_id", "order_id", "product_id", "quantity", "unit_price"])
    unique_rows, invalid = _dedupe(
        evaluated,
        "order_item_id",
        "DUPLICATE_ORDER_ITEM_ID",
        [completeness(*ORDER_ITEM_COLUMNS).desc(), col("quantity").desc_nulls_last()],
    )
    checked = flag_orphans(unique_rows, "order_id", orders, "order_id", "ORPHAN_ORDER_ID")
    checked = flag_orphans(checked, "product_id", products, "product_id", "ORPHAN_PRODUCT_ID")
    survivors, rejected = split_by_validation(checked)
    invalid = invalid.unionByName(rejected)
    silver = to_silver_order_items(survivors)
    published, quarantined = _publish("order_items", silver, invalid, config)
    return published, _metric(
        "order_items", total, published.count(), quarantined.count(), duplicates, nulls, started
    )


def build_payments(df: DataFrame, orders: DataFrame, config: PipelineConfig) -> tuple[DataFrame, dict[str, object]]:
    started = time.perf_counter()
    total = df.count()
    evaluated = evaluate_payments(df)
    duplicates = count_duplicate_extras(evaluated, "payment_id")
    nulls = count_null_rows(evaluated, ["payment_id", "order_id", "amount", "payment_status"])
    unique_rows, invalid = _dedupe(
        evaluated,
        "payment_id",
        "DUPLICATE_PAYMENT_ID",
        [completeness(*PAYMENT_COLUMNS).desc(), col("payment_date").desc_nulls_last()],
    )
    checked = flag_orphans(unique_rows, "order_id", orders, "order_id", "ORPHAN_ORDER_ID")
    survivors, rejected = split_by_validation(checked)
    invalid = invalid.unionByName(rejected)
    silver = to_silver_payments(survivors)
    published, quarantined = _publish("payments", silver, invalid, config)
    return published, _metric("payments", total, published.count(), quarantined.count(), duplicates, nulls, started)


def build_silver(spark: SparkSession, config: PipelineConfig) -> list[dict[str, object]]:
    customers_bronze = read_table(spark, table_path(config.bronze_dir, "customers"))
    products_bronze = read_table(spark, table_path(config.bronze_dir, "products"))
    orders_bronze = read_table(spark, table_path(config.bronze_dir, "orders"))
    items_bronze = read_table(spark, table_path(config.bronze_dir, "order_items"))
    payments_bronze = read_table(spark, table_path(config.bronze_dir, "payments"))

    customers, customer_metric = build_customers(customers_bronze, config)
    products, product_metric = build_products(products_bronze, config)
    orders, order_metric = build_orders(orders_bronze, customers, config)
    _, item_metric = build_order_items(items_bronze, orders, products, config)
    _, payment_metric = build_payments(payments_bronze, orders, config)
    logger.info("Data quality validation completed")
    logger.info("Silver transformations completed")
    return [customer_metric, product_metric, order_metric, item_metric, payment_metric]
