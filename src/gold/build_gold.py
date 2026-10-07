"""Business aggregates for analytics.

Revenue includes CONFIRMED, SHIPPED, and DELIVERED orders only.
PENDING and CANCELLED orders stay in Silver and are excluded here.
Line amounts are calculated in Silver; Gold only aggregates them.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    countDistinct,
    lit,
    max as spark_max,
    min as spark_min,
    month,
    round as spark_round,
    sum as spark_sum,
    when,
    year,
)
from pyspark.sql.types import DecimalType

from src.config import REVENUE_ORDER_STATUSES, GOLD_DATASETS, PipelineConfig
from src.logger import get_logger
from src.storage import read_table, table_path, write_table

logger = get_logger("shopsphere.gold")


def _money(column):
    return spark_round(column, 2).cast(DecimalType(18, 2))


def _average_order_value(revenue_column: str, orders_column: str = "total_orders"):
    return when(col(orders_column) == 0, lit(None)).otherwise(
        spark_round(col(revenue_column) / col(orders_column), 2).cast(DecimalType(18, 2))
    )


def sales_fact(orders: DataFrame, items: DataFrame, products: DataFrame) -> DataFrame:
    """Join revenue orders to line items and product attributes.

    Products are broadcast because the catalog is a small dimension.
    At a much larger catalog size this hint should be removed.
    """
    revenue_orders = orders.filter(col("order_status").isin(*REVENUE_ORDER_STATUSES)).select(
        "order_id", "customer_id", "order_date"
    )
    line_items = items.select(
        "order_id", "product_id", "quantity", "gross_sales", "discount_amount", "net_sales"
    )
    catalog = products.select("product_id", "product_name", "category")
    return line_items.join(revenue_orders, "order_id", "inner").join(broadcast(catalog), "product_id", "inner")


def build_gold_frames(
    orders: DataFrame, items: DataFrame, products: DataFrame, payments: DataFrame
) -> dict[str, DataFrame]:
    fact = sales_fact(orders, items, products).cache()
    daily = (
        fact.groupBy(col("order_date").alias("date"))
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_sum("quantity").cast("long").alias("total_items"),
            _money(spark_sum("gross_sales")).alias("gross_sales"),
            _money(spark_sum("discount_amount")).alias("discount_amount"),
            _money(spark_sum("net_sales")).alias("net_sales"),
        )
        .withColumn("average_order_value", _average_order_value("net_sales"))
        .orderBy("date")
    )
    customers = (
        fact.groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            _money(spark_sum("net_sales")).alias("total_spend"),
            spark_min("order_date").alias("first_order_date"),
            spark_max("order_date").alias("last_order_date"),
        )
        .withColumn("average_order_value", _average_order_value("total_spend"))
        .orderBy("customer_id")
    )
    product_sales = (
        fact.groupBy("product_id", "product_name", "category")
        .agg(
            spark_sum("quantity").cast("long").alias("units_sold"),
            _money(spark_sum("gross_sales")).alias("gross_revenue"),
            _money(spark_sum("discount_amount")).alias("discount_amount"),
            _money(spark_sum("net_sales")).alias("net_revenue"),
        )
        .orderBy(col("net_revenue").desc())
    )
    categories = (
        fact.groupBy("category")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_sum("quantity").cast("long").alias("units_sold"),
            _money(spark_sum("net_sales")).alias("revenue"),
        )
        .orderBy("category")
    )
    monthly = (
        fact.groupBy(year("order_date").alias("year"), month("order_date").alias("month"))
        .agg(
            countDistinct("order_id").alias("total_orders"),
            _money(spark_sum("net_sales")).alias("total_revenue"),
        )
        .withColumn("average_order_value", _average_order_value("total_revenue"))
        .orderBy("year", "month")
    )
    payment_summary = (
        payments.groupBy("payment_method")
        .agg(
            spark_sum(when(col("payment_status") == "SUCCESS", lit(1)).otherwise(lit(0)))
            .cast("long")
            .alias("successful_payments"),
            spark_sum(when(col("payment_status") == "FAILED", lit(1)).otherwise(lit(0)))
            .cast("long")
            .alias("failed_payments"),
            _money(spark_sum(when(col("payment_status") == "SUCCESS", col("amount")).otherwise(lit(0)))).alias(
                "total_amount"
            ),
        )
        .orderBy("payment_method")
    )
    # The writes below are separate actions, so the cached fact is reused.
    tables = {
        "daily_sales": daily,
        "customer_sales_summary": customers,
        "product_sales_summary": product_sales,
        "category_sales_summary": categories,
        "payment_summary": payment_summary,
        "monthly_sales_summary": monthly,
    }
    tables["_fact"] = fact
    return tables


def build_gold(spark: SparkSession, config: PipelineConfig) -> None:
    orders = read_table(spark, table_path(config.silver_dir, "orders"))
    items = read_table(spark, table_path(config.silver_dir, "order_items"))
    products = read_table(spark, table_path(config.silver_dir, "products"))
    payments = read_table(spark, table_path(config.silver_dir, "payments"))
    tables = build_gold_frames(orders, items, products, payments)
    fact = tables.pop("_fact")
    try:
        for name in GOLD_DATASETS:
            write_table(tables[name], table_path(config.gold_dir, name))
            logger.info("Gold %s created", name)
    finally:
        fact.unpersist()
    logger.info("Gold transformations completed")
