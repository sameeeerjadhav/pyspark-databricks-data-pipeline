"""Silver order transformations."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, initcap, lit, to_date, upper, year

from src.reference import COUNTRY_LOOKUP, state_lookup
from src.transformations.common import apply_lookup


def to_silver_orders(df: DataFrame) -> DataFrame:
    timestamp = col("ingestion_timestamp") if "ingestion_timestamp" in df.columns else current_timestamp()
    source_file = col("source_file") if "source_file" in df.columns else lit(None).cast("string")
    order_date = to_date(col("order_date"), "yyyy-MM-dd")
    return df.select(
        col("order_id"),
        col("customer_id"),
        order_date.alias("order_date"),
        year(order_date).cast("string").alias("order_year"),
        upper(col("order_status")).alias("order_status"),
        initcap(col("shipping_city")).alias("shipping_city"),
        apply_lookup("shipping_state", state_lookup()).alias("shipping_state"),
        apply_lookup("shipping_country", COUNTRY_LOOKUP).alias("shipping_country"),
        timestamp.alias("ingestion_timestamp"),
        source_file.alias("source_file"),
    )
