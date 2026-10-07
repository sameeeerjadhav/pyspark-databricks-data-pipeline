"""Silver order-item transformations."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, current_timestamp, lit, trim
from pyspark.sql.types import DecimalType, IntegerType

from src.transformations.common import as_decimal, with_line_amounts


def to_silver_order_items(df: DataFrame) -> DataFrame:
    timestamp = col("ingestion_timestamp") if "ingestion_timestamp" in df.columns else current_timestamp()
    source_file = col("source_file") if "source_file" in df.columns else lit(None).cast("string")
    typed = df.select(
        col("order_item_id"),
        col("order_id"),
        col("product_id"),
        trim(col("quantity")).cast(IntegerType()).alias("quantity"),
        as_decimal("unit_price").cast(DecimalType(18, 2)).alias("unit_price"),
        coalesce(as_decimal("discount"), lit(0)).cast(DecimalType(18, 2)).alias("discount"),
        timestamp.alias("ingestion_timestamp"),
        source_file.alias("source_file"),
    )
    return with_line_amounts(typed)
