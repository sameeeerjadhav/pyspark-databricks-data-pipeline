"""Silver product transformations."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, initcap, lit, trim
from pyspark.sql.types import DecimalType, IntegerType

from src.transformations.common import as_decimal


def to_silver_products(df: DataFrame) -> DataFrame:
    timestamp = col("ingestion_timestamp") if "ingestion_timestamp" in df.columns else current_timestamp()
    source_file = col("source_file") if "source_file" in df.columns else lit(None).cast("string")
    return df.select(
        col("product_id"),
        trim(col("product_name")).alias("product_name"),
        initcap(trim(col("category"))).alias("category"),
        initcap(trim(col("subcategory"))).alias("subcategory"),
        as_decimal("price").cast(DecimalType(18, 2)).alias("price"),
        as_decimal("cost").cast(DecimalType(18, 2)).alias("cost"),
        trim(col("stock_quantity")).cast(IntegerType()).alias("stock_quantity"),
        timestamp.alias("ingestion_timestamp"),
        source_file.alias("source_file"),
    )
