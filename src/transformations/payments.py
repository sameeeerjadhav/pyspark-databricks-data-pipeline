"""Silver payment transformations."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import coalesce, col, create_map, current_timestamp, lit, regexp_replace, to_date, trim, upper, when
from pyspark.sql.types import DecimalType

from src.config import PAYMENT_METHOD_MAP, PAYMENT_STATUS_MAP
from src.transformations.common import as_decimal


def _mapped(column: str, lookup: dict[str, str]) -> Column:
    pairs: list[Column] = []
    for key, value in lookup.items():
        pairs.extend([lit(key), lit(value)])
    mapped = create_map(*pairs)[upper(trim(col(column)))]
    fallback = regexp_replace(upper(trim(col(column))), r"\s+", "_")
    return when(col(column).isNull(), lit(None)).otherwise(coalesce(mapped, fallback))


def to_silver_payments(df: DataFrame) -> DataFrame:
    timestamp = col("ingestion_timestamp") if "ingestion_timestamp" in df.columns else current_timestamp()
    source_file = col("source_file") if "source_file" in df.columns else lit(None).cast("string")
    return df.select(
        col("payment_id"),
        col("order_id"),
        to_date(col("payment_date"), "yyyy-MM-dd").alias("payment_date"),
        _mapped("payment_method", PAYMENT_METHOD_MAP).alias("payment_method"),
        _mapped("payment_status", PAYMENT_STATUS_MAP).alias("payment_status"),
        as_decimal("amount").cast(DecimalType(18, 2)).alias("amount"),
        timestamp.alias("ingestion_timestamp"),
        source_file.alias("source_file"),
    )
