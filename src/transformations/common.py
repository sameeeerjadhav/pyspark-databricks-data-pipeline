"""Shared DataFrame helpers used by validation and silver transforms."""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import coalesce, col, create_map, initcap, lit, trim, upper, when
from pyspark.sql.types import DecimalType

SENTINELS = ("NULL", "N/A", "NA", "NONE")


def is_blank(column: str) -> Column:
    return col(column).isNull() | (trim(col(column)) == "")


def cleaned_text(column: str) -> Column:
    trimmed = trim(col(column))
    return when(trimmed.isNull() | (trimmed == "") | upper(trimmed).isin(*SENTINELS), lit(None)).otherwise(trimmed)


def standardize_strings(df: DataFrame, columns: tuple[str, ...] | list[str]) -> DataFrame:
    for name in columns:
        df = df.withColumn(name, cleaned_text(name))
    return df


def completeness(*columns: str) -> Column:
    score = lit(0)
    for name in columns:
        score = score + when(~is_blank(name), lit(1)).otherwise(lit(0))
    return score


def apply_lookup(column: str, lookup: dict[str, str]) -> Column:
    """Map a trimmed uppercase value through a dictionary, otherwise title-case it."""
    pairs: list[Column] = []
    for key, value in lookup.items():
        pairs.extend([lit(key), lit(value)])
    mapped = create_map(*pairs)[upper(trim(col(column)))]
    return when(col(column).isNull(), lit(None)).otherwise(coalesce(mapped, initcap(trim(col(column)))))


def as_decimal(column: str) -> Column:
    return trim(col(column)).cast(DecimalType(18, 2))


def with_line_amounts(df: DataFrame) -> DataFrame:
    """gross = quantity * unit_price; discount is a percentage of gross."""
    quantity = col("quantity").cast(DecimalType(18, 2))
    unit_price = col("unit_price").cast(DecimalType(18, 2))
    discount = col("discount").cast(DecimalType(18, 2))
    hundred = lit(Decimal("100")).cast(DecimalType(18, 2))
    gross = (quantity * unit_price).cast(DecimalType(18, 2))
    discount_amount = (gross * discount / hundred).cast(DecimalType(18, 2))
    net = (gross - discount_amount).cast(DecimalType(18, 2))
    return (
        df.withColumn("gross_sales", gross)
        .withColumn("discount_amount", discount_amount)
        .withColumn("net_sales", net)
    )
