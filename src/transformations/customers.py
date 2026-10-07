"""Silver customer transformations."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, current_timestamp, initcap, lit, lower, to_date

from src.reference import COUNTRY_LOOKUP, state_lookup
from src.transformations.common import apply_lookup


def to_silver_customers(df: DataFrame) -> DataFrame:
    timestamp = col("ingestion_timestamp") if "ingestion_timestamp" in df.columns else current_timestamp()
    source_file = col("source_file") if "source_file" in df.columns else lit(None).cast("string")
    return df.select(
        col("customer_id"),
        initcap(col("first_name")).alias("first_name"),
        initcap(col("last_name")).alias("last_name"),
        concat_ws(" ", initcap(col("first_name")), initcap(col("last_name"))).alias("full_name"),
        lower(col("email")).alias("email"),
        col("phone"),
        initcap(col("city")).alias("city"),
        apply_lookup("state", state_lookup()).alias("state"),
        apply_lookup("country", COUNTRY_LOOKUP).alias("country"),
        to_date(col("signup_date"), "yyyy-MM-dd").alias("signup_date"),
        timestamp.alias("ingestion_timestamp"),
        source_file.alias("source_file"),
    )
