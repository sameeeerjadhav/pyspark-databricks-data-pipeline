"""Land CSV and JSON sources in Bronze with ingestion metadata.

Bronze keeps source values. The only added fields are technical:
ingestion time, source file name, source system, and an order-year
partition column derived so small order data is not split by day.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, element_at, input_file_name, lit, split, to_date, when, year
from pyspark.sql.types import StructType

from src.config import PipelineConfig
from src.logger import get_logger
from src.schemas import (
    CUSTOMER_SCHEMA,
    ORDER_ITEM_SCHEMA,
    ORDER_SCHEMA,
    PAYMENT_SCHEMA,
    PRODUCT_SCHEMA,
)
from src.storage import table_path, write_table

logger = get_logger("shopsphere.ingestion")

DATASETS = (
    ("customers", "customers.csv", CUSTOMER_SCHEMA, "shopsphere_crm", "csv"),
    ("products", "products.csv", PRODUCT_SCHEMA, "shopsphere_catalog", "csv"),
    ("orders", "orders.csv", ORDER_SCHEMA, "shopsphere_oms", "csv"),
    ("order_items", "order_items.csv", ORDER_ITEM_SCHEMA, "shopsphere_oms", "csv"),
    ("payments", "payments.json", PAYMENT_SCHEMA, "shopsphere_payments", "json"),
)


def _with_metadata(df: DataFrame, source_system: str) -> DataFrame:
    source_file = element_at(split(input_file_name(), "/"), -1)
    return df.withColumn("ingestion_timestamp", current_timestamp()).withColumn(
        "source_file", source_file
    ).withColumn("source_system", lit(source_system))


def _read_source(
    spark: SparkSession,
    path: str,
    schema: StructType,
    file_format: str,
) -> DataFrame:
    if not path.startswith(("abfss://", "dbfs:", "s3://")) and not Path(path).exists():
        raise FileNotFoundError(f"Raw file not found: {path}")
    reader = spark.read.schema(schema).option("mode", "PERMISSIVE")
    if file_format == "csv":
        return reader.option("header", True).csv(path)
    return reader.option("columnNameOfCorruptRecord", "_corrupt_record").json(path)


def _add_order_year(df: DataFrame) -> DataFrame:
    parsed = to_date(df["order_date"], "yyyy-MM-dd")
    return df.withColumn(
        "order_year",
        when(parsed.isNotNull(), year(parsed).cast("string")).otherwise(lit("unknown")),
    )


def ingest_file(
    spark: SparkSession,
    config: PipelineConfig,
    dataset: str,
    filename: str,
    schema: StructType,
    source_system: str,
    file_format: str,
    source_dir: str | None = None,
) -> DataFrame:
    source_dir = source_dir or config.raw_dir
    source_path = str(Path(source_dir) / filename) if not str(source_dir).startswith(("abfss://", "dbfs:", "s3://")) else f"{source_dir.rstrip('/')}/{filename}"
    logger.info("Reading %s", filename)
    frame = _with_metadata(_read_source(spark, source_path, schema, file_format), source_system)
    partition_by = None
    if dataset == "orders":
        frame = _add_order_year(frame)
        partition_by = ["order_year"]
    record_count = frame.count()
    logger.info("%s records: %s", dataset, record_count)
    write_table(frame, table_path(config.bronze_dir, dataset), partition_by=partition_by)
    logger.info("Bronze %s written", dataset)
    return frame


def ingest_bronze(spark: SparkSession, config: PipelineConfig) -> dict[str, int]:
    counts: dict[str, int] = {}
    for dataset, filename, schema, source_system, file_format in DATASETS:
        frame = ingest_file(spark, config, dataset, filename, schema, source_system, file_format)
        counts[dataset] = frame.count()
    logger.info("Bronze ingestion completed")
    return counts
