"""Read and write Bronze, Silver, Gold, and Quarantine datasets."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from src.config import PipelineConfig, load_config
from src.logger import get_logger

logger = get_logger("shopsphere.storage")


def table_path(directory: str, dataset: str) -> str:
    return str(Path(directory) / dataset) if not directory.startswith(("abfss://", "dbfs:", "s3://")) else f"{directory.rstrip('/')}/{dataset}"


def table_exists(spark: SparkSession, path: str, storage_format: str) -> bool:
    if not path.startswith(("abfss://", "dbfs:", "s3://")):
        return Path(path).exists()
    try:
        spark.read.format(storage_format).load(path).limit(1).collect()
        return True
    except AnalysisException:
        return False


def write_table(
    df: DataFrame,
    path: str,
    partition_by: list[str] | None = None,
    storage_format: str | None = None,
) -> None:
    """Overwrite a dataset. Full refresh keeps the demo idempotent."""
    storage_format = storage_format or load_config().storage_format
    if not path.startswith(("abfss://", "dbfs:", "s3://")):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    writer = df.write.mode("overwrite").format(storage_format).option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    logger.info("Wrote %s dataset to %s", storage_format, path)


def read_table(spark: SparkSession, path: str, storage_format: str | None = None) -> DataFrame:
    storage_format = storage_format or load_config().storage_format
    return spark.read.format(storage_format).load(path)


def storage_format_of(config: PipelineConfig | None = None) -> str:
    return (config or load_config()).storage_format
