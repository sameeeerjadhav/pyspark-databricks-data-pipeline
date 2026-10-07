"""Build the dataset-level data-quality report."""

from __future__ import annotations

import csv
from pathlib import Path

from pyspark.sql import SparkSession

from src.config import PipelineConfig
from src.logger import get_logger

logger = get_logger("shopsphere.quality")

REPORT_COLUMNS = (
    "dataset",
    "total_records",
    "valid_records",
    "invalid_records",
    "duplicate_records",
    "null_records",
    "quality_percentage",
)


def quality_percentage(valid_records: int, total_records: int) -> str:
    if total_records == 0:
        return "0.00%"
    return f"{(valid_records / total_records) * 100:.2f}%"


def write_quality_report(spark: SparkSession, config: PipelineConfig, metrics: list[dict[str, object]]) -> None:
    """Write the report as CSV and return it as a Spark DataFrame via a temp view."""
    rows = []
    for metric in metrics:
        total = int(metric["total_records"])
        valid = int(metric["valid_records"])
        rows.append(
            {
                "dataset": str(metric["dataset"]),
                "total_records": total,
                "valid_records": valid,
                "invalid_records": int(metric["invalid_records"]),
                "duplicate_records": int(metric["duplicate_records"]),
                "null_records": int(metric["null_records"]),
                "quality_percentage": quality_percentage(valid, total),
            }
        )
    report = spark.createDataFrame(rows)
    report.createOrReplaceTempView("data_quality_report")

    if config.local:
        target = Path(config.quality_report_csv)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(REPORT_COLUMNS))
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        logger.info("Data quality report written to %s", target)
    else:
        from src.storage import write_table

        write_table(report, config.quality_report_csv)
        logger.info("Data quality report written to %s", config.quality_report_csv)
