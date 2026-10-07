"""Persist a small execution report for each pipeline run."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

from src.config import PipelineConfig
from src.logger import get_logger

logger = get_logger("shopsphere.monitoring")

METRIC_COLUMNS = (
    "pipeline_start_time",
    "pipeline_end_time",
    "dataset",
    "input_record_count",
    "valid_record_count",
    "invalid_record_count",
    "processing_duration_seconds",
    "pipeline_status",
)


def write_pipeline_metrics(
    config: PipelineConfig,
    started_at: datetime,
    dataset_metrics: list[dict[str, object]],
    status: str,
) -> None:
    ended_at = datetime.now(timezone.utc)
    start_text = started_at.astimezone(timezone.utc).isoformat()
    end_text = ended_at.isoformat()
    rows = []
    for metric in dataset_metrics:
        rows.append(
            {
                "pipeline_start_time": start_text,
                "pipeline_end_time": end_text,
                "dataset": metric.get("dataset", ""),
                "input_record_count": int(metric.get("total_records", 0)),
                "valid_record_count": int(metric.get("valid_records", 0)),
                "invalid_record_count": int(metric.get("invalid_records", 0)),
                "processing_duration_seconds": metric.get("duration_seconds", 0),
                "pipeline_status": status,
            }
        )
    rows.append(
        {
            "pipeline_start_time": start_text,
            "pipeline_end_time": end_text,
            "dataset": "_pipeline",
            "input_record_count": sum(int(row["input_record_count"]) for row in rows),
            "valid_record_count": sum(int(row["valid_record_count"]) for row in rows),
            "invalid_record_count": sum(int(row["invalid_record_count"]) for row in rows),
            "processing_duration_seconds": round((ended_at - started_at.astimezone(timezone.utc)).total_seconds(), 3),
            "pipeline_status": status,
        }
    )
    if not config.local:
        logger.info("Pipeline metrics ready for path %s", config.pipeline_metrics_csv)
        return
    target = Path(config.pipeline_metrics_csv)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(METRIC_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Pipeline metrics written to %s", target)
