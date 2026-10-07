"""Runtime configuration for local and Azure execution.

Paths come from the repository location or from environment variables.
Azure credentials are never read from source files.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

VALID_ORDER_STATUSES = ("PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED")
REVENUE_ORDER_STATUSES = ("CONFIRMED", "SHIPPED", "DELIVERED")
VALID_CATEGORIES = ("Electronics", "Home", "Apparel", "Beauty", "Sports", "Books", "Grocery")

# Source values accepted before they are standardized.
PAYMENT_STATUS_MAP = {
    "SUCCESS": "SUCCESS",
    "PAID": "SUCCESS",
    "COMPLETED": "SUCCESS",
    "FAILED": "FAILED",
    "DECLINED": "FAILED",
    "PENDING": "PENDING",
    "REFUNDED": "REFUNDED",
}
PAYMENT_METHOD_MAP = {
    "CREDIT_CARD": "CREDIT_CARD",
    "CREDIT CARD": "CREDIT_CARD",
    "CREDITCARD": "CREDIT_CARD",
    "CC": "CREDIT_CARD",
    "DEBIT_CARD": "DEBIT_CARD",
    "DEBIT CARD": "DEBIT_CARD",
    "PAYPAL": "PAYPAL",
    "UPI": "UPI",
    "GIFT_CARD": "GIFT_CARD",
    "GIFT CARD": "GIFT_CARD",
}

SILVER_DATASETS = ("customers", "products", "orders", "order_items", "payments")
GOLD_DATASETS = (
    "daily_sales",
    "customer_sales_summary",
    "product_sales_summary",
    "category_sales_summary",
    "payment_summary",
    "monthly_sales_summary",
)


def _project_root() -> Path:
    configured = os.environ.get("PIPELINE_BASE_PATH", "").strip()
    if configured:
        return Path(configured).resolve()
    return Path(__file__).resolve().parents[1]


def _join(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *parts])


@dataclass(frozen=True)
class PipelineConfig:
    """Paths and runtime settings for one pipeline execution."""

    environment: str
    project_root: Path
    raw_dir: str
    sample_dir: str
    bronze_dir: str
    silver_dir: str
    gold_dir: str
    quarantine_dir: str
    checkpoint_dir: str
    quality_report_csv: str
    pipeline_metrics_csv: str
    storage_format: str
    spark_app_name: str
    spark_master: str
    azure_storage_account: str
    azure_container: str
    azure_adls_base_path: str

    @property
    def local(self) -> bool:
        return self.environment != "azure"


def load_config() -> PipelineConfig:
    """Build configuration from environment variables and the repo location."""
    environment = os.environ.get("ENVIRONMENT", "local").strip().lower()
    project_root = _project_root()
    storage_format = os.environ.get("PIPELINE_STORAGE_FORMAT", "").strip().lower()
    on_databricks = bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))
    if not storage_format:
        storage_format = "delta" if environment == "azure" or on_databricks else "parquet"
    if storage_format not in {"parquet", "delta"}:
        raise ValueError("PIPELINE_STORAGE_FORMAT must be 'parquet' or 'delta'")

    account = os.environ.get("AZURE_STORAGE_ACCOUNT", "").strip()
    container = os.environ.get("AZURE_CONTAINER", "shopsphere").strip() or "shopsphere"
    adls_base = os.environ.get("AZURE_ADLS_BASE_PATH", "").strip().rstrip("/")

    if environment == "azure":
        if not adls_base:
            if not account:
                raise ValueError(
                    "ENVIRONMENT=azure requires AZURE_ADLS_BASE_PATH or AZURE_STORAGE_ACCOUNT"
                )
            adls_base = f"abfss://{container}@{account}.dfs.core.windows.net"
        raw_dir = _join(adls_base, "raw")
        sample_dir = _join(adls_base, "sample")
        bronze_dir = _join(adls_base, "bronze")
        silver_dir = _join(adls_base, "silver")
        gold_dir = _join(adls_base, "gold")
        quarantine_dir = _join(adls_base, "quarantine")
        checkpoint_dir = _join(adls_base, "checkpoints")
        quality_report_csv = _join(adls_base, "monitoring", "data_quality_report")
        pipeline_metrics_csv = _join(adls_base, "monitoring", "pipeline_metrics")
    else:
        data_dir = project_root / "data"
        raw_dir = str(data_dir / "raw")
        sample_dir = str(data_dir / "sample")
        bronze_dir = str(data_dir / "bronze")
        silver_dir = str(data_dir / "silver")
        gold_dir = str(data_dir / "gold")
        quarantine_dir = str(data_dir / "quarantine")
        checkpoint_dir = str(data_dir / "checkpoints")
        quality_report_csv = str(project_root / "data_quality_report.csv")
        pipeline_metrics_csv = str(project_root / "pipeline_metrics.csv")

    return PipelineConfig(
        environment=environment,
        project_root=project_root,
        raw_dir=raw_dir,
        sample_dir=sample_dir,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        quarantine_dir=quarantine_dir,
        checkpoint_dir=checkpoint_dir,
        quality_report_csv=quality_report_csv,
        pipeline_metrics_csv=pipeline_metrics_csv,
        storage_format=storage_format,
        spark_app_name=os.environ.get("SPARK_APP_NAME", "shopsphere-medallion-pipeline"),
        spark_master=os.environ.get("SPARK_MASTER", "local[2]"),
        azure_storage_account=account,
        azure_container=container,
        azure_adls_base_path=adls_base,
    )
