"""Spark session factory for local runs and Databricks notebooks."""

from __future__ import annotations

import os
import shutil
import sys
import urllib.request
from pathlib import Path

# Must be set before PySpark is imported. PySpark reads this at import time.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

from src.config import PipelineConfig, load_config
from src.logger import get_logger

logger = get_logger("shopsphere.spark")


def _ensure_java_home() -> None:
    """Find a JDK when JAVA_HOME is unset. Databricks already provides Java."""
    if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("JAVA_HOME"):
        return
    java_binary = shutil.which("java")
    if java_binary:
        os.environ["JAVA_HOME"] = str(Path(java_binary).resolve().parents[1])
        return
    program_files = os.environ.get("ProgramFiles", r"C:\Program Files")
    candidates = sorted(Path(program_files).glob("Eclipse Adoptium/jdk-17*"), reverse=True)
    candidates.extend(sorted(Path(program_files).glob("Java/jdk-17*"), reverse=True))
    if not candidates:
        raise RuntimeError(
            "JAVA_HOME is not set and Java was not found on PATH. "
            "Install JDK 17 and set JAVA_HOME before running the pipeline."
        )
    os.environ["JAVA_HOME"] = str(candidates[0])
    os.environ["PATH"] = str(candidates[0] / "bin") + os.pathsep + os.environ.get("PATH", "")


def _ensure_hadoop_home(project_root: Path) -> None:
    """Point Hadoop at winutils on Windows so local Parquet writes can set permissions."""
    if os.name != "nt" or os.environ.get("HADOOP_HOME"):
        return
    home = project_root / ".hadoop"
    bin_dir = home / "bin"
    executable = bin_dir / "winutils.exe"
    library = bin_dir / "hadoop.dll"
    if not executable.exists() or not library.exists():
        bin_dir.mkdir(parents=True, exist_ok=True)
        base_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/"
        logger.info("Downloading winutils into %s for local Windows file output", home)
        urllib.request.urlretrieve(base_url + "winutils.exe", executable)
        urllib.request.urlretrieve(base_url + "hadoop.dll", library)
    os.environ["HADOOP_HOME"] = str(home)
    os.environ["PATH"] = str(bin_dir) + os.pathsep + os.environ.get("PATH", "")


def get_spark_session(config: PipelineConfig | None = None) -> SparkSession:
    """Return the active Spark session, or a local session configured for this project."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    config = config or load_config()
    _ensure_java_home()
    _ensure_hadoop_home(config.project_root)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    warehouse = str((config.project_root / "spark-warehouse").resolve())
    local_dir = str((config.project_root / "spark-tmp").resolve())
    (config.project_root / "spark-warehouse").mkdir(parents=True, exist_ok=True)
    (config.project_root / "spark-tmp").mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(config.spark_app_name)
        .master(config.spark_master)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.local.dir", local_dir)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    )
    if config.storage_format == "delta":
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        from delta import configure_spark_with_delta_pip

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        logger.info("Spark session started with Delta Lake enabled")
    else:
        spark = builder.getOrCreate()
        logger.info("Spark session started with Parquet storage")
    spark.sparkContext.setLogLevel("WARN")
    return spark
