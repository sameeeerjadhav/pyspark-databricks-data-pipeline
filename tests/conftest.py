"""Shared Spark session for local tests. Azure is not required."""

from __future__ import annotations

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["ENVIRONMENT"] = "local"
os.environ["PIPELINE_STORAGE_FORMAT"] = "parquet"

import pytest


@pytest.fixture(scope="session")
def spark():
    from src.spark_session import get_spark_session

    session = get_spark_session()
    yield session
    session.stop()
