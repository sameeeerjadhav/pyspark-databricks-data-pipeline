# Deployment

Nothing in this repository has been deployed to Azure or to a Databricks workspace. The steps below are the intended setup.

## Local

Requirements:

- Windows, macOS, or Linux
- Python 3.11 or 3.12
- JDK 17
- `JAVA_HOME` pointing at that JDK

From the repository root on Windows:

```text
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python -m src.pipeline
pytest
```

The pipeline writes Parquet under `data/bronze`, `data/silver`, `data/gold`, and `data/quarantine`. It also writes `data_quality_report.csv` and `pipeline_metrics.csv` in the repository root. Those generated files are gitignored.

To exercise Delta locally:

```text
set PIPELINE_STORAGE_FORMAT=delta
python -m src.pipeline
```

The first Delta run downloads Delta Lake jars. If that download or the local Hadoop binaries fail, use the Parquet default. The MERGE function is still in `src/incremental.py` and runs when the format is Delta.

## Databricks

1. Create a workspace and a small all-purpose cluster for development. DBR 13.3 LTS or newer includes Spark 3.4+ and Delta.
2. Import this repository with Repos, or import the `notebooks/*.py` files. They use the Databricks source format.
3. Set environment variables on the cluster, or notebook widgets that export them:

```text
ENVIRONMENT=azure
PIPELINE_STORAGE_FORMAT=delta
AZURE_STORAGE_ACCOUNT=<storage-account>
AZURE_CONTAINER=shopsphere
PIPELINE_BASE_PATH=abfss://shopsphere@<storage-account>.dfs.core.windows.net
```

4. Upload `data/raw` and `data/sample` to the `raw/` and `sample/` prefixes, or let ADF copy them.
5. Attach the cluster and run notebooks 01, 03, 04, and 06 in that order. Notebook 02 is a quality review. Notebook 05 is the optimization walkthrough.
6. For a scheduled run, point an ADF Databricks notebook activity at those notebooks and use a job cluster.

`get_spark_session()` detects `DATABRICKS_RUNTIME_VERSION` and reuses the existing session. It does not force `local[*]` inside a notebook.

To publish a table into the metastore from a notebook:

```python
frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("shopsphere.daily_sales")
```

That line is not part of the local pipeline, because a laptop does not have the same catalog.

## Azure shape

```text
Azure Data Factory
    -> ADLS Gen2
    -> Azure Databricks job cluster
    -> Delta tables
    -> Databricks SQL
```

Details live in `azure/adf/pipeline-design.md` and `azure/adls/storage-layout.md`.

Authentication is a managed identity on ADF and on the Databricks cluster. Grant the Factory identity write access to `raw/`. Grant the cluster identity read access to `raw/` and write access to `bronze/`, `silver/`, `gold/`, `quarantine/`, `checkpoints/`, and `monitoring/`.

If a token is required, store it in Key Vault and reference it from the linked service. Do not paste it into a notebook or into Git.

## CI

`.github/workflows/ci.yml` checks out the code, installs Temurin 17 and Python 3.12, compiles the packages, and runs pytest. It does not deploy infrastructure. A green run means the local tests passed on GitHub-hosted Ubuntu.

## Failure behavior

- A missing raw file raises `FileNotFoundError` and the pipeline exits with status 1.
- A reconciliation mismatch between Bronze, Silver, and Quarantine fails the run.
- An empty Gold table fails the run.
- Quarantine rows do not fail the run. They are the record of bad source data.
- `pipeline_metrics.csv` is written on success and on failure, with `pipeline_status` set accordingly.
