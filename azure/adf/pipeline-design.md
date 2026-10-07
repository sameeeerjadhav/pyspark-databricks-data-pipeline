# ADF pipeline design

Pipeline name: `pl_shopsphere_medallion`

This is a design, plus a JSON sketch in `shopsphere_pipeline.json`. It has not been published to Azure Data Factory.

## Trigger

- Daily schedule at 02:00 UTC
- The trigger passes `load_date` and `environment`
- A manual trigger is enough for a backfill of one date

## Activities

```text
Schedule trigger
    -> Copy Customers
    -> Copy Products
    -> Copy Orders
    -> Copy Order Items
    -> Copy Payments
    -> Notebook 01 Bronze
    -> Notebook 02 Data quality
    -> Notebook 03 Silver
    -> Notebook 04 Gold
    -> Notebook 06 SQL validation
    -> On failure: fail the pipeline and keep the Databricks run URL
```

The five copy activities can run in parallel. Bronze starts only after all five succeed. Each later notebook depends on the previous notebook.

| Activity | Depends on | Retry | Notes |
| --- | --- | --- | --- |
| Copy source files | Trigger | 3 times, 30 seconds apart | Lands files in `raw/` |
| Bronze notebook | All copy activities | 1 retry | `01_bronze_ingestion` |
| Quality notebook | Bronze | 0 | Read-only check; Silver performs the write |
| Silver notebook | Bronze | 1 retry | Writes Silver and Quarantine |
| Gold notebook | Silver | 1 retry | Rebuilds aggregates |
| SQL notebook | Gold | 0 | Fails the pipeline if a query cannot run |

## Parameters

| Name | Example | Used for |
| --- | --- | --- |
| environment | dev | Folder and cluster selection |
| load_date | 2026-10-07 | Raw file partition |
| storage_account | shopspheredev | Passed to Databricks as `AZURE_STORAGE_ACCOUNT` |
| container | shopsphere | Passed as `AZURE_CONTAINER` |
| storage_format | delta | Passed as `PIPELINE_STORAGE_FORMAT` |

Databricks notebook activities set:

```text
ENVIRONMENT=azure
PIPELINE_STORAGE_FORMAT=delta
AZURE_STORAGE_ACCOUNT=@{pipeline().parameters.storage_account}
AZURE_CONTAINER=@{pipeline().parameters.container}
```

Base path inside the notebook:

```text
abfss://@{pipeline().parameters.container}@@{pipeline().parameters.storage_account}.dfs.core.windows.net
```

## Failure handling

- A failed copy does not start Spark.
- A failed notebook fails the ADF pipeline. The pipeline does not mark itself successful after a partial Silver write.
- Quarantine rows are data failures, not pipeline failures. The Spark job completes and records them in the quality report.
- Retries are safe because each Spark stage overwrites its own table for the full refresh. The incremental MERGE is idempotent for a repeated order id.
- ADF's monitor view is the operational log. `pipeline_metrics.csv` is the data log written by Spark.

## What is intentionally absent

- No deployed factory
- No integration runtime in this repository
- No alerts resource
- No production schedule that has actually fired
