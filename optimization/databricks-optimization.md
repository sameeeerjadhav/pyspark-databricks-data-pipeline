# Databricks optimization

## Implemented in this project

- The same PySpark functions run locally and in the Databricks notebooks.
- On Databricks, `get_spark_session()` reuses the notebook session instead of starting `local[*]`.
- `ENVIRONMENT=azure` or `DATABRICKS_RUNTIME_VERSION` selects Delta unless `PIPELINE_STORAGE_FORMAT` overrides it.
- Delta MERGE is the upsert path when the storage format is Delta. Local Parquet uses an equivalent overwrite of the newest key.
- Orders are partitioned by `order_year` so a cloud table would not create one small file per day.
- Full refreshes use `overwriteSchema` so a rerun does not fail on a column change during development.
- Pipeline metrics and the quality report record row counts and status for each dataset.

## Recommended production practice

These are not executed in this repository.

| Practice | Why |
| --- | --- |
| Job clusters | A job cluster starts for the ADF run and terminates afterward. An all-purpose cluster is for development. |
| Photon | Databricks' vectorized engine can speed SQL and Parquet/Delta scans. Turn it on for the job cluster and compare the Spark UI, rather than assuming a gain. |
| OPTIMIZE | Compacts small Delta files after many incremental merges. Run it on Silver orders and Gold tables on a schedule, not after every tiny batch. |
| ZORDER | On a large orders table, `ZORDER BY (order_date)` can help date-filtered reads. It is not useful at this sample size. |
| VACUUM | Removes files that are no longer referenced after retention. The default retention is 7 days. Do not vacuum to 0 hours unless you accept losing time travel. |
| Target file size | Aim for files around 128 MB to 1 GB. The sample files are far smaller. Compacting them would not change an interview-scale workload. |
| Unity Catalog | Store tables as `catalog.shopsphere.silver_orders` instead of raw paths, and grant privileges per table. |
| Cluster sizing | Start with a small job cluster, read the Spark UI for spill and skew, then add workers. More workers do not help a 5,000-row sample. |
| Spark UI / job monitoring | Use the SQL and Spark UI for stage time, shuffle bytes, and skewed tasks. ADF stores the notebook run URL. |
| Time travel | `VERSION AS OF` or `TIMESTAMP AS OF` can restore a bad overwrite. It requires Delta, which is the cloud format for this design. |

## Delta commands that belong in production, not in the local sample run

```sql
OPTIMIZE shopsphere.silver_orders ZORDER BY (order_date);
VACUUM shopsphere.silver_orders RETAIN 168 HOURS;
DESCRIBE HISTORY shopsphere.silver_orders;
```

Do not present these as commands that were run against a production workspace.
