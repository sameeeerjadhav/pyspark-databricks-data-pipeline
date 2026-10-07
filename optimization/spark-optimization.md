# Spark optimization

The ShopSphere sample is a few thousand rows. These choices are the ones that matter when the same jobs run on large fact tables. They are not a claim that this laptop run is faster than an unoptimized version by a measured percentage.

## Implemented in this project

| Technique | Where | Why |
| --- | --- | --- |
| Explicit schemas | `src/schemas.py` | CSV and JSON are read as strings. Spark does not guess types and drop bad values. |
| Column pruning | Gold fact and notebook 05 | Joins select only the columns they aggregate. |
| Predicate before join | Incremental watermark filter, notebook 05 | Newer orders are filtered before validation and before the customer join. |
| Broadcast of small dimensions | Foreign-key checks and the product join in Gold | Customers and products are small lookups. The fact side should not be broadcast. |
| No Python UDFs | All transforms | Trims, casts, maps, and amounts use built-in functions, which stay inside the JVM. |
| No full `collect()` | Pipeline | `collect()` is used only for the quality report and pipeline metrics, which are a handful of summary rows. |
| Cache only for reuse | Silver publish and Gold fact | Silver is counted and written. The Gold fact feeds six aggregates, then it is unpersisted. |
| Partition by year, not by day | Bronze and Silver orders | A daily partition on this sample would create many tiny files. Three year partitions are enough. |
| `coalesce` for a small plan | Notebook 05 | `coalesce` avoids a full shuffle when reducing partitions before a small write. `repartition` is not used to "optimize" this sample. |
| Adaptive query execution | Spark session | `spark.sql.adaptive.enabled` is on so Spark can coalesce shuffle partitions. |
| Static partition overwrite | Spark session | A rerun replaces the table instead of leaving stale partitions behind. |

## Broadcast join

A broadcast join copies the small side to every executor and probes it while scanning the large side. That avoids shuffling the large table.

Use it when the dimension is small enough for executor memory. Do not broadcast orders or order items. The pipeline broadcasts customer keys during referential checks and the product catalog during the Gold join.

Notebook `05_spark_optimization.py` disables automatic broadcast, prints a join without a hint, then prints the same join with `broadcast()`. On a laptop the broadcast plan can take longer. That result is expected: the overhead is larger than the shuffle it avoids. The plan is still the thing to inspect. Look for `BroadcastHashJoin` versus `SortMergeJoin`.

## What causes a shuffle

A shuffle happens when Spark must move rows that share a key onto the same partition. Joins, `groupBy`, and window functions over a business key all shuffle unless one side is broadcast or the data is already partitioned by that key.

`orderBy` before a Gold write is also a shuffle. It is acceptable here because the aggregated tables are small and a stable order makes the output easier to review.

## Predicate pushdown

Filters on Parquet and Delta column statistics can skip files. The incremental job filters `order_date` before the Silver rules run, so old late-arriving rows are not validated or merged. Reading Parquet with a filter in notebook 05 is the same idea: filter first, then join.

## Repartition versus coalesce

- `repartition(n)` reshuffles into `n` partitions. Use it to increase parallelism or to partition by a key before a heavy join.
- `coalesce(n)` combines existing partitions and does not do a full shuffle. Use it to cut down partitions before writing a small result.

This project does not call `repartition` on the sample. Doing so would add a shuffle without a benefit.

## Query plans

`explain()` prints the physical plan without running the job. Use it before timing anything. `count()` is the action that actually runs the plan. Timing one `count()` on a laptop is noisy because the JVM, disk cache, and antivirus all interfere. Notebook 05 prints both the plan and one timed action so the plan can be discussed even when the timing is not meaningful.
