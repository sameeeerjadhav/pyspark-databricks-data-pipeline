# Interview guide

Short answers for a junior data engineer interview. ShopSphere is a fictional sample. It has not been deployed to production, and it does not process production traffic.

## 1. What problem does this project solve?

ShopSphere's order, customer, product, and payment extracts do not agree. The pipeline lands them in one lake, rejects rows that break the rules, and publishes sales tables that SQL can query without repeating the cleaning logic.

## 2. Why PySpark?

The transformations are joins, filters, and aggregations. PySpark expresses those as DataFrame operations that run on a cluster when the data no longer fits one machine. The same code runs on a laptop for this sample.

## 3. Why Databricks?

Databricks provides a managed Spark runtime, notebooks, job clusters, and SQL on top of the lake. The notebooks in this repo are the Databricks entry points. I have not run them in a workspace as part of this project.

## 4. Why Medallion Architecture?

Each layer has a different contract. Bronze keeps the source. Silver is clean and relational. Gold is aggregated for a question the business asks often. Mixing those steps in one table makes reruns and debugging harder.

## 5. Why Bronze, Silver, and Gold?

Bronze answers "what did we receive?" Silver answers "what is trustworthy?" Gold answers "what did we sell?" Quarantine sits beside Silver so bad rows are still explainable.

## 6. Why Delta instead of only Parquet?

Parquet is a columnar file format. Delta adds a transaction log: commits, schema checks, MERGE, and time travel. Local runs use Parquet because it does not need extra jars. The cloud design uses Delta, and the upsert code calls `MERGE` when the format is Delta.

## 7. Why ADF?

ADF is the scheduler and the copy tool. It moves files into the lake and then starts the Databricks notebooks. Spark should not also be the file-transfer tool. The factory in this repo is a design, not a deployed factory.

## 8. Why ADLS?

ADLS Gen2 is the storage layer: cheap files, hierarchical folders, and access control per container. Bronze, Silver, Gold, and Quarantine are prefixes in one container so the layout is obvious.

## 9. How is data quality handled?

Each dataset has explicit rules for nulls, types, ranges, allowed values, duplicates, and foreign keys. Rules append error codes. A row with any code goes to Quarantine.

## 10. What happens to bad records?

They are not dropped quietly. Quarantine stores the original JSON, the dataset name, the error codes, the validation time, and the source file. Silver only contains rows that passed.

## 11. How do you handle duplicates?

I group by the business key, keep the most complete row, and quarantine the extra copies with `DUPLICATE_CUSTOMER_ID` or the equivalent code. Completeness is the count of populated fields, with the latest date as the tie breaker.

## 12. How do you handle null values?

Required nulls fail validation. Optional fields, such as phone and discount, become null or zero. Sentinel strings such as `N/A` are treated as null before the rules run. The original text is still in the quarantine payload.

## 13. How do you optimize Spark?

Select only needed columns, filter before a join, broadcast small dimensions, avoid Python UDFs, and cache only when several actions reuse a DataFrame. I read `explain()` before changing a join. Notebook 05 shows a broadcast plan next to a shuffle plan.

## 14. What is a broadcast join?

Spark sends the small table to every executor and joins it while scanning the large table. That avoids shuffling the large table. It is appropriate for products and customers, not for order lines once those lines are large.

## 15. Repartition versus coalesce?

`repartition` reshuffles into a chosen number of partitions. `coalesce` combines partitions without a full shuffle. I would coalesce a small write. I would repartition only to add parallelism or to align a large join key.

## 16. What causes a shuffle?

A shuffle moves rows so that matching keys land in the same partition. Joins, group-bys, and window functions do this unless one side is broadcast or the table is already partitioned by that key.

## 17. What is predicate pushdown?

The filter is applied while reading files, using column statistics, instead of after the whole dataset is loaded. The incremental job filters `order_date` before it validates or merges.

## 18. How does Delta provide reliability?

Each write is a commit in the Delta log. Readers see a complete snapshot or the previous one, not a half-written file. Schema enforcement rejects a surprise column. Time travel can read an older version after a bad overwrite.

## 19. How would you scale this to billions of records?

Keep the same layers. Partition large facts by date, avoid broadcasting them, compact files with OPTIMIZE, and make Gold incremental for the dates that changed. Use a job cluster sized from the Spark UI, not from a guess. I have not run this sample at that scale.

## 20. How would you monitor production pipelines?

ADF shows activity success, duration, and retries. Spark writes `pipeline_metrics` with input, valid, and invalid counts. I would alert when the pipeline fails, when the invalid percentage jumps, or when Gold row counts drop to zero. The Spark UI covers skew and shuffle.

## 21. How would you implement incremental processing?

Store the last processed timestamp. Read only newer source rows, validate them, and MERGE them into Silver. This project does that for orders using `order_date`. A better production watermark is `updated_at` or a change-data-capture log, because a late status change on an old order is invisible to an order-date filter.

## 22. How would you deploy this in production?

ADF copies files into ADLS with a managed identity, then runs the notebooks on a job cluster that writes Delta. CI runs pytest on every push. A separate release step would publish the notebooks. That release step is not built here.

## 23. What would you improve next?

Add an `updated_at` watermark, reconcile payment amount to order net, publish Unity Catalog tables, and compact Delta files after merges. I would also split the Gold refresh so only changed dates are recomputed. I would not add another tool until one of those gaps is the actual bottleneck.
