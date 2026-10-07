# ADLS Gen2 storage layout

This layout is the cloud design for ShopSphere. It is not a live storage account. No container was created for this repository.

```text
abfss://shopsphere@<storage-account>.dfs.core.windows.net/
├── raw/
│   ├── customers/
│   ├── products/
│   ├── orders/
│   ├── order_items/
│   └── payments/
├── sample/
│   ├── late_orders.csv
│   └── late_order_items.csv
├── bronze/
│   ├── customers/
│   ├── products/
│   ├── orders/          # partitioned by order_year
│   ├── order_items/
│   └── payments/
├── silver/
│   ├── customers/
│   ├── products/
│   ├── orders/
│   ├── order_items/
│   └── payments/
├── gold/
│   ├── daily_sales/
│   ├── customer_sales_summary/
│   ├── product_sales_summary/
│   ├── category_sales_summary/
│   ├── payment_summary/
│   └── monthly_sales_summary/
├── quarantine/
│   ├── customers/
│   ├── products/
│   ├── orders/
│   ├── order_items/
│   ├── payments/
│   ├── incremental_orders/
│   └── incremental_order_items/
├── checkpoints/
│   └── orders_watermark/
└── monitoring/
    ├── data_quality_report/
    └── pipeline_metrics/
```

## Why each zone exists

| Zone | Purpose |
| --- | --- |
| raw | Immutable files copied by Azure Data Factory. Spark does not clean them here. |
| bronze | The same files as Delta/Parquet, plus ingestion time, source file, and source system. |
| silver | Valid, typed, de-duplicated records that pass referential checks. |
| gold | Aggregates safe to query for revenue reporting. |
| quarantine | Rejected records and the rule that rejected them. |
| checkpoints | Latest processed `order_date`, used by the incremental batch. |
| monitoring | Quality report and pipeline run metrics. |

Set the root with `AZURE_ADLS_BASE_PATH` or with `AZURE_STORAGE_ACCOUNT` and `AZURE_CONTAINER`. The pipeline builds `abfss://<container>@<account>.dfs.core.windows.net` from those variables. Do not put an account key in the repository.

Local execution uses the same zone names under `data/`.
