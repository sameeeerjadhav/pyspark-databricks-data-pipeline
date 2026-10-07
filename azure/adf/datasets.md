# ADF datasets

Design only. These datasets describe files in ADLS. They are not deployed.

| Dataset | Linked service | Path | Format |
| --- | --- | --- | --- |
| DS_Customers_Raw | LS_ADLS_ShopSphere | `raw/customers/` | CSV, header present |
| DS_Products_Raw | LS_ADLS_ShopSphere | `raw/products/` | CSV, header present |
| DS_Orders_Raw | LS_ADLS_ShopSphere | `raw/orders/` | CSV, header present |
| DS_OrderItems_Raw | LS_ADLS_ShopSphere | `raw/order_items/` | CSV, header present |
| DS_Payments_Raw | LS_ADLS_ShopSphere | `raw/payments/` | JSON, one object per line |

Parameters on every dataset:

- `environment`: `dev`, `test`, or `prod`
- `load_date`: `yyyy-MM-dd`, used in the raw folder name when the source starts sending daily drops

Example parameterized path:

```text
raw/orders/@{dataset().load_date}/
```

The sample files in this repository are a single batch, so the local pipeline reads `data/raw/*.csv` directly. The parameterized folder is the production shape of the same idea.
