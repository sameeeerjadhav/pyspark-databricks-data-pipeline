# Data quality

Invalid rows are written to `data/quarantine/<dataset>/`. Each row stores:

- `source_dataset`
- `validation_errors`, several codes joined with `|`
- `validation_timestamp`
- `source_file`
- `record_json`, the original payload before sentinel cleanup

A row enters Silver only when every rule passes. Duplicate copies are quarantined with a `DUPLICATE_*` code. The surviving copy is the most complete row, then the latest business date.

## Rules

| Dataset | Rule | Code |
| --- | --- | --- |
| Customers | `customer_id` required | `MISSING_CUSTOMER_ID` |
| Customers | Email required and must contain a local part, `@`, and a domain | `MISSING_EMAIL`, `INVALID_EMAIL` |
| Customers | City required | `MISSING_CITY` |
| Customers | `signup_date` must parse as `yyyy-MM-dd` | `MISSING_SIGNUP_DATE`, `INVALID_SIGNUP_DATE` |
| Customers | Extra copy of `customer_id` | `DUPLICATE_CUSTOMER_ID` |
| Products | `product_id` required | `MISSING_PRODUCT_ID` |
| Products | Price numeric and greater than 0 | `INVALID_PRICE` |
| Products | Cost numeric and greater than or equal to 0 | `INVALID_COST` |
| Products | Stock is an integer greater than or equal to 0 | `INVALID_STOCK_QUANTITY` |
| Products | Category is one of Electronics, Home, Apparel, Beauty, Sports, Books, Grocery | `INVALID_CATEGORY` |
| Products | Extra copy of `product_id` | `DUPLICATE_PRODUCT_ID` |
| Orders | `order_id` and `customer_id` required | `MISSING_ORDER_ID`, `MISSING_CUSTOMER_ID` |
| Orders | `order_date` must parse as `yyyy-MM-dd` | `MISSING_ORDER_DATE`, `INVALID_ORDER_DATE` |
| Orders | Status is PENDING, CONFIRMED, SHIPPED, DELIVERED, or CANCELLED | `INVALID_ORDER_STATUS` |
| Orders | `customer_id` must exist in Silver customers | `ORPHAN_CUSTOMER_ID` |
| Orders | `order_date` cannot be before that customer's signup | `ORDER_BEFORE_SIGNUP` |
| Orders | Extra copy of `order_id` | `DUPLICATE_ORDER_ID` |
| Order items | Quantity is an integer greater than 0 | `INVALID_QUANTITY` |
| Order items | Unit price numeric and greater than 0 | `INVALID_UNIT_PRICE` |
| Order items | Discount, when present, is from 0 through 100 | `INVALID_DISCOUNT` |
| Order items | A blank discount is treated as 0 and is valid | |
| Order items | `order_id` and `product_id` must exist in Silver | `ORPHAN_ORDER_ID`, `ORPHAN_PRODUCT_ID` |
| Payments | Amount numeric and greater than 0 | `INVALID_PAYMENT_AMOUNT` |
| Payments | Status is SUCCESS, PAID, COMPLETED, FAILED, DECLINED, PENDING, or REFUNDED | `INVALID_PAYMENT_STATUS` |
| Payments | `order_id` must exist in Silver orders | `ORPHAN_ORDER_ID` |
| Payments | A JSON line that does not match the schema | `MALFORMED_JSON` |
| Payments | Extra copy of `payment_id` | `DUPLICATE_PAYMENT_ID` |

`PAID` and `COMPLETED` become `SUCCESS`. `DECLINED` becomes `FAILED`. Country and state aliases such as `USA` and `tx` are standardized. They are not quarantined.

`N/A`, `NULL`, `NA`, and `NONE` are treated as null before the rules run. The quarantine payload still contains the original text.

## Report columns

`data_quality_report.csv` has one row per dataset:

- `total_records`: Bronze rows, including malformed JSON lines
- `valid_records`: rows written to Silver
- `invalid_records`: rows written to Quarantine
- `duplicate_records`: extra copies of a non-null business key
- `null_records`: rows with at least one required field null
- `quality_percentage`: `valid_records / total_records`

`valid_records + invalid_records` equals `total_records`. Duplicate and null counts can overlap with invalid rows, so they are not added into that equation.

The incremental batch is reported as `incremental_orders` and `incremental_order_items`.
