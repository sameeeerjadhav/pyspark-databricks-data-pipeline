# Data dictionary

ShopSphere is a fictional retailer. Identifiers are synthetic. Dates in the sample run from 2021 through September 2026, with a small incremental batch after 15 September 2026.

`payments.json` is newline-delimited JSON. A normal JSON array would hide a single broken object. Newline-delimited JSON lets the permissive parser quarantine that line and keep the rest.

## Source files

### customers.csv

| Column | Description |
| --- | --- |
| customer_id | Business key, `CUST-100000` style |
| first_name, last_name | Customer name |
| email | Contact email |
| phone | Optional phone number |
| city, state, country | Signup location, with inconsistent casing and aliases |
| signup_date | `yyyy-MM-dd` when valid |

### products.csv

| Column | Description |
| --- | --- |
| product_id | Business key, `PRD-10000` style |
| product_name | Catalog name, including a variant such as color |
| category, subcategory | Merchandising hierarchy |
| price, cost | Decimal strings |
| stock_quantity | Integer stock on hand |

### orders.csv

| Column | Description |
| --- | --- |
| order_id | Business key |
| customer_id | Customer who placed the order |
| order_date | Order date |
| order_status | Lifecycle status |
| shipping_city, shipping_state, shipping_country | Shipping destination |

### order_items.csv

| Column | Description |
| --- | --- |
| order_item_id | Business key |
| order_id, product_id | Foreign keys |
| quantity | Units sold |
| unit_price | Price charged on the line |
| discount | Percent off the line, from 0 to 100 |

### payments.json

| Column | Description |
| --- | --- |
| payment_id | Business key |
| order_id | Order being paid |
| payment_date | Date the payment was recorded |
| payment_method | Card, PayPal, UPI, or gift card, before standardization |
| payment_status | Outcome, before standardization |
| amount | Payment amount |

## Silver additions

Silver keeps the cleaned business columns and adds `ingestion_timestamp` and `source_file`.

- `silver_customers.full_name` is the trimmed first and last name.
- `silver_orders.order_year` is the partition column.
- `silver_order_items.gross_sales`, `discount_amount`, and `net_sales` are the line amounts.
- Status, country, state, and payment method are stored in canonical form.

## Gold tables

| Table | Grain | Notes |
| --- | --- | --- |
| daily_sales | Order date | Recognized orders only |
| customer_sales_summary | Customer | Spend, order count, first and last order date |
| product_sales_summary | Product | Units, gross, discount, net |
| category_sales_summary | Category | Orders, units, net revenue |
| payment_summary | Payment method | Success count, failure count, successful amount |
| monthly_sales_summary | Year and month | Revenue and average order value |

Recognized orders are `CONFIRMED`, `SHIPPED`, and `DELIVERED`.

## Line math

```text
gross_sales = quantity * unit_price
discount_amount = gross_sales * discount / 100
net_sales = gross_sales - discount_amount
```

Average order value is net sales divided by distinct recognized orders.

Payment amount is not required to equal the order net. Matching them would be a separate reconciliation and is not a current rule.
