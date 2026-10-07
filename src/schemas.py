"""Explicit source schemas.

Bronze ingestion reads every business field as a string so malformed values
are preserved and validated later instead of being dropped by Spark's parser.
"""

from pyspark.sql.types import StringType, StructField, StructType


def _strings(*columns: str) -> StructType:
    return StructType([StructField(name, StringType(), True) for name in columns])


CUSTOMER_COLUMNS = (
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "city",
    "state",
    "country",
    "signup_date",
)
PRODUCT_COLUMNS = (
    "product_id",
    "product_name",
    "category",
    "subcategory",
    "price",
    "cost",
    "stock_quantity",
)
ORDER_COLUMNS = (
    "order_id",
    "customer_id",
    "order_date",
    "order_status",
    "shipping_city",
    "shipping_state",
    "shipping_country",
)
ORDER_ITEM_COLUMNS = (
    "order_item_id",
    "order_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount",
)
PAYMENT_COLUMNS = (
    "payment_id",
    "order_id",
    "payment_date",
    "payment_method",
    "payment_status",
    "amount",
)

CUSTOMER_SCHEMA = _strings(*CUSTOMER_COLUMNS)
PRODUCT_SCHEMA = _strings(*PRODUCT_COLUMNS)
ORDER_SCHEMA = _strings(*ORDER_COLUMNS)
ORDER_ITEM_SCHEMA = _strings(*ORDER_ITEM_COLUMNS)
PAYMENT_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in PAYMENT_COLUMNS]
    + [StructField("_corrupt_record", StringType(), True)]
)
