"""Validation rules on small DataFrames."""

from __future__ import annotations

from pyspark.sql.functions import col

from src.quality.validators import (
    evaluate_customers,
    evaluate_order_items,
    evaluate_orders,
    evaluate_products,
    flag_duplicate_extras,
    flag_orphans,
)
from src.schemas import CUSTOMER_COLUMNS, CUSTOMER_SCHEMA, ORDER_ITEM_SCHEMA, ORDER_SCHEMA, PRODUCT_SCHEMA
from src.transformations.common import completeness


def _errors(df, key_column: str, key_value: str) -> list[str]:
    row = df.filter(col(key_column) == key_value).select("validation_errors").first()
    assert row is not None
    return list(row["validation_errors"])


def test_null_customer_id_and_invalid_email(spark):
    rows = [
        ("", "Ava", "Sharma", "ava@example.com", "555", "Austin", "TX", "USA", "2024-01-01"),
        ("CUST-2", "Noah", "Patel", "not-an-email", "555", "Seattle", "WA", "USA", "2024-01-02"),
        ("CUST-3", "Mia", "Lee", "", "555", "Dallas", "TX", "USA", "2024-01-03"),
    ]
    result = evaluate_customers(spark.createDataFrame(rows, CUSTOMER_SCHEMA))
    assert "MISSING_CUSTOMER_ID" in result.filter(col("customer_id").isNull()).first()["validation_errors"]
    assert "INVALID_EMAIL" in _errors(result, "customer_id", "CUST-2")
    assert "MISSING_EMAIL" in _errors(result, "customer_id", "CUST-3")


def test_duplicate_customer_keeps_the_more_complete_row(spark):
    rows = [
        ("CUST-1", "Ava", "Sharma", "ava@example.com", "555", "Austin", "TX", "USA", "2024-01-01"),
        ("CUST-1", "Ava", "Sharma", "ava@example.com", "", "Austin", "TX", "USA", "2024-01-01"),
    ]
    evaluated = evaluate_customers(spark.createDataFrame(rows, CUSTOMER_SCHEMA))
    flagged = flag_duplicate_extras(
        evaluated,
        "customer_id",
        "DUPLICATE_CUSTOMER_ID",
        [completeness(*CUSTOMER_COLUMNS).desc(), col("signup_date").desc_nulls_last()],
    )
    kept = flagged.filter(col("phone").isNotNull()).first()
    duplicate = flagged.filter(col("phone").isNull()).first()
    assert list(kept["validation_errors"]) == []
    assert "DUPLICATE_CUSTOMER_ID" in list(duplicate["validation_errors"])


def test_invalid_price_and_category(spark):
    rows = [
        ("PRD-1", "Lamp", "Electronics", "Decor", "-5.00", "2.00", "4"),
        ("PRD-2", "Mug", "Miscellaneous", "Kitchen", "12.00", "4.00", "3"),
        ("PRD-3", "Cup", "Home", "Kitchen", "8.00", "3.00", "-1"),
    ]
    result = evaluate_products(spark.createDataFrame(rows, PRODUCT_SCHEMA))
    assert "INVALID_PRICE" in _errors(result, "product_id", "PRD-1")
    assert "INVALID_CATEGORY" in _errors(result, "product_id", "PRD-2")
    assert "INVALID_STOCK_QUANTITY" in _errors(result, "product_id", "PRD-3")


def test_invalid_quantity_price_and_status(spark):
    items = spark.createDataFrame(
        [
            ("ITM-1", "ORD-1", "PRD-1", "0", "10.00", "0"),
            ("ITM-2", "ORD-1", "PRD-1", "1", "0", "0"),
            ("ITM-3", "ORD-1", "PRD-1", "1", "10.00", "-5"),
        ],
        ORDER_ITEM_SCHEMA,
    )
    item_errors = evaluate_order_items(items)
    assert "INVALID_QUANTITY" in _errors(item_errors, "order_item_id", "ITM-1")
    assert "INVALID_UNIT_PRICE" in _errors(item_errors, "order_item_id", "ITM-2")
    assert "INVALID_DISCOUNT" in _errors(item_errors, "order_item_id", "ITM-3")

    orders = evaluate_orders(
        spark.createDataFrame(
            [("ORD-9", "CUST-1", "2024-06-01", "RETURNED", "Austin", "TX", "USA")],
            ORDER_SCHEMA,
        )
    )
    assert "INVALID_ORDER_STATUS" in _errors(orders, "order_id", "ORD-9")


def test_referential_integrity(spark):
    orders = evaluate_orders(
        spark.createDataFrame(
            [("ORD-1", "CUST-404", "2024-06-01", "DELIVERED", "Austin", "TX", "USA")],
            ORDER_SCHEMA,
        )
    )
    parents = spark.createDataFrame([("CUST-1",)], ["customer_id"])
    flagged = flag_orphans(orders, "customer_id", parents, "customer_id", "ORPHAN_CUSTOMER_ID")
    assert "ORPHAN_CUSTOMER_ID" in _errors(flagged, "order_id", "ORD-1")
