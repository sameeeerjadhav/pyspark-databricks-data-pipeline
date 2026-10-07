"""Silver calculations and the local upsert path."""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType

from src.incremental import filter_newer_than, upsert_by_key
from src.storage import read_table
from src.transformations.common import with_line_amounts
from src.transformations.customers import to_silver_customers
from src.transformations.orders import to_silver_orders


def test_revenue_calculation(spark):
    frame = spark.createDataFrame(
        [("ITM-1", 2, Decimal("10.00"), Decimal("10.00"))],
        StructType(
            [
                StructField("order_item_id", StringType()),
                StructField("quantity", IntegerType()),
                StructField("unit_price", DecimalType(18, 2)),
                StructField("discount", DecimalType(18, 2)),
            ]
        ),
    )
    row = with_line_amounts(frame).first()
    assert row["gross_sales"] == Decimal("20.00")
    assert row["discount_amount"] == Decimal("2.00")
    assert row["net_sales"] == Decimal("18.00")


def test_country_and_status_are_standardized(spark):
    customers = to_silver_customers(
        spark.createDataFrame(
            [("CUST-1", "ava", "sharma", "Ava@Example.com", "555", "austin", "tx", "USA", "2024-01-01")],
            ["customer_id", "first_name", "last_name", "email", "phone", "city", "state", "country", "signup_date"],
        )
    ).first()
    assert customers["country"] == "United States"
    assert customers["state"] == "Texas"
    assert customers["email"] == "ava@example.com"
    assert customers["city"] == "Austin"

    orders = to_silver_orders(
        spark.createDataFrame(
            [("ORD-1", "CUST-1", "2024-06-01", "delivered", "Austin", "TX", "usa")],
            ["order_id", "customer_id", "order_date", "order_status", "shipping_city", "shipping_state", "shipping_country"],
        )
    ).first()
    assert orders["order_status"] == "DELIVERED"
    assert orders["order_year"] == "2024"
    assert orders["shipping_country"] == "United States"


def test_watermark_filter_keeps_only_newer_orders(spark):
    orders = spark.createDataFrame(
        [("ORD-1", "2024-06-01"), ("ORD-2", "2024-07-01")],
        ["order_id", "order_date"],
    )
    kept = [row["order_id"] for row in filter_newer_than(orders, "order_date", "2024-06-15").collect()]
    assert kept == ["ORD-2"]


def test_parquet_upsert_updates_existing_key_and_inserts_new_key(spark, tmp_path):
    path = str(tmp_path / "orders")
    existing = spark.createDataFrame(
        [("ORD-1", "SHIPPED", "2024-06-01 00:00:00")],
        ["order_id", "order_status", "ingestion_timestamp"],
    )
    upsert_by_key(spark, path, existing, "order_id", storage_format="parquet")
    incoming = spark.createDataFrame(
        [
            ("ORD-1", "DELIVERED", "2024-06-02 00:00:00"),
            ("ORD-2", "CONFIRMED", "2024-06-03 00:00:00"),
        ],
        ["order_id", "order_status", "ingestion_timestamp"],
    )
    upsert_by_key(spark, path, incoming, "order_id", storage_format="parquet")
    rows = {row["order_id"]: row["order_status"] for row in read_table(spark, path, "parquet").collect()}
    assert rows == {"ORD-1": "DELIVERED", "ORD-2": "CONFIRMED"}
