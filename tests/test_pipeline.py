"""End-to-end run on a tiny local sample, including Gold revenue."""

from __future__ import annotations

import csv
import json
from decimal import Decimal
from pathlib import Path

from pyspark.sql.functions import col

from src.config import load_config
from src.gold.build_gold import build_gold_frames
from src.pipeline import run_pipeline
from src.storage import read_table


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def _seed_raw_data(root: Path) -> None:
    raw = root / "data" / "raw"
    sample = root / "data" / "sample"
    _write_csv(
        raw / "customers.csv",
        ["customer_id", "first_name", "last_name", "email", "phone", "city", "state", "country", "signup_date"],
        [
            ["CUST-1", "Ava", "Sharma", "ava@example.com", "555", "Austin", "TX", "USA", "2024-01-01"],
            ["CUST-2", "Noah", "Patel", "noah@example.com", "555", "Seattle", "WA", "United States", "2024-01-02"],
            ["CUST-1", "Ava", "Sharma", "ava@example.com", "", "Austin", "TX", "USA", "2024-01-01"],
            ["", "Mia", "Lee", "mia@example.com", "555", "Austin", "TX", "USA", "2024-01-03"],
            ["CUST-3", "Liam", "Brown", "not-an-email", "555", "Austin", "TX", "USA", "2024-01-04"],
        ],
    )
    _write_csv(
        raw / "products.csv",
        ["product_id", "product_name", "category", "subcategory", "price", "cost", "stock_quantity"],
        [
            ["PRD-1", "Headphones", "Electronics", "Audio", "10.00", "4.00", "5"],
            ["PRD-2", "Speaker", "Electronics", "Audio", "20.00", "8.00", "3"],
            ["PRD-3", "Bad Price", "Electronics", "Audio", "-1.00", "1.00", "2"],
        ],
    )
    _write_csv(
        raw / "orders.csv",
        ["order_id", "customer_id", "order_date", "order_status", "shipping_city", "shipping_state", "shipping_country"],
        [
            ["ORD-1", "CUST-1", "2024-06-01", "delivered", "Austin", "TX", "USA"],
            ["ORD-2", "CUST-2", "2024-06-02", "CANCELLED", "Seattle", "WA", "United States"],
            ["ORD-3", "CUST-1", "2024-06-03", "RETURNED", "Austin", "tx", "usa"],
            ["ORD-4", "CUST-999", "2024-06-04", "DELIVERED", "Austin", "TX", "USA"],
        ],
    )
    _write_csv(
        raw / "order_items.csv",
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount"],
        [
            ["ITM-1", "ORD-1", "PRD-1", "2", "10.00", "10"],
            ["ITM-2", "ORD-1", "PRD-2", "1", "20.00", "0"],
            ["ITM-3", "ORD-2", "PRD-1", "1", "10.00", "0"],
            ["ITM-4", "ORD-1", "PRD-1", "0", "10.00", "0"],
            ["ITM-5", "ORD-1", "PRD-1", "1", "0", "0"],
        ],
    )
    payments = raw / "payments.json"
    payments.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "payment_id": "PAY-1",
                        "order_id": "ORD-1",
                        "payment_date": "2024-06-01",
                        "payment_method": "credit card",
                        "payment_status": "Paid",
                        "amount": 38.0,
                    }
                ),
                json.dumps(
                    {
                        "payment_id": "PAY-2",
                        "order_id": "ORD-2",
                        "payment_date": "2024-06-02",
                        "payment_method": "paypal",
                        "payment_status": "FAILED",
                        "amount": 10.0,
                    }
                ),
                json.dumps(
                    {
                        "payment_id": "PAY-3",
                        "order_id": "ORD-1",
                        "payment_date": "2024-06-01",
                        "payment_method": "UPI",
                        "payment_status": "SUCCESS",
                        "amount": -5,
                    }
                ),
                '{"payment_id": "PAY-BAD", "order_id": ',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    _write_csv(
        sample / "late_orders.csv",
        ["order_id", "customer_id", "order_date", "order_status", "shipping_city", "shipping_state", "shipping_country"],
        [
            ["ORD-9", "CUST-1", "2024-07-01", "DELIVERED", "Austin", "TX", "USA"],
            ["ORD-8", "CUST-1", "2024-01-15", "CONFIRMED", "Austin", "TX", "USA"],
        ],
    )
    _write_csv(
        sample / "late_order_items.csv",
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount"],
        [["ITM-9", "ORD-9", "PRD-1", "1", "10.00", "0"]],
    )


def test_gold_frames_exclude_cancelled_orders(spark):
    from datetime import date

    orders = spark.createDataFrame(
        [("ORD-1", "CUST-1", date(2024, 6, 1), "DELIVERED"), ("ORD-2", "CUST-1", date(2024, 6, 2), "CANCELLED")],
        ["order_id", "customer_id", "order_date", "order_status"],
    )
    items = spark.createDataFrame(
        [
            ("ORD-1", "PRD-1", 2, Decimal("20.00"), Decimal("2.00"), Decimal("18.00")),
            ("ORD-1", "PRD-2", 1, Decimal("20.00"), Decimal("0.00"), Decimal("20.00")),
            ("ORD-2", "PRD-1", 1, Decimal("10.00"), Decimal("0.00"), Decimal("10.00")),
        ],
        ["order_id", "product_id", "quantity", "gross_sales", "discount_amount", "net_sales"],
    )
    products = spark.createDataFrame(
        [("PRD-1", "Headphones", "Electronics"), ("PRD-2", "Speaker", "Electronics")],
        ["product_id", "product_name", "category"],
    )
    payments = spark.createDataFrame(
        [("CREDIT_CARD", "SUCCESS", Decimal("38.00")), ("PAYPAL", "FAILED", Decimal("10.00"))],
        ["payment_method", "payment_status", "amount"],
    )
    tables = build_gold_frames(orders, items, products, payments)
    try:
        daily = {row["date"].isoformat(): row["net_sales"] for row in tables["daily_sales"].collect()}
        assert daily == {"2024-06-01": Decimal("38.00")}
        customer = tables["customer_sales_summary"].first()
        assert customer["total_orders"] == 1
        assert customer["total_spend"] == Decimal("38.00")
        assert customer["average_order_value"] == Decimal("38.00")
    finally:
        tables["_fact"].unpersist()


def test_pipeline_end_to_end(spark, tmp_path, monkeypatch):
    _seed_raw_data(tmp_path)
    monkeypatch.setenv("PIPELINE_BASE_PATH", str(tmp_path))
    monkeypatch.setenv("ENVIRONMENT", "local")
    monkeypatch.setenv("PIPELINE_STORAGE_FORMAT", "parquet")
    config = load_config()
    run_pipeline(spark, config)

    customers = read_table(spark, str(tmp_path / "data" / "silver" / "customers"))
    assert customers.filter(col("customer_id") == "CUST-1").first()["country"] == "United States"
    assert customers.count() == 2

    daily = {
        row["date"].isoformat(): row["net_sales"]
        for row in read_table(spark, str(tmp_path / "data" / "gold" / "daily_sales")).collect()
    }
    assert daily["2024-06-01"] == Decimal("38.00")
    assert daily["2024-07-01"] == Decimal("10.00")

    quarantine = read_table(spark, str(tmp_path / "data" / "quarantine" / "customers"))
    assert quarantine.count() >= 1
    report = (tmp_path / "data_quality_report.csv").read_text(encoding="utf-8")
    assert "customers" in report
    metrics = (tmp_path / "pipeline_metrics.csv").read_text(encoding="utf-8")
    assert "SUCCESS" in metrics
    assert (tmp_path / "data" / "checkpoints" / "orders_watermark").exists()
