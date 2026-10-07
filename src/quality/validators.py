"""Reusable PySpark validation rules.

Rules add a validation_errors array. An empty array means the row passed.
Duplicate extras and referential failures are applied after the field rules so
a valid copy of a key is not discarded because a worse copy failed first.
"""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.functions import (
    array,
    array_compact,
    array_join,
    array_union,
    broadcast,
    col,
    count,
    current_timestamp,
    initcap,
    lit,
    row_number,
    size,
    struct,
    sum as spark_sum,
    to_date,
    to_json,
    trim,
    upper,
    when,
)
from pyspark.sql.types import DecimalType

from src.config import PAYMENT_STATUS_MAP, VALID_CATEGORIES, VALID_ORDER_STATUSES
from src.schemas import (
    CUSTOMER_COLUMNS,
    ORDER_COLUMNS,
    ORDER_ITEM_COLUMNS,
    PAYMENT_COLUMNS,
    PRODUCT_COLUMNS,
)
from src.transformations.common import completeness, is_blank, standardize_strings

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
NUMBER_PATTERN = r"^-?\d+(\.\d+)?$"
INTEGER_PATTERN = r"^-?\d+$"


def assert_required_columns(df: DataFrame, columns: tuple[str, ...], dataset: str) -> None:
    missing = [name for name in columns if name not in df.columns]
    if missing:
        raise ValueError(f"{dataset} schema validation failed, missing columns: {missing}")


def prepare(df: DataFrame, columns: tuple[str, ...]) -> DataFrame:
    """Keep the original payload, then normalize blanks and sentinel strings."""
    original = df.withColumn("_record_json", to_json(struct(*[col(name) for name in columns])))
    return standardize_strings(original, columns)


def with_error_codes(df: DataFrame, rules: list[tuple[Column, str]]) -> DataFrame:
    flags = [when(condition, lit(code)) for condition, code in rules]
    return df.withColumn("validation_errors", array_compact(array(*flags)))


def split_by_validation(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df.filter(size(col("validation_errors")) == 0)
    invalid = df.filter(size(col("validation_errors")) > 0)
    return valid, invalid


def is_not_number(column: str) -> Column:
    return ~is_blank(column) & ~trim(col(column)).rlike(NUMBER_PATTERN)


def is_not_integer(column: str) -> Column:
    return ~is_blank(column) & ~trim(col(column)).rlike(INTEGER_PATTERN)


def is_bad_date(column: str) -> Column:
    return ~is_blank(column) & to_date(trim(col(column)), "yyyy-MM-dd").isNull()


def number_value(column: str) -> Column:
    return trim(col(column)).cast(DecimalType(18, 2))


def zero() -> Column:
    return lit(Decimal("0")).cast(DecimalType(18, 2))


def hundred() -> Column:
    return lit(Decimal("100")).cast(DecimalType(18, 2))


def customer_rules() -> list[tuple[Column, str]]:
    return [
        (is_blank("customer_id"), "MISSING_CUSTOMER_ID"),
        (is_blank("email"), "MISSING_EMAIL"),
        (~is_blank("email") & ~trim(col("email")).rlike(EMAIL_PATTERN), "INVALID_EMAIL"),
        (is_blank("city"), "MISSING_CITY"),
        (is_blank("signup_date"), "MISSING_SIGNUP_DATE"),
        (is_bad_date("signup_date"), "INVALID_SIGNUP_DATE"),
    ]


def product_rules() -> list[tuple[Column, str]]:
    category = initcap(trim(col("category")))
    return [
        (is_blank("product_id"), "MISSING_PRODUCT_ID"),
        (is_blank("price") | is_not_number("price") | (number_value("price") <= zero()), "INVALID_PRICE"),
        (is_blank("cost") | is_not_number("cost") | (number_value("cost") < zero()), "INVALID_COST"),
        (
            is_blank("stock_quantity")
            | is_not_integer("stock_quantity")
            | (trim(col("stock_quantity")).cast("int") < 0),
            "INVALID_STOCK_QUANTITY",
        ),
        (is_blank("category") | ~category.isin(*VALID_CATEGORIES), "INVALID_CATEGORY"),
    ]


def order_rules() -> list[tuple[Column, str]]:
    status = upper(trim(col("order_status")))
    return [
        (is_blank("order_id"), "MISSING_ORDER_ID"),
        (is_blank("customer_id"), "MISSING_CUSTOMER_ID"),
        (is_blank("order_date"), "MISSING_ORDER_DATE"),
        (is_bad_date("order_date"), "INVALID_ORDER_DATE"),
        (is_blank("order_status") | ~status.isin(*VALID_ORDER_STATUSES), "INVALID_ORDER_STATUS"),
    ]


def order_item_rules() -> list[tuple[Column, str]]:
    return [
        (is_blank("order_item_id"), "MISSING_ORDER_ITEM_ID"),
        (is_blank("order_id"), "MISSING_ORDER_ID"),
        (is_blank("product_id"), "MISSING_PRODUCT_ID"),
        (
            is_blank("quantity") | is_not_integer("quantity") | (trim(col("quantity")).cast("int") <= 0),
            "INVALID_QUANTITY",
        ),
        (is_blank("unit_price") | is_not_number("unit_price") | (number_value("unit_price") <= zero()), "INVALID_UNIT_PRICE"),
        (
            ~is_blank("discount")
            & (is_not_number("discount") | (number_value("discount") < zero()) | (number_value("discount") > hundred())),
            "INVALID_DISCOUNT",
        ),
    ]


def payment_rules(has_corrupt_column: bool = False) -> list[tuple[Column, str]]:
    corrupt = col("_corrupt_record").isNotNull() if has_corrupt_column else lit(False)
    status = upper(trim(col("payment_status")))
    rules = [
        (~corrupt & is_blank("payment_id"), "MISSING_PAYMENT_ID"),
        (~corrupt & is_blank("order_id"), "MISSING_ORDER_ID"),
        (~corrupt & (is_blank("amount") | is_not_number("amount") | (number_value("amount") <= zero())), "INVALID_PAYMENT_AMOUNT"),
        (~corrupt & (is_blank("payment_status") | ~status.isin(*PAYMENT_STATUS_MAP.keys())), "INVALID_PAYMENT_STATUS"),
        (~corrupt & is_bad_date("payment_date"), "INVALID_PAYMENT_DATE"),
    ]
    if has_corrupt_column:
        return [(corrupt, "MALFORMED_JSON"), *rules]
    return rules


def evaluate_customers(df: DataFrame) -> DataFrame:
    assert_required_columns(df, CUSTOMER_COLUMNS, "customers")
    return with_error_codes(prepare(df, CUSTOMER_COLUMNS), customer_rules())


def evaluate_products(df: DataFrame) -> DataFrame:
    assert_required_columns(df, PRODUCT_COLUMNS, "products")
    return with_error_codes(prepare(df, PRODUCT_COLUMNS), product_rules())


def evaluate_orders(df: DataFrame) -> DataFrame:
    assert_required_columns(df, ORDER_COLUMNS, "orders")
    return with_error_codes(prepare(df, ORDER_COLUMNS), order_rules())


def evaluate_order_items(df: DataFrame) -> DataFrame:
    assert_required_columns(df, ORDER_ITEM_COLUMNS, "order_items")
    return with_error_codes(prepare(df, ORDER_ITEM_COLUMNS), order_item_rules())


def evaluate_payments(df: DataFrame) -> DataFrame:
    assert_required_columns(df, PAYMENT_COLUMNS, "payments")
    prepared = prepare(df, PAYMENT_COLUMNS)
    return with_error_codes(prepared, payment_rules("_corrupt_record" in df.columns))


def flag_duplicate_extras(
    df: DataFrame,
    key: str,
    error_code: str,
    rank_columns: list[Column],
) -> DataFrame:
    """Mark every copy after the preferred row. Null keys are not duplicates of each other."""
    window = Window.partitionBy(col(key)).orderBy(*rank_columns)
    ranked = df.withColumn(
        "_key_rank",
        when(col(key).isNotNull(), row_number().over(window)).otherwise(lit(1)),
    )
    duplicate = col("_key_rank") > 1
    flagged = ranked.withColumn(
        "validation_errors",
        when(duplicate, array_union(col("validation_errors"), array(lit(error_code)))).otherwise(col("validation_errors")),
    )
    return flagged.drop("_key_rank")


def flag_orphans(
    df: DataFrame,
    foreign_key: str,
    parents: DataFrame,
    parent_key: str,
    error_code: str,
) -> DataFrame:
    parent_keys = parents.select(col(parent_key).alias("_parent_key")).dropDuplicates(["_parent_key"])
    joined = df.join(broadcast(parent_keys), col(foreign_key) == col("_parent_key"), "left")
    orphan = col(foreign_key).isNotNull() & col("_parent_key").isNull()
    flagged = joined.withColumn(
        "validation_errors",
        when(orphan, array_union(col("validation_errors"), array(lit(error_code)))).otherwise(col("validation_errors")),
    )
    return flagged.drop("_parent_key")


def flag_orders_before_signup(orders: DataFrame, customers: DataFrame) -> DataFrame:
    signups = customers.select(col("customer_id").alias("_cid"), col("signup_date").alias("_signup"))
    joined = orders.join(broadcast(signups), col("customer_id") == col("_cid"), "left")
    too_early = (
        col("_signup").isNotNull()
        & to_date(col("order_date"), "yyyy-MM-dd").isNotNull()
        & (to_date(col("order_date"), "yyyy-MM-dd") < col("_signup"))
    )
    flagged = joined.withColumn(
        "validation_errors",
        when(too_early, array_union(col("validation_errors"), array(lit("ORDER_BEFORE_SIGNUP")))).otherwise(
            col("validation_errors")
        ),
    )
    return flagged.drop("_cid", "_signup")


def count_duplicate_extras(df: DataFrame, key: str) -> int:
    grouped = df.filter(col(key).isNotNull()).groupBy(col(key)).agg(count("*").alias("key_count"))
    extras = grouped.filter(col("key_count") > 1).agg(spark_sum(col("key_count") - 1).alias("extras")).first()
    if extras is None or extras["extras"] is None:
        return 0
    return int(extras["extras"])


def count_null_rows(df: DataFrame, columns: list[str]) -> int:
    condition = is_blank(columns[0])
    for name in columns[1:]:
        condition = condition | is_blank(name)
    return df.filter(condition).count()


def to_quarantine(df: DataFrame, dataset: str) -> DataFrame:
    source_file = col("source_file") if "source_file" in df.columns else lit(None).cast("string")
    record_json = col("_record_json") if "_record_json" in df.columns else lit("{}")
    return df.select(
        lit(dataset).alias("source_dataset"),
        array_join(col("validation_errors"), "|").alias("validation_errors"),
        current_timestamp().alias("validation_timestamp"),
        source_file.alias("source_file"),
        record_json.alias("record_json"),
    )
