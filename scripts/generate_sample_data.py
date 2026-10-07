"""Generate the ShopSphere raw sample datasets.

The output is deterministic (seed 42) and includes realistic data-quality
defects so the pipeline has something meaningful to quarantine.

Run from the repository root:

    python scripts/generate_sample_data.py
"""

from __future__ import annotations

import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 42
ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
SAMPLE_DIR = ROOT / "data" / "sample"

FIRST_NAMES = [
    "Ava", "Noah", "Mia", "Liam", "Sophia", "Ethan", "Isla", "Mason",
    "Amelia", "Lucas", "Charlotte", "James", "Harper", "Benjamin", "Evelyn",
    "Henry", "Abigail", "Alexander", "Emily", "Daniel", "Elizabeth", "Michael",
    "Sofia", "William", "Avery", "Oliver", "Ella", "Jack", "Scarlett", "Leo",
    "Grace", "Samuel", "Chloe", "David", "Victoria", "Joseph", "Hannah", "Owen",
    "Lily", "Gabriel", "Zoe", "Carter", "Nora", "Wyatt", "Riley", "John",
    "Aria", "Luke", "Layla", "Nathan", "Penelope", "Caleb", "Aurora", "Isaac",
    "Savannah", "Adrian", "Brooklyn", "Jonathan", "Claire", "Thomas", "Paisley",
]
LAST_NAMES = [
    "Sharma", "Patel", "Johnson", "Williams", "Brown", "Garcia", "Martinez",
    "Davis", "Rodriguez", "Wilson", "Anderson", "Thomas", "Moore", "Jackson",
    "Martin", "Lee", "Perez", "Thompson", "White", "Harris", "Clark", "Lewis",
    "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott", "Green",
    "Baker", "Adams", "Nelson", "Hill", "Campbell", "Mitchell", "Roberts",
    "Carter", "Phillips", "Evans", "Turner", "Torres", "Parker", "Collins",
    "Edwards", "Stewart", "Morris", "Murphy", "Cook", "Rogers", "Morgan",
    "Peterson", "Cooper", "Reed", "Bailey", "Bell", "Gomez", "Kelly", "Howard",
    "Iyer", "Nair",
]
CITIES = [
    ("Austin", "TX", "Texas", "United States"),
    ("Seattle", "WA", "Washington", "United States"),
    ("Denver", "CO", "Colorado", "United States"),
    ("Chicago", "IL", "Illinois", "United States"),
    ("Boston", "MA", "Massachusetts", "United States"),
    ("Miami", "FL", "Florida", "United States"),
    ("Phoenix", "AZ", "Arizona", "United States"),
    ("Portland", "OR", "Oregon", "United States"),
    ("Atlanta", "GA", "Georgia", "United States"),
    ("Dallas", "TX", "Texas", "United States"),
    ("San Diego", "CA", "California", "United States"),
    ("Minneapolis", "MN", "Minnesota", "United States"),
    ("Nashville", "TN", "Tennessee", "United States"),
    ("Charlotte", "NC", "North Carolina", "United States"),
    ("Columbus", "OH", "Ohio", "United States"),
    ("Toronto", "ON", "Ontario", "Canada"),
    ("Vancouver", "BC", "British Columbia", "Canada"),
    ("London", "ENG", "England", "United Kingdom"),
    ("Manchester", "ENG", "England", "United Kingdom"),
    ("Mumbai", "MH", "Maharashtra", "India"),
    ("Pune", "MH", "Maharashtra", "India"),
    ("Bengaluru", "KA", "Karnataka", "India"),
]
COUNTRY_VARIANTS = {
    "United States": ["United States", "USA", "usa", "US", "United States of America"],
    "Canada": ["Canada", "canada", "CA"],
    "United Kingdom": ["United Kingdom", "UK", "uk"],
    "India": ["India", "india", "IN"],
}
CATALOG = [
    ("Electronics", "Audio", "AeroBeat Wireless Headphones", 49.0, 159.0),
    ("Electronics", "Audio", "Nimbus Bluetooth Speaker", 29.0, 99.0),
    ("Electronics", "Computers", "Harbor Laptop Sleeve", 24.0, 64.0),
    ("Electronics", "Computers", "Lumen USB-C Hub", 35.0, 89.0),
    ("Electronics", "Accessories", "Northline Phone Charger", 15.0, 39.0),
    ("Electronics", "Accessories", "Fieldday Smartwatch Band", 12.0, 34.0),
    ("Home", "Kitchen", "Copperlane Pour-Over Kettle", 28.0, 72.0),
    ("Home", "Kitchen", "Brightpath Chef Knife", 32.0, 110.0),
    ("Home", "Bedding", "Kinfolk Cotton Duvet Cover", 45.0, 140.0),
    ("Home", "Bedding", "Summit Linen Pillowcase Set", 22.0, 58.0),
    ("Home", "Decor", "Aero Ceramic Table Lamp", 38.0, 96.0),
    ("Home", "Decor", "Harbor Oak Picture Frame", 18.0, 46.0),
    ("Apparel", "Men", "Northline Oxford Shirt", 34.0, 78.0),
    ("Apparel", "Men", "Fieldday Chino Trousers", 42.0, 92.0),
    ("Apparel", "Women", "Lumen Wrap Dress", 48.0, 120.0),
    ("Apparel", "Women", "Kinfolk Merino Sweater", 54.0, 130.0),
    ("Apparel", "Kids", "Brightpath Rain Jacket", 36.0, 74.0),
    ("Apparel", "Kids", "Summit Canvas Sneakers", 40.0, 85.0),
    ("Beauty", "Skincare", "Aero Daily Moisturizer", 16.0, 38.0),
    ("Beauty", "Skincare", "Nimbus Mineral Sunscreen", 14.0, 32.0),
    ("Beauty", "Hair", "Harbor Repair Shampoo", 12.0, 26.0),
    ("Beauty", "Hair", "Copperlane Leave-In Conditioner", 13.0, 28.0),
    ("Sports", "Fitness", "Fieldday Yoga Mat", 25.0, 60.0),
    ("Sports", "Fitness", "Summit Adjustable Dumbbell", 45.0, 150.0),
    ("Sports", "Outdoor", "Northline Hiking Bottle", 18.0, 36.0),
    ("Sports", "Outdoor", "Lumen Daypack", 42.0, 110.0),
    ("Books", "Fiction", "Kinfolk Press River Town", 14.0, 24.0),
    ("Books", "Fiction", "Brightpath The Glass Orchard", 15.0, 26.0),
    ("Books", "Business", "Harbor Notes on Operations", 22.0, 36.0),
    ("Books", "Business", "Aero Practical Analytics", 28.0, 42.0),
    ("Grocery", "Pantry", "Copperlane Olive Oil", 11.0, 22.0),
    ("Grocery", "Pantry", "Nimbus Rolled Oats", 6.0, 12.0),
    ("Grocery", "Beverages", "Summit Single-Origin Coffee", 13.0, 24.0),
    ("Grocery", "Beverages", "Fieldday Green Tea", 8.0, 16.0),
    ("Electronics", "Audio", "Kinfolk Studio Microphone", 70.0, 180.0),
    ("Home", "Kitchen", "Lumen Cast Iron Skillet", 30.0, 80.0),
]
VARIANTS = ["Black", "Silver", "Navy", "Sand", "Forest", "Slate"]
ORDER_STATUSES = ["DELIVERED", "DELIVERED", "DELIVERED", "SHIPPED", "CONFIRMED", "PENDING", "CANCELLED"]
PAYMENT_METHODS = ["credit card", "Credit Card", "CREDIT_CARD", "debit card", "paypal", "UPI", "gift card"]
PAYMENT_STATUSES = ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "Paid", "FAILED", "PENDING", "REFUNDED", "Declined"]
INVALID_EMAILS = ["not-an-email", "emma.shop", "jane@", "noah@@example.com", "liam example.com", "mia@.com"]
INVALID_DATES = ["2024-13-40", "15/01/2024", "yesterday", "2024-02-31", "00-00-0000", "2026/09/01"]
INVALID_STATUSES = ["RETURNED", "HOLD", "unknown", "IN_TRANSIT", "lost", ""]


def _money(value: float) -> str:
    return f"{value:.2f}"


def _phone(index: int) -> str:
    return f"({200 + (index % 700):03d}) 555-{index % 10000:04d}"


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _country_value(canonical: str, index: int) -> str:
    options = COUNTRY_VARIANTS[canonical]
    if index % 5 == 0:
        return options[index % len(options)]
    return canonical


def _state_value(abbr: str, full_name: str, index: int) -> str:
    if index % 4 == 0:
        return full_name
    if index % 4 == 1:
        return abbr.lower()
    return abbr


def build_customers(rng: random.Random) -> tuple[list[dict[str, str]], list[str]]:
    rows: list[dict[str, str]] = []
    clean_ids: list[str] = []
    start = date(2021, 3, 1)
    for index in range(500):
        customer_id = f"CUST-{100000 + index}"
        first = FIRST_NAMES[index % len(FIRST_NAMES)]
        last = LAST_NAMES[(index * 3) % len(LAST_NAMES)]
        city, abbr, full_state, country = CITIES[index % len(CITIES)]
        signup = start + timedelta(days=(index * 3) % 1100)
        row = {
            "customer_id": customer_id,
            "first_name": f"  {first}  " if index % 17 == 0 else first,
            "last_name": last,
            "email": f"{first}.{last}.{index}@example.com".lower(),
            "phone": _phone(index),
            "city": city,
            "state": _state_value(abbr, full_state, index),
            "country": _country_value(country, index),
            "signup_date": signup.isoformat(),
        }
        if index < 12:
            row["email"] = ""
        elif index < 20:
            row["email"] = INVALID_EMAILS[(index - 12) % len(INVALID_EMAILS)]
        elif index < 35:
            row["city"] = ""
        elif index < 41:
            row["signup_date"] = INVALID_DATES[(index - 35) % len(INVALID_DATES)]
        else:
            clean_ids.append(customer_id)
        rows.append(row)

    for offset in range(10):
        source = dict(rows[80 + offset])
        source["phone"] = ""
        source["first_name"] = source["first_name"].strip()
        rows.append(source)

    for _ in range(8):
        blank = dict(rows[100])
        blank["customer_id"] = ""
        blank["email"] = "missing.id@example.com"
        rows.append(blank)
    return rows, clean_ids


def build_products() -> tuple[list[dict[str, str]], list[str]]:
    rows: list[dict[str, str]] = []
    index = 0
    for category, subcategory, name, low, high in CATALOG:
        for variant_index, variant in enumerate(VARIANTS):
            if index >= 210:
                break
            price = low + ((high - low) * ((variant_index + 1) / len(VARIANTS)))
            cost = price * (0.45 + (variant_index * 0.03))
            rows.append(
                {
                    "product_id": f"PRD-{10000 + index}",
                    "product_name": f"{name} - {variant}",
                    "category": category,
                    "subcategory": subcategory,
                    "price": _money(price),
                    "cost": _money(cost),
                    "stock_quantity": str(20 + ((index * 7) % 180)),
                }
            )
            index += 1
        if index >= 210:
            break

    for defect_index in range(6):
        rows[defect_index]["price"] = _money(-1 * (defect_index + 1))
    for defect_index in range(6, 11):
        rows[defect_index]["price"] = "N/A" if defect_index % 2 == 0 else ""
    for defect_index in range(11, 16):
        rows[defect_index]["stock_quantity"] = str(-1 * (defect_index - 10))
    for defect_index in range(16, 22):
        rows[defect_index]["category"] = "Miscellaneous" if defect_index % 2 == 0 else "Unknown"
    for defect_index in range(22, 25):
        rows[defect_index]["cost"] = ""

    clean_ids = [row["product_id"] for row in rows[25:]]
    for offset in range(6):
        source = dict(rows[40 + offset])
        source["subcategory"] = ""
        rows.append(source)
    for _ in range(4):
        blank = dict(rows[50])
        blank["product_id"] = ""
        rows.append(blank)
    return rows, clean_ids


def build_orders(rng: random.Random, clean_customer_ids: list[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    window_start = date(2024, 6, 1)
    window_days = (date(2026, 9, 15) - window_start).days
    for index in range(2000):
        city, abbr, full_state, country = CITIES[index % len(CITIES)]
        order_date = window_start + timedelta(days=rng.randrange(window_days + 1))
        status = ORDER_STATUSES[index % len(ORDER_STATUSES)]
        if index % 3 == 0:
            status = status.lower()
        row = {
            "order_id": f"ORD-{1000000 + index}",
            "customer_id": clean_customer_ids[index % len(clean_customer_ids)],
            "order_date": order_date.isoformat(),
            "order_status": status,
            "shipping_city": city,
            "shipping_state": _state_value(abbr, full_state, index),
            "shipping_country": _country_value(country, index),
        }
        if index < 20:
            row["customer_id"] = ""
        elif index < 45:
            row["customer_id"] = "CUST-DOES-NOT-EXIST"
        elif index < 57:
            row["order_date"] = INVALID_DATES[(index - 45) % len(INVALID_DATES)]
        elif index < 82:
            row["order_status"] = INVALID_STATUSES[(index - 57) % len(INVALID_STATUSES)]
        elif index < 92:
            row["order_date"] = "2020-01-15"
            row["order_status"] = "DELIVERED"
        rows.append(row)

    for offset in range(15):
        source = dict(rows[200 + offset])
        source["shipping_city"] = ""
        rows.append(source)
    for _ in range(10):
        blank = dict(rows[300])
        blank["order_id"] = ""
        rows.append(blank)
    return rows


def build_order_items(
    rng: random.Random, orders: list[dict[str, str]], clean_product_ids: list[str]
) -> tuple[list[dict[str, str]], dict[str, float]]:
    rows: list[dict[str, str]] = []
    nets: dict[str, float] = {}
    sequence = 0
    for order_index, order in enumerate(orders):
        if not order["order_id"]:
            continue
        item_count = 2 if order_index % 2 == 0 else 3
        order_net = 0.0
        for _ in range(item_count):
            price = 8 + (sequence % 40) + ((sequence % 4) * 0.25)
            discount = (sequence % 5) * 5
            quantity = 1 + (sequence % 3)
            rows.append(
                {
                    "order_item_id": f"ITM-{10000000 + sequence}",
                    "order_id": order["order_id"],
                    "product_id": clean_product_ids[sequence % len(clean_product_ids)],
                    "quantity": str(quantity),
                    "unit_price": _money(price),
                    "discount": str(discount),
                }
            )
            order_net += quantity * price * (1 - discount / 100)
            sequence += 1
        nets[order["order_id"]] = round(order_net, 2)

    for index in range(20):
        rows[index]["quantity"] = "0" if index % 2 == 0 else "-1"
    for index in range(20, 40):
        rows[index]["unit_price"] = "0" if index % 2 == 0 else "-5.00"
    for index in range(40, 55):
        rows[index]["discount"] = "-10"
    for index in range(55, 70):
        rows[index]["discount"] = "150"
    for index in range(70, 95):
        rows[index]["product_id"] = "PRD-MISSING"
    for index in range(95, 120):
        rows[index]["order_id"] = "ORD-MISSING"

    for offset in range(8):
        source = dict(rows[400 + offset])
        source["discount"] = ""
        rows.append(source)
    for _ in range(5):
        blank = dict(rows[500])
        blank["order_item_id"] = ""
        rows.append(blank)
    return rows, nets


def build_payments(
    rng: random.Random, orders: list[dict[str, str]], nets: dict[str, float]
) -> list[dict[str, object]]:
    payments: list[dict[str, object]] = []
    sequence = 0
    for order in orders:
        if not order["order_id"]:
            continue
        order_date = order["order_date"]
        try:
            paid_on = date.fromisoformat(order_date) + timedelta(days=sequence % 3)
            payment_date = paid_on.isoformat()
        except ValueError:
            payment_date = "2024-13-01"
        amount = nets.get(order["order_id"], round(rng.uniform(15, 180), 2))
        payments.append(
            {
                "payment_id": f"PAY-{1000000 + sequence}",
                "order_id": order["order_id"],
                "payment_date": payment_date,
                "payment_method": PAYMENT_METHODS[sequence % len(PAYMENT_METHODS)],
                "payment_status": PAYMENT_STATUSES[sequence % len(PAYMENT_STATUSES)],
                "amount": round(float(amount), 2),
            }
        )
        sequence += 1

    for index in range(15):
        payments[index]["order_id"] = None
    for index in range(15, 30):
        payments[index]["amount"] = -1 * (index - 14)
    for index in range(30, 42):
        payments[index]["payment_status"] = "CHARGEBACK" if index % 2 == 0 else "unknown"
    for index in range(42, 50):
        payments[index]["payment_date"] = "2024-02-31"

    for offset in range(10):
        source = dict(payments[80 + offset])
        source["payment_date"] = "2024-06-01"
        payments.append(source)
    return payments


def build_late_orders(clean_customer_ids: list[str], clean_product_ids: list[str]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    late_orders: list[dict[str, str]] = []
    late_items: list[dict[str, str]] = []
    for index in range(6):
        order_id = f"ORD-{900001 + index}"
        city, abbr, full_state, country = CITIES[index]
        late_orders.append(
            {
                "order_id": order_id,
                "customer_id": clean_customer_ids[index],
                "order_date": (date(2026, 9, 20) + timedelta(days=index)).isoformat(),
                "order_status": "DELIVERED",
                "shipping_city": city,
                "shipping_state": abbr,
                "shipping_country": country,
            }
        )
        late_items.append(
            {
                "order_item_id": f"ITM-{90000001 + index}",
                "order_id": order_id,
                "product_id": clean_product_ids[index],
                "quantity": "2",
                "unit_price": "25.00",
                "discount": "10",
            }
        )
    for index in range(2):
        city, abbr, _, country = CITIES[index]
        late_orders.append(
            {
                "order_id": f"ORD-{910001 + index}",
                "customer_id": clean_customer_ids[index],
                "order_date": "2024-06-01",
                "order_status": "CONFIRMED",
                "shipping_city": city,
                "shipping_state": abbr,
                "shipping_country": country,
            }
        )
    return late_orders, late_items


def main() -> None:
    rng = random.Random(SEED)
    customers, clean_customer_ids = build_customers(rng)
    products, clean_product_ids = build_products()
    orders = build_orders(rng, clean_customer_ids)
    items, nets = build_order_items(rng, orders, clean_product_ids)
    payments = build_payments(rng, orders, nets)
    late_orders, late_items = build_late_orders(clean_customer_ids, clean_product_ids)

    _write_csv(
        RAW_DIR / "customers.csv",
        ["customer_id", "first_name", "last_name", "email", "phone", "city", "state", "country", "signup_date"],
        customers,
    )
    _write_csv(
        RAW_DIR / "products.csv",
        ["product_id", "product_name", "category", "subcategory", "price", "cost", "stock_quantity"],
        products,
    )
    _write_csv(
        RAW_DIR / "orders.csv",
        ["order_id", "customer_id", "order_date", "order_status", "shipping_city", "shipping_state", "shipping_country"],
        orders,
    )
    _write_csv(
        RAW_DIR / "order_items.csv",
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount"],
        items,
    )
    payments_path = RAW_DIR / "payments.json"
    payments_path.parent.mkdir(parents=True, exist_ok=True)
    with payments_path.open("w", encoding="utf-8") as handle:
        for payment in payments:
            handle.write(json.dumps(payment) + "\n")
        handle.write('{"payment_id": "PAY-BAD", "order_id": \n')
        handle.write("this is not json\n")
        handle.write('{"payment_id": "PAY-BAD2", "amount": }\n')
        handle.write('{"payment_id": \n')
        handle.write("{broken\n")
        handle.write('["not", "an", "object"]\n')
        handle.write('{"payment_id": "PAY-BAD3", "payment_status": \n')
        handle.write("}{\n")

    _write_csv(
        SAMPLE_DIR / "late_orders.csv",
        ["order_id", "customer_id", "order_date", "order_status", "shipping_city", "shipping_state", "shipping_country"],
        late_orders,
    )
    _write_csv(
        SAMPLE_DIR / "late_order_items.csv",
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount"],
        late_items,
    )
    print(f"customers={len(customers)}")
    print(f"products={len(products)}")
    print(f"orders={len(orders)}")
    print(f"order_items={len(items)}")
    print(f"payments={len(payments)} plus 8 malformed lines")
    print(f"late_orders={len(late_orders)}")
    print(f"late_order_items={len(late_items)}")
    print(f"clean_customers={len(clean_customer_ids)} clean_products={len(clean_product_ids)}")


if __name__ == "__main__":
    main()
