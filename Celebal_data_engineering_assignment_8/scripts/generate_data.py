"""
generate_data.py
-----------------
Spits out four raw CSVs (customers, products, orders, order_items) that LOOK like
they came from a real, slightly messy production database. That means duplicate
rows, missing values, inconsistent formatting, orphaned foreign keys, and a few
outright garbage records thrown in on purpose.

Why bother making fake data dirty? Because the whole point of this project is
to practice cleaning and validating data. If the input were already perfect
there'd be nothing to clean.

Run it standalone:
    python generate_data.py --customers 500 --orders 4000
"""

import argparse
import csv
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)  # keep runs reproducible while still "random enough"

OUT_DIR = Path(__file__).resolve().parent.parent / "data"

FIRST_NAMES = ["Aarav", "Isha", "Rohan", "Priya", "Kabir", "Ananya", "Vivaan",
               "Diya", "Arjun", "Meera", "Sai", "Neha", "Karan", "Riya", "Dev",
               "Tanya", "Aditya", "Sneha", "Rahul", "Pooja"]
LAST_NAMES = ["Sharma", "Verma", "Gupta", "Iyer", "Khan", "Patel", "Reddy",
              "Nair", "Chopra", "Malhotra", "Singh", "Das", "Bose", "Mehta"]

CITIES = [("Mumbai", "MH"), ("Delhi", "DL"), ("Bengaluru", "KA"),
          ("Jaipur", "RJ"), ("Pune", "MH"), ("Hyderabad", "TS"),
          ("Chennai", "TN"), ("Kolkata", "WB"), ("Ahmedabad", "GJ"),
          ("Lucknow", "UP")]

CATEGORIES = {
    "Electronics": ["Wireless Earbuds", "Bluetooth Speaker", "Smartwatch",
                    "Power Bank", "USB-C Cable", "Laptop Stand"],
    "Home & Kitchen": ["Non-stick Pan", "Electric Kettle", "Air Fryer",
                       "Storage Containers", "LED Bulb Pack"],
    "Fashion": ["Cotton T-Shirt", "Running Shoes", "Denim Jacket",
                "Leather Wallet", "Sunglasses"],
    "Books": ["Fiction Novel", "Self-Help Guide", "Cookbook",
              "Kids Storybook"],
    "Beauty": ["Face Serum", "Sunscreen SPF50", "Lip Balm Set", "Hair Oil"],
}

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "COD", "Net Banking",
                    "upi", "Cash on Delivery"]  # inconsistent casing/labels on purpose
ORDER_STATUSES = ["Delivered", "Shipped", "Cancelled", "Returned", "Pending",
                   "delivered", "DELIVERED"]  # again, messy on purpose


def _rand_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def _maybe_mangle_email(email: str) -> str:
    """Randomly break the email a bit -> blank, uppercase, extra whitespace, typo domain."""
    roll = random.random()
    if roll < 0.05:
        return ""  # missing
    if roll < 0.10:
        return email.upper()
    if roll < 0.14:
        return f"  {email} "  # stray whitespace
    if roll < 0.17:
        return email.replace(".com", ".con")  # typo
    return email


def generate_customers(n):
    rows = []
    used_ids = set()
    for i in range(1, n + 1):
        cust_id = f"CUST{i:05d}"
        used_ids.add(cust_id)
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        city, state = random.choice(CITIES)
        email = f"{first.lower()}.{last.lower()}{i}@example.com"
        email = _maybe_mangle_email(email)
        signup = _rand_date(datetime(2022, 1, 1), datetime(2025, 6, 30))

        rows.append({
            "customer_id": cust_id,
            "first_name": first if random.random() > 0.03 else first.lower(),
            "last_name": last,
            "email": email,
            "city": city if random.random() > 0.02 else city.upper(),
            "state": state,
            "signup_date": signup.strftime("%Y-%m-%d") if random.random() > 0.1
                           else signup.strftime("%d/%m/%Y"),  # inconsistent date format
        })

    # inject exact duplicate customer rows (simulates double form-submits)
    for _ in range(max(3, n // 100)):
        rows.append(dict(random.choice(rows)))

    # inject a couple of rows with a missing customer_id (bad export)
    for _ in range(2):
        bad = dict(random.choice(rows))
        bad["customer_id"] = ""
        rows.append(bad)

    random.shuffle(rows)
    return rows


def generate_products():
    rows = []
    pid = 1
    for category, items in CATEGORIES.items():
        for item in items:
            base_price = round(random.uniform(150, 8000), 2)
            rows.append({
                "product_id": f"P{pid:04d}",
                "product_name": item,
                "category": category,
                "unit_price": base_price,
                "active": random.choice([1, 1, 1, 0]),  # a few discontinued
            })
            pid += 1

    # a product with a null/garbage price (data entry error)
    rows.append({
        "product_id": f"P{pid:04d}",
        "product_name": "Mystery Combo Pack",
        "category": "Electronics",
        "unit_price": "",
        "active": 1,
    })
    pid += 1
    # a negative price glitch
    rows.append({
        "product_id": f"P{pid:04d}",
        "product_name": "Clearance Item",
        "category": "Fashion",
        "unit_price": -199.0,
        "active": 1,
    })
    return rows


def generate_orders(n, customer_ids):
    rows = []
    for i in range(1, n + 1):
        order_id = f"ORD{i:06d}"
        cust = random.choice(customer_ids)
        order_date = _rand_date(datetime(2023, 1, 1), datetime(2025, 12, 31))
        status = random.choice(ORDER_STATUSES)
        payment = random.choice(PAYMENT_METHODS)

        rows.append({
            "order_id": order_id,
            "customer_id": cust,
            "order_date": order_date.strftime("%Y-%m-%d"),
            "status": status,
            "payment_method": payment,
        })

    # a handful of orders pointing at a customer_id that doesn't exist (orphaned FK)
    for i in range(5):
        rows.append({
            "order_id": f"ORD{n + i + 1:06d}",
            "customer_id": f"CUST{99000 + i}",  # never generated
            "order_date": _rand_date(datetime(2023, 1, 1), datetime(2025, 12, 31)).strftime("%Y-%m-%d"),
            "status": random.choice(ORDER_STATUSES),
            "payment_method": random.choice(PAYMENT_METHODS),
        })

    # duplicate order_id, different data (simulates a retry bug in the checkout service)
    dupe = dict(rows[10])
    dupe["status"] = "Cancelled"
    rows.append(dupe)

    # an order with a totally malformed date
    rows[20] = dict(rows[20])
    rows[20]["order_date"] = "not-a-date"

    random.shuffle(rows)
    return rows


def generate_order_items(orders, products):
    rows = []
    item_no = 1
    product_ids = [p["product_id"] for p in products]

    for order in orders:
        # skip a few orders on purpose so they end up with zero items (edge case)
        if random.random() < 0.01:
            continue
        num_items = random.choices([1, 2, 3, 4], weights=[50, 30, 15, 5])[0]
        for _ in range(num_items):
            product_id = random.choice(product_ids)
            qty = random.choices([1, 2, 3, -1], weights=[70, 20, 8, 2])[0]  # -1 is a glitch
            rows.append({
                "order_item_id": f"OI{item_no:07d}",
                "order_id": order["order_id"],
                "product_id": product_id,
                "quantity": qty,
                "unit_price_at_purchase": "",  # filled in during cleaning from products table
            })
            item_no += 1

    # a few order_items referencing an order_id that was never created (orphaned FK)
    for i in range(4):
        rows.append({
            "order_item_id": f"OI{item_no:07d}",
            "order_id": f"ORD{900000 + i}",
            "product_id": random.choice(product_ids),
            "quantity": 1,
            "unit_price_at_purchase": "",
        })
        item_no += 1

    # a few referencing a product_id that doesn't exist
    for i in range(3):
        rows.append({
            "order_item_id": f"OI{item_no:07d}",
            "order_id": random.choice(orders)["order_id"],
            "product_id": "P9999",
            "quantity": 1,
            "unit_price_at_purchase": "",
        })
        item_no += 1

    return rows


def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows):>6} rows -> {path}")


def main():
    parser = argparse.ArgumentParser(description="Generate messy e-commerce sample data.")
    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--orders", type=int, default=4000)
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    customers = generate_customers(args.customers)
    valid_customer_ids = [c["customer_id"] for c in customers if c["customer_id"]]

    products = generate_products()
    orders = generate_orders(args.orders, valid_customer_ids)
    order_items = generate_order_items(orders, products)

    write_csv(OUT_DIR / "customers.csv", customers,
              ["customer_id", "first_name", "last_name", "email", "city", "state", "signup_date"])
    write_csv(OUT_DIR / "products.csv", products,
              ["product_id", "product_name", "category", "unit_price", "active"])
    write_csv(OUT_DIR / "orders.csv", orders,
              ["order_id", "customer_id", "order_date", "status", "payment_method"])
    write_csv(OUT_DIR / "order_items.csv", order_items,
              ["order_item_id", "order_id", "product_id", "quantity", "unit_price_at_purchase"])


if __name__ == "__main__":
    main()
