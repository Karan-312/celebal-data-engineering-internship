"""
clean_data.py
--------------
Takes the raw, messy CSVs in /data and turns them into a clean SQLite database
(ecommerce.db) that respects referential integrity.

This is the "boring but important" part of the project. Nothing here is
clever -- it's mostly a checklist of the same problems you'd hit with any
real export: duplicates, missing values, inconsistent formatting, orphaned
foreign keys, and a couple of rows that are just broken.

Every cleaning decision gets logged so the transformation isn't a black box.
Run it with:
    python clean_data.py
"""

import sqlite3
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SQL_DIR = BASE_DIR / "sql"
DB_PATH = BASE_DIR / "ecommerce.db"
LOG_PATH = BASE_DIR / "reports" / "cleaning_log.txt"

log_lines = []


def log(msg):
    print(msg)
    log_lines.append(msg)


def parse_messy_date(series):
    """Dates show up as YYYY-MM-DD, DD/MM/YYYY, or plain garbage. Try both known
    formats before giving up and marking it NaT."""
    parsed = pd.to_datetime(series, format="%Y-%m-%d", errors="coerce")
    still_missing = parsed.isna()
    fallback = pd.to_datetime(series[still_missing], format="%d/%m/%Y", errors="coerce")
    parsed.loc[still_missing] = fallback
    return parsed


def clean_customers():
    df = pd.read_csv(DATA_DIR / "customers.csv", dtype=str)
    start = len(df)

    # drop rows with no customer_id at all -- can't do anything with those
    df = df[df["customer_id"].notna() & (df["customer_id"].str.strip() != "")]
    log(f"customers: dropped {start - len(df)} rows with missing customer_id")

    # exact duplicate rows (same id, same everything) -> keep first
    before = len(df)
    df = df.drop_duplicates(subset="customer_id", keep="first")
    log(f"customers: dropped {before - len(df)} duplicate customer_id rows")

    # normalize whitespace + casing
    df["first_name"] = df["first_name"].str.strip().str.title()
    df["last_name"] = df["last_name"].str.strip().str.title()
    df["city"] = df["city"].str.strip().str.title()
    df["state"] = df["state"].str.strip().str.upper()

    # emails: strip whitespace, lowercase, fix the ".con" typo, blank out anything
    # that still doesn't look like an email
    df["email"] = df["email"].fillna("").str.strip().str.lower()
    df["email"] = df["email"].str.replace(r"\.con$", ".com", regex=True)
    bad_email_mask = ~df["email"].str.contains(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", regex=True)
    n_bad_emails = (bad_email_mask & (df["email"] != "")).sum()
    df.loc[bad_email_mask, "email"] = pd.NA
    log(f"customers: blanked {n_bad_emails} malformed emails (kept the row)")

    # dates: handle the two formats we know about
    df["signup_date"] = parse_messy_date(df["signup_date"])
    missing_dates = df["signup_date"].isna().sum()
    if missing_dates:
        # fall back to the earliest known signup date rather than dropping the customer
        fallback_date = df["signup_date"].min()
        df["signup_date"] = df["signup_date"].fillna(fallback_date)
        log(f"customers: {missing_dates} unparsable signup dates filled with earliest known date")

    df["signup_date"] = df["signup_date"].dt.strftime("%Y-%m-%d")

    return df.reset_index(drop=True)


def clean_products():
    df = pd.read_csv(DATA_DIR / "products.csv", dtype=str)

    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    # missing price -> use the category median as a reasonable stand-in
    missing = df["unit_price"].isna().sum()
    df["unit_price"] = df.groupby("category")["unit_price"].transform(
        lambda s: s.fillna(s.median())
    )
    log(f"products: filled {missing} missing prices with category median")

    # negative prices are a data-entry glitch, not a real discount -> take absolute value
    neg_count = (df["unit_price"] < 0).sum()
    df["unit_price"] = df["unit_price"].abs()
    log(f"products: fixed {neg_count} negative unit_price values (took absolute value)")

    df["unit_price"] = df["unit_price"].round(2)
    df["active"] = pd.to_numeric(df["active"], errors="coerce").fillna(1).astype(int)

    return df.reset_index(drop=True)


def clean_orders(valid_customer_ids):
    df = pd.read_csv(DATA_DIR / "orders.csv", dtype=str)
    start = len(df)

    # drop exact duplicate order_ids, keep the first occurrence
    before = len(df)
    df = df.drop_duplicates(subset="order_id", keep="first")
    log(f"orders: dropped {before - len(df)} duplicate order_id rows")

    # drop orders pointing at a customer that doesn't exist in the cleaned customers table
    before = len(df)
    df = df[df["customer_id"].isin(valid_customer_ids)]
    log(f"orders: dropped {before - len(df)} orders with an unknown customer_id (orphaned FK)")

    # unparsable order_date -> can't safely guess this one, drop the order
    df["order_date"] = parse_messy_date(df["order_date"])
    before = len(df)
    df = df[df["order_date"].notna()]
    log(f"orders: dropped {before - len(df)} orders with an unparsable order_date")
    df["order_date"] = df["order_date"].dt.strftime("%Y-%m-%d")

    # normalize status + payment method casing/labels
    df["status"] = df["status"].str.strip().str.title()
    df["payment_method"] = df["payment_method"].str.strip()
    df["payment_method"] = df["payment_method"].replace({
        "upi": "UPI",
        "Cash On Delivery": "COD",
        "Cash on Delivery": "COD",
    })

    log(f"orders: {start} raw rows -> {len(df)} clean rows")
    return df.reset_index(drop=True)


def clean_order_items(valid_order_ids, products_df):
    df = pd.read_csv(DATA_DIR / "order_items.csv", dtype=str)
    start = len(df)

    price_lookup = products_df.set_index("product_id")["unit_price"]

    # drop items pointing at an order that doesn't exist
    before = len(df)
    df = df[df["order_id"].isin(valid_order_ids)]
    log(f"order_items: dropped {before - len(df)} items with an unknown order_id (orphaned FK)")

    # drop items pointing at a product that doesn't exist
    before = len(df)
    df = df[df["product_id"].isin(price_lookup.index)]
    log(f"order_items: dropped {before - len(df)} items with an unknown product_id (orphaned FK)")

    # negative/zero quantity is a glitch -> drop those line items entirely,
    # since we can't confidently guess what quantity was actually intended
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    before = len(df)
    df = df[df["quantity"] > 0]
    log(f"order_items: dropped {before - len(df)} items with a zero/negative/invalid quantity")

    # fill in the purchase-time price from the products table (this is the source
    # of truth since the raw file left it blank on purpose)
    df["unit_price_at_purchase"] = df["product_id"].map(price_lookup)

    log(f"order_items: {start} raw rows -> {len(df)} clean rows")
    return df.reset_index(drop=True)


def load_into_sqlite(customers, products, orders, order_items):
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    conn.executescript((SQL_DIR / "schema.sql").read_text())

    customers.to_sql("customers", conn, if_exists="append", index=False)
    products.to_sql("products", conn, if_exists="append", index=False)
    orders.to_sql("orders", conn, if_exists="append", index=False)
    order_items.to_sql("order_items", conn, if_exists="append", index=False)

    conn.commit()
    conn.close()
    log(f"\nloaded clean tables into {DB_PATH}")


def sanity_check():
    """After loading, re-open the DB and run a couple of integrity checks so we're
    not just trusting that the pandas step did the right thing."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*) FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)
    orphan_orders = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM order_items oi
        LEFT JOIN orders o ON oi.order_id = o.order_id
        WHERE o.order_id IS NULL
    """)
    orphan_items = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM order_items WHERE quantity <= 0")
    bad_qty = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM products WHERE unit_price < 0")
    bad_price = cur.fetchone()[0]

    conn.close()

    log("\n--- post-load sanity check ---")
    log(f"orphaned orders (no matching customer): {orphan_orders}")
    log(f"orphaned order_items (no matching order): {orphan_items}")
    log(f"order_items with bad quantity: {bad_qty}")
    log(f"products with negative price: {bad_price}")

    assert orphan_orders == 0, "referential integrity broken: orders -> customers"
    assert orphan_items == 0, "referential integrity broken: order_items -> orders"
    assert bad_qty == 0, "bad quantities slipped through"
    assert bad_price == 0, "negative prices slipped through"
    log("all checks passed.")


def main():
    (BASE_DIR / "reports").mkdir(exist_ok=True)

    log("=== cleaning customers ===")
    customers = clean_customers()

    log("\n=== cleaning products ===")
    products = clean_products()

    log("\n=== cleaning orders ===")
    orders = clean_orders(set(customers["customer_id"]))

    log("\n=== cleaning order_items ===")
    order_items = clean_order_items(set(orders["order_id"]), products)

    load_into_sqlite(customers, products, orders, order_items)
    sanity_check()

    LOG_PATH.write_text("\n".join(log_lines))
    print(f"\nfull cleaning log written to {LOG_PATH}")


if __name__ == "__main__":
    main()
