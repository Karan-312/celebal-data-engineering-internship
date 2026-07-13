#!/usr/bin/env python3
"""
report.py
----------
A small CLI on top of ecommerce.db. Nothing fancy -- argparse subcommands,
each one runs a query and prints it as a plain-text table (or dumps to CSV
if you pass --export).

Examples:
    python report.py revenue-by-category
    python report.py monthly-trend
    python report.py top-customers --limit 10
    python report.py cohort-retention
    python report.py segments
    python report.py all --export reports/
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "ecommerce.db"


def get_connection():
    if not DB_PATH.exists():
        sys.exit(
            f"Couldn't find {DB_PATH}. Run generate_data.py and clean_data.py first."
        )
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def print_table(rows, title=None):
    if title:
        print(f"\n{title}")
        print("-" * len(title))
    if not rows:
        print("(no rows returned)")
        return
    headers = rows[0].keys()
    widths = [max(len(h), max((len(str(r[h])) for r in rows), default=0)) for h in headers]
    line = " | ".join(h.ljust(w) for h, w in zip(headers, widths))
    print(line)
    print("-" * len(line))
    for r in rows:
        print(" | ".join(str(r[h]).ljust(w) for h, w in zip(headers, widths)))


def export_csv(rows, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        if not rows:
            f.write("")
            return
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows([dict(r) for r in rows])
    print(f"exported -> {path}")


# --- each function below is a self-contained report ---------------------

def revenue_by_category(conn):
    return conn.execute("""
        SELECT
            p.category,
            COUNT(DISTINCT oi.order_id) AS orders_with_category,
            SUM(oi.quantity) AS units_sold,
            ROUND(SUM(oi.quantity * oi.unit_price_at_purchase), 2) AS revenue
        FROM order_items oi
        JOIN products p ON p.product_id = oi.product_id
        JOIN orders o ON o.order_id = oi.order_id
        WHERE o.status NOT IN ('Cancelled', 'Returned')
        GROUP BY p.category
        ORDER BY revenue DESC
    """).fetchall()


def monthly_trend(conn):
    return conn.execute("""
        WITH monthly_revenue AS (
            SELECT strftime('%Y-%m', o.order_date) AS order_month,
                   ROUND(SUM(oi.quantity * oi.unit_price_at_purchase), 2) AS revenue
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.order_id
            WHERE o.status NOT IN ('Cancelled', 'Returned')
            GROUP BY order_month
        )
        SELECT
            order_month,
            revenue,
            LAG(revenue) OVER (ORDER BY order_month) AS prev_month_revenue,
            ROUND((revenue - LAG(revenue) OVER (ORDER BY order_month)) * 100.0
                  / NULLIF(LAG(revenue) OVER (ORDER BY order_month), 0), 1) AS mom_growth_pct
        FROM monthly_revenue
        ORDER BY order_month
    """).fetchall()


def top_customers(conn, limit):
    return conn.execute("""
        SELECT
            c.customer_id,
            c.first_name || ' ' || c.last_name AS customer_name,
            COUNT(DISTINCT o.order_id) AS total_orders,
            ROUND(SUM(oi.quantity * oi.unit_price_at_purchase), 2) AS lifetime_value
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        JOIN order_items oi ON oi.order_id = o.order_id
        WHERE o.status NOT IN ('Cancelled', 'Returned')
        GROUP BY c.customer_id, customer_name
        ORDER BY lifetime_value DESC
        LIMIT ?
    """, (limit,)).fetchall()


def cohort_retention(conn):
    return conn.execute("""
        WITH cohorts AS (
            SELECT customer_id, strftime('%Y-%m', signup_date) AS cohort_month
            FROM customers
        ),
        order_activity AS (
            SELECT DISTINCT o.customer_id, strftime('%Y-%m', o.order_date) AS activity_month
            FROM orders o
            WHERE o.status NOT IN ('Cancelled', 'Returned')
        ),
        cohort_activity AS (
            SELECT
                c.cohort_month,
                oa.customer_id,
                (CAST(strftime('%Y', oa.activity_month || '-01') AS INT) - CAST(strftime('%Y', c.cohort_month || '-01') AS INT)) * 12
                + (CAST(strftime('%m', oa.activity_month || '-01') AS INT) - CAST(strftime('%m', c.cohort_month || '-01') AS INT)) AS month_number
            FROM order_activity oa
            JOIN cohorts c ON c.customer_id = oa.customer_id
        ),
        cohort_size AS (
            SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_customers
            FROM cohorts GROUP BY cohort_month
        )
        SELECT
            ca.cohort_month,
            cs.cohort_customers,
            ca.month_number,
            COUNT(DISTINCT ca.customer_id) AS active_customers,
            ROUND(100.0 * COUNT(DISTINCT ca.customer_id) / cs.cohort_customers, 1) AS retention_pct
        FROM cohort_activity ca
        JOIN cohort_size cs ON cs.cohort_month = ca.cohort_month
        WHERE ca.month_number >= 0
        GROUP BY ca.cohort_month, ca.month_number
        ORDER BY ca.cohort_month, ca.month_number
    """).fetchall()


def customer_segments(conn):
    return conn.execute("""
        WITH order_value AS (
            SELECT o.order_id, o.customer_id, o.order_date,
                   SUM(oi.quantity * oi.unit_price_at_purchase) AS order_total
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.order_id
            WHERE o.status NOT IN ('Cancelled', 'Returned')
            GROUP BY o.order_id, o.customer_id, o.order_date
        ),
        rfm_base AS (
            SELECT
                customer_id,
                JULIANDAY((SELECT MAX(order_date) FROM order_value)) - JULIANDAY(MAX(order_date)) AS recency_days,
                COUNT(*) AS frequency,
                ROUND(SUM(order_total), 2) AS monetary
            FROM order_value
            GROUP BY customer_id
        ),
        scored AS (
            SELECT *,
                NTILE(4) OVER (ORDER BY recency_days DESC) AS recency_score,
                NTILE(4) OVER (ORDER BY frequency ASC) AS frequency_score,
                NTILE(4) OVER (ORDER BY monetary ASC) AS monetary_score
            FROM rfm_base
        )
        SELECT
            customer_id, recency_days, frequency, monetary,
            recency_score + frequency_score + monetary_score AS rfm_total,
            CASE
                WHEN recency_score + frequency_score + monetary_score >= 10 THEN 'Champion'
                WHEN recency_score + frequency_score + monetary_score >= 7 THEN 'Loyal'
                WHEN recency_score + frequency_score + monetary_score >= 5 THEN 'At Risk'
                ELSE 'Dormant'
            END AS segment
        FROM scored
        ORDER BY rfm_total DESC
    """).fetchall()


def segment_summary(conn):
    """A shorter, more CLI-friendly rollup of customer_segments()."""
    rows = customer_segments(conn)
    counts = {}
    for r in rows:
        counts.setdefault(r["segment"], {"customers": 0, "total_spend": 0.0})
        counts[r["segment"]]["customers"] += 1
        counts[r["segment"]]["total_spend"] += r["monetary"]

    class FakeRow(dict):
        def keys(self):
            return super().keys()

    out = []
    for seg, vals in sorted(counts.items(), key=lambda kv: -kv[1]["total_spend"]):
        out.append(FakeRow(
            segment=seg,
            customers=vals["customers"],
            total_spend=round(vals["total_spend"], 2),
            avg_spend=round(vals["total_spend"] / vals["customers"], 2),
        ))
    return out


REPORTS = {
    "revenue-by-category": lambda conn, args: revenue_by_category(conn),
    "monthly-trend": lambda conn, args: monthly_trend(conn),
    "top-customers": lambda conn, args: top_customers(conn, args.limit),
    "cohort-retention": lambda conn, args: cohort_retention(conn),
    "segments": lambda conn, args: segment_summary(conn),
}


def run_report(name, conn, args):
    rows = REPORTS[name](conn, args)
    title = name.replace("-", " ").title()
    print_table(rows, title)
    if args.export:
        target = Path(args.export)
        if target.is_dir() or args.export.endswith("/"):
            target = target / f"{name}.csv"
        export_csv(rows, target)


def main():
    parser = argparse.ArgumentParser(
        description="Query the cleaned e-commerce database and print business reports."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    for name in REPORTS:
        p = sub.add_parser(name, help=f"show the '{name}' report")
        p.add_argument("--export", help="write results to this CSV file/folder")
        if name == "top-customers":
            p.add_argument("--limit", type=int, default=10, help="how many customers to show")

    p_all = sub.add_parser("all", help="run every report in sequence")
    p_all.add_argument("--export", help="folder to write all reports as CSV into")
    p_all.add_argument("--limit", type=int, default=10)

    args = parser.parse_args()
    conn = get_connection()

    try:
        if args.command == "all":
            for name in REPORTS:
                run_report(name, conn, args)
        else:
            run_report(args.command, conn, args)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
