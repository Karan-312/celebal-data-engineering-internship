# E-Commerce Order Analytics Pipeline

A small end-to-end data analytics project: it generates a realistic (and
intentionally messy) e-commerce dataset, cleans it with Pandas, loads it into
a proper relational SQLite database, runs a set of increasingly complex SQL
queries against it, and wraps the whole thing in a command-line reporting
tool. Built as a mini intern project to practice the full data pipeline —
generation, cleaning, modeling, analysis, and reporting — rather than just
one slice of it.

## Why it's built this way

Real e-commerce exports are never clean. Emails have typos, dates come in
two formats, someone's checkout button gets double-clicked and creates a
duplicate order, a product loses its price in an export glitch. Instead of
pretending that doesn't happen, this project generates data with those exact
problems baked in on purpose, and then the cleaning step has to actually
notice and fix them — the same as it would with a real vendor dump.

## Project layout

```
ecommerce_analytics/
├── data/                   # raw, messy CSVs (generated)
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   └── order_items.csv
├── sql/
│   ├── schema.sql          # clean table definitions + constraints
│   └── queries.sql         # 10 analytical queries, documented
├── scripts/
│   ├── generate_data.py    # step 1: create messy raw data
│   ├── clean_data.py       # step 2: clean with Pandas, load into SQLite
│   └── report.py           # step 3: CLI reporting tool
├── reports/                # cleaning log + CSV exports land here
├── ecommerce.db            # SQLite database (generated)
├── requirements.txt
└── README.md
```

## Data model

Four tables, one clear responsibility each:

- **customers** — one row per person: name, email, city/state, signup date
- **products** — catalog: name, category, unit price, active flag
- **orders** — one row per order: which customer, when, status, payment method
- **order_items** — line items: which product, quantity, price *at the time
  of purchase* (kept separate from the current catalog price on purpose,
  since prices change over time and historical orders shouldn't retroactively
  change value)

Foreign keys tie it together: `orders.customer_id → customers.customer_id`,
`order_items.order_id → orders.order_id`, `order_items.product_id →
products.product_id`. `schema.sql` enforces these plus a couple of `CHECK`
constraints (no negative prices, no zero/negative quantities).

## The messiness, on purpose

`generate_data.py` seeds these problems into the raw CSVs so the cleaning
step has real work to do:

| Problem | Where |
|---|---|
| Duplicate rows (same ID, resubmitted) | customers, orders |
| Missing primary key | customers |
| Inconsistent casing (`upi` vs `UPI`, `MUMBAI` vs `Mumbai`) | customers, orders |
| Two different date formats in the same column | customers, orders |
| Unparsable date string | orders |
| Missing / negative product price | products |
| Malformed or blank emails | customers |
| Orphaned foreign keys (order → nonexistent customer, item → nonexistent order/product) | orders, order_items |
| Negative / zero quantities | order_items |
| Orders with zero line items | orders |

## Cleaning rules (`clean_data.py`)

Every fix is a judgment call, and the script is explicit about which one it
made and why:

- Rows with a **missing primary key** are dropped — there's nothing to key
  them on.
- **Exact duplicates** are deduped, keeping the first occurrence.
- **Orphaned foreign keys** are dropped — an order item for an order that
  doesn't exist can't be attributed to anything, so it's noise, not signal.
- **Missing/negative prices** are repaired (median-fill by category, absolute
  value) rather than dropped, since the product itself is still valid.
- **Bad quantities** are dropped at the line-item level — there's no safe way
  to guess whether a `-1` was supposed to be a `1` or a `2`.
- **Unparsable order dates** cause the order to be dropped (can't safely
  guess), but an unparsable *signup* date is filled with the earliest known
  signup date rather than losing the customer entirely.

Every decision is logged to `reports/cleaning_log.txt`, and after loading,
`clean_data.py` re-queries the database to confirm referential integrity
holds (zero orphaned rows, zero bad quantities/prices) before declaring
success.

## SQL queries (`sql/queries.sql`)

Ten queries, grouped by complexity:

1. Revenue by category (join + aggregation)
2. Monthly revenue trend with month-over-month growth (CTE + `LAG`)
3. Cumulative revenue (window function running total)
4. Top 3 products per category (`RANK() ... PARTITION BY`)
5. RFM customer segmentation (Recency/Frequency/Monetary, `NTILE`, nested CTEs)
6. Cohort retention analysis — the centerpiece: for each signup-month cohort,
   what percentage of customers were still ordering N months later
7. Customer lifetime value leaderboard (`DENSE_RANK`)
8. Cancellation rate by payment method
9. Repeat vs one-time buyers
10. New vs returning customer revenue split per month

## The CLI tool (`scripts/report.py`)

```bash
python report.py revenue-by-category
python report.py monthly-trend
python report.py top-customers --limit 15
python report.py cohort-retention
python report.py segments
python report.py all --export ../reports/
```

Each report prints a plain-text table to the terminal. Add `--export
path/to/file.csv` (or a folder, for `all`) to also dump it as CSV. If the
database doesn't exist yet, it tells you to run the pipeline first instead of
crashing with a raw traceback.

## Running it end to end

```bash
pip install -r requirements.txt

cd scripts
python generate_data.py --customers 600 --orders 5000   # step 1: make messy data
python clean_data.py                                    # step 2: clean + load
python report.py all --export ../reports/                # step 3: generate reports
```

Re-running `generate_data.py` with different `--customers`/`--orders` counts
and re-running `clean_data.py` rebuilds `ecommerce.db` from scratch each
time — it's a repeatable pipeline, not a one-shot script.

## Known limitations / next steps

This was built as a learning project, so a few things are intentionally
simple rather than production-grade:

- SQLite instead of Postgres/MySQL — great for a self-contained project, but
  the schema and queries are close enough to standard SQL that porting to
  Postgres would mostly be a find-and-replace on date functions
  (`strftime` → `to_char`, etc.).
- The CLI prints plain text tables; piping into `column -t` or swapping in
  `tabulate` would make it prettier.
- No incremental/upsert loading — every run is a full rebuild. Fine for a
  batch analytics project, not how you'd want it in production.
