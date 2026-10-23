#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
02_load_DWH_Problem3_SQLServer2012.py

Loads the three project packages into SQL Server 2012:
  1) rd3nf_output.zip       -> core.*
  2) dw_problem3.zip        -> dw.*
  3) gold_problem3.zip   -> gold.*

The loader streams CSV / CSV.GZ directly from ZIP files; extraction is not required.

Example - Windows Authentication:
    python 02_load_DWH_Problem3_SQLServer2012.py ^
      --server ".\\SQLEXPRESS" ^
      --database DWH_Problem3 ^
      --trusted ^
      --core rd3nf_output.zip ^
      --dw dw_problem3.zip ^
      --gold gold_problem3.zip

Example - SQL Login:
    python 02_load_DWH_Problem3_SQLServer2012.py ^
      --server "localhost" ^
      --database DWH_Problem3 ^
      --username sa ^
      --password "YourPassword" ^
      --core rd3nf_output.zip ^
      --dw dw_problem3.zip ^
      --gold gold_problem3.zip

Dependencies:
    pip install pyodbc

Notes:
- Run 01_create_DWH_Problem3_SQLServer2012.sql in SSMS first.
- Default ODBC driver is "ODBC Driver 17 for SQL Server".
  You can use --driver "SQL Server Native Client 11.0" on older Windows clients.
- Use --replace to DELETE existing rows in FK-safe reverse order before loading.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import sys
import time
import zipfile
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Sequence, Tuple

# -----------------------------------------------------------------------------
# Physical SQL types used by the loader for value conversion.
# Text lengths are enforced by SQL Server; the converter only needs type family.
# -----------------------------------------------------------------------------
TABLE_TYPES: Dict[str, Dict[str, str]] = {
    # core
    "core.GEOGRAPHY": {
        "zip":"TEXT","city":"TEXT","district":"TEXT","region":"TEXT"},
    "core.CUSTOMERS": {
        "customer_id":"BIGINT","zip":"TEXT","signup_date":"DATE","gender":"TEXT",
        "age_group":"TEXT","acquisition_channel":"TEXT"},
    "core.SALES_EMPLOYEES": {
        "sales_employee_id":"TEXT","sales_employee_name":"TEXT","marital_status":"TEXT",
        "education_level":"TEXT","years_experience":"INT"},
    "core.ORDERS": {
        "order_id":"BIGINT","customer_id":"BIGINT","sales_employee_id":"TEXT",
        "order_date":"DATE","order_status":"TEXT","device_type":"TEXT",
        "order_source":"TEXT","comment":"TEXT"},
    "core.PRODUCTS": {
        "product_id":"BIGINT","product_name":"TEXT","category":"TEXT","segment":"TEXT",
        "size":"TEXT","color":"TEXT","price":"DECIMAL","cogs":"DECIMAL"},
    "core.ORDER_ITEMS": {
        "order_item_id":"BIGINT","order_id":"BIGINT","product_id":"BIGINT","quantity":"INT",
        "unit_price":"DECIMAL","discount_amount":"DECIMAL"},
    "core.PROMOTIONS": {
        "promo_id":"TEXT","promo_name":"TEXT","promo_type":"TEXT","discount_value":"DECIMAL",
        "start_date":"DATE","end_date":"DATE","applicable_category":"TEXT",
        "promo_channel":"TEXT","stackable_flag":"BIT","min_order_value":"DECIMAL"},
    "core.ORDER_ITEM_PROMOTION": {
        "order_item_id":"BIGINT","promo_id":"TEXT","promotion_order":"INT"},
    "core.PAYMENTS": {
        "order_id":"BIGINT","payment_method":"TEXT","payment_value":"DECIMAL","installments":"INT"},
    "core.SHIPMENTS": {
        "order_id":"BIGINT","shipper_id":"TEXT","ship_date":"DATE","delivery_date":"DATE",
        "shipping_fee":"DECIMAL"},
    "core.SHIPPERS": {
        "shipper_id":"TEXT","shipper_name":"TEXT","shipper_phone":"TEXT",
        "shipper_gender":"TEXT","shipper_age":"INT","shipper_marital_status":"TEXT",
        "shipper_education":"TEXT","shipper_company":"TEXT","shipper_vehicle":"TEXT",
        "shipper_experience_years":"INT","shipper_rating":"DECIMAL",
        "delivery_success_rate":"DECIMAL","average_delivery_time":"DECIMAL",
        "working_shift":"TEXT","join_date":"DATE","city":"TEXT","region":"TEXT","district":"TEXT"},
    "core.REVIEWS": {
        "review_id":"TEXT","order_id":"BIGINT","product_id":"BIGINT","review_date":"DATE",
        "rating":"INT","review_title":"TEXT"},
    "core.RETURNS": {
        "return_id":"TEXT","order_id":"BIGINT","product_id":"BIGINT","return_date":"DATE",
        "return_reason":"TEXT","return_quantity":"INT","refund_amount":"DECIMAL"},
    "core.INVENTORY_SNAPSHOT": {
        "product_id":"BIGINT","snapshot_date":"DATE","stock_on_hand":"INT",
        "units_received":"INT","units_sold":"INT","stockout_days":"INT",
        "days_of_supply":"DECIMAL","fill_rate":"DECIMAL","stockout_flag":"BIT",
        "overstock_flag":"BIT","reorder_flag":"BIT","sell_through_rate":"DECIMAL"},

    # dw
    "dw.DIM_DATE": {
        "date_key":"INT","full_date":"DATE","day":"INT","day_name":"TEXT","day_of_week":"INT",
        "week_of_year":"INT","month":"INT","month_name":"TEXT","quarter":"INT","year":"INT",
        "year_month":"TEXT","is_weekend":"BIT"},
    "dw.DIM_GEOGRAPHY": {
        "geo_key":"BIGINT","zip":"TEXT","city":"TEXT","district":"TEXT","region":"TEXT"},
    "dw.DIM_CUSTOMER": {
        "customer_key":"BIGINT","customer_id":"BIGINT","zip":"TEXT","geo_key":"BIGINT",
        "signup_date":"DATE","gender":"TEXT","age_group":"TEXT","acquisition_channel":"TEXT",
        "scd_version":"INT","is_current":"BIT"},
    "dw.DIM_PRODUCT": {
        "product_key":"BIGINT","product_id":"BIGINT","product_name":"TEXT","category":"TEXT",
        "segment":"TEXT","size":"TEXT","color":"TEXT","list_price":"DECIMAL","cogs":"DECIMAL",
        "scd_version":"INT","is_current":"BIT"},
    "dw.DIM_PROMOTION": {
        "promotion_key":"BIGINT","promo_id":"TEXT","promo_name":"TEXT","promo_type":"TEXT",
        "discount_value":"DECIMAL","start_date":"DATE","end_date":"DATE",
        "applicable_category":"TEXT","promo_channel":"TEXT","stackable_flag":"BIT",
        "min_order_value":"DECIMAL"},
    "dw.DIM_CHANNEL": {
        "channel_key":"BIGINT","order_source":"TEXT","device_type":"TEXT","payment_method":"TEXT"},
    "dw.DIM_ORDER_STATUS": {
        "order_status_key":"BIGINT","order_status":"TEXT"},
    "dw.FACT_SALES_LINE": {
        "sales_line_key":"BIGINT","order_item_id":"BIGINT","order_id":"BIGINT","date_key":"INT",
        "customer_key":"BIGINT","product_key":"BIGINT","geo_key":"BIGINT","channel_key":"BIGINT",
        "order_status_key":"BIGINT","quantity":"INT","unit_price":"DECIMAL",
        "discount_amount":"DECIMAL","gross_sales":"DECIMAL","net_sales":"DECIMAL"},
    "dw.FACT_ORDER": {
        "order_key":"BIGINT","order_id":"BIGINT","date_key":"INT","customer_key":"BIGINT",
        "geo_key":"BIGINT","channel_key":"BIGINT","order_status_key":"BIGINT",
        "gross_order_value":"DECIMAL","discount_total":"DECIMAL","order_value":"DECIMAL",
        "basket_quantity":"BIGINT","distinct_products":"INT","line_count":"INT",
        "payment_value":"DECIMAL","installments":"INT"},
    "dw.FACT_CUSTOMER_MONTHLY_SNAPSHOT": {
        "snapshot_month_key":"INT","customer_key":"BIGINT","monthly_order_count":"INT",
        "monthly_quantity":"BIGINT","monthly_net_sales":"DECIMAL","monthly_aov":"DECIMAL",
        "monthly_basket_size":"DECIMAL","cumulative_order_count":"BIGINT",
        "cumulative_quantity":"BIGINT","cumulative_net_sales":"DECIMAL",
        "cumulative_aov":"DECIMAL","cumulative_basket_size":"DECIMAL"},
    "dw.BRIDGE_SALES_LINE_PROMOTION": {
        "sales_line_key":"BIGINT","promotion_key":"BIGINT","promotion_order":"INT",
        "promotion_count_on_line":"INT","allocation_weight":"DECIMAL"},

    # gold
    "gold.FACT_CUSTOMER_ORDER_ITEM": {
        "order_item_id":"BIGINT","order_id":"BIGINT","customer_id":"BIGINT","order_date":"DATE",
        "year":"INT","month":"INT","year_month":"TEXT","day_of_week":"TEXT","product_id":"BIGINT",
        "category":"TEXT","quantity":"INT","unit_price":"DECIMAL","discount_amount":"DECIMAL",
        "line_gross_sales":"DECIMAL","line_net_sales":"DECIMAL"},
    "gold.FACT_CUSTOMER_ORDER": {
        "order_id":"BIGINT","customer_id":"BIGINT","order_date":"DATE","year_month":"TEXT",
        "day_of_week":"TEXT","order_status":"TEXT","order_value":"DECIMAL",
        "basket_quantity":"BIGINT","distinct_products":"INT","line_count":"INT"},
    "gold.MART_CUSTOMER_360": {
        "customer_id":"BIGINT","signup_date":"DATE","gender":"TEXT","age_group":"TEXT",
        "acquisition_channel":"TEXT","zip":"TEXT","city":"TEXT","district":"TEXT","region":"TEXT",
        "first_order_date":"DATE","last_order_date":"DATE","order_count":"BIGINT",
        "total_quantity":"BIGINT","total_net_sales":"DECIMAL","avg_order_value":"DECIMAL",
        "avg_basket_size":"DECIMAL","avg_days_between_orders":"DECIMAL",
        "customer_lifespan_days":"INT","frequency":"BIGINT","monetary":"DECIMAL",
        "frequency_level":"TEXT","monetary_level":"TEXT","customer_type":"TEXT",
        "customer_segment":"TEXT","historical_clv":"DECIMAL","preferred_category":"TEXT",
        "preferred_category_share":"DECIMAL"},
    "gold.MART_CUSTOMER_MONTHLY": {
        "customer_id":"BIGINT","year_month":"TEXT","orders":"INT","units":"BIGINT",
        "net_sales":"DECIMAL","avg_order_value":"DECIMAL","avg_basket_size":"DECIMAL",
        "customer_type":"TEXT"},
    "gold.MART_CUSTOMER_CATEGORY": {
        "customer_id":"BIGINT","category":"TEXT","orders":"INT","units":"BIGINT",
        "net_sales":"DECIMAL","revenue_share":"DECIMAL","category_rank":"INT",
        "is_preferred_category":"BIT"},
    "gold.MART_CUSTOMER_TIME": {
        "time_grain":"TEXT","period_key":"TEXT","period_label":"TEXT","orders":"INT",
        "customers":"INT","units":"BIGINT","net_sales":"DECIMAL","avg_order_value":"DECIMAL",
        "avg_basket_size":"DECIMAL","repeat_customers":"INT","one_time_customers":"INT"},
    "gold.MART_CUSTOMER_SEGMENT_SUMMARY": {
        "customer_segment":"TEXT","customer_count":"INT","customer_share":"DECIMAL",
        "orders":"BIGINT","units":"BIGINT","net_sales":"DECIMAL","revenue_share":"DECIMAL",
        "avg_frequency":"DECIMAL","avg_monetary":"DECIMAL","avg_aov":"DECIMAL",
        "avg_basket_size":"DECIMAL","avg_historical_clv":"DECIMAL"},
}

# FK-safe load order and ZIP member tails.
LOAD_PLAN: List[Tuple[str, str, str]] = [
    # core
    ("core", "core.GEOGRAPHY", "normalized_3nf/GEOGRAPHY.csv"),
    ("core", "core.PRODUCTS", "normalized_3nf/PRODUCTS.csv"),
    ("core", "core.PROMOTIONS", "normalized_3nf/PROMOTIONS.csv"),
    ("core", "core.SALES_EMPLOYEES", "normalized_3nf/SALES_EMPLOYEES.csv"),
    ("core", "core.SHIPPERS", "normalized_3nf/SHIPPERS.csv"),
    ("core", "core.CUSTOMERS", "normalized_3nf/CUSTOMERS.csv"),
    ("core", "core.ORDERS", "normalized_3nf/ORDERS.csv"),
    ("core", "core.ORDER_ITEMS", "normalized_3nf/ORDER_ITEMS.csv"),
    ("core", "core.ORDER_ITEM_PROMOTION", "normalized_3nf/ORDER_ITEM_PROMOTION.csv"),
    ("core", "core.PAYMENTS", "normalized_3nf/PAYMENTS.csv"),
    ("core", "core.SHIPMENTS", "normalized_3nf/SHIPMENTS.csv"),
    ("core", "core.REVIEWS", "normalized_3nf/REVIEWS.csv"),
    ("core", "core.RETURNS", "normalized_3nf/RETURNS.csv"),
    ("core", "core.INVENTORY_SNAPSHOT", "normalized_3nf/INVENTORY_SNAPSHOT.csv"),

    # dw
    ("dw", "dw.DIM_DATE", "dimensions/DIM_DATE.csv"),
    ("dw", "dw.DIM_GEOGRAPHY", "dimensions/DIM_GEOGRAPHY.csv"),
    ("dw", "dw.DIM_PRODUCT", "dimensions/DIM_PRODUCT.csv"),
    ("dw", "dw.DIM_PROMOTION", "dimensions/DIM_PROMOTION.csv"),
    ("dw", "dw.DIM_CHANNEL", "dimensions/DIM_CHANNEL.csv"),
    ("dw", "dw.DIM_ORDER_STATUS", "dimensions/DIM_ORDER_STATUS.csv"),
    ("dw", "dw.DIM_CUSTOMER", "dimensions/DIM_CUSTOMER.csv"),
    ("dw", "dw.FACT_SALES_LINE", "facts/FACT_SALES_LINE.csv.gz"),
    ("dw", "dw.FACT_ORDER", "facts/FACT_ORDER.csv.gz"),
    ("dw", "dw.FACT_CUSTOMER_MONTHLY_SNAPSHOT", "facts/FACT_CUSTOMER_MONTHLY_SNAPSHOT.csv.gz"),
    ("dw", "dw.BRIDGE_SALES_LINE_PROMOTION", "bridges/BRIDGE_SALES_LINE_PROMOTION.csv.gz"),

    # gold
    ("gold", "gold.FACT_CUSTOMER_ORDER_ITEM", "facts/FACT_CUSTOMER_ORDER_ITEM.csv.gz"),
    ("gold", "gold.FACT_CUSTOMER_ORDER", "facts/FACT_CUSTOMER_ORDER.csv.gz"),
    ("gold", "gold.MART_CUSTOMER_360", "marts/MART_CUSTOMER_360.csv"),
    ("gold", "gold.MART_CUSTOMER_MONTHLY", "marts/MART_CUSTOMER_MONTHLY.csv"),
    ("gold", "gold.MART_CUSTOMER_CATEGORY", "marts/MART_CUSTOMER_CATEGORY.csv"),
    ("gold", "gold.MART_CUSTOMER_TIME", "marts/MART_CUSTOMER_TIME.csv"),
    ("gold", "gold.MART_CUSTOMER_SEGMENT_SUMMARY", "marts/MART_CUSTOMER_SEGMENT_SUMMARY.csv"),
]

REVERSE_DELETE_ORDER = [x[1] for x in reversed(LOAD_PLAN)]


def qname(table: str) -> str:
    schema, name = table.split(".", 1)
    return f"[{schema}].[{name}]"


def build_connection_string(args) -> str:
    parts = [
        f"DRIVER={{{args.driver}}}",
        f"SERVER={args.server}",
        f"DATABASE={args.database}",
    ]
    if args.trusted:
        parts.append("Trusted_Connection=yes")
    else:
        if not args.username or args.password is None:
            raise ValueError("Use --trusted OR provide --username and --password.")
        parts.extend([f"UID={args.username}", f"PWD={args.password}"])
    # Compatible with SQL Server 2012; harmless for local/self-signed environments.
    if args.trust_server_certificate:
        parts.append("TrustServerCertificate=yes")
    return ";".join(parts) + ";"


def find_member(zf: zipfile.ZipFile, tail: str) -> str:
    norm_tail = tail.replace("\\", "/").lower()
    matches = [
        n for n in zf.namelist()
        if n.replace("\\", "/").lower().endswith(norm_tail)
    ]
    if len(matches) != 1:
        raise FileNotFoundError(
            f"Expected exactly one ZIP member ending with {tail!r}; found {matches}"
        )
    return matches[0]


def open_csv_member(zf: zipfile.ZipFile, member: str):
    raw = zf.open(member, "r")
    if member.lower().endswith(".gz"):
        stream = gzip.GzipFile(fileobj=raw, mode="rb")
    else:
        stream = raw
    # utf-8-sig handles BOM if present.
    return io.TextIOWrapper(stream, encoding="utf-8-sig", newline="")


def convert_value(value: str, kind: str):
    if value is None or value == "":
        return None
    if kind == "TEXT":
        return value
    if kind in ("INT", "BIGINT"):
        # Decimal handles CSV integers occasionally serialized as "1.0".
        return int(Decimal(value))
    if kind == "DECIMAL":
        return Decimal(value)
    if kind == "BIT":
        s = value.strip().lower()
        if s in ("1", "true", "t", "yes", "y"):
            return 1
        if s in ("0", "false", "f", "no", "n"):
            return 0
        raise ValueError(f"Cannot parse BIT value: {value!r}")
    if kind == "DATE":
        # Current project files use ISO YYYY-MM-DD.
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    raise ValueError(f"Unknown conversion kind: {kind}")


def batched_rows(reader: csv.DictReader, columns: Sequence[str],
                 type_map: Dict[str, str], batch_size: int) -> Iterator[List[tuple]]:
    batch: List[tuple] = []
    for row_no, row in enumerate(reader, start=2):
        try:
            values = tuple(convert_value(row.get(c), type_map[c]) for c in columns)
        except Exception as exc:
            raise ValueError(f"Conversion error at CSV row {row_no}: {exc}") from exc
        batch.append(values)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def count_rows(cursor, table: str) -> int:
    cursor.execute(f"SELECT COUNT_BIG(*) FROM {qname(table)}")
    return int(cursor.fetchone()[0])


def ensure_table_exists(cursor, table: str):
    schema, name = table.split(".", 1)
    cursor.execute(
        "SELECT COUNT(*) FROM sys.tables t "
        "JOIN sys.schemas s ON s.schema_id=t.schema_id "
        "WHERE s.name=? AND t.name=?",
        schema, name
    )
    if cursor.fetchone()[0] != 1:
        raise RuntimeError(
            f"Target table {table} does not exist. "
            "Run 01_create_DWH_Problem3_SQLServer2012.sql first."
        )


def delete_existing(cursor, conn):
    print("[REPLACE] Deleting existing rows in FK-safe reverse order...")
    for table in REVERSE_DELETE_ORDER:
        ensure_table_exists(cursor, table)
        cursor.execute(f"DELETE FROM {qname(table)}")
        conn.commit()
        print(f"  cleared {table}")


def load_one(cursor, conn, zf: zipfile.ZipFile, table: str, member_tail: str,
             batch_size: int) -> int:
    ensure_table_exists(cursor, table)
    type_map = TABLE_TYPES[table]
    member = find_member(zf, member_tail)

    with open_csv_member(zf, member) as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"{member} has no CSV header.")

        columns = list(type_map.keys())
        actual = list(reader.fieldnames)
        missing = [c for c in columns if c not in actual]
        extra = [c for c in actual if c not in columns]
        if missing or extra:
            raise ValueError(
                f"Schema mismatch for {table} from {member}. "
                f"Missing={missing}, Extra={extra}"
            )

        col_sql = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        sql = f"INSERT INTO {qname(table)} ({col_sql}) VALUES ({placeholders})"

        cursor.fast_executemany = True
        inserted = 0
        started = time.time()

        for batch in batched_rows(reader, columns, type_map, batch_size):
            cursor.executemany(sql, batch)
            conn.commit()
            inserted += len(batch)
            elapsed = max(time.time() - started, 0.001)
            print(
                f"\r  {table}: {inserted:,} rows "
                f"({inserted/elapsed:,.0f} rows/s)",
                end="", flush=True
            )
        print()

    db_count = count_rows(cursor, table)
    if db_count != inserted:
        raise RuntimeError(
            f"Row-count mismatch for {table}: inserted={inserted}, database={db_count}"
        )
    return inserted


def main():
    p = argparse.ArgumentParser(description="Load Problem 3 Core/DW/Gold packages into SQL Server 2012.")
    p.add_argument("--server", required=True, help=r"SQL Server name, e.g. .\SQLEXPRESS")
    p.add_argument("--database", default="DWH_Problem3")
    p.add_argument("--driver", default="ODBC Driver 17 for SQL Server")
    p.add_argument("--trusted", action="store_true", help="Use Windows Authentication.")
    p.add_argument("--username")
    p.add_argument("--password")
    p.add_argument("--trust-server-certificate", action="store_true")
    p.add_argument("--core", required=True, type=Path, help="rd3nf_output.zip")
    p.add_argument("--dw", required=True, type=Path, help="dw_problem3.zip")
    p.add_argument("--gold", required=True, type=Path, help="gold_problem3.zip")
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--replace", action="store_true",
                   help="DELETE all current core/dw/gold rows before loading.")
    args = p.parse_args()

    for path in (args.core, args.dw, args.gold):
        if not path.exists():
            p.error(f"Input not found: {path}")
        if not zipfile.is_zipfile(path):
            p.error(f"Input is not a ZIP file: {path}")

    try:
        import pyodbc
    except ImportError as exc:
        raise SystemExit("pyodbc is not installed. Run: pip install pyodbc") from exc

    conn_str = build_connection_string(args)
    print(f"Connecting to {args.server} / {args.database} ...")
    conn = pyodbc.connect(conn_str, autocommit=False)
    cursor = conn.cursor()

    try:
        if args.replace:
            delete_existing(cursor, conn)

        packages = {
            "core": zipfile.ZipFile(args.core, "r"),
            "dw": zipfile.ZipFile(args.dw, "r"),
            "gold": zipfile.ZipFile(args.gold, "r"),
        }
        totals = {}
        try:
            for package_key, table, member_tail in LOAD_PLAN:
                zf = packages[package_key]
                print(f"[LOAD] {table} <- {member_tail}")
                totals[table] = load_one(
                    cursor, conn, zf, table, member_tail, args.batch_size
                )
        finally:
            for zf in packages.values():
                zf.close()

        print("\nLOAD COMPLETE")
        for table, n in totals.items():
            print(f"  {table:<45} {n:>10,}")

        print(
            "\nNext step: run 03_validate_DWH_Problem3_SQLServer2012.sql in SSMS."
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
