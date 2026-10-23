#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3NF -> Dimensional Data Warehouse ETL for Problem 3
===================================================

Purpose
-------
Build a reusable dimensional warehouse layer between the normalized 3NF model
and the Problem 3 Gold Data Mart.

Input
-----
- rd3nf_output.zip, or
- a directory containing normalized_3nf/*.csv

Output
------
dw_problem3/
├── dimensions/
│   ├── DIM_DATE.csv
│   ├── DIM_CUSTOMER.csv
│   ├── DIM_GEOGRAPHY.csv
│   ├── DIM_PRODUCT.csv
│   ├── DIM_PROMOTION.csv
│   ├── DIM_CHANNEL.csv
│   └── DIM_ORDER_STATUS.csv
├── facts/
│   ├── FACT_SALES_LINE.csv.gz
│   ├── FACT_ORDER.csv.gz
│   └── FACT_CUSTOMER_MONTHLY_SNAPSHOT.csv.gz
├── bridges/
│   └── BRIDGE_SALES_LINE_PROMOTION.csv.gz
└── reports/
    ├── dw_validation.csv
    ├── dw_reconciliation.csv
    ├── dw_schema_dictionary.csv
    ├── dw_lineage.csv
    └── dw_run_metadata.csv

Selected physical model
-----------------------
Fact constellation / galaxy schema. Each individual fact with its shared
conformed dimensions is a Star Schema. A Snowflake alternative is documented
separately, but is not used as the physical v1 implementation because the BI
and Problem 3 workload benefits from denormalized dimensions and fewer joins.

Important rules
---------------
- No order-status rows are silently excluded. All order statuses are preserved
  through DIM_ORDER_STATUS so downstream Gold/EDA can apply an explicit scope.
- Surrogate keys are deterministic for the same input: source business keys are
  sorted before integer keys are assigned.
- Current sources contain only one version of each customer/product profile.
  Therefore DIM_CUSTOMER and DIM_PRODUCT are loaded as current-state Type 1
  dimensions in v1. No synthetic SCD2 history is invented.
- Net Sales = quantity * unit_price - discount_amount.
- Payment value is retained in FACT_ORDER for reconciliation.

Google Colab example
--------------------
!python /content/3nf_to_dw_colab.py \
    --input /content/rd3nf_output.zip \
    --output /content/dw_problem3

The script also creates <output>.zip next to the output directory.
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

SCRIPT_VERSION = "1.0.0"


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def _norm_colnames(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).replace("\ufeff", "").strip() for c in out.columns]
    return out


def _require_columns(df: pd.DataFrame, table: str, cols: Sequence[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{table}: missing required columns: {missing}")


def _assert_unique(df: pd.DataFrame, cols: Sequence[str], table: str) -> None:
    dup = df.duplicated(list(cols), keep=False)
    if dup.any():
        raise ValueError(
            f"{table}: key {list(cols)} is not unique; duplicate rows={int(dup.sum()):,}."
        )


def _to_numeric(s: pd.Series, name: str, allow_null: bool = False) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    if not allow_null and x.isna().any():
        raise ValueError(f"{name}: {int(x.isna().sum()):,} invalid numeric/NULL values")
    return x


def _to_date(s: pd.Series, name: str, allow_null: bool = False) -> pd.Series:
    x = pd.to_datetime(s, errors="coerce")
    if not allow_null and x.isna().any():
        raise ValueError(f"{name}: {int(x.isna().sum()):,} invalid/NULL dates")
    return x


def _clean_zip_string(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)


def _date_key(s: pd.Series) -> pd.Series:
    return s.dt.strftime("%Y%m%d").astype("int64")


def _iso_date(s: pd.Series) -> pd.Series:
    out = s.dt.strftime("%Y-%m-%d")
    return out.where(s.notna(), pd.NA)


def _safe_divide(num: pd.Series, den: pd.Series) -> pd.Series:
    n = pd.to_numeric(num, errors="coerce")
    d = pd.to_numeric(den, errors="coerce")
    return n / d.replace(0, np.nan)


def _status(ok: bool) -> str:
    return "PASS" if bool(ok) else "FAIL"


def _eq_money(a: float, b: float, tol: float) -> bool:
    return abs(float(a) - float(b)) <= tol


def _stable_surrogate(df: pd.DataFrame, sort_cols: Sequence[str], key_col: str) -> pd.DataFrame:
    out = df.sort_values(list(sort_cols), kind="mergesort", na_position="first").reset_index(drop=True).copy()
    out.insert(0, key_col, np.arange(1, len(out) + 1, dtype=np.int64))
    return out


def _fmt(x) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    if isinstance(x, (np.integer, int)):
        return f"{int(x):,}"
    if isinstance(x, (np.floating, float)):
        return f"{float(x):,.6f}"
    return str(x)


# -----------------------------------------------------------------------------
# Reader
# -----------------------------------------------------------------------------

class ThreeNFReader:
    REQUIRED_TABLES = [
        "CUSTOMERS",
        "GEOGRAPHY",
        "ORDERS",
        "ORDER_ITEMS",
        "PRODUCTS",
        "PROMOTIONS",
        "ORDER_ITEM_PROMOTION",
        "PAYMENTS",
    ]

    def __init__(self, source: Path):
        self.source = source
        if not source.exists():
            raise FileNotFoundError(f"Input not found: {source}")
        self._zip: Optional[zipfile.ZipFile] = None
        if source.is_file():
            if not zipfile.is_zipfile(source):
                raise ValueError(f"Input file must be ZIP: {source}")
            self._zip = zipfile.ZipFile(source, "r")

    def close(self) -> None:
        if self._zip is not None:
            self._zip.close()

    def _zip_member(self, table: str) -> str:
        assert self._zip is not None
        targets = [f"normalized_3nf/{table}.csv", f"{table}.csv"]
        names = self._zip.namelist()
        for target in targets:
            for name in names:
                if name.replace("\\", "/").endswith(target):
                    return name
        raise FileNotFoundError(f"Could not find {table}.csv in {self.source}")

    def _dir_file(self, table: str) -> Path:
        candidates = [
            self.source / "normalized_3nf" / f"{table}.csv",
            self.source / f"{table}.csv",
        ]
        for p in candidates:
            if p.exists():
                return p
        hits = list(self.source.rglob(f"{table}.csv"))
        if hits:
            return hits[0]
        raise FileNotFoundError(f"Could not find {table}.csv under {self.source}")

    def read(self, table: str, dtype: Optional[dict] = None) -> pd.DataFrame:
        if table not in self.REQUIRED_TABLES:
            raise ValueError(f"Unexpected table requested: {table}")
        kwargs = {"low_memory": False, "encoding": "utf-8-sig"}
        if dtype:
            kwargs["dtype"] = dtype
        if self._zip is not None:
            with self._zip.open(self._zip_member(table)) as f:
                return _norm_colnames(pd.read_csv(f, **kwargs))
        return _norm_colnames(pd.read_csv(self._dir_file(table), **kwargs))


# -----------------------------------------------------------------------------
# Load and standardize source
# -----------------------------------------------------------------------------

def load_source(reader: ThreeNFReader) -> Dict[str, pd.DataFrame]:
    t: Dict[str, pd.DataFrame] = {}
    t["CUSTOMERS"] = reader.read("CUSTOMERS", dtype={"zip": "string"})
    t["GEOGRAPHY"] = reader.read("GEOGRAPHY", dtype={"zip": "string"})
    for name in ["ORDERS", "ORDER_ITEMS", "PRODUCTS", "PROMOTIONS", "ORDER_ITEM_PROMOTION", "PAYMENTS"]:
        t[name] = reader.read(name)

    _require_columns(t["CUSTOMERS"], "CUSTOMERS", [
        "customer_id", "zip", "signup_date", "gender", "age_group", "acquisition_channel"
    ])
    _require_columns(t["GEOGRAPHY"], "GEOGRAPHY", ["zip", "city", "district", "region"])
    _require_columns(t["ORDERS"], "ORDERS", [
        "order_id", "customer_id", "order_date", "order_status", "device_type", "order_source"
    ])
    _require_columns(t["ORDER_ITEMS"], "ORDER_ITEMS", [
        "order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount"
    ])
    _require_columns(t["PRODUCTS"], "PRODUCTS", [
        "product_id", "product_name", "category", "segment", "size", "color", "price", "cogs"
    ])
    _require_columns(t["PROMOTIONS"], "PROMOTIONS", [
        "promo_id", "promo_name", "promo_type", "discount_value", "start_date", "end_date",
        "applicable_category", "promo_channel", "stackable_flag", "min_order_value"
    ])
    _require_columns(t["ORDER_ITEM_PROMOTION"], "ORDER_ITEM_PROMOTION", [
        "order_item_id", "promo_id", "promotion_order"
    ])
    _require_columns(t["PAYMENTS"], "PAYMENTS", [
        "order_id", "payment_method", "payment_value", "installments"
    ])

    # Business keys / PK checks before any dimensional joins.
    _assert_unique(t["CUSTOMERS"], ["customer_id"], "CUSTOMERS")
    _assert_unique(t["GEOGRAPHY"], ["zip"], "GEOGRAPHY")
    _assert_unique(t["ORDERS"], ["order_id"], "ORDERS")
    _assert_unique(t["ORDER_ITEMS"], ["order_item_id"], "ORDER_ITEMS")
    _assert_unique(t["PRODUCTS"], ["product_id"], "PRODUCTS")
    _assert_unique(t["PROMOTIONS"], ["promo_id"], "PROMOTIONS")
    _assert_unique(t["ORDER_ITEM_PROMOTION"], ["order_item_id", "promo_id"], "ORDER_ITEM_PROMOTION")
    _assert_unique(t["PAYMENTS"], ["order_id"], "PAYMENTS")

    # Standardize identifiers / dates / numerics.
    t["CUSTOMERS"]["customer_id"] = _to_numeric(t["CUSTOMERS"]["customer_id"], "CUSTOMERS.customer_id").astype("int64")
    t["CUSTOMERS"]["zip"] = _clean_zip_string(t["CUSTOMERS"]["zip"])
    t["CUSTOMERS"]["signup_date"] = _to_date(t["CUSTOMERS"]["signup_date"], "CUSTOMERS.signup_date", allow_null=True)

    t["GEOGRAPHY"]["zip"] = _clean_zip_string(t["GEOGRAPHY"]["zip"])

    for c in ["order_id", "customer_id"]:
        t["ORDERS"][c] = _to_numeric(t["ORDERS"][c], f"ORDERS.{c}").astype("int64")
    t["ORDERS"]["order_date"] = _to_date(t["ORDERS"]["order_date"], "ORDERS.order_date")

    for c in ["order_item_id", "order_id", "product_id", "quantity"]:
        t["ORDER_ITEMS"][c] = _to_numeric(t["ORDER_ITEMS"][c], f"ORDER_ITEMS.{c}").astype("int64")
    for c in ["unit_price", "discount_amount"]:
        t["ORDER_ITEMS"][c] = _to_numeric(t["ORDER_ITEMS"][c], f"ORDER_ITEMS.{c}").astype("float64")

    t["PRODUCTS"]["product_id"] = _to_numeric(t["PRODUCTS"]["product_id"], "PRODUCTS.product_id").astype("int64")
    for c in ["price", "cogs"]:
        t["PRODUCTS"][c] = _to_numeric(t["PRODUCTS"][c], f"PRODUCTS.{c}", allow_null=True).astype("float64")

    t["PROMOTIONS"]["start_date"] = _to_date(t["PROMOTIONS"]["start_date"], "PROMOTIONS.start_date", allow_null=True)
    t["PROMOTIONS"]["end_date"] = _to_date(t["PROMOTIONS"]["end_date"], "PROMOTIONS.end_date", allow_null=True)
    for c in ["discount_value", "min_order_value"]:
        t["PROMOTIONS"][c] = _to_numeric(t["PROMOTIONS"][c], f"PROMOTIONS.{c}", allow_null=True).astype("float64")

    t["ORDER_ITEM_PROMOTION"]["order_item_id"] = _to_numeric(
        t["ORDER_ITEM_PROMOTION"]["order_item_id"], "ORDER_ITEM_PROMOTION.order_item_id"
    ).astype("int64")
    t["ORDER_ITEM_PROMOTION"]["promotion_order"] = _to_numeric(
        t["ORDER_ITEM_PROMOTION"]["promotion_order"], "ORDER_ITEM_PROMOTION.promotion_order"
    ).astype("int64")

    t["PAYMENTS"]["order_id"] = _to_numeric(t["PAYMENTS"]["order_id"], "PAYMENTS.order_id").astype("int64")
    t["PAYMENTS"]["payment_value"] = _to_numeric(t["PAYMENTS"]["payment_value"], "PAYMENTS.payment_value").astype("float64")
    t["PAYMENTS"]["installments"] = _to_numeric(t["PAYMENTS"]["installments"], "PAYMENTS.installments", allow_null=True).astype("Int64")

    return t


# -----------------------------------------------------------------------------
# Dimension builders
# -----------------------------------------------------------------------------

def build_dim_date(orders: pd.DataFrame) -> pd.DataFrame:
    d0 = orders["order_date"].min().normalize()
    d1 = orders["order_date"].max().normalize()
    dates = pd.date_range(d0, d1, freq="D")
    d = pd.DataFrame({"full_date_dt": dates})
    d["date_key"] = _date_key(d["full_date_dt"])
    d["full_date"] = d["full_date_dt"].dt.strftime("%Y-%m-%d")
    d["day"] = d["full_date_dt"].dt.day.astype("int16")
    d["day_name"] = d["full_date_dt"].dt.day_name()
    d["day_of_week"] = (d["full_date_dt"].dt.dayofweek + 1).astype("int8")
    d["week_of_year"] = d["full_date_dt"].dt.isocalendar().week.astype("int16")
    d["month"] = d["full_date_dt"].dt.month.astype("int8")
    d["month_name"] = d["full_date_dt"].dt.month_name()
    d["quarter"] = d["full_date_dt"].dt.quarter.astype("int8")
    d["year"] = d["full_date_dt"].dt.year.astype("int16")
    d["year_month"] = d["full_date_dt"].dt.strftime("%Y-%m")
    d["is_weekend"] = d["full_date_dt"].dt.dayofweek.isin([5, 6])
    return d.drop(columns=["full_date_dt"])[[
        "date_key", "full_date", "day", "day_name", "day_of_week", "week_of_year",
        "month", "month_name", "quarter", "year", "year_month", "is_weekend"
    ]]


def build_dim_geography(geo: pd.DataFrame) -> pd.DataFrame:
    cols = ["zip", "city", "district", "region"]
    d = geo[cols].drop_duplicates().copy()
    return _stable_surrogate(d, ["zip"], "geo_key")


def build_dim_customer(customers: pd.DataFrame, dim_geo: pd.DataFrame) -> pd.DataFrame:
    gm = dim_geo[["geo_key", "zip"]]
    d = customers[["customer_id", "zip", "signup_date", "gender", "age_group", "acquisition_channel"]].merge(
        gm, on="zip", how="left", validate="many_to_one"
    )
    if d["geo_key"].isna().any():
        raise ValueError(f"DIM_CUSTOMER: {int(d['geo_key'].isna().sum()):,} customer rows missing geography key")
    d["signup_date"] = _iso_date(d["signup_date"])
    # Current source contains one profile version per customer; do not invent SCD2 history.
    d["scd_version"] = 1
    d["is_current"] = True
    d = d[[
        "customer_id", "zip", "geo_key", "signup_date", "gender", "age_group",
        "acquisition_channel", "scd_version", "is_current"
    ]]
    return _stable_surrogate(d, ["customer_id"], "customer_key")


def build_dim_product(products: pd.DataFrame) -> pd.DataFrame:
    d = products[[
        "product_id", "product_name", "category", "segment", "size", "color", "price", "cogs"
    ]].copy()
    d = d.rename(columns={"price": "list_price"})
    d["scd_version"] = 1
    d["is_current"] = True
    return _stable_surrogate(d, ["product_id"], "product_key")


def build_dim_promotion(promotions: pd.DataFrame) -> pd.DataFrame:
    d = promotions[[
        "promo_id", "promo_name", "promo_type", "discount_value", "start_date", "end_date",
        "applicable_category", "promo_channel", "stackable_flag", "min_order_value"
    ]].copy()
    d["start_date"] = _iso_date(d["start_date"])
    d["end_date"] = _iso_date(d["end_date"])
    return _stable_surrogate(d, ["promo_id"], "promotion_key")


def build_dim_channel(orders: pd.DataFrame, payments: pd.DataFrame) -> pd.DataFrame:
    x = orders[["order_id", "order_source", "device_type"]].merge(
        payments[["order_id", "payment_method"]], on="order_id", how="left", validate="one_to_one"
    )
    if x["payment_method"].isna().any():
        raise ValueError(f"DIM_CHANNEL: {int(x['payment_method'].isna().sum()):,} orders missing payment method")
    d = x[["order_source", "device_type", "payment_method"]].drop_duplicates().copy()
    return _stable_surrogate(d, ["order_source", "device_type", "payment_method"], "channel_key")


def build_dim_order_status(orders: pd.DataFrame) -> pd.DataFrame:
    d = orders[["order_status"]].drop_duplicates().copy()
    return _stable_surrogate(d, ["order_status"], "order_status_key")


# -----------------------------------------------------------------------------
# Fact / bridge builders
# -----------------------------------------------------------------------------

def build_order_context(
    orders: pd.DataFrame,
    payments: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_geo: pd.DataFrame,
    dim_channel: pd.DataFrame,
    dim_status: pd.DataFrame,
) -> pd.DataFrame:
    x = orders[[
        "order_id", "customer_id", "order_date", "order_status", "order_source", "device_type"
    ]].merge(
        payments[["order_id", "payment_method", "payment_value", "installments"]],
        on="order_id", how="left", validate="one_to_one"
    )
    x["date_key"] = _date_key(x["order_date"])
    x = x.merge(dim_customer[["customer_key", "customer_id", "geo_key"]], on="customer_id", how="left", validate="many_to_one")
    x = x.merge(
        dim_channel[["channel_key", "order_source", "device_type", "payment_method"]],
        on=["order_source", "device_type", "payment_method"], how="left", validate="many_to_one"
    )
    x = x.merge(dim_status[["order_status_key", "order_status"]], on="order_status", how="left", validate="many_to_one")
    for col in ["customer_key", "geo_key", "channel_key", "order_status_key"]:
        if x[col].isna().any():
            raise ValueError(f"Order context: {int(x[col].isna().sum()):,} missing {col}")
    return x


def build_fact_sales_line(
    order_items: pd.DataFrame,
    products: pd.DataFrame,
    order_ctx: pd.DataFrame,
    dim_product: pd.DataFrame,
) -> pd.DataFrame:
    x = order_items[[
        "order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount"
    ]].merge(
        order_ctx[[
            "order_id", "date_key", "customer_key", "geo_key", "channel_key", "order_status_key"
        ]], on="order_id", how="left", validate="many_to_one"
    )
    x = x.merge(dim_product[["product_key", "product_id"]], on="product_id", how="left", validate="many_to_one")
    for col in ["date_key", "customer_key", "geo_key", "channel_key", "order_status_key", "product_key"]:
        if x[col].isna().any():
            raise ValueError(f"FACT_SALES_LINE: {int(x[col].isna().sum()):,} missing {col}")
    x["gross_sales"] = x["quantity"].astype("float64") * x["unit_price"]
    x["net_sales"] = x["gross_sales"] - x["discount_amount"]
    x = x.sort_values("order_item_id", kind="mergesort").reset_index(drop=True)
    x.insert(0, "sales_line_key", np.arange(1, len(x) + 1, dtype=np.int64))
    return x[[
        "sales_line_key", "order_item_id", "order_id", "date_key", "customer_key", "product_key",
        "geo_key", "channel_key", "order_status_key", "quantity", "unit_price", "discount_amount",
        "gross_sales", "net_sales"
    ]]


def build_fact_order(
    fact_line: pd.DataFrame,
    order_ctx: pd.DataFrame,
) -> pd.DataFrame:
    agg = fact_line.groupby("order_id", as_index=False).agg(
        gross_order_value=("gross_sales", "sum"),
        discount_total=("discount_amount", "sum"),
        order_value=("net_sales", "sum"),
        basket_quantity=("quantity", "sum"),
        distinct_products=("product_key", "nunique"),
        line_count=("order_item_id", "count"),
    )
    x = order_ctx[[
        "order_id", "date_key", "customer_key", "geo_key", "channel_key", "order_status_key",
        "payment_value", "installments"
    ]].merge(agg, on="order_id", how="left", validate="one_to_one")
    if x["order_value"].isna().any():
        raise ValueError(f"FACT_ORDER: {int(x['order_value'].isna().sum()):,} orders have no line items")
    x = x.sort_values("order_id", kind="mergesort").reset_index(drop=True)
    x.insert(0, "order_key", np.arange(1, len(x) + 1, dtype=np.int64))
    return x[[
        "order_key", "order_id", "date_key", "customer_key", "geo_key", "channel_key",
        "order_status_key", "gross_order_value", "discount_total", "order_value", "basket_quantity",
        "distinct_products", "line_count", "payment_value", "installments"
    ]]


def build_bridge_promotions(
    src_bridge: pd.DataFrame,
    fact_line: pd.DataFrame,
    dim_promo: pd.DataFrame,
) -> pd.DataFrame:
    x = src_bridge.merge(
        fact_line[["sales_line_key", "order_item_id"]], on="order_item_id", how="left", validate="many_to_one"
    ).merge(
        dim_promo[["promotion_key", "promo_id"]], on="promo_id", how="left", validate="many_to_one"
    )
    if x[["sales_line_key", "promotion_key"]].isna().any().any():
        raise ValueError("BRIDGE_SALES_LINE_PROMOTION: missing fact/dimension surrogate keys")
    promo_counts = x.groupby("sales_line_key")["promotion_key"].transform("count")
    x["promotion_count_on_line"] = promo_counts.astype("int16")
    x["allocation_weight"] = 1.0 / promo_counts.astype("float64")
    x = x.sort_values(["sales_line_key", "promotion_order", "promotion_key"], kind="mergesort").reset_index(drop=True)
    return x[[
        "sales_line_key", "promotion_key", "promotion_order", "promotion_count_on_line", "allocation_weight"
    ]]


def build_fact_customer_monthly_snapshot(fact_order: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
    date_lu = dim_date[["date_key", "full_date", "year_month"]].copy()
    date_lu["full_date_dt"] = pd.to_datetime(date_lu["full_date"])
    x = fact_order.merge(date_lu[["date_key", "year_month"]], on="date_key", how="left", validate="many_to_one")
    monthly = x.groupby(["customer_key", "year_month"], as_index=False).agg(
        monthly_order_count=("order_id", "count"),
        monthly_quantity=("basket_quantity", "sum"),
        monthly_net_sales=("order_value", "sum"),
    )
    monthly["monthly_aov"] = _safe_divide(monthly["monthly_net_sales"], monthly["monthly_order_count"])
    monthly["monthly_basket_size"] = _safe_divide(monthly["monthly_quantity"], monthly["monthly_order_count"])

    # Snapshot key points to the first calendar date of each month in DIM_DATE.
    ym_to_key = (
        dim_date.assign(_rank=dim_date.groupby("year_month")["date_key"].rank(method="first"))
        .sort_values(["year_month", "date_key"])
        .drop_duplicates("year_month")
        .set_index("year_month")["date_key"]
    )
    monthly["snapshot_month_key"] = monthly["year_month"].map(ym_to_key).astype("int64")

    monthly = monthly.sort_values(["customer_key", "snapshot_month_key"], kind="mergesort").reset_index(drop=True)
    monthly["cumulative_order_count"] = monthly.groupby("customer_key")["monthly_order_count"].cumsum()
    monthly["cumulative_quantity"] = monthly.groupby("customer_key")["monthly_quantity"].cumsum()
    monthly["cumulative_net_sales"] = monthly.groupby("customer_key")["monthly_net_sales"].cumsum()
    monthly["cumulative_aov"] = _safe_divide(monthly["cumulative_net_sales"], monthly["cumulative_order_count"])
    monthly["cumulative_basket_size"] = _safe_divide(monthly["cumulative_quantity"], monthly["cumulative_order_count"])
    return monthly[[
        "snapshot_month_key", "customer_key", "monthly_order_count", "monthly_quantity", "monthly_net_sales",
        "monthly_aov", "monthly_basket_size", "cumulative_order_count", "cumulative_quantity",
        "cumulative_net_sales", "cumulative_aov", "cumulative_basket_size"
    ]]


# -----------------------------------------------------------------------------
# Metadata / validation
# -----------------------------------------------------------------------------

def schema_dictionary() -> pd.DataFrame:
    rows: List[dict] = []
    def add(table, field, dtype, key, source, note=""):
        rows.append({"table": table, "field": field, "data_type": dtype, "key": key, "source_or_formula": source, "note": note})

    for field, dtype, key, src, note in [
        ("date_key", "INT", "PK", "YYYYMMDD(full_date)", "Conformed date key"),
        ("full_date", "DATE", "", "calendar spine", "Continuous from min to max order_date"),
        ("day", "SMALLINT", "", "full_date", ""),
        ("day_name", "VARCHAR(12)", "", "full_date", ""),
        ("day_of_week", "TINYINT", "", "Monday=1..Sunday=7", ""),
        ("week_of_year", "SMALLINT", "", "ISO week", ""),
        ("month", "TINYINT", "", "full_date", ""),
        ("month_name", "VARCHAR(12)", "", "full_date", ""),
        ("quarter", "TINYINT", "", "full_date", ""),
        ("year", "SMALLINT", "", "full_date", ""),
        ("year_month", "CHAR(7)", "", "YYYY-MM", ""),
        ("is_weekend", "BOOLEAN", "", "day_of_week", ""),
    ]: add("DIM_DATE", field, dtype, key, src, note)

    dimension_specs = {
        "DIM_GEOGRAPHY": [
            ("geo_key","BIGINT","PK","surrogate"),("zip","VARCHAR(20)","BK","GEOGRAPHY.zip"),
            ("city","VARCHAR(100)","","GEOGRAPHY.city"),("district","VARCHAR(100)","","GEOGRAPHY.district"),
            ("region","VARCHAR(100)","","GEOGRAPHY.region")],
        "DIM_CUSTOMER": [
            ("customer_key","BIGINT","PK","surrogate"),("customer_id","BIGINT","BK","CUSTOMERS.customer_id"),
            ("zip","VARCHAR(20)","","CUSTOMERS.zip"),("geo_key","BIGINT","FK","DIM_GEOGRAPHY.geo_key"),
            ("signup_date","DATE","","CUSTOMERS.signup_date"),("gender","VARCHAR(20)","","CUSTOMERS.gender"),
            ("age_group","VARCHAR(20)","","CUSTOMERS.age_group"),("acquisition_channel","VARCHAR(50)","","CUSTOMERS.acquisition_channel"),
            ("scd_version","INT","","1","v1 current-state profile"),("is_current","BOOLEAN","","TRUE","No synthetic history")],
        "DIM_PRODUCT": [
            ("product_key","BIGINT","PK","surrogate"),("product_id","BIGINT","BK","PRODUCTS.product_id"),
            ("product_name","VARCHAR(255)","","PRODUCTS.product_name"),("category","VARCHAR(50)","","PRODUCTS.category"),
            ("segment","VARCHAR(50)","","PRODUCTS.segment"),("size","VARCHAR(50)","","PRODUCTS.size"),
            ("color","VARCHAR(50)","","PRODUCTS.color"),("list_price","DECIMAL(18,2)","","PRODUCTS.price"),
            ("cogs","DECIMAL(18,2)","","PRODUCTS.cogs"),("scd_version","INT","","1"),("is_current","BOOLEAN","","TRUE")],
        "DIM_PROMOTION": [
            ("promotion_key","BIGINT","PK","surrogate"),("promo_id","VARCHAR(20)","BK","PROMOTIONS.promo_id"),
            ("promo_name","VARCHAR(255)","","PROMOTIONS.promo_name"),("promo_type","VARCHAR(50)","","PROMOTIONS.promo_type"),
            ("discount_value","DECIMAL(18,2)","","PROMOTIONS.discount_value"),("start_date","DATE","","PROMOTIONS.start_date"),
            ("end_date","DATE","","PROMOTIONS.end_date"),("applicable_category","VARCHAR(100)","","PROMOTIONS.applicable_category"),
            ("promo_channel","VARCHAR(50)","","PROMOTIONS.promo_channel"),("stackable_flag","BOOLEAN","","PROMOTIONS.stackable_flag"),
            ("min_order_value","DECIMAL(18,2)","","PROMOTIONS.min_order_value")],
        "DIM_CHANNEL": [
            ("channel_key","BIGINT","PK","surrogate"),("order_source","VARCHAR(50)","BK part","ORDERS.order_source"),
            ("device_type","VARCHAR(30)","BK part","ORDERS.device_type"),("payment_method","VARCHAR(50)","BK part","PAYMENTS.payment_method")],
        "DIM_ORDER_STATUS": [
            ("order_status_key","BIGINT","PK","surrogate"),("order_status","VARCHAR(30)","BK","ORDERS.order_status")],
    }
    for table, specs in dimension_specs.items():
        for field, dtype, key, src, *rest in specs:
            add(table, field, dtype, key, src, rest[0] if rest else "")

    for field, dtype, key, src, note in [
        ("sales_line_key","BIGINT","PK","surrogate","1 row per order_item_id"),
        ("order_item_id","BIGINT","Degenerate/BK","ORDER_ITEMS.order_item_id","source line id"),
        ("order_id","BIGINT","Degenerate","ORDER_ITEMS.order_id","business transaction id"),
        ("date_key","INT","FK","DIM_DATE",""),("customer_key","BIGINT","FK","DIM_CUSTOMER",""),
        ("product_key","BIGINT","FK","DIM_PRODUCT",""),("geo_key","BIGINT","FK","DIM_GEOGRAPHY","customer geography"),
        ("channel_key","BIGINT","FK","DIM_CHANNEL",""),("order_status_key","BIGINT","FK","DIM_ORDER_STATUS",""),
        ("quantity","INT","Measure","ORDER_ITEMS.quantity","additive"),("unit_price","DECIMAL(18,2)","Measure","ORDER_ITEMS.unit_price","non-additive unit rate"),
        ("discount_amount","DECIMAL(18,2)","Measure","ORDER_ITEMS.discount_amount","additive"),
        ("gross_sales","DECIMAL(18,2)","Measure","quantity*unit_price","additive"),
        ("net_sales","DECIMAL(18,2)","Measure","gross_sales-discount_amount","additive"),
    ]: add("FACT_SALES_LINE", field, dtype, key, src, note)

    for field, dtype, key, src, note in [
        ("order_key","BIGINT","PK","surrogate","1 row per order_id"),("order_id","BIGINT","Degenerate/BK","ORDERS.order_id",""),
        ("date_key","INT","FK","DIM_DATE",""),("customer_key","BIGINT","FK","DIM_CUSTOMER",""),
        ("geo_key","BIGINT","FK","DIM_GEOGRAPHY",""),("channel_key","BIGINT","FK","DIM_CHANNEL",""),
        ("order_status_key","BIGINT","FK","DIM_ORDER_STATUS",""),("gross_order_value","DECIMAL(18,2)","Measure","SUM gross_sales",""),
        ("discount_total","DECIMAL(18,2)","Measure","SUM discount_amount",""),("order_value","DECIMAL(18,2)","Measure","SUM net_sales",""),
        ("basket_quantity","BIGINT","Measure","SUM quantity",""),("distinct_products","INT","Measure","COUNT DISTINCT product_key",""),
        ("line_count","INT","Measure","COUNT order_item_id",""),("payment_value","DECIMAL(18,2)","Measure","PAYMENTS.payment_value","reconciliation"),
        ("installments","INT","Attribute","PAYMENTS.installments","transaction-level attribute"),
    ]: add("FACT_ORDER", field, dtype, key, src, note)

    for field, dtype, key, src, note in [
        ("sales_line_key","BIGINT","PK/FK part","FACT_SALES_LINE.sales_line_key",""),
        ("promotion_key","BIGINT","PK/FK part","DIM_PROMOTION.promotion_key",""),
        ("promotion_order","SMALLINT","","ORDER_ITEM_PROMOTION.promotion_order","preserves source promo slot 1/2"),
        ("promotion_count_on_line","SMALLINT","","COUNT promotions/line",""),
        ("allocation_weight","DECIMAL(12,8)","","1/promotion_count_on_line","optional anti-double-counting weight"),
    ]: add("BRIDGE_SALES_LINE_PROMOTION", field, dtype, key, src, note)

    for field, dtype, key, src, note in [
        ("snapshot_month_key","INT","PK/FK part","DIM_DATE.date_key","first date of month"),
        ("customer_key","BIGINT","PK/FK part","DIM_CUSTOMER.customer_key",""),
        ("monthly_order_count","INT","Measure","COUNT orders/customer-month",""),
        ("monthly_quantity","BIGINT","Measure","SUM basket_quantity",""),
        ("monthly_net_sales","DECIMAL(18,2)","Measure","SUM order_value",""),
        ("monthly_aov","DECIMAL(18,2)","Measure","monthly_net_sales/monthly_order_count",""),
        ("monthly_basket_size","DECIMAL(18,4)","Measure","monthly_quantity/monthly_order_count",""),
        ("cumulative_order_count","BIGINT","Measure","running SUM monthly_order_count",""),
        ("cumulative_quantity","BIGINT","Measure","running SUM monthly_quantity",""),
        ("cumulative_net_sales","DECIMAL(18,2)","Measure","running SUM monthly_net_sales",""),
        ("cumulative_aov","DECIMAL(18,2)","Measure","cumulative_net_sales/cumulative_order_count",""),
        ("cumulative_basket_size","DECIMAL(18,4)","Measure","cumulative_quantity/cumulative_order_count",""),
    ]: add("FACT_CUSTOMER_MONTHLY_SNAPSHOT", field, dtype, key, src, note)

    return pd.DataFrame(rows)


def lineage_table() -> pd.DataFrame:
    rows = [
        ["CUSTOMERS", "DIM_CUSTOMER", "customer profile; geography surrogate", "P3.1-P3.4 customer context"],
        ["GEOGRAPHY", "DIM_GEOGRAPHY", "zip/city/district/region", "profile segmentation by geography"],
        ["PRODUCTS", "DIM_PRODUCT", "product/category/segment", "P3.5 category behavior"],
        ["PROMOTIONS", "DIM_PROMOTION", "promotion attributes", "future promotion drilldown; not primary P3 KPI"],
        ["ORDERS + PAYMENTS", "DIM_CHANNEL", "source/device/payment method", "customer behavior by channel"],
        ["ORDERS", "DIM_ORDER_STATUS", "status lookup", "explicit downstream status scoping"],
        ["ORDER_ITEMS + ORDERS + dimensions", "FACT_SALES_LINE", "1 row/order_item_id; net sales", "P3.1/P3.4/P3.5 transaction base"],
        ["ORDERS + ORDER_ITEMS + PAYMENTS", "FACT_ORDER", "1 row/order; order value/basket", "P3.2/P3.3"],
        ["ORDER_ITEM_PROMOTION", "BRIDGE_SALES_LINE_PROMOTION", "M:N promotion bridge", "traceability / future promotion analysis"],
        ["FACT_ORDER + DIM_DATE", "FACT_CUSTOMER_MONTHLY_SNAPSHOT", "customer x month snapshot", "P3.5 trends / cumulative behavior"],
    ]
    return pd.DataFrame(rows, columns=["3nf_source", "dw_target", "transformation", "problem3_use"])


def validate_dw(
    src: Dict[str, pd.DataFrame],
    dims: Dict[str, pd.DataFrame],
    facts: Dict[str, pd.DataFrame],
    bridges: Dict[str, pd.DataFrame],
    tolerance: float,
) -> pd.DataFrame:
    recs: List[dict] = []
    def add(cid: str, name: str, ok: bool, actual, expected, detail=""):
        recs.append({
            "check_id": cid, "check_name": name, "status": _status(ok),
            "actual": _fmt(actual), "expected": _fmt(expected), "detail": detail
        })

    dl = dims["DIM_DATE"]
    dc = dims["DIM_CUSTOMER"]
    dg = dims["DIM_GEOGRAPHY"]
    dp = dims["DIM_PRODUCT"]
    dpr = dims["DIM_PROMOTION"]
    dch = dims["DIM_CHANNEL"]
    ds = dims["DIM_ORDER_STATUS"]
    fl = facts["FACT_SALES_LINE"]
    fo = facts["FACT_ORDER"]
    fm = facts["FACT_CUSTOMER_MONTHLY_SNAPSHOT"]
    bp = bridges["BRIDGE_SALES_LINE_PROMOTION"]

    add("DW01", "DIM_CUSTOMER row count = CUSTOMERS", len(dc) == len(src["CUSTOMERS"]), len(dc), len(src["CUSTOMERS"]))
    add("DW02", "DIM_PRODUCT row count = PRODUCTS", len(dp) == len(src["PRODUCTS"]), len(dp), len(src["PRODUCTS"]))
    add("DW03", "DIM_GEOGRAPHY row count = GEOGRAPHY", len(dg) == len(src["GEOGRAPHY"]), len(dg), len(src["GEOGRAPHY"]))
    add("DW04", "DIM_PROMOTION row count = PROMOTIONS", len(dpr) == len(src["PROMOTIONS"]), len(dpr), len(src["PROMOTIONS"]))
    add("DW05", "FACT_SALES_LINE row count = ORDER_ITEMS", len(fl) == len(src["ORDER_ITEMS"]), len(fl), len(src["ORDER_ITEMS"]))
    add("DW06", "FACT_ORDER row count = ORDERS", len(fo) == len(src["ORDERS"]), len(fo), len(src["ORDERS"]))
    add("DW07", "Bridge row count = ORDER_ITEM_PROMOTION", len(bp) == len(src["ORDER_ITEM_PROMOTION"]), len(bp), len(src["ORDER_ITEM_PROMOTION"]))
    add("DW08", "FACT_SALES_LINE sales_line_key unique", fl["sales_line_key"].is_unique, int(fl["sales_line_key"].nunique()), len(fl))
    add("DW09", "FACT_ORDER order_id unique", fo["order_id"].is_unique, int(fo["order_id"].nunique()), len(fo))
    add("DW10", "DIM_CHANNEL natural combination unique", not dch.duplicated(["order_source","device_type","payment_method"]).any(), int(dch.drop_duplicates(["order_source","device_type","payment_method"]).shape[0]), len(dch))
    add("DW11", "DIM_ORDER_STATUS covers all source statuses", set(ds["order_status"].astype(str)) == set(src["ORDERS"]["order_status"].astype(str)), len(ds), src["ORDERS"]["order_status"].nunique())

    fact_fk_checks = {
        "date_key": set(dl["date_key"]),
        "customer_key": set(dc["customer_key"]),
        "product_key": set(dp["product_key"]),
        "geo_key": set(dg["geo_key"]),
        "channel_key": set(dch["channel_key"]),
        "order_status_key": set(ds["order_status_key"]),
    }
    missing_fk = 0
    for col, valid in fact_fk_checks.items():
        missing_fk += int((~fl[col].isin(valid)).sum())
    add("DW12", "FACT_SALES_LINE all dimension FKs valid", missing_fk == 0, missing_fk, 0)

    fact_order_fk_checks = {k:v for k,v in fact_fk_checks.items() if k != "product_key"}
    missing_fk2 = 0
    for col, valid in fact_order_fk_checks.items():
        missing_fk2 += int((~fo[col].isin(valid)).sum())
    add("DW13", "FACT_ORDER all dimension FKs valid", missing_fk2 == 0, missing_fk2, 0)

    missing_bridge_fk = int((~bp["sales_line_key"].isin(set(fl["sales_line_key"]))).sum()) + int((~bp["promotion_key"].isin(set(dpr["promotion_key"]))).sum())
    add("DW14", "Promotion bridge FKs valid", missing_bridge_fk == 0, missing_bridge_fk, 0)

    line_sales = float(fl["net_sales"].sum())
    order_sales = float(fo["order_value"].sum())
    payment_sales = float(src["PAYMENTS"]["payment_value"].sum())
    snapshot_sales = float(fm["monthly_net_sales"].sum())
    add("DW15", "SUM line net_sales = SUM order_value", _eq_money(line_sales, order_sales, tolerance), line_sales, order_sales, f"tol={tolerance}")
    add("DW16", "SUM order_value = SUM payment_value", _eq_money(order_sales, payment_sales, tolerance), order_sales, payment_sales, f"tol={tolerance}")
    add("DW17", "SUM monthly snapshot net sales = FACT_ORDER", _eq_money(snapshot_sales, order_sales, tolerance), snapshot_sales, order_sales, f"tol={tolerance}")
    add("DW18", "SUM monthly snapshot orders = FACT_ORDER rows", int(fm["monthly_order_count"].sum()) == len(fo), int(fm["monthly_order_count"].sum()), len(fo))

    expected_min = int(src["ORDERS"]["order_date"].min().strftime("%Y%m%d"))
    expected_max = int(src["ORDERS"]["order_date"].max().strftime("%Y%m%d"))
    add("DW19", "DIM_DATE min/max covers order dates", int(dl["date_key"].min()) == expected_min and int(dl["date_key"].max()) == expected_max, f"{int(dl['date_key'].min())}-{int(dl['date_key'].max())}", f"{expected_min}-{expected_max}")

    promo_order_ok = set(bp["promotion_order"].dropna().astype(int).unique()).issubset({1,2})
    add("DW20", "promotion_order remains in {1,2}", promo_order_ok, sorted(bp["promotion_order"].dropna().astype(int).unique().tolist()), [1,2])

    weights_ok = True
    if len(bp):
        weights = bp.groupby("sales_line_key")["allocation_weight"].sum()
        weights_ok = bool(np.allclose(weights.values, 1.0, atol=1e-10))
    add("DW21", "Promotion allocation weights sum to 1 per promoted line", weights_ok, "all" if weights_ok else "mismatch", "1.0")

    snapshot_unique = not fm.duplicated(["snapshot_month_key", "customer_key"]).any()
    add("DW22", "Monthly snapshot grain unique", snapshot_unique, int(fm.drop_duplicates(["snapshot_month_key","customer_key"]).shape[0]), len(fm))

    # Explicit safeguard: no order status excluded by the warehouse layer.
    source_status_counts = src["ORDERS"].groupby("order_status").size().sort_index()
    status_map = ds.set_index("order_status")["order_status_key"]
    fact_status_counts = fo.assign(order_status=fo["order_status_key"].map(ds.set_index("order_status_key")["order_status"])).groupby("order_status").size().sort_index()
    status_equal = source_status_counts.equals(fact_status_counts)
    add("DW23", "Order-status counts preserved exactly", status_equal, fact_status_counts.to_dict(), source_status_counts.to_dict())

    return pd.DataFrame(recs)


def reconciliation_table(src, dims, facts, bridges) -> pd.DataFrame:
    fl = facts["FACT_SALES_LINE"]
    fo = facts["FACT_ORDER"]
    fm = facts["FACT_CUSTOMER_MONTHLY_SNAPSHOT"]
    rows = [
        ["SOURCE_CUSTOMERS", len(src["CUSTOMERS"]), "rows"],
        ["DIM_CUSTOMER", len(dims["DIM_CUSTOMER"]), "rows"],
        ["SOURCE_ORDERS", len(src["ORDERS"]), "rows"],
        ["FACT_ORDER", len(fo), "rows"],
        ["SOURCE_ORDER_ITEMS", len(src["ORDER_ITEMS"]), "rows"],
        ["FACT_SALES_LINE", len(fl), "rows"],
        ["SOURCE_ORDER_ITEM_PROMOTION", len(src["ORDER_ITEM_PROMOTION"]), "rows"],
        ["BRIDGE_SALES_LINE_PROMOTION", len(bridges["BRIDGE_SALES_LINE_PROMOTION"]), "rows"],
        ["FACT_LINE_NET_SALES", float(fl["net_sales"].sum()), "currency"],
        ["FACT_ORDER_VALUE", float(fo["order_value"].sum()), "currency"],
        ["SOURCE_PAYMENT_VALUE", float(src["PAYMENTS"]["payment_value"].sum()), "currency"],
        ["MONTHLY_SNAPSHOT_NET_SALES", float(fm["monthly_net_sales"].sum()), "currency"],
        ["MONTHLY_SNAPSHOT_ORDER_COUNT", int(fm["monthly_order_count"].sum()), "orders"],
    ]
    return pd.DataFrame(rows, columns=["metric", "value", "unit"])


# -----------------------------------------------------------------------------
# Output
# -----------------------------------------------------------------------------

def write_outputs(output: Path, dims, facts, bridges, validation, reconciliation, src, input_path: Path) -> None:
    dim_dir = output / "dimensions"
    fact_dir = output / "facts"
    bridge_dir = output / "bridges"
    report_dir = output / "reports"
    for p in [dim_dir, fact_dir, bridge_dir, report_dir]:
        p.mkdir(parents=True, exist_ok=True)

    for name, df in dims.items():
        df.to_csv(dim_dir / f"{name}.csv", index=False, encoding="utf-8-sig")
    for name, df in facts.items():
        df.to_csv(fact_dir / f"{name}.csv.gz", index=False, encoding="utf-8-sig", compression={"method":"gzip","compresslevel":1})
    for name, df in bridges.items():
        df.to_csv(bridge_dir / f"{name}.csv.gz", index=False, encoding="utf-8-sig", compression={"method":"gzip","compresslevel":1})

    validation.to_csv(report_dir / "dw_validation.csv", index=False, encoding="utf-8-sig")
    reconciliation.to_csv(report_dir / "dw_reconciliation.csv", index=False, encoding="utf-8-sig")
    schema_dictionary().to_csv(report_dir / "dw_schema_dictionary.csv", index=False, encoding="utf-8-sig")
    lineage_table().to_csv(report_dir / "dw_lineage.csv", index=False, encoding="utf-8-sig")

    metadata = pd.DataFrame([
        ["script_version", SCRIPT_VERSION],
        ["run_utc", datetime.now(timezone.utc).isoformat()],
        ["input", str(input_path)],
        ["physical_model", "Fact Constellation / shared conformed dimensions"],
        ["star_schema", "Primary dimensional design"],
        ["snowflake_schema", "Documented alternative; not physical v1"],
        ["order_status_scope", "ALL source statuses preserved; no silent filtering"],
        ["customer_scd", "Type 1 current-state in v1; source has no historical profile versions"],
        ["product_scd", "Type 1 current-state in v1"],
        ["net_sales_formula", "quantity * unit_price - discount_amount"],
        ["validation_pass", int((validation["status"] == "PASS").sum())],
        ["validation_fail", int((validation["status"] == "FAIL").sum())],
    ], columns=["key", "value"])
    metadata.to_csv(report_dir / "dw_run_metadata.csv", index=False, encoding="utf-8-sig")

    readme = f"""Problem 3 - Dimensional Data Warehouse Output
================================================
Generated by: 3nf_to_dw_colab.py v{SCRIPT_VERSION}
Input: {input_path}

Physical model:
- Fact constellation / galaxy schema
- Shared conformed dimensions form Star Schemas around each fact
- Snowflake alternative is design documentation only in v1

Dimensions:
- DIM_DATE
- DIM_CUSTOMER
- DIM_GEOGRAPHY
- DIM_PRODUCT
- DIM_PROMOTION
- DIM_CHANNEL
- DIM_ORDER_STATUS

Facts:
- FACT_SALES_LINE      (grain: 1 row / order_item_id)
- FACT_ORDER           (grain: 1 row / order_id)
- FACT_CUSTOMER_MONTHLY_SNAPSHOT (grain: customer x month)

Bridge:
- BRIDGE_SALES_LINE_PROMOTION

Important:
- All order statuses are preserved in the DW. Any completed-order scope must be
  defined explicitly in Gold/EDA; this ETL does not silently filter statuses.
- Customer/Product dimensions use current-state Type 1 loading in v1 because
  the source has no historical dimension-version records.
"""
    (output / "README.txt").write_text(readme, encoding="utf-8")


def make_zip(output: Path) -> Path:
    zip_path = output.with_suffix(".zip")
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as z:
        for p in sorted(output.rglob("*")):
            if p.is_file():
                z.write(p, p.relative_to(output.parent))
    return zip_path


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def run(input_path: Path, output: Path, tolerance: float) -> int:
    print(f"[DW] Input : {input_path}")
    print(f"[DW] Output: {output}")
    if output.exists():
        shutil.rmtree(output)

    reader = ThreeNFReader(input_path)
    try:
        src = load_source(reader)
    finally:
        reader.close()

    print("[1/6] Building conformed dimensions...")
    dims: Dict[str, pd.DataFrame] = {}
    dims["DIM_DATE"] = build_dim_date(src["ORDERS"])
    dims["DIM_GEOGRAPHY"] = build_dim_geography(src["GEOGRAPHY"])
    dims["DIM_CUSTOMER"] = build_dim_customer(src["CUSTOMERS"], dims["DIM_GEOGRAPHY"])
    dims["DIM_PRODUCT"] = build_dim_product(src["PRODUCTS"])
    dims["DIM_PROMOTION"] = build_dim_promotion(src["PROMOTIONS"])
    dims["DIM_CHANNEL"] = build_dim_channel(src["ORDERS"], src["PAYMENTS"])
    dims["DIM_ORDER_STATUS"] = build_dim_order_status(src["ORDERS"])

    print("[2/6] Building order context...")
    ctx = build_order_context(
        src["ORDERS"], src["PAYMENTS"], dims["DIM_CUSTOMER"], dims["DIM_GEOGRAPHY"],
        dims["DIM_CHANNEL"], dims["DIM_ORDER_STATUS"]
    )

    print("[3/6] Building transaction facts...")
    facts: Dict[str, pd.DataFrame] = {}
    facts["FACT_SALES_LINE"] = build_fact_sales_line(src["ORDER_ITEMS"], src["PRODUCTS"], ctx, dims["DIM_PRODUCT"])
    facts["FACT_ORDER"] = build_fact_order(facts["FACT_SALES_LINE"], ctx)

    print("[4/6] Building promotion bridge and monthly snapshot...")
    bridges = {
        "BRIDGE_SALES_LINE_PROMOTION": build_bridge_promotions(
            src["ORDER_ITEM_PROMOTION"], facts["FACT_SALES_LINE"], dims["DIM_PROMOTION"]
        )
    }
    facts["FACT_CUSTOMER_MONTHLY_SNAPSHOT"] = build_fact_customer_monthly_snapshot(
        facts["FACT_ORDER"], dims["DIM_DATE"]
    )

    print("[5/6] Validating and reconciling...")
    validation = validate_dw(src, dims, facts, bridges, tolerance)
    reconciliation = reconciliation_table(src, dims, facts, bridges)
    fail_count = int((validation["status"] == "FAIL").sum())

    print("[6/6] Writing outputs...")
    write_outputs(output, dims, facts, bridges, validation, reconciliation, src, input_path)
    zip_path = make_zip(output)

    print("\nDimension rows:")
    for k, v in dims.items():
        print(f"  {k:28s} {len(v):>12,}")
    print("Fact rows:")
    for k, v in facts.items():
        print(f"  {k:35s} {len(v):>12,}")
    print("Bridge rows:")
    for k, v in bridges.items():
        print(f"  {k:35s} {len(v):>12,}")
    print(f"Validation: {(validation['status']=='PASS').sum()} PASS / {fail_count} FAIL")
    print(f"ZIP: {zip_path}")

    if fail_count:
        print("ERROR: DW validation has FAIL checks. Review reports/dw_validation.csv", file=sys.stderr)
        return 2
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build dimensional DW from normalized 3NF output")
    p.add_argument("--input", default="/content/rd3nf_output.zip", help="3NF ZIP or directory")
    p.add_argument("--output", default="/content/dw_problem3", help="Output directory")
    p.add_argument("--payment-tolerance", type=float, default=0.01, help="Absolute reconciliation tolerance")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    raise SystemExit(run(Path(args.input), Path(args.output), args.payment_tolerance))
