from __future__ import annotations

"""
ETL Silver Data -> 14 bảng RD 3NF + Validation Report
======================================================
Thiết kế đích bám theo RD(3) đã chốt:
  1. GEOGRAPHY
  2. CUSTOMERS
  3. SALES_EMPLOYEES
  4. ORDERS
  5. PRODUCTS
  6. ORDER_ITEMS
  7. PROMOTIONS
  8. ORDER_ITEM_PROMOTION
  9. PAYMENTS
 10. SHIPMENTS
 11. SHIPPERS
 12. REVIEWS
 13. RETURNS
 14. INVENTORY_SNAPSHOT

Mặc định chạy trên Google Colab:
  python /content/silver_to_3nf_colab.py \
      --input /content/silver_output.zip \
      --output /content/rd3nf_output

--input có thể là:
  * silver_output.zip (chứa thư mục silver_data/), hoặc
  * thư mục silver_data/ trực tiếp.

Kết quả:
  rd3nf_output/
    normalized_3nf/          14 CSV chuẩn hóa
    reports/                 validation + reconciliation + schema + mapping
    rd3nf_output.zip         gói tải về
"""

import argparse
import json
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import numpy as np
import pandas as pd


# ============================================================
# CONFIG / SCHEMA
# ============================================================

SILVER_FILES = {
    "customers": "customers_silver.csv",
    "geography": "geography_silver.csv",
    "inventory": "inventory_silver.csv",
    "order_items": "order_items_silver.csv",
    "orders": "orders_silver.csv",
    "payments": "payments_silver.csv",
    "products": "products_silver.csv",
    "promotions": "promotions_silver.csv",
    "returns": "returns_silver.csv",
    "reviews": "reviews_silver.csv",
    "shipments": "shipments_silver.csv",
}

# 4 nguồn Silver không đưa vào core ERD/RD theo quyết định phân tích hiện tại.
EXCLUDED_SILVER_SOURCES = {
    "epd_silver.csv": "Nguồn tích hợp/đối chiếu sản phẩm; canonical PRODUCTS được dùng trong core RD.",
    "eprom_silver.csv": "Nguồn promotion instance; core RD dùng PROMOTIONS master + bridge ORDER_ITEM_PROMOTION.",
    "tf_silver.csv": "Nguồn traffic/analytics độc lập, không có FK nghiệp vụ với core transaction.",
    "web_traffic_silver.csv": "Nguồn traffic/analytics độc lập, không có FK nghiệp vụ với core transaction.",
}

TARGET_FILES = {
    "GEOGRAPHY": "GEOGRAPHY.csv",
    "CUSTOMERS": "CUSTOMERS.csv",
    "SALES_EMPLOYEES": "SALES_EMPLOYEES.csv",
    "ORDERS": "ORDERS.csv",
    "PRODUCTS": "PRODUCTS.csv",
    "ORDER_ITEMS": "ORDER_ITEMS.csv",
    "PROMOTIONS": "PROMOTIONS.csv",
    "ORDER_ITEM_PROMOTION": "ORDER_ITEM_PROMOTION.csv",
    "PAYMENTS": "PAYMENTS.csv",
    "SHIPMENTS": "SHIPMENTS.csv",
    "SHIPPERS": "SHIPPERS.csv",
    "REVIEWS": "REVIEWS.csv",
    "RETURNS": "RETURNS.csv",
    "INVENTORY_SNAPSHOT": "INVENTORY_SNAPSHOT.csv",
}

SCHEMA_3NF = {
    "GEOGRAPHY": [
        ("zip", "VARCHAR(20)", "PK", "NOT NULL"),
        ("city", "VARCHAR(100)", "", "NOT NULL"),
        ("district", "VARCHAR(100)", "", "NOT NULL"),
        ("region", "VARCHAR(100)", "", "NOT NULL"),
    ],
    "CUSTOMERS": [
        ("customer_id", "BIGINT", "PK", "NOT NULL"),
        ("zip", "VARCHAR(20)", "FK -> GEOGRAPHY(zip)", "NOT NULL"),
        ("signup_date", "DATE", "", "NOT NULL"),
        ("gender", "VARCHAR(20)", "", "NOT NULL"),
        ("age_group", "VARCHAR(20)", "", "NOT NULL"),
        ("acquisition_channel", "VARCHAR(50)", "", "NOT NULL"),
    ],
    "SALES_EMPLOYEES": [
        ("sales_employee_id", "VARCHAR(20)", "PK", "NOT NULL"),
        ("sales_employee_name", "VARCHAR(100)", "", "NOT NULL"),
        ("marital_status", "VARCHAR(20)", "", "NOT NULL"),
        ("education_level", "VARCHAR(50)", "", "NOT NULL"),
        ("years_experience", "INT", "", "NOT NULL"),
    ],
    "ORDERS": [
        ("order_id", "BIGINT", "PK", "NOT NULL"),
        ("customer_id", "BIGINT", "FK -> CUSTOMERS(customer_id)", "NOT NULL"),
        ("sales_employee_id", "VARCHAR(20)", "FK -> SALES_EMPLOYEES(sales_employee_id)", "NOT NULL"),
        ("order_date", "DATE", "", "NOT NULL"),
        ("order_status", "VARCHAR(30)", "", "NOT NULL"),
        ("device_type", "VARCHAR(30)", "", "NOT NULL"),
        ("order_source", "VARCHAR(50)", "", "NOT NULL"),
        ("comment", "VARCHAR(255)", "", "NULL allowed if source permits"),
    ],
    "PRODUCTS": [
        ("product_id", "BIGINT", "PK", "NOT NULL"),
        ("product_name", "VARCHAR(100)", "", "NOT NULL"),
        ("category", "VARCHAR(50)", "", "NOT NULL"),
        ("segment", "VARCHAR(50)", "", "NOT NULL"),
        ("size", "VARCHAR(20)", "", "NOT NULL"),
        ("color", "VARCHAR(20)", "", "NOT NULL"),
        ("price", "DECIMAL(18,2)", "", "NOT NULL"),
        ("cogs", "DECIMAL(18,2)", "", "NOT NULL"),
    ],
    "ORDER_ITEMS": [
        ("order_item_id", "BIGINT", "PK", "NOT NULL"),
        ("order_id", "BIGINT", "FK -> ORDERS(order_id)", "NOT NULL"),
        ("product_id", "BIGINT", "FK -> PRODUCTS(product_id)", "NOT NULL"),
        ("quantity", "INT", "", "NOT NULL"),
        ("unit_price", "DECIMAL(18,2)", "", "NOT NULL"),
        ("discount_amount", "DECIMAL(18,2)", "", "NOT NULL"),
    ],
    "PROMOTIONS": [
        ("promo_id", "VARCHAR(20)", "PK", "NOT NULL"),
        ("promo_name", "VARCHAR(100)", "", "NOT NULL"),
        ("promo_type", "VARCHAR(30)", "", "NOT NULL"),
        ("discount_value", "DECIMAL(18,2)", "", "NOT NULL"),
        ("start_date", "DATE", "", "NOT NULL"),
        ("end_date", "DATE", "", "NOT NULL"),
        ("applicable_category", "VARCHAR(50)", "", "NULL = all categories"),
        ("promo_channel", "VARCHAR(50)", "", "NOT NULL"),
        ("stackable_flag", "BOOLEAN", "", "NOT NULL"),
        ("min_order_value", "DECIMAL(18,2)", "", "NOT NULL"),
    ],
    "ORDER_ITEM_PROMOTION": [
        ("order_item_id", "BIGINT", "PK, FK -> ORDER_ITEMS(order_item_id)", "NOT NULL"),
        ("promo_id", "VARCHAR(20)", "PK, FK -> PROMOTIONS(promo_id)", "NOT NULL"),
        ("promotion_order", "SMALLINT", "UNIQUE(order_item_id, promotion_order)", "NOT NULL; expected 1 or 2"),
    ],
    "PAYMENTS": [
        ("order_id", "BIGINT", "PK, FK -> ORDERS(order_id)", "NOT NULL"),
        ("payment_method", "VARCHAR(30)", "", "NOT NULL"),
        ("payment_value", "DECIMAL(18,2)", "", "NOT NULL"),
        ("installments", "INT", "", "NOT NULL"),
    ],
    "SHIPMENTS": [
        ("order_id", "BIGINT", "PK, FK -> ORDERS(order_id)", "NOT NULL"),
        ("shipper_id", "VARCHAR(20)", "FK -> SHIPPERS(shipper_id)", "NOT NULL"),
        ("ship_date", "DATE", "", "NOT NULL"),
        ("delivery_date", "DATE", "", "NOT NULL"),
        ("shipping_fee", "DECIMAL(18,2)", "", "NOT NULL"),
    ],
    "SHIPPERS": [
        ("shipper_id", "VARCHAR(20)", "PK", "NOT NULL"),
        ("shipper_name", "VARCHAR(100)", "", "NOT NULL"),
        ("shipper_phone", "VARCHAR(20)", "", "NOT NULL"),
        ("shipper_gender", "VARCHAR(20)", "", "NOT NULL"),
        ("shipper_age", "INT", "", "NOT NULL"),
        ("shipper_marital_status", "VARCHAR(20)", "", "NOT NULL"),
        ("shipper_education", "VARCHAR(50)", "", "NOT NULL"),
        ("shipper_company", "VARCHAR(100)", "", "NOT NULL"),
        ("shipper_vehicle", "VARCHAR(50)", "", "NOT NULL"),
        ("shipper_experience_years", "INT", "", "NOT NULL"),
        ("shipper_rating", "DECIMAL(3,2)", "", "NOT NULL"),
        ("delivery_success_rate", "DECIMAL(5,4)", "", "NOT NULL"),
        ("average_delivery_time", "DECIMAL(10,2)", "", "NOT NULL"),
        ("working_shift", "VARCHAR(50)", "", "NOT NULL"),
        ("join_date", "DATE", "", "NOT NULL"),
        ("city", "VARCHAR(100)", "", "NOT NULL"),
        ("region", "VARCHAR(100)", "", "NOT NULL"),
        ("district", "VARCHAR(100)", "", "NOT NULL"),
    ],
    "REVIEWS": [
        ("review_id", "VARCHAR(20)", "PK", "NOT NULL"),
        ("order_id", "BIGINT", "FK -> ORDERS(order_id)", "NOT NULL"),
        ("product_id", "BIGINT", "FK -> PRODUCTS(product_id)", "NOT NULL"),
        ("review_date", "DATE", "", "NOT NULL"),
        ("rating", "INT", "", "NOT NULL"),
        ("review_title", "VARCHAR(255)", "", "NOT NULL"),
    ],
    "RETURNS": [
        ("return_id", "VARCHAR(20)", "PK", "NOT NULL"),
        ("order_id", "BIGINT", "FK -> ORDERS(order_id)", "NOT NULL"),
        ("product_id", "BIGINT", "FK -> PRODUCTS(product_id)", "NOT NULL"),
        ("return_date", "DATE", "", "NOT NULL"),
        ("return_reason", "VARCHAR(100)", "", "NOT NULL"),
        ("return_quantity", "INT", "", "NOT NULL"),
        ("refund_amount", "DECIMAL(18,2)", "", "NOT NULL"),
    ],
    "INVENTORY_SNAPSHOT": [
        ("product_id", "BIGINT", "PK, FK -> PRODUCTS(product_id)", "NOT NULL"),
        ("snapshot_date", "DATE", "PK", "NOT NULL"),
        ("stock_on_hand", "INT", "", "NOT NULL"),
        ("units_received", "INT", "", "NOT NULL"),
        ("units_sold", "INT", "", "NOT NULL"),
        ("stockout_days", "INT", "", "NOT NULL"),
        ("days_of_supply", "DECIMAL(10,2)", "", "NOT NULL"),
        ("fill_rate", "DECIMAL(5,4)", "", "NOT NULL"),
        ("stockout_flag", "BOOLEAN", "", "NOT NULL"),
        ("overstock_flag", "BOOLEAN", "", "NOT NULL"),
        ("reorder_flag", "BOOLEAN", "", "NOT NULL"),
        ("sell_through_rate", "DECIMAL(5,4)", "", "NOT NULL"),
    ],
}

PKS = {
    "GEOGRAPHY": ["zip"],
    "CUSTOMERS": ["customer_id"],
    "SALES_EMPLOYEES": ["sales_employee_id"],
    "ORDERS": ["order_id"],
    "PRODUCTS": ["product_id"],
    "ORDER_ITEMS": ["order_item_id"],
    "PROMOTIONS": ["promo_id"],
    "ORDER_ITEM_PROMOTION": ["order_item_id", "promo_id"],
    "PAYMENTS": ["order_id"],
    "SHIPMENTS": ["order_id"],
    "SHIPPERS": ["shipper_id"],
    "REVIEWS": ["review_id"],
    "RETURNS": ["return_id"],
    "INVENTORY_SNAPSHOT": ["product_id", "snapshot_date"],
}

FKS = [
    ("CUSTOMERS", ["zip"], "GEOGRAPHY", ["zip"]),
    ("ORDERS", ["customer_id"], "CUSTOMERS", ["customer_id"]),
    ("ORDERS", ["sales_employee_id"], "SALES_EMPLOYEES", ["sales_employee_id"]),
    ("ORDER_ITEMS", ["order_id"], "ORDERS", ["order_id"]),
    ("ORDER_ITEMS", ["product_id"], "PRODUCTS", ["product_id"]),
    ("ORDER_ITEM_PROMOTION", ["order_item_id"], "ORDER_ITEMS", ["order_item_id"]),
    ("ORDER_ITEM_PROMOTION", ["promo_id"], "PROMOTIONS", ["promo_id"]),
    ("PAYMENTS", ["order_id"], "ORDERS", ["order_id"]),
    ("SHIPMENTS", ["order_id"], "ORDERS", ["order_id"]),
    ("SHIPMENTS", ["shipper_id"], "SHIPPERS", ["shipper_id"]),
    ("REVIEWS", ["order_id"], "ORDERS", ["order_id"]),
    ("REVIEWS", ["product_id"], "PRODUCTS", ["product_id"]),
    ("RETURNS", ["order_id"], "ORDERS", ["order_id"]),
    ("RETURNS", ["product_id"], "PRODUCTS", ["product_id"]),
    ("INVENTORY_SNAPSHOT", ["product_id"], "PRODUCTS", ["product_id"]),
]

VALIDATION_ROWS: List[dict] = []
EXCEPTION_ROWS: List[dict] = []
RECON_ROWS: List[dict] = []


# ============================================================
# UTILITIES
# ============================================================

def add_check(category: str, check: str, status: str, actual, expected="", note="") -> None:
    VALIDATION_ROWS.append({
        "category": category,
        "check": check,
        "status": status,
        "actual": actual,
        "expected": expected,
        "note": note,
    })


def add_exception(dataset: str, key, issue: str, details="") -> None:
    EXCEPTION_ROWS.append({
        "dataset": dataset,
        "record_key": key,
        "issue": issue,
        "details": details,
    })


def add_recon(item: str, source_value, target_value, passed: bool, note="") -> None:
    RECON_ROWS.append({
        "reconciliation": item,
        "source_value": source_value,
        "target_value": target_value,
        "status": "PASS" if passed else "FAIL",
        "note": note,
    })


def ensure_columns(df: pd.DataFrame, columns: Sequence[str], label: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(f"{label}: thiếu cột bắt buộc: {missing}")


def to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def to_string(series: pd.Series) -> pd.Series:
    out = series.astype("string")
    return out.str.strip()


def to_date_string(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.strftime("%Y-%m-%d")


def to_bool(series: pd.Series) -> pd.Series:
    """Chấp nhận 0/1, True/False và một số biểu diễn chuỗi phổ biến."""
    if pd.api.types.is_bool_dtype(series):
        return series.astype("boolean")
    s = series.astype("string").str.strip().str.lower()
    mapping = {
        "1": True, "true": True, "t": True, "yes": True, "y": True,
        "0": False, "false": False, "f": False, "no": False, "n": False,
    }
    return s.map(mapping).astype("boolean")


def normalize_zip(series: pd.Series) -> pd.Series:
    # ZIP là code, không phải đại lượng. Nếu Silver đã là chuỗi thì giữ.
    s = series.astype("string").str.strip()
    # Loại hậu tố .0 do CSV parser nếu có.
    s = s.str.replace(r"\.0$", "", regex=True)
    return s


def locate_silver_dir(input_path: Path, work_dir: Path) -> Path:
    if not input_path.exists():
        raise FileNotFoundError(f"Không tìm thấy input: {input_path}")

    if input_path.is_dir():
        # Nếu input chính là silver_data hoặc có silver_data ở dưới.
        if all((input_path / f).exists() for f in SILVER_FILES.values()):
            return input_path
        candidates = [p for p in input_path.rglob("silver_data") if p.is_dir()]
        for c in candidates:
            if all((c / f).exists() for f in SILVER_FILES.values()):
                return c
        raise FileNotFoundError("Không tìm thấy bộ silver_data đầy đủ trong thư mục input.")

    if input_path.suffix.lower() == ".zip":
        extract_dir = work_dir / "_silver_extracted"
        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(input_path, "r") as z:
            z.extractall(extract_dir)
        candidates = [p for p in extract_dir.rglob("silver_data") if p.is_dir()]
        # Cũng hỗ trợ zip mà CSV nằm ngay root.
        candidates.append(extract_dir)
        for c in candidates:
            if all((c / f).exists() for f in SILVER_FILES.values()):
                return c
        raise FileNotFoundError("ZIP không chứa thư mục silver_data đầy đủ theo tên file kỳ vọng.")

    raise ValueError("--input phải là file .zip hoặc thư mục silver_data.")


def load_silver(silver_dir: Path) -> Dict[str, pd.DataFrame]:
    data = {}
    for key, filename in SILVER_FILES.items():
        path = silver_dir / filename
        data[key] = pd.read_csv(path, low_memory=False)
    return data


def fd_violation_keys(df: pd.DataFrame, determinant: str, dependents: Sequence[str]) -> pd.DataFrame:
    """Trả về determinant vi phạm FD determinant -> dependents."""
    ensure_columns(df, [determinant, *dependents], "FD validation")
    # dropna=False để NULL cũng được xem là một giá trị khi kiểm tra consistency.
    grouped = df.groupby(determinant, dropna=False)[list(dependents)].nunique(dropna=False)
    bad = grouped[(grouped > 1).any(axis=1)]
    return bad.reset_index()


def validate_fd_or_raise(
    df: pd.DataFrame,
    determinant: str,
    dependents: Sequence[str],
    label: str,
    exception_dataset: str,
) -> None:
    bad = fd_violation_keys(df, determinant, dependents)
    passed = bad.empty
    add_check("FD", label, "PASS" if passed else "FAIL", len(bad), 0,
              f"{determinant} phải xác định duy nhất profile attributes.")
    if not passed:
        for _, row in bad.head(100).iterrows():
            add_exception(exception_dataset, row[determinant], "Functional dependency violation", label)
        raise ValueError(f"Không thể ETL an toàn: {label} có {len(bad)} determinant vi phạm FD.")


def pk_check(table_name: str, df: pd.DataFrame, pk_cols: Sequence[str]) -> None:
    null_rows = int(df[list(pk_cols)].isna().any(axis=1).sum())
    dup_rows = int(df.duplicated(list(pk_cols), keep=False).sum())
    add_check("PK", f"{table_name}: PK NOT NULL", "PASS" if null_rows == 0 else "FAIL",
              null_rows, 0, f"PK={list(pk_cols)}")
    add_check("PK", f"{table_name}: PK UNIQUE", "PASS" if dup_rows == 0 else "FAIL",
              dup_rows, 0, f"PK={list(pk_cols)}")


def fk_orphan_count(child: pd.DataFrame, child_cols: Sequence[str], parent: pd.DataFrame, parent_cols: Sequence[str]) -> int:
    c = child[list(child_cols)].copy()
    p = parent[list(parent_cols)].copy().drop_duplicates()
    # SQL-like behavior: nếu FK NULL thì không gọi orphan. Tuy nhiên core RD chủ yếu NOT NULL.
    nonnull = ~c.isna().any(axis=1)
    c2 = c.loc[nonnull].copy()
    c2.columns = [f"k{i}" for i in range(len(child_cols))]
    p.columns = [f"k{i}" for i in range(len(parent_cols))]
    m = c2.merge(p.assign(_parent_exists=1), on=list(c2.columns), how="left")
    return int(m["_parent_exists"].isna().sum())


def write_schema_report(report_dir: Path) -> None:
    rows = []
    for table, cols in SCHEMA_3NF.items():
        for ordinal, (column, dtype, key, nullability) in enumerate(cols, start=1):
            rows.append({
                "table": table,
                "ordinal": ordinal,
                "column": column,
                "data_type": dtype,
                "key_constraint": key,
                "nullability_rule": nullability,
            })
    pd.DataFrame(rows).to_csv(report_dir / "schema_3nf.csv", index=False, encoding="utf-8-sig")


# ============================================================
# ETL: SILVER -> 14 TABLES
# ============================================================

def build_3nf(s: Dict[str, pd.DataFrame]) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    # ----- validate FD trước khi tách master/profile -----
    employee_deps = ["sales_employee_name", "marital_status", "education_level", "years_experience"]
    validate_fd_or_raise(
        s["orders"], "sales_employee_id", employee_deps,
        "sales_employee_id -> sales employee profile", "orders_silver"
    )

    shipper_deps = [
        "shipper_name", "shipper_phone", "shipper_gender", "shipper_age",
        "shipper_marital_status", "shipper_education", "shipper_company",
        "shipper_vehicle", "shipper_experience_years", "shipper_rating",
        "delivery_success_rate", "average_delivery_time", "working_shift",
        "join_date", "city", "region", "district",
    ]
    validate_fd_or_raise(
        s["shipments"], "shipper_id", shipper_deps,
        "shipper_id -> shipper profile", "shipments_silver"
    )

    t: Dict[str, pd.DataFrame] = {}

    # 1 GEOGRAPHY
    gcols = ["zip", "city", "district", "region"]
    ensure_columns(s["geography"], gcols, "geography_silver")
    g = s["geography"][gcols].copy()
    g["zip"] = normalize_zip(g["zip"])
    for c in ["city", "district", "region"]:
        g[c] = to_string(g[c])
    t["GEOGRAPHY"] = g

    # 2 CUSTOMERS: bỏ city vì suy ra qua zip -> GEOGRAPHY
    ccols = ["customer_id", "zip", "signup_date", "gender", "age_group", "acquisition_channel"]
    ensure_columns(s["customers"], ccols, "customers_silver")
    c = s["customers"][ccols].copy()
    c["customer_id"] = to_int(c["customer_id"])
    c["zip"] = normalize_zip(c["zip"])
    c["signup_date"] = to_date_string(c["signup_date"])
    for col in ["gender", "age_group", "acquisition_channel"]:
        c[col] = to_string(c[col])
    t["CUSTOMERS"] = c

    # 3 SALES_EMPLOYEES: tách profile khỏi ORDERS
    ecols = ["sales_employee_id", *employee_deps]
    ensure_columns(s["orders"], ecols, "orders_silver")
    e = s["orders"][ecols].drop_duplicates("sales_employee_id", keep="first").copy()
    e["sales_employee_id"] = to_string(e["sales_employee_id"])
    e["years_experience"] = to_int(e["years_experience"])
    for col in ["sales_employee_name", "marital_status", "education_level"]:
        e[col] = to_string(e[col])
    t["SALES_EMPLOYEES"] = e

    # 4 ORDERS: bỏ geography duplicate + employee profile + payment_method duplicate
    ocols = [
        "order_id", "customer_id", "sales_employee_id", "order_date",
        "order_status", "device_type", "order_source", "comment",
    ]
    ensure_columns(s["orders"], ocols, "orders_silver")
    o = s["orders"][ocols].copy()
    for col in ["order_id", "customer_id"]:
        o[col] = to_int(o[col])
    o["sales_employee_id"] = to_string(o["sales_employee_id"])
    o["order_date"] = to_date_string(o["order_date"])
    for col in ["order_status", "device_type", "order_source", "comment"]:
        o[col] = to_string(o[col])
    t["ORDERS"] = o

    # 5 PRODUCTS
    pcols = ["product_id", "product_name", "category", "segment", "size", "color", "price", "cogs"]
    ensure_columns(s["products"], pcols, "products_silver")
    p = s["products"][pcols].copy()
    p["product_id"] = to_int(p["product_id"])
    for col in ["product_name", "category", "segment", "size", "color"]:
        p[col] = to_string(p[col])
    p["price"] = to_float(p["price"])
    p["cogs"] = to_float(p["cogs"])
    t["PRODUCTS"] = p

    # 6 ORDER_ITEMS: sinh surrogate key deterministic theo thứ tự Silver
    src_oi = s["order_items"].reset_index(drop=False).rename(columns={"index": "source_row_index"}).copy()
    oicols = ["order_id", "product_id", "quantity", "unit_price", "discount_amount", "promo_id", "promo_id_2"]
    ensure_columns(src_oi, oicols, "order_items_silver")
    src_oi["order_item_id"] = pd.Series(np.arange(1, len(src_oi) + 1), dtype="Int64")

    oi = src_oi[["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount"]].copy()
    for col in ["order_item_id", "order_id", "product_id", "quantity"]:
        oi[col] = to_int(oi[col])
    oi["unit_price"] = to_float(oi["unit_price"])
    oi["discount_amount"] = to_float(oi["discount_amount"])
    t["ORDER_ITEMS"] = oi

    order_item_mapping = src_oi[["source_row_index", "order_item_id", "order_id", "product_id"]].copy()

    # 7 PROMOTIONS
    prcols = [
        "promo_id", "promo_name", "promo_type", "discount_value", "start_date", "end_date",
        "applicable_category", "promo_channel", "stackable_flag", "min_order_value",
    ]
    ensure_columns(s["promotions"], prcols, "promotions_silver")
    pr = s["promotions"][prcols].copy()
    for col in ["promo_id", "promo_name", "promo_type", "applicable_category", "promo_channel"]:
        pr[col] = to_string(pr[col])
    pr["discount_value"] = to_float(pr["discount_value"])
    pr["start_date"] = to_date_string(pr["start_date"])
    pr["end_date"] = to_date_string(pr["end_date"])
    pr["stackable_flag"] = to_bool(pr["stackable_flag"])
    pr["min_order_value"] = to_float(pr["min_order_value"])
    t["PROMOTIONS"] = pr

    # 8 ORDER_ITEM_PROMOTION: chuyển promo_id/promo_id_2 thành bridge rows
    bridge_parts = []
    for source_col, promotion_order in [("promo_id", 1), ("promo_id_2", 2)]:
        b = src_oi[["order_item_id", source_col]].copy().rename(columns={source_col: "promo_id"})
        b["promo_id"] = to_string(b["promo_id"])
        # Silver NULL = không có promotion; bỏ khỏi bridge.
        b = b[b["promo_id"].notna() & b["promo_id"].ne("")].copy()
        b["promotion_order"] = pd.Series(promotion_order, index=b.index, dtype="Int64")
        bridge_parts.append(b)
    bridge = pd.concat(bridge_parts, ignore_index=True) if bridge_parts else pd.DataFrame(
        columns=["order_item_id", "promo_id", "promotion_order"]
    )
    bridge["order_item_id"] = to_int(bridge["order_item_id"])
    bridge["promo_id"] = to_string(bridge["promo_id"])
    bridge["promotion_order"] = to_int(bridge["promotion_order"])

    # Không silently deduplicate: duplicate PK là lỗi dữ liệu cần FAIL.
    bridge_dup_pk = bridge.duplicated(["order_item_id", "promo_id"], keep=False)
    if bridge_dup_pk.any():
        for _, row in bridge.loc[bridge_dup_pk].head(100).iterrows():
            add_exception(
                "ORDER_ITEM_PROMOTION",
                f"{row['order_item_id']}|{row['promo_id']}",
                "Duplicate bridge primary key",
                f"promotion_order={row['promotion_order']}"
            )
    t["ORDER_ITEM_PROMOTION"] = bridge

    # 9 PAYMENTS
    paycols = ["order_id", "payment_method", "payment_value", "installments"]
    ensure_columns(s["payments"], paycols, "payments_silver")
    pay = s["payments"][paycols].copy()
    pay["order_id"] = to_int(pay["order_id"])
    pay["payment_method"] = to_string(pay["payment_method"])
    pay["payment_value"] = to_float(pay["payment_value"])
    pay["installments"] = to_int(pay["installments"])
    t["PAYMENTS"] = pay

    # 10/11 SHIPMENTS + SHIPPERS
    shipcols = ["order_id", "shipper_id", "ship_date", "delivery_date", "shipping_fee"]
    ensure_columns(s["shipments"], shipcols + shipper_deps, "shipments_silver")
    sh = s["shipments"][shipcols].copy()
    sh["order_id"] = to_int(sh["order_id"])
    sh["shipper_id"] = to_string(sh["shipper_id"])
    sh["ship_date"] = to_date_string(sh["ship_date"])
    sh["delivery_date"] = to_date_string(sh["delivery_date"])
    sh["shipping_fee"] = to_float(sh["shipping_fee"])
    t["SHIPMENTS"] = sh

    spcols = ["shipper_id", *shipper_deps]
    sp = s["shipments"][spcols].drop_duplicates("shipper_id", keep="first").copy()
    sp["shipper_id"] = to_string(sp["shipper_id"])
    for col in ["shipper_age", "shipper_experience_years"]:
        sp[col] = to_int(sp[col])
    for col in ["shipper_rating", "delivery_success_rate", "average_delivery_time"]:
        sp[col] = to_float(sp[col])
    sp["join_date"] = to_date_string(sp["join_date"])
    string_cols = [
        "shipper_name", "shipper_phone", "shipper_gender", "shipper_marital_status",
        "shipper_education", "shipper_company", "shipper_vehicle", "working_shift",
        "city", "region", "district",
    ]
    for col in string_cols:
        sp[col] = to_string(sp[col])
    t["SHIPPERS"] = sp

    # 12 REVIEWS: bỏ customer_id vì suy ra qua order_id -> ORDERS.customer_id
    rvcols = ["review_id", "order_id", "product_id", "review_date", "rating", "review_title"]
    ensure_columns(s["reviews"], rvcols, "reviews_silver")
    rv = s["reviews"][rvcols].copy()
    rv["review_id"] = to_string(rv["review_id"])
    for col in ["order_id", "product_id", "rating"]:
        rv[col] = to_int(rv[col])
    rv["review_date"] = to_date_string(rv["review_date"])
    rv["review_title"] = to_string(rv["review_title"])
    t["REVIEWS"] = rv

    # 13 RETURNS
    rtcols = [
        "return_id", "order_id", "product_id", "return_date", "return_reason",
        "return_quantity", "refund_amount",
    ]
    ensure_columns(s["returns"], rtcols, "returns_silver")
    rt = s["returns"][rtcols].copy()
    rt["return_id"] = to_string(rt["return_id"])
    for col in ["order_id", "product_id", "return_quantity"]:
        rt[col] = to_int(rt[col])
    rt["return_date"] = to_date_string(rt["return_date"])
    rt["return_reason"] = to_string(rt["return_reason"])
    rt["refund_amount"] = to_float(rt["refund_amount"])
    t["RETURNS"] = rt

    # 14 INVENTORY_SNAPSHOT: bỏ product attrs + year/month
    invcols = [
        "product_id", "snapshot_date", "stock_on_hand", "units_received", "units_sold",
        "stockout_days", "days_of_supply", "fill_rate", "stockout_flag", "overstock_flag",
        "reorder_flag", "sell_through_rate",
    ]
    ensure_columns(s["inventory"], invcols, "inventory_silver")
    inv = s["inventory"][invcols].copy()
    for col in ["product_id", "stock_on_hand", "units_received", "units_sold", "stockout_days"]:
        inv[col] = to_int(inv[col])
    inv["snapshot_date"] = to_date_string(inv["snapshot_date"])
    for col in ["days_of_supply", "fill_rate", "sell_through_rate"]:
        inv[col] = to_float(inv[col])
    for col in ["stockout_flag", "overstock_flag", "reorder_flag"]:
        inv[col] = to_bool(inv[col])
    t["INVENTORY_SNAPSHOT"] = inv

    return t, order_item_mapping


# ============================================================
# VALIDATION / RECONCILIATION
# ============================================================

def validate_tables(s: Dict[str, pd.DataFrame], t: Dict[str, pd.DataFrame]) -> None:
    # A. PK checks
    for table_name, pk_cols in PKS.items():
        pk_check(table_name, t[table_name], pk_cols)

    # B. FK checks
    for child_name, child_cols, parent_name, parent_cols in FKS:
        count = fk_orphan_count(t[child_name], child_cols, t[parent_name], parent_cols)
        add_check(
            "FK",
            f"{child_name}({','.join(child_cols)}) -> {parent_name}({','.join(parent_cols)})",
            "PASS" if count == 0 else "FAIL",
            count,
            0,
            "orphan FK rows",
        )

    # C. Bridge constraints
    b = t["ORDER_ITEM_PROMOTION"]
    bad_order = int((~b["promotion_order"].isin([1, 2])).sum()) if len(b) else 0
    add_check("BUSINESS", "ORDER_ITEM_PROMOTION.promotion_order in {1,2}",
              "PASS" if bad_order == 0 else "FAIL", bad_order, 0)

    dup_position = int(b.duplicated(["order_item_id", "promotion_order"], keep=False).sum()) if len(b) else 0
    add_check("UNIQUE", "UNIQUE(order_item_id, promotion_order)",
              "PASS" if dup_position == 0 else "FAIL", dup_position, 0)

    # D. 1:1 / optional 1:1 rules
    # PAYMENT: theo dataset hiện tại mỗi order có 1 payment.
    order_ids = set(t["ORDERS"]["order_id"].dropna().astype(int))
    payment_ids = set(t["PAYMENTS"]["order_id"].dropna().astype(int))
    missing_payments = len(order_ids - payment_ids)
    extra_payments = len(payment_ids - order_ids)
    add_check("CARDINALITY", "ORDERS 1 : 1 PAYMENTS - missing payment for order",
              "PASS" if missing_payments == 0 else "FAIL", missing_payments, 0)
    add_check("CARDINALITY", "ORDERS 1 : 1 PAYMENTS - extra payment without order",
              "PASS" if extra_payments == 0 else "FAIL", extra_payments, 0)

    # SHIPMENT: 0..1 per order is enforced by shipment order_id PK.
    shipment_dup = int(t["SHIPMENTS"].duplicated(["order_id"], keep=False).sum())
    add_check("CARDINALITY", "ORDERS 1 : 0..1 SHIPMENTS",
              "PASS" if shipment_dup == 0 else "FAIL", shipment_dup, 0,
              "shipment order_id must be unique")

    # E. Order Items business membership for Reviews/Returns
    order_product = t["ORDER_ITEMS"][["order_id", "product_id"]].drop_duplicates()
    for child_name in ["REVIEWS", "RETURNS"]:
        child_pairs = t[child_name][["order_id", "product_id"]].copy()
        merged = child_pairs.merge(order_product.assign(_exists=1), on=["order_id", "product_id"], how="left")
        missing = int(merged["_exists"].isna().sum())
        add_check("BUSINESS", f"{child_name} order-product exists in ORDER_ITEMS",
                  "PASS" if missing == 0 else "FAIL", missing, 0)

    # F. Temporal sanity retained after ETL
    ship_d = pd.to_datetime(t["SHIPMENTS"]["ship_date"], errors="coerce")
    deliv_d = pd.to_datetime(t["SHIPMENTS"]["delivery_date"], errors="coerce")
    invalid_delivery = int((deliv_d < ship_d).sum())
    add_check("TEMPORAL", "SHIPMENTS.delivery_date >= ship_date",
              "PASS" if invalid_delivery == 0 else "FAIL", invalid_delivery, 0)

    # join_date issue is a known source anomaly -> REVIEW, not auto-correct.
    ship_join = t["SHIPMENTS"][["shipper_id", "ship_date"]].merge(
        t["SHIPPERS"][["shipper_id", "join_date"]], on="shipper_id", how="left"
    )
    sd = pd.to_datetime(ship_join["ship_date"], errors="coerce")
    jd = pd.to_datetime(ship_join["join_date"], errors="coerce")
    join_anomaly = int((sd < jd).sum())
    add_check("TEMPORAL", "SHIPMENTS.ship_date >= SHIPPERS.join_date",
              "PASS" if join_anomaly == 0 else "REVIEW", join_anomaly,
              "0 or documented source anomaly",
              "Known source-level inconsistency; values are preserved, not auto-corrected.")

    # G. Numeric/domain sanity
    invalid_rating = int((~t["REVIEWS"]["rating"].between(1, 5, inclusive="both")).sum())
    add_check("DOMAIN", "REVIEWS.rating in [1,5]",
              "PASS" if invalid_rating == 0 else "FAIL", invalid_rating, 0)

    invalid_qty = int((t["ORDER_ITEMS"]["quantity"] <= 0).sum())
    add_check("DOMAIN", "ORDER_ITEMS.quantity > 0",
              "PASS" if invalid_qty == 0 else "FAIL", invalid_qty, 0)

    invalid_return_qty = int((t["RETURNS"]["return_quantity"] <= 0).sum())
    add_check("DOMAIN", "RETURNS.return_quantity > 0",
              "PASS" if invalid_return_qty == 0 else "FAIL", invalid_return_qty, 0)

    # H. Payment reconciliation from normalized order items
    calc = t["ORDER_ITEMS"].copy()
    calc["_line_net"] = calc["quantity"].astype(float) * calc["unit_price"].astype(float) - calc["discount_amount"].astype(float)
    order_total = calc.groupby("order_id", as_index=False)["_line_net"].sum()
    pay_comp = t["PAYMENTS"][["order_id", "payment_value"]].merge(order_total, on="order_id", how="left")
    pay_diff = (pay_comp["payment_value"].astype(float) - pay_comp["_line_net"].astype(float)).abs()
    mismatch = int((pay_diff > 0.01).sum())
    max_diff = float(pay_diff.max()) if len(pay_diff) else 0.0
    add_check("RECONCILIATION", "Payment equals SUM(quantity*unit_price-discount_amount) by order",
              "PASS" if mismatch == 0 else "FAIL", mismatch, 0, f"max_abs_diff={max_diff:.12g}")

    # I. Source field payment_method duplication in ORDERS vs PAYMENTS (if present)
    if "payment_method" in s["orders"].columns:
        so = s["orders"][["order_id", "payment_method"]].copy()
        so["order_id"] = to_int(so["order_id"])
        so["payment_method"] = to_string(so["payment_method"])
        cmp = so.merge(t["PAYMENTS"][["order_id", "payment_method"]], on="order_id", suffixes=("_orders", "_payments"))
        mism = int((cmp["payment_method_orders"] != cmp["payment_method_payments"]).sum())
        add_check("SOURCE_OF_TRUTH", "ORDERS.payment_method matches PAYMENTS.payment_method before dropping duplicate",
                  "PASS" if mism == 0 else "FAIL", mism, 0)

    # J. Source -> target row-count reconciliation
    row_recons = [
        ("GEOGRAPHY", len(s["geography"]), len(t["GEOGRAPHY"]), "1 source row -> 1 normalized row"),
        ("CUSTOMERS", len(s["customers"]), len(t["CUSTOMERS"]), "drop redundant city only"),
        ("ORDERS", len(s["orders"]), len(t["ORDERS"]), "no order loss"),
        ("PRODUCTS", len(s["products"]), len(t["PRODUCTS"]), "canonical product master preserved"),
        ("ORDER_ITEMS", len(s["order_items"]), len(t["ORDER_ITEMS"]), "surrogate key added; no line loss"),
        ("PROMOTIONS", len(s["promotions"]), len(t["PROMOTIONS"]), "promotion master preserved"),
        ("PAYMENTS", len(s["payments"]), len(t["PAYMENTS"]), "one payment row preserved"),
        ("SHIPMENTS", len(s["shipments"]), len(t["SHIPMENTS"]), "shipment fact preserved"),
        ("REVIEWS", len(s["reviews"]), len(t["REVIEWS"]), "customer_id removed only"),
        ("RETURNS", len(s["returns"]), len(t["RETURNS"]), "return rows preserved"),
        ("INVENTORY_SNAPSHOT", len(s["inventory"]), len(t["INVENTORY_SNAPSHOT"]), "redundant attrs removed only"),
    ]
    for name, src_count, tgt_count, note in row_recons:
        add_recon(f"{name} row count", src_count, tgt_count, src_count == tgt_count, note)

    # Master extraction counts
    src_emp_unique = int(s["orders"]["sales_employee_id"].nunique(dropna=True))
    tgt_emp = len(t["SALES_EMPLOYEES"])
    add_recon("SALES_EMPLOYEES distinct IDs", src_emp_unique, tgt_emp, src_emp_unique == tgt_emp,
              "extracted from orders_silver")

    src_shipper_unique = int(s["shipments"]["shipper_id"].nunique(dropna=True))
    tgt_shipper = len(t["SHIPPERS"])
    add_recon("SHIPPERS distinct IDs", src_shipper_unique, tgt_shipper, src_shipper_unique == tgt_shipper,
              "extracted from shipments_silver")

    # Bridge count: number of non-null promo slots should equal bridge rows unless same promo duplicated within same item.
    promo1 = to_string(s["order_items"]["promo_id"])
    promo2 = to_string(s["order_items"]["promo_id_2"])
    expected_bridge = int((promo1.notna() & promo1.ne("")).sum() + (promo2.notna() & promo2.ne("")).sum())
    actual_bridge = len(t["ORDER_ITEM_PROMOTION"])
    add_recon("ORDER_ITEM_PROMOTION promo slot count", expected_bridge, actual_bridge,
              expected_bridge == actual_bridge,
              "promo_id -> promotion_order=1; promo_id_2 -> promotion_order=2")

    # Global monetary conservation
    src_oi = s["order_items"].copy()
    src_total = float((pd.to_numeric(src_oi["quantity"], errors="coerce") * pd.to_numeric(src_oi["unit_price"], errors="coerce") - pd.to_numeric(src_oi["discount_amount"], errors="coerce")).sum())
    tgt_oi = t["ORDER_ITEMS"]
    tgt_total = float((tgt_oi["quantity"].astype(float) * tgt_oi["unit_price"].astype(float) - tgt_oi["discount_amount"].astype(float)).sum())
    add_recon("Global net order-item value", round(src_total, 6), round(tgt_total, 6), abs(src_total - tgt_total) <= 0.01,
              "financial values must be conserved by normalization")


def build_summary(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for name, df in tables.items():
        rows.append({
            "table": name,
            "rows": len(df),
            "columns": len(df.columns),
            "null_cells": int(df.isna().sum().sum()),
            "exact_duplicate_rows": int(df.duplicated().sum()),
            "primary_key": ", ".join(PKS[name]),
        })
    return pd.DataFrame(rows)


def source_mapping_report() -> pd.DataFrame:
    rows = [
        {"silver_source": "geography_silver.csv", "target_3nf": "GEOGRAPHY", "action": "direct + datatype normalization"},
        {"silver_source": "customers_silver.csv", "target_3nf": "CUSTOMERS", "action": "drop redundant city; keep zip FK"},
        {"silver_source": "orders_silver.csv", "target_3nf": "ORDERS + SALES_EMPLOYEES", "action": "decompose employee profile; drop duplicated geography/payment_method"},
        {"silver_source": "products_silver.csv", "target_3nf": "PRODUCTS", "action": "direct canonical product master"},
        {"silver_source": "order_items_silver.csv", "target_3nf": "ORDER_ITEMS + ORDER_ITEM_PROMOTION", "action": "add surrogate order_item_id; unpivot promo_id/promo_id_2"},
        {"silver_source": "promotions_silver.csv", "target_3nf": "PROMOTIONS", "action": "direct master"},
        {"silver_source": "payments_silver.csv", "target_3nf": "PAYMENTS", "action": "order_id as PK/FK"},
        {"silver_source": "shipments_silver.csv", "target_3nf": "SHIPMENTS + SHIPPERS", "action": "decompose shipper profile"},
        {"silver_source": "reviews_silver.csv", "target_3nf": "REVIEWS", "action": "drop transitive customer_id"},
        {"silver_source": "returns_silver.csv", "target_3nf": "RETURNS", "action": "direct fact"},
        {"silver_source": "inventory_silver.csv", "target_3nf": "INVENTORY_SNAPSHOT", "action": "drop product attrs + year/month"},
    ]
    for src, reason in EXCLUDED_SILVER_SOURCES.items():
        rows.append({"silver_source": src, "target_3nf": "EXCLUDED FROM CORE RD", "action": reason})
    return pd.DataFrame(rows)


def write_outputs(output_dir: Path, tables: Dict[str, pd.DataFrame], order_item_mapping: pd.DataFrame) -> None:
    table_dir = output_dir / "normalized_3nf"
    report_dir = output_dir / "reports"
    table_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    # 14 normalized CSVs
    for name, df in tables.items():
        df.to_csv(table_dir / TARGET_FILES[name], index=False, encoding="utf-8-sig")

    # Reports
    validation = pd.DataFrame(VALIDATION_ROWS)
    validation.to_csv(report_dir / "validation_checks.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(RECON_ROWS).to_csv(report_dir / "etl_reconciliation.csv", index=False, encoding="utf-8-sig")
    build_summary(tables).to_csv(report_dir / "table_summary.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(EXCEPTION_ROWS, columns=["dataset", "record_key", "issue", "details"]).to_csv(
        report_dir / "exceptions.csv", index=False, encoding="utf-8-sig"
    )
    order_item_mapping.to_csv(report_dir / "order_item_id_mapping.csv", index=False, encoding="utf-8-sig")
    source_mapping_report().to_csv(report_dir / "source_to_3nf_mapping.csv", index=False, encoding="utf-8-sig")
    write_schema_report(report_dir)

    # Human-readable summary
    fail_count = int((validation["status"] == "FAIL").sum()) if not validation.empty else 0
    review_count = int((validation["status"] == "REVIEW").sum()) if not validation.empty else 0
    pass_count = int((validation["status"] == "PASS").sum()) if not validation.empty else 0

    readme = f"""RD 3NF ETL OUTPUT\n=================\n\n14 normalized tables: {len(tables)}\nValidation PASS: {pass_count}\nValidation REVIEW: {review_count}\nValidation FAIL: {fail_count}\n\nFolders:\n- normalized_3nf/: 14 CSV tables\n- reports/validation_checks.csv: PK/FK/domain/cardinality/business checks\n- reports/etl_reconciliation.csv: source-vs-target row/financial reconciliation\n- reports/table_summary.csv: row/column/null/duplicate summary\n- reports/exceptions.csv: sample/critical exceptions detected during ETL\n- reports/order_item_id_mapping.csv: source row -> generated order_item_id lineage\n- reports/source_to_3nf_mapping.csv: Silver source -> normalized relation mapping\n- reports/schema_3nf.csv: target datatype + PK/FK/nullability specification\n\nInterpretation:\n- FAIL: phải xử lý trước khi coi 3NF dataset là hợp lệ.\n- REVIEW: anomaly nguồn đã biết/cần business confirmation; không tự sửa.\n- PASS: check đạt.\n"""
    (output_dir / "README.txt").write_text(readme, encoding="utf-8")

    # Zip everything except the zip itself.
    zip_path = output_dir / "rd3nf_output.zip"
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=1) as z:
        for path in output_dir.rglob("*"):
            if path.is_file() and path != zip_path:
                z.write(path, arcname=path.relative_to(output_dir))


def print_console_summary(output_dir: Path) -> None:
    v = pd.DataFrame(VALIDATION_ROWS)
    print("\n" + "=" * 72)
    print("ETL SILVER -> RD 3NF HOÀN TẤT")
    print("=" * 72)
    if not v.empty:
        print(v["status"].value_counts().to_string())
        failed = v[v["status"] == "FAIL"]
        if not failed.empty:
            print("\nCác check FAIL:")
            print(failed[["category", "check", "actual", "expected"]].to_string(index=False))
        review = v[v["status"] == "REVIEW"]
        if not review.empty:
            print("\nCác check REVIEW:")
            print(review[["category", "check", "actual", "expected"]].to_string(index=False))
    print(f"\nOutput folder: {output_dir}")
    print(f"ZIP tải về: {output_dir / 'rd3nf_output.zip'}")


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="ETL Silver Data -> 14 bảng 3NF + Validation Report")
    parser.add_argument(
        "--input",
        default="/content/silver_output.zip",
        help="silver_output.zip hoặc thư mục silver_data (default: /content/silver_output.zip)",
    )
    parser.add_argument(
        "--output",
        default="/content/rd3nf_output",
        help="thư mục output (default: /content/rd3nf_output)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output)
    work_dir = output_dir.parent / "_rd3nf_work"

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    try:
        silver_dir = locate_silver_dir(input_path, work_dir)
        print(f"Silver data: {silver_dir}")
        s = load_silver(silver_dir)
        print("Đã load Silver sources dùng cho core RD.")

        tables, order_item_mapping = build_3nf(s)
        validate_tables(s, tables)
        write_outputs(output_dir, tables, order_item_mapping)
        print_console_summary(output_dir)

        fail_count = sum(1 for r in VALIDATION_ROWS if r["status"] == "FAIL")
        # Không xóa output khi FAIL để người dùng có report điều tra.
        return 0 if fail_count == 0 else 2

    except Exception as exc:
        # Ghi error report tối thiểu để debug trong Colab.
        report_dir = output_dir / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        error_payload = {
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }
        (report_dir / "pipeline_error.json").write_text(
            json.dumps(error_payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"\nETL ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
