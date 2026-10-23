from __future__ import annotations

import json
import re
import zipfile
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

# ============================================================
# CONFIG
# ============================================================
ZIP_PATH = Path('/content/student_data.zip')   # Colab default; change if needed
WORK_DIR = Path('/content/data_pipeline')
RAW_DIR = WORK_DIR / 'raw'
SILVER_DIR = WORK_DIR / 'silver_data'
REPORT_DIR = WORK_DIR / 'reports'

EXPECTED_FILES = {
    'customers': 'customers.csv',
    'geography': 'geography.csv',
    'inventory': 'inventory.csv',
    'order_items': 'order_items.csv',
    'payments': 'payments.csv',
    'products': 'products.csv',
    'promotions': 'promotions.csv',
    'returns': 'returns.csv',
    'reviews': 'reviews.csv',
    'web_traffic': 'web_traffic.csv',
    'eprom': 'eprom.json',
    'epd': 'epd.json',
    'tf': 'tf.json',
    'shipments': 'shipments_realistic.csv',
    'orders': 'orders_enriched.csv',
}

DATE_COLS = {
    'customers': ['signup_date'],
    'inventory': ['snapshot_date'],
    'orders': ['order_date'],
    'promotions': ['start_date', 'end_date'],
    'returns': ['return_date'],
    'reviews': ['review_date'],
    'shipments': ['ship_date', 'delivery_date', 'join_date'],
    'web_traffic': ['date'],
}

# Aggregated logs, not every row-level modification.
CLEANING_LOG: List[dict] = []
QUALITY_FLAGS: List[dict] = []
VALIDATION_CHECKS: List[dict] = []


def log_clean(dataset: str, field: str, issue: str, action: str, affected_rows: int, note: str = ''):
    CLEANING_LOG.append({
        'dataset': dataset,
        'field': field,
        'issue': issue,
        'action': action,
        'affected_rows': int(affected_rows),
        'note': note,
    })


def add_flag(dataset: str, row_index, record_key, issue: str, severity='REVIEW', note=''):
    QUALITY_FLAGS.append({
        'dataset': dataset,
        'row_index': row_index,
        'record_key': record_key,
        'issue': issue,
        'severity': severity,
        'note': note,
    })


def add_check(check_name: str, passed: bool, actual, expected='', note=''):
    VALIDATION_CHECKS.append({
        'check': check_name,
        'status': 'PASS' if passed else 'FAIL',
        'actual': actual,
        'expected': expected,
        'note': note,
    })


def strip_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Trim strings; preserve actual NaN/None values."""
    df = df.copy()
    for c in df.select_dtypes(include=['object', 'string']).columns:
        mask = df[c].notna()
        df.loc[mask, c] = df.loc[mask, c].astype(str).str.strip()
    return df


def to_iso_date(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors='coerce')
    return dt.dt.strftime('%Y-%m-%d')


def safe_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors='coerce').astype('Int64')


def safe_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors='coerce')


def normalize_zip(series: pd.Series) -> pd.Series:
    """ZIP is a code, not a quantity. Do not invent leading zeroes."""
    n = pd.to_numeric(series, errors='coerce').astype('Int64')
    return n.astype('string')


def extract_zip():
    if not ZIP_PATH.exists():
        raise FileNotFoundError(
            f'Không tìm thấy {ZIP_PATH}. Hãy upload student_data.zip vào Colab '
            'hoặc sửa biến ZIP_PATH ở đầu script.'
        )
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(ZIP_PATH, 'r') as z:
        z.extractall(RAW_DIR)


def locate_file(filename: str) -> Path:
    matches = list(RAW_DIR.rglob(filename))
    if len(matches) != 1:
        raise FileNotFoundError(f'Expected exactly one {filename}, found: {matches}')
    return matches[0]


def load_raw() -> Dict[str, pd.DataFrame]:
    data = {}
    for name, filename in EXPECTED_FILES.items():
        path = locate_file(filename)
        if path.suffix.lower() == '.csv':
            data[name] = pd.read_csv(path, low_memory=False)
        else:
            with open(path, 'r', encoding='utf-8') as f:
                obj = json.load(f)
            data[name] = pd.DataFrame(obj)
    return data


# ============================================================
# BASIC CLEANING: source-preserving Silver layer
# ============================================================

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    x = strip_object_columns(df)
    x['customer_id'] = safe_int(x['customer_id'])
    x['zip'] = normalize_zip(x['zip'])
    x['signup_date'] = to_iso_date(x['signup_date'])
    return x


def clean_geography(df: pd.DataFrame) -> pd.DataFrame:
    x = strip_object_columns(df)
    x['zip'] = normalize_zip(x['zip'])
    return x


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    x = strip_object_columns(df)
    x['product_id'] = safe_int(x['product_id'])
    x['price'] = safe_float(x['price'])
    x['cogs'] = safe_float(x['cogs'])
    # Do NOT add gross_profit here: it is a derived/Gold attribute.
    return x


def clean_inventory(df: pd.DataFrame) -> pd.DataFrame:
    x = strip_object_columns(df)
    x['snapshot_date'] = to_iso_date(x['snapshot_date'])
    x['product_id'] = safe_int(x['product_id'])
    int_cols = ['stock_on_hand', 'units_received', 'units_sold', 'stockout_days',
                'stockout_flag', 'overstock_flag', 'reorder_flag', 'year', 'month']
    float_cols = ['days_of_supply', 'fill_rate', 'sell_through_rate']
    for c in int_cols:
        if c in x.columns:
            x[c] = safe_int(x[c])
    for c in float_cols:
        if c in x.columns:
            x[c] = safe_float(x[c])
    # year/month are source fields; validate rather than invent values.
    d = pd.to_datetime(x['snapshot_date'], errors='coerce')
    if 'year' in x and 'month' in x:
        bad = (d.notna() & ((x['year'] != d.dt.year) | (x['month'] != d.dt.month)))
        if bad.any():
            x.loc[bad, 'year'] = d.loc[bad].dt.year.astype('Int64')
            x.loc[bad, 'month'] = d.loc[bad].dt.month.astype('Int64')
            log_clean('inventory', 'year/month', 'Inconsistent with snapshot_date',
                      'Recomputed from snapshot_date', bad.sum())
    return x


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    x = strip_object_columns(df)
    x['order_id'] = safe_int(x['order_id'])
    x['customer_id'] = safe_int(x['customer_id'])
    x['zip'] = normalize_zip(x['zip'])
    x['order_date'] = to_iso_date(x['order_date'])
    x['years_experience'] = safe_int(x['years_experience'])
    # Preserve all 646,945 raw orders; never drop rows just because fields repeat.
    return x


def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    x = strip_object_columns(df)
    for c in ['order_id', 'product_id', 'quantity']:
        x[c] = safe_int(x[c])
    for c in ['unit_price', 'discount_amount']:
        x[c] = safe_float(x[c])
    # promo_id and promo_id_2: NULL means no promotion; keep NULL.
    for c in ['promo_id', 'promo_id_2']:
        x[c] = x[c].replace({'': pd.NA, 'NO_PROMO': pd.NA, 'NONE': pd.NA})
    # Do NOT drop duplicated (order_id, product_id): this pair is not a true key.
    dup = x.duplicated(['order_id', 'product_id'], keep=False)
    if dup.any():
        log_clean('order_items', 'order_id + product_id',
                  'Composite pair is non-unique', 'Preserved all rows; do not deduplicate',
                  dup.sum(), 'A surrogate order_item_id will be created at RD/3NF load stage.')
    return x


def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    x = strip_object_columns(df)
    x['order_id'] = safe_int(x['order_id'])
    x['payment_value'] = safe_float(x['payment_value'])
    x['installments'] = safe_int(x['installments'])
    return x


def clean_promotions(df: pd.DataFrame) -> pd.DataFrame:
    x = strip_object_columns(df)
    x['promo_id'] = x['promo_id'].astype('string').str.strip()
    x['discount_value'] = safe_float(x['discount_value'])
    x['start_date'] = to_iso_date(x['start_date'])
    x['end_date'] = to_iso_date(x['end_date'])
    x['stackable_flag'] = safe_int(x['stackable_flag'])
    x['min_order_value'] = safe_float(x['min_order_value'])
    # applicable_category NULL is meaningful = all categories; preserve it.
    return x


def clean_returns(df: pd.DataFrame) -> pd.DataFrame:
    x = strip_object_columns(df)
    for c in ['order_id', 'product_id', 'return_quantity']:
        x[c] = safe_int(x[c])
    x['refund_amount'] = safe_float(x['refund_amount'])
    x['return_date'] = to_iso_date(x['return_date'])
    return x


def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    x = strip_object_columns(df)
    for c in ['order_id', 'product_id', 'customer_id', 'rating']:
        x[c] = safe_int(x[c])
    x['review_date'] = to_iso_date(x['review_date'])
    return x


def clean_web_traffic(df: pd.DataFrame) -> pd.DataFrame:
    x = strip_object_columns(df)
    x['date'] = to_iso_date(x['date'])
    for c in ['sessions', 'unique_visitors', 'page_views']:
        x[c] = safe_int(x[c])
    for c in ['bounce_rate', 'avg_session_duration_sec']:
        x[c] = safe_float(x[c])
    return x


def clean_shipments(df: pd.DataFrame) -> pd.DataFrame:
    x = strip_object_columns(df)
    x['order_id'] = safe_int(x['order_id'])
    for c in ['ship_date', 'delivery_date', 'join_date']:
        x[c] = to_iso_date(x[c])
    numeric_int = ['shipper_experience_years', 'average_delivery_time', 'shipper_age']
    numeric_float = ['shipping_fee', 'shipper_rating', 'delivery_success_rate']
    for c in numeric_int:
        x[c] = safe_int(x[c])
    for c in numeric_float:
        x[c] = safe_float(x[c])
    # Phone is an identifier, not a number; we cannot restore a leading 0 already lost in source.
    x['shipper_phone'] = pd.to_numeric(x['shipper_phone'], errors='coerce').astype('Int64').astype('string')

    sd = pd.to_datetime(x['ship_date'], errors='coerce')
    dd = pd.to_datetime(x['delivery_date'], errors='coerce')
    jd = pd.to_datetime(x['join_date'], errors='coerce')

    bad_delivery = sd.notna() & dd.notna() & (dd < sd)
    # Save only a small sample of row-level flags; aggregated count is in cleaning_log.
    for idx in x.index[bad_delivery][:50]:
        add_flag('shipments', idx, x.at[idx, 'order_id'],
                 'delivery_date < ship_date', 'ERROR', 'Do not auto-correct without source evidence.')

    bad_join = sd.notna() & jd.notna() & (sd < jd)
    for idx in x.index[bad_join][:50]:
        add_flag('shipments', idx, x.at[idx, 'shipper_id'],
                 'ship_date < join_date', 'REVIEW',
                 'Sample only. Full affected count is recorded in cleaning_log.')
    log_clean('shipments', 'join_date', 'ship_date earlier than join_date',
              'Retained original value and flagged for business review', bad_join.sum())
    return x


# ============================================================
# EPD: preserve source ID, create canonical mapping separately
# ============================================================

def clean_epd(epd: pd.DataFrame, products: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    x = strip_object_columns(epd)
    x['product_id'] = safe_int(x['product_id'])  # keep source product_id; nullable / duplicated is allowed in Silver source table
    x['price'] = safe_float(x['price'])
    x['cogs'] = safe_float(x['cogs'])

    # Flag source identifier issues instead of inventing IDs.
    missing = x['product_id'].isna()
    dups = x['product_id'].notna() & x.duplicated('product_id', keep=False)
    for idx in x.index[missing]:
        add_flag('epd', idx, '', 'Missing source product_id', 'REVIEW', 'Do not invent a new source ID.')
    for idx in x.index[dups]:
        add_flag('epd', idx, x.at[idx, 'product_id'], 'Duplicated source product_id', 'REVIEW',
                 'Use canonical product mapping, not source ID alone.')

    # Match by stable descriptive attributes + rounded price/cogs.
    attrs = ['product_name', 'category', 'segment', 'size', 'color']
    e = x.copy()
    p = products.copy()
    for c in attrs:
        e[c + '_k'] = e[c].astype('string').str.strip().str.casefold()
        p[c + '_k'] = p[c].astype('string').str.strip().str.casefold()
    e['price_k'] = e['price'].round(6)
    e['cogs_k'] = e['cogs'].round(6)
    p['price_k'] = p['price'].round(6)
    p['cogs_k'] = p['cogs'].round(6)
    keys = [c + '_k' for c in attrs] + ['price_k', 'cogs_k']

    pkey = p[keys + ['product_id']].copy()
    counts = pkey.groupby(keys, dropna=False)['product_id'].transform('count')
    pkey['candidate_count'] = counts

    merged = e.reset_index(names='source_row_index').merge(pkey, on=keys, how='left', suffixes=('', '_canonical'))
    # If same EPD row got >1 candidate, mapping is ambiguous. Collapse to one row per source row.
    map_rows = []
    for src_idx, g in merged.groupby('source_row_index', sort=True):
        candidates = g['product_id_canonical'].dropna().astype('Int64').unique().tolist()
        source_id = x.loc[src_idx, 'product_id']
        if len(candidates) == 1:
            canonical = candidates[0]
            status = 'MATCHED_EXACT_ATTRIBUTES'
        elif len(candidates) == 0:
            canonical = pd.NA
            status = 'UNMATCHED'
        else:
            canonical = pd.NA
            status = 'AMBIGUOUS'
        map_rows.append({
            'source_row_index': src_idx,
            'source_product_id': source_id,
            'canonical_product_id': canonical,
            'match_status': status,
        })
        if status != 'MATCHED_EXACT_ATTRIBUTES':
            add_flag('epd', src_idx, source_id, f'Product mapping {status.lower()}', 'REVIEW',
                     'Manual matching or additional source information required.')

    mapping = pd.DataFrame(map_rows)
    mapping['canonical_product_id'] = mapping['canonical_product_id'].astype('Int64')
    log_clean('epd', 'product_id', 'Missing/duplicated source identifiers',
              'Preserved source IDs; created separate canonical product mapping',
              int(missing.sum() + dups.sum()))
    return x, mapping


# ============================================================
# EPROM: repair only demonstrably invalid values using Promotion master
# ============================================================

def clean_eprom(eprom: pd.DataFrame, promotions: pd.DataFrame) -> pd.DataFrame:
    x = strip_object_columns(eprom)
    before_id = x['promo_id'].copy()
    x['promo_id'] = x['promo_id'].astype('string').str.strip()
    changed_id = before_id.astype('string') != x['promo_id']
    if changed_id.any():
        log_clean('eprom', 'promo_id', 'Leading/trailing whitespace', 'Trimmed whitespace', changed_id.sum())

    x['base_promo_id'] = x['promo_id'].str.extract(r'^(PROMO-\d{4})', expand=False)
    master = promotions.set_index('promo_id')

    def master_value(row, col):
        base = row['base_promo_id']
        if base in master.index:
            return master.at[base, col]
        return pd.NA

    # Missing/blank promo name -> master
    blank_name = x['promo_name'].isna() | x['promo_name'].astype('string').str.strip().eq('')
    for idx in x.index[blank_name]:
        x.at[idx, 'promo_name'] = master_value(x.loc[idx], 'promo_name')
    if blank_name.any():
        log_clean('eprom', 'promo_name', 'Missing/blank promo name', 'Filled from Promotions master', blank_name.sum())

    # promo_type: only repair missing/invalid values, do not overwrite valid source values.
    valid_types = {'percentage', 'fixed'}
    bad_type = x['promo_type'].isna() | ~x['promo_type'].astype('string').isin(valid_types)
    for idx in x.index[bad_type]:
        x.at[idx, 'promo_type'] = master_value(x.loc[idx], 'promo_type')
    if bad_type.any():
        log_clean('eprom', 'promo_type', 'Missing/invalid type', 'Filled from Promotions master', bad_type.sum())

    disc = pd.to_numeric(x['discount_value'], errors='coerce')
    bad_disc = disc.isna() | (disc < 0) | ((x['promo_type'] == 'percentage') & (disc > 100))
    x['discount_value'] = disc
    for idx in x.index[bad_disc]:
        x.at[idx, 'discount_value'] = master_value(x.loc[idx], 'discount_value')
    if bad_disc.any():
        log_clean('eprom', 'discount_value', 'Malformed/out-of-domain discount', 'Replaced from Promotions master', bad_disc.sum())

    sd = pd.to_datetime(x['start_date'], errors='coerce')
    ed = pd.to_datetime(x['end_date'], errors='coerce')
    bad_dates = sd.isna() | ed.isna() | (sd > ed)
    for idx in x.index[bad_dates]:
        x.at[idx, 'start_date'] = master_value(x.loc[idx], 'start_date')
        x.at[idx, 'end_date'] = master_value(x.loc[idx], 'end_date')
    if bad_dates.any():
        log_clean('eprom', 'start_date/end_date', 'Invalid/inverted date range', 'Replaced from Promotions master', bad_dates.sum())
    x['start_date'] = to_iso_date(x['start_date'])
    x['end_date'] = to_iso_date(x['end_date'])

    bad_channel = x['promo_channel'].isna() | x['promo_channel'].astype('string').str.upper().isin({'', 'N/A', 'NA', 'NONE'})
    for idx in x.index[bad_channel]:
        x.at[idx, 'promo_channel'] = master_value(x.loc[idx], 'promo_channel')
    if bad_channel.any():
        log_clean('eprom', 'promo_channel', 'Missing/N/A channel', 'Replaced from Promotions master', bad_channel.sum())

    x['stackable_flag'] = safe_int(x['stackable_flag'])
    bad_stack = x['stackable_flag'].isna() | ~x['stackable_flag'].isin([0, 1])
    for idx in x.index[bad_stack]:
        x.at[idx, 'stackable_flag'] = master_value(x.loc[idx], 'stackable_flag')
    if bad_stack.any():
        log_clean('eprom', 'stackable_flag', 'Invalid flag', 'Replaced from Promotions master', bad_stack.sum())

    x['min_order_value'] = safe_float(x['min_order_value'])
    bad_min = x['min_order_value'].isna() | (x['min_order_value'] < 0)
    for idx in x.index[bad_min]:
        x.at[idx, 'min_order_value'] = master_value(x.loc[idx], 'min_order_value')
    if bad_min.any():
        log_clean('eprom', 'min_order_value', 'Invalid minimum order value', 'Replaced from Promotions master', bad_min.sum())

    # applicable_category NULL can be meaningful; preserve it.
    missing_base = x['base_promo_id'].isna() | ~x['base_promo_id'].isin(promotions['promo_id'])
    for idx in x.index[missing_base]:
        add_flag('eprom', idx, x.at[idx, 'promo_id'], 'Cannot resolve base promotion', 'ERROR')
    return x


# ============================================================
# TF: recover known corrupted values from canonical Web Traffic
# ============================================================

def clean_tf(tf: pd.DataFrame, web: pd.DataFrame) -> pd.DataFrame:
    x = strip_object_columns(tf).reset_index(drop=True)
    w = web.copy()

    # Preserve the original seven business fields in output.
    original_cols = ['date', 'sessions', 'unique_visitors', 'page_views',
                     'bounce_rate', 'avg_session_duration_sec', 'traffic_source']

    # Trim source and detect channel / variant internally.
    x['traffic_source'] = x['traffic_source'].astype('string').str.strip()
    source_pat = r'^(organic_search|paid_search|social_media|email_campaign|referral|direct)\s+Variant\s+(\d{4})$'
    extracted = x['traffic_source'].str.extract(source_pat)
    x['_channel'] = extracted[0]
    x['_variant'] = extracted[1]

    # Verify source invariant: all available Variant IDs follow row_number + 1.
    expected_variant = pd.Series(np.arange(1, len(x) + 1), index=x.index).astype(int)
    parsed_variant = pd.to_numeric(x['_variant'], errors='coerce')
    invariant_ok = ((parsed_variant.isna()) | (parsed_variant == expected_variant)).all()

    # Parse numeric fields first.
    for c in ['sessions', 'unique_visitors', 'page_views']:
        x[c] = pd.to_numeric(x[c], errors='coerce')
    for c in ['bounce_rate', 'avg_session_duration_sec']:
        x[c] = pd.to_numeric(x[c], errors='coerce')

    # Recover invalid dates by matching channel + all available valid metrics to Web Traffic.
    parsed_date = pd.to_datetime(x['date'], errors='coerce')
    bad_date_idx = x.index[parsed_date.isna()].tolist()
    recovered_date_count = 0
    for idx in bad_date_idx:
        row = x.loc[idx]
        cand = w.copy()
        if pd.notna(row['_channel']):
            cand = cand[cand['traffic_source'].astype(str).str.strip().eq(row['_channel'])]
        for c in ['sessions', 'unique_visitors', 'page_views', 'bounce_rate', 'avg_session_duration_sec']:
            val = row[c]
            if pd.notna(val) and np.isfinite(float(val)) and float(val) >= 0:
                cc = pd.to_numeric(cand[c], errors='coerce')
                # Metrics are exact enough in this source; float tolerance for decimals.
                tol = 1e-9 if c in ['bounce_rate', 'avg_session_duration_sec'] else 0
                cand = cand[(cc - float(val)).abs() <= tol]
        if len(cand) == 1:
            x.at[idx, 'date'] = str(cand.iloc[0]['date'])
            recovered_date_count += 1
        else:
            add_flag('tf', idx, '', 'Invalid date could not be uniquely recovered', 'ERROR',
                     f'candidate_count={len(cand)}')
    if recovered_date_count:
        log_clean('tf', 'date', 'Invalid date text', 'Recovered from unique Web Traffic metric match', recovered_date_count)

    # Date is now normalized.
    x['date'] = to_iso_date(x['date'])

    # Recover malformed traffic_source (blank/N/A or whitespace) using web channel on same date.
    malformed_source = x['_channel'].isna()
    source_fixed = 0
    for idx in x.index[malformed_source]:
        date = x.at[idx, 'date']
        cand = w[w['date'].astype(str).eq(str(date))]
        if len(cand) == 1 and invariant_ok:
            channel = str(cand.iloc[0]['traffic_source']).strip()
            variant = f'{idx + 1:04d}'
            x.at[idx, 'traffic_source'] = f'{channel} Variant {variant}'
            x.at[idx, '_channel'] = channel
            x.at[idx, '_variant'] = variant
            source_fixed += 1
        else:
            add_flag('tf', idx, '', 'Malformed traffic_source could not be safely recovered', 'ERROR')
    if source_fixed:
        log_clean('tf', 'traffic_source', 'Blank/N/A/malformed source',
                  'Recovered channel from Web Traffic and Variant from verified row-sequence invariant', source_fixed)

    # Re-extract after corrections.
    extracted = x['traffic_source'].str.extract(source_pat)
    x['_channel'] = extracted[0]
    x['_variant'] = extracted[1]

    # Recover bad sessions and unique_visitors from Web Traffic for same date+channel.
    def recover_metric(col: str, bad_mask: pd.Series):
        count = 0
        for idx in x.index[bad_mask]:
            date = x.at[idx, 'date']
            channel = x.at[idx, '_channel']
            cand = w[(w['date'].astype(str).eq(str(date))) &
                     (w['traffic_source'].astype(str).str.strip().eq(str(channel)))]
            if len(cand) == 1:
                x.at[idx, col] = cand.iloc[0][col]
                count += 1
            else:
                add_flag('tf', idx, '', f'Invalid {col} could not be uniquely recovered', 'ERROR')
        if count:
            log_clean('tf', col, f'Invalid/missing {col}', 'Recovered from Web Traffic by date+channel', count)

    sessions = pd.to_numeric(x['sessions'], errors='coerce')
    bad_sessions = sessions.isna() | (sessions < 0) | (sessions > 1_000_000)
    recover_metric('sessions', bad_sessions)

    visitors = pd.to_numeric(x['unique_visitors'], errors='coerce')
    bad_visitors = visitors.isna() | (visitors < 0) | (visitors > 1_000_000)
    recover_metric('unique_visitors', bad_visitors)

    for c in ['sessions', 'unique_visitors', 'page_views']:
        x[c] = safe_int(x[c])
    for c in ['bounce_rate', 'avg_session_duration_sec']:
        x[c] = safe_float(x[c])

    # Do NOT create channel_type, variant_id, pages_per_session, sessions_per_user in Silver.
    return x[original_cols]


# ============================================================
# QUALITY FLAGS that depend on multiple datasets
# ============================================================

def cross_dataset_quality_flags(s: Dict[str, pd.DataFrame]):
    # Returns vs purchased order-product totals.
    oi = s['order_items'].copy()
    oi['_net'] = oi['quantity'].astype(float) * oi['unit_price'] - oi['discount_amount']
    purchased = oi.groupby(['order_id', 'product_id'], as_index=False).agg(
        purchased_quantity=('quantity', 'sum'),
        purchased_net_value=('_net', 'sum')
    )
    r = s['returns'].merge(purchased, on=['order_id', 'product_id'], how='left')

    missing_pair = r['purchased_quantity'].isna()
    for idx in r.index[missing_pair][:50]:
        add_flag('returns', idx, r.at[idx, 'return_id'], 'Returned order-product not found in Order Items', 'ERROR')

    over_qty = (~missing_pair) & (r['return_quantity'].astype(float) > r['purchased_quantity'].astype(float))
    for idx in r.index[over_qty][:50]:
        add_flag('returns', idx, r.at[idx, 'return_id'], 'return_quantity exceeds purchased quantity', 'ERROR')

    over_refund = (~missing_pair) & (r['refund_amount'] > r['purchased_net_value'] + 0.01)
    for idx in r.index[over_refund][:50]:
        add_flag('returns', idx, r.at[idx, 'return_id'],
                 'refund_amount exceeds aggregated net item value', 'REVIEW',
                 'Sample only; full count is in cleaning_log. May include shipping/tax/compensation.')
    log_clean('returns', 'refund_amount', 'Refund greater than net purchased line value',
              'Retained value and flagged for business review', over_refund.sum())

    # Customer signup vs order date: observation only because signup semantics are not fully defined.
    oc = s['orders'][['order_id', 'customer_id', 'order_date']].merge(
        s['customers'][['customer_id', 'signup_date']], on='customer_id', how='left')
    od = pd.to_datetime(oc['order_date'], errors='coerce')
    sd = pd.to_datetime(oc['signup_date'], errors='coerce')
    count = int((od < sd).sum())
    VALIDATION_CHECKS.append({
        'check': 'Orders before customer signup_date (informational)',
        'status': 'REVIEW' if count else 'PASS',
        'actual': count,
        'expected': 'Business definition required',
        'note': 'Do not auto-correct until signup_date semantics are confirmed.'
    })


# ============================================================
# VALIDATION
# ============================================================

def validate_silver(raw: Dict[str, pd.DataFrame], s: Dict[str, pd.DataFrame], epd_map: pd.DataFrame):
    # Row count preservation for 15 logical datasets.
    for name in EXPECTED_FILES:
        add_check(f'{name}: row count preserved', len(s[name]) == len(raw[name]),
                  len(s[name]), len(raw[name]))

    # Candidate-key uniqueness checks.
    key_checks = [
        ('customers', ['customer_id']),
        ('geography', ['zip']),
        ('inventory', ['product_id', 'snapshot_date']),
        ('orders', ['order_id']),
        ('payments', ['order_id']),
        ('products', ['product_id']),
        ('promotions', ['promo_id']),
        ('returns', ['return_id']),
        ('reviews', ['review_id']),
        ('shipments', ['order_id']),
    ]
    for name, keys in key_checks:
        dup = int(s[name].duplicated(keys).sum())
        add_check(f'{name}: candidate key {keys} unique', dup == 0, dup, 0, 'duplicate rows by candidate key')

    # Order-items composite pair is explicitly NOT a key.
    oi_pair_dup_rows = int(s['order_items'].duplicated(['order_id', 'product_id'], keep=False).sum())
    VALIDATION_CHECKS.append({
        'check': 'order_items: (order_id, product_id) non-unique rows (expected observation)',
        'status': 'INFO',
        'actual': oi_pair_dup_rows,
        'expected': 'May be > 0; do not use as PK',
        'note': 'Create surrogate order_item_id when loading RD/3NF.'
    })

    def fk_check(child_name, child_col, parent_name, parent_col, ignore_null=True):
        child = s[child_name][child_col]
        if ignore_null:
            child = child.dropna()
        parent = set(s[parent_name][parent_col].dropna().astype(str))
        vals = child.astype(str)
        orphan = int((~vals.isin(parent)).sum())
        add_check(f'FK {child_name}.{child_col} -> {parent_name}.{parent_col}', orphan == 0, orphan, 0, 'orphan FK rows')

    fk_check('orders', 'customer_id', 'customers', 'customer_id')
    fk_check('reviews', 'customer_id', 'customers', 'customer_id')
    fk_check('customers', 'zip', 'geography', 'zip')
    fk_check('orders', 'zip', 'geography', 'zip')

    for child in ['inventory', 'order_items', 'returns', 'reviews']:
        fk_check(child, 'product_id', 'products', 'product_id')
    for child in ['order_items', 'payments', 'returns', 'reviews', 'shipments']:
        fk_check(child, 'order_id', 'orders', 'order_id')

    # Promotion FKs; null means no promo and is ignored.
    for c in ['promo_id', 'promo_id_2']:
        vals = s['order_items'][c].dropna().astype(str)
        parent = set(s['promotions']['promo_id'].dropna().astype(str))
        orphan = int((~vals.isin(parent)).sum())
        add_check(f'FK order_items.{c} -> promotions.promo_id (NULL ignored)', orphan == 0, orphan, 0)

    # EPROM base IDs.
    parent = set(s['promotions']['promo_id'].astype(str))
    orphan = int((~s['eprom']['base_promo_id'].dropna().astype(str).isin(parent)).sum())
    add_check('FK eprom.base_promo_id -> promotions.promo_id', orphan == 0, orphan, 0)

    # Payment reconciliation.
    oi = s['order_items'].copy()
    oi['_line_total'] = oi['quantity'].astype(float) * oi['unit_price'] - oi['discount_amount']
    totals = oi.groupby('order_id', as_index=False)['_line_total'].sum()
    pm = s['payments'].merge(totals, on='order_id', how='left')
    diff = (pm['payment_value'] - pm['_line_total']).abs()
    bad = int((diff > 0.01).sum())
    add_check('Payment value reconciles to Order Items net total', bad == 0, bad, 0, 'orders with difference > 0.01')

    # Reviews/Returns order-product membership.
    pairs = s['order_items'][['order_id', 'product_id']].drop_duplicates()
    for name in ['reviews', 'returns']:
        m = s[name].merge(pairs.assign(_exists=1), on=['order_id', 'product_id'], how='left')
        miss = int(m['_exists'].isna().sum())
        add_check(f'{name}: (order_id, product_id) exists in order_items', miss == 0, miss, 0)

    # Date rules.
    sh = s['shipments']
    ship = pd.to_datetime(sh['ship_date'], errors='coerce')
    delivery = pd.to_datetime(sh['delivery_date'], errors='coerce')
    invalid = int((delivery < ship).sum())
    add_check('shipments: delivery_date >= ship_date', invalid == 0, invalid, 0)

    # TF validity after cleaning.
    tf = s['tf']
    bad_date = int(pd.to_datetime(tf['date'], errors='coerce').isna().sum())
    add_check('tf: all dates valid after cleaning', bad_date == 0, bad_date, 0)
    ses = pd.to_numeric(tf['sessions'], errors='coerce')
    uv = pd.to_numeric(tf['unique_visitors'], errors='coerce')
    bad_metric = int((ses.isna() | uv.isna() | (ses < 0) | (uv < 0) | (uv > ses)).sum())
    add_check('tf: sessions/unique_visitors valid', bad_metric == 0, bad_metric, 0)
    pattern = r'^(organic_search|paid_search|social_media|email_campaign|referral|direct)\s+Variant\s+\d{4}$'
    bad_src = int((~tf['traffic_source'].astype(str).str.match(pattern)).sum())
    add_check('tf: traffic_source pattern valid', bad_src == 0, bad_src, 0)

    # EPD mapping status is intentionally not forced to 100%.
    matched = int(epd_map['canonical_product_id'].notna().sum())
    VALIDATION_CHECKS.append({
        'check': 'EPD canonical product mapping coverage',
        'status': 'INFO',
        'actual': f'{matched}/{len(epd_map)}',
        'expected': 'Unmatched/ambiguous rows require review, not invented IDs',
        'note': ''
    })


# ============================================================
# SAVE
# ============================================================

def save_silver(s: Dict[str, pd.DataFrame], epd_map: pd.DataFrame):
    # Main 15 logical Silver datasets as CSV.
    output_names = {
        'customers': 'customers_silver.csv',
        'geography': 'geography_silver.csv',
        'inventory': 'inventory_silver.csv',
        'order_items': 'order_items_silver.csv',
        'payments': 'payments_silver.csv',
        'products': 'products_silver.csv',
        'promotions': 'promotions_silver.csv',
        'returns': 'returns_silver.csv',
        'reviews': 'reviews_silver.csv',
        'web_traffic': 'web_traffic_silver.csv',
        'eprom': 'eprom_silver.csv',
        'epd': 'epd_silver.csv',
        'tf': 'tf_silver.csv',
        'shipments': 'shipments_silver.csv',
        'orders': 'orders_silver.csv',
    }
    for name, filename in output_names.items():
        s[name].to_csv(SILVER_DIR / filename, index=False, encoding='utf-8-sig')

    # Also preserve JSON representation for the 3 JSON-origin sources.
    for name in ['epd', 'eprom', 'tf']:
        with open(SILVER_DIR / f'{name}_silver.json', 'w', encoding='utf-8') as f:
            json.dump(s[name].replace({np.nan: None, pd.NA: None}).to_dict(orient='records'),
                      f, ensure_ascii=False, indent=2, default=str)

    epd_map.to_csv(SILVER_DIR / 'product_source_map.csv', index=False, encoding='utf-8-sig')

    pd.DataFrame(CLEANING_LOG).to_csv(REPORT_DIR / 'cleaning_log.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(QUALITY_FLAGS).to_csv(REPORT_DIR / 'quality_flags.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(VALIDATION_CHECKS).to_csv(REPORT_DIR / 'validation_checks.csv', index=False, encoding='utf-8-sig')

    # Dataset-level summary.
    summary = []
    for name in EXPECTED_FILES:
        df = s[name]
        summary.append({
            'dataset': name,
            'silver_rows': len(df),
            'silver_columns': len(df.columns),
            'null_cells': int(df.isna().sum().sum()),
            'exact_duplicate_rows': int(df.duplicated().sum()),
        })
    pd.DataFrame(summary).to_csv(REPORT_DIR / 'silver_dataset_summary.csv', index=False, encoding='utf-8-sig')

    # Zip all deliverables.
    zip_out = WORK_DIR / 'silver_output.zip'
    with zipfile.ZipFile(zip_out, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        for folder in [SILVER_DIR, REPORT_DIR]:
            for p in folder.rglob('*'):
                if p.is_file():
                    z.write(p, p.relative_to(WORK_DIR))
    return zip_out


# ============================================================
# MAIN
# ============================================================

def main():
    print('1) Extracting raw ZIP...')
    extract_zip()
    print('2) Loading 15 raw datasets...')
    raw = load_raw()
    print({k: len(v) for k, v in raw.items()})

    print('3) Cleaning Raw -> Silver...')
    s: Dict[str, pd.DataFrame] = {}
    s['customers'] = clean_customers(raw['customers'])
    s['geography'] = clean_geography(raw['geography'])
    s['products'] = clean_products(raw['products'])
    s['inventory'] = clean_inventory(raw['inventory'])
    s['orders'] = clean_orders(raw['orders'])
    s['order_items'] = clean_order_items(raw['order_items'])
    s['payments'] = clean_payments(raw['payments'])
    s['promotions'] = clean_promotions(raw['promotions'])
    s['returns'] = clean_returns(raw['returns'])
    s['reviews'] = clean_reviews(raw['reviews'])
    s['web_traffic'] = clean_web_traffic(raw['web_traffic'])
    s['shipments'] = clean_shipments(raw['shipments'])

    s['epd'], epd_map = clean_epd(raw['epd'], s['products'])
    s['eprom'] = clean_eprom(raw['eprom'], s['promotions'])
    s['tf'] = clean_tf(raw['tf'], s['web_traffic'])

    print('4) Cross-dataset quality checks / flags...')
    cross_dataset_quality_flags(s)

    print('5) Validating Silver...')
    validate_silver(raw, s, epd_map)

    print('6) Saving outputs...')
    zip_out = save_silver(s, epd_map)

    checks = pd.DataFrame(VALIDATION_CHECKS)
    print('\n=== VALIDATION STATUS ===')
    print(checks['status'].value_counts(dropna=False).to_string())
    failed = checks[checks['status'] == 'FAIL']
    if len(failed):
        print('\nFAILED CHECKS:')
        print(failed[['check', 'actual', 'expected', 'note']].to_string(index=False))
    else:
        print('\nKhông có validation check nào FAIL.')

    print(f'\nSilver folder : {SILVER_DIR}')
    print(f'Reports folder: {REPORT_DIR}')
    print(f'ZIP output    : {zip_out}')
    print('\nHoàn tất Raw -> Silver.')
    return zip_out


if __name__ == '__main__':
    main()
