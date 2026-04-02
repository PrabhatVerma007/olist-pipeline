import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from pathlib import Path
import mysql.connector
import os
import sys

# -------------------------------
# CONFIG
# -------------------------------
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

RAW_FOLDER = Path('data/raw')

# -------------------------------
# CONNECTIONS
# -------------------------------
engine = create_engine(
    f'mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}',
    pool_pre_ping=True
)

conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)
cursor = conn.cursor(dictionary=True)

# -------------------------------
# CLEANING FUNCTION
# -------------------------------
def clean_dataframe(df):
    df.columns = df.columns.str.strip().str.lower()
    df.dropna(how='all', inplace=True)
    df.replace('', pd.NA, inplace=True)
    df.drop_duplicates(inplace=True)

    key_cols = ['customer_id', 'order_id', 'product_id', 'seller_id']
    for col in key_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    for col in df.columns:
        if 'date' in col or 'timestamp' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        if any(x in col for x in ['price', 'value', 'amount', 'payment']):
            df[col] = pd.to_numeric(df[col], errors='coerce')

    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].astype(str).str.strip().str.slice(0, 255)

    return df

# -------------------------------
# WATERMARK FUNCTIONS
# -------------------------------
def get_watermark(table_name):
    cursor.execute(
        'SELECT last_loaded_at, run_number FROM pipeline_watermark WHERE table_name = %s',
        (table_name,)
    )
    result = cursor.fetchone()
    if result:
        return result['last_loaded_at'], result['run_number']
    return datetime(2000, 1, 1), 0


def update_watermark(table_name, rows_loaded, load_type, run_number):
    cursor.execute('''
        INSERT INTO pipeline_watermark
        (table_name, last_loaded_at, rows_last_loaded, load_type, run_number)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            last_loaded_at   = VALUES(last_loaded_at),
            rows_last_loaded = VALUES(rows_last_loaded),
            load_type        = VALUES(load_type),
            run_number       = VALUES(run_number)
    ''', (table_name, datetime.now(), rows_loaded, load_type, run_number))
    conn.commit()


def print_section(title):
    print(f'\n{"─"*60}')
    print(f'  {title}')
    print(f'{"─"*60}')

# -------------------------------
# LOAD FUNCTIONS
# -------------------------------

# ✅ FULL LOAD
def full_load(table_name, filepath):
    df = pd.read_csv(filepath, low_memory=False)
    df = clean_dataframe(df)

    df.to_sql(table_name, con=engine, if_exists='replace', index=False, chunksize=5000)

    print(f'  Full load completed: {table_name}')
    update_watermark(table_name, len(df), 'full', 1)


# ✅ CDC (timestamp)
def cdc_by_timestamp(table_name, filepath, timestamp_col):
    last_loaded, run_number = get_watermark(table_name)
    run_number += 1

    df = pd.read_csv(filepath, low_memory=False)
    df = clean_dataframe(df)

    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')

    is_first_run = (last_loaded.year == 2000)

    if not is_first_run:
        df = df[df[timestamp_col] > pd.Timestamp(last_loaded)]
        mode = 'append'
        load_type = 'incremental'
    else:
        mode = 'replace'
        load_type = 'full'

    if df.empty:
        print('  No new rows')
        update_watermark(table_name, 0, 'no_change', run_number)
        return

    df.to_sql(table_name, con=engine, if_exists=mode, index=False, chunksize=5000)
    update_watermark(table_name, len(df), load_type, run_number)

    print(f'  Rows loaded : {len(df)}')
    print(f'  Mode        : {mode}')


# ✅ CDC (ID based) — FIXED WITH PROPER KEYS
def cdc_by_id(table_name, filepath, id_cols):
    last_loaded, run_number = get_watermark(table_name)
    run_number += 1

    df = pd.read_csv(filepath, low_memory=False)
    df = clean_dataframe(df)

    is_first_run = (last_loaded.year == 2000)

    if not is_first_run:
        try:
            existing = pd.read_sql(
                f"SELECT {', '.join(id_cols)} FROM {table_name}", engine
            )

            # Normalize
            for col in id_cols:
                df[col] = df[col].astype(str).str.strip().str.upper()
                existing[col] = existing[col].astype(str).str.strip().str.upper()

            existing_keys = set(tuple(row) for row in existing[id_cols].values)
            df = df[~df[id_cols].apply(tuple, axis=1).isin(existing_keys)]

        except Exception:
            pass

        mode = 'append'
        load_type = 'incremental'
    else:
        mode = 'replace'
        load_type = 'full'

    # ✅ FIX: use composite keys
    df.drop_duplicates(subset=id_cols, inplace=True)

    if df.empty:
        print('  No new rows')
        update_watermark(table_name, 0, 'no_change', run_number)
        return

    df.to_sql(table_name, con=engine, if_exists=mode, index=False, chunksize=5000)
    update_watermark(table_name, len(df), load_type, run_number)

    print(f'  Rows loaded : {len(df)}')
    print(f'  Mode        : {mode}')


# -------------------------------
# PIPELINE START
# -------------------------------
print('='*60)
print('CDC PIPELINE START')
print('='*60)

jobs = [
    # ✅ FULL LOADS (no CDC needed)
    ('stg_orders', 'olist_orders_dataset.csv', 'full', None),
    ('stg_customers', 'olist_customers_dataset.csv', 'full', None),

    # 🚨 FIX: geolocation is NOT unique → must be full load
    ('stg_geolocation', 'olist_geolocation_dataset.csv', 'full', None),

    # ⚠️ safer as full load (optional but recommended)
    ('stg_products', 'olist_products_dataset.csv', 'full', None),

    # ✅ CDC with correct composite keys
    ('stg_order_items', 'olist_order_items_dataset.csv', 'id', ['order_id', 'order_item_id']),
    ('stg_order_payments', 'olist_order_payments_dataset.csv', 'id', ['order_id', 'payment_sequential']),

    # ✅ timestamp CDC (safe)
    ('stg_order_reviews', 'olist_order_reviews_dataset.csv', 'timestamp', 'review_creation_date'),

    # ✅ small table → ID-based CDC is fine
    ('stg_sellers', 'olist_sellers_dataset.csv', 'id', ['seller_id']),
]

for i, (table, file, cdc_type, col) in enumerate(jobs, start=1):
    print_section(f'{i} of {len(jobs)} — {table}')

    path = RAW_FOLDER / file

    if cdc_type == 'timestamp':
        cdc_by_timestamp(table, path, col)
    elif cdc_type == 'id':
        cdc_by_id(table, path, col)
    elif cdc_type == 'full':
        full_load(table, path)

# -------------------------------
# CLEANUP
# -------------------------------
cursor.close()
conn.close()

print('\nPipeline Completed Successfully')