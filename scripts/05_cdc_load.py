import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from pathlib import Path
import mysql.connector
import os
import sys

# ─────────────────────────────────────────
# CONFIG IMPORT (avoid naming conflict with built-in secrets module)
# ─────────────────────────────────────────
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

RAW_FOLDER = Path('data/raw')

# ─────────────────────────────────────────
# CONNECTIONS (use context-safe handling)
# ─────────────────────────────────────────
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

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

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


# ─────────────────────────────────────────
# CDC FUNCTIONS (optimized)
# ─────────────────────────────────────────

def cdc_by_timestamp(table_name, filepath, timestamp_col, last_loaded, run_number, load_type):
    df = pd.read_csv(filepath, low_memory=False)
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')

    if load_type == 'incremental':
        df = df[df[timestamp_col] > pd.Timestamp(last_loaded)]
        mode = 'append'
    else:
        mode = 'replace'

    rows = len(df)

    if rows == 0:
        print(f'  No new rows')
        update_watermark(table_name, 0, 'incremental_no_change', run_number)
        return

    df.to_sql(table_name, con=engine, if_exists=mode, index=False, chunksize=5000)
    update_watermark(table_name, rows, load_type, run_number)

    print(f'  Rows loaded : {rows}')
    print(f'  Mode        : {mode}')


def cdc_by_id(table_name, filepath, id_col, run_number, load_type):
    df = pd.read_csv(filepath, low_memory=False)

    if load_type == 'incremental':
        try:
            existing = pd.read_sql(f'SELECT {id_col} FROM {table_name}', engine)
            existing_set = set(existing[id_col])
            df = df[~df[id_col].isin(existing_set)]
        except Exception:
            # table might not exist yet
            pass
        mode = 'append'
    else:
        mode = 'replace'

    rows = len(df)

    if rows == 0:
        print(f'  No new rows')
        update_watermark(table_name, 0, 'incremental_no_change', run_number)
        return

    df.to_sql(table_name, con=engine, if_exists=mode, index=False, chunksize=5000)
    update_watermark(table_name, rows, load_type, run_number)

    print(f'  Rows loaded : {rows}')
    print(f'  Mode        : {mode}')


# ─────────────────────────────────────────
# PIPELINE START
# ─────────────────────────────────────────

print('='*60)
print('CDC PIPELINE START')
print('='*60)

last_loaded, run_number = get_watermark('stg_orders')

is_first_run = (last_loaded.year == 2000)
load_type = 'full' if is_first_run else 'incremental'
run_number += 1

print(f'Mode: {load_type.upper()}')

# ─────────────────────────────────────────
# EXECUTION
# ─────────────────────────────────────────

jobs = [
    ('stg_orders', 'olist_orders_dataset.csv', 'timestamp', 'order_purchase_timestamp'),
    ('stg_customers', 'olist_customers_dataset.csv', 'id', 'customer_id'),
    ('stg_order_items', 'olist_order_items_dataset.csv', 'id', 'order_id'),
    ('stg_order_payments', 'olist_order_payments_dataset.csv', 'id', 'order_id'),
    ('stg_order_reviews', 'olist_order_reviews_dataset.csv', 'timestamp', 'review_creation_date'),
    ('stg_products', 'olist_products_dataset.csv', 'id', 'product_id'),
    ('stg_sellers', 'olist_sellers_dataset.csv', 'id', 'seller_id'),
    ('stg_geolocation', 'olist_geolocation_dataset.csv', 'id', 'geolocation_zip_code_prefix')
]

for i, (table, file, cdc_type, col) in enumerate(jobs, start=1):
    print_section(f'{i} of {len(jobs)} — {table}')

    path = RAW_FOLDER / file

    if cdc_type == 'timestamp':
        cdc_by_timestamp(table, path, col, last_loaded, run_number, load_type)
    else:
        cdc_by_id(table, path, col, run_number, load_type)

# ─────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────
cursor.close()
conn.close()

print('\nPipeline Completed Successfully')