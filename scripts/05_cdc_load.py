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
# HELPERS
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
# CDC FUNCTIONS
# -------------------------------
def cdc_by_timestamp(table_name, filepath, timestamp_col):
    last_loaded, run_number = get_watermark(table_name)
    run_number += 1

    df = pd.read_csv(filepath, low_memory=False)
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')

    is_first_run = (last_loaded.year == 2000)
    load_type = 'full' if is_first_run else 'incremental'

    if load_type == 'incremental':
        df = df[df[timestamp_col] > pd.Timestamp(last_loaded)]
        mode = 'append'
    else:
        mode = 'replace'

    rows = len(df)

    if rows == 0:
        print('  No new rows')
        update_watermark(table_name, 0, 'no_change', run_number)
        return

    df.to_sql(table_name, con=engine, if_exists=mode, index=False, chunksize=5000)
    update_watermark(table_name, rows, load_type, run_number)

    print(f'  Rows loaded : {rows}')
    print(f'  Mode        : {mode}')


def cdc_by_id(table_name, filepath, id_col):
    last_loaded, run_number = get_watermark(table_name)
    run_number += 1

    df = pd.read_csv(filepath, low_memory=False)

    is_first_run = (last_loaded.year == 2000)
    load_type = 'full' if is_first_run else 'incremental'

    if load_type == 'incremental':
        try:
            existing = pd.read_sql(f'SELECT {id_col} FROM {table_name}', engine)
            df = df[~df[id_col].isin(set(existing[id_col]))]
        except Exception:
            pass
        mode = 'append'
    else:
        mode = 'replace'

    rows = len(df)

    if rows == 0:
        print('  No new rows')
        update_watermark(table_name, 0, 'no_change', run_number)
        return

    df.to_sql(table_name, con=engine, if_exists=mode, index=False, chunksize=5000)
    update_watermark(table_name, rows, load_type, run_number)

    print(f'  Rows loaded : {rows}')
    print(f'  Mode        : {mode}')


# -------------------------------
# PIPELINE START
# -------------------------------
print('='*60)
print('CDC PIPELINE START')
print('='*60)

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
        cdc_by_timestamp(table, path, col)
    else:
        cdc_by_id(table, path, col)

# -------------------------------
# CLEANUP
# -------------------------------
cursor.close()
conn.close()

print('\nPipeline Completed Successfully')