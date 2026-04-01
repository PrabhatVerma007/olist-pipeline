import pandas as pd
from sqlalchemy import create_engine
import os
import sys
from pathlib import Path

# Fix import path
CURRENT_DIR = Path(__file__).resolve().parent
sys.path.append(str(CURRENT_DIR))

from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

# DB connection
url = f'mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}'
print("FINAL URL:", url)

engine = create_engine(url)

# Absolute base path (important)
BASE_DIR = CURRENT_DIR.parent

tables = {
    'stg_orders':         BASE_DIR / 'data/raw/olist_orders_dataset.csv',
    'stg_order_items':    BASE_DIR / 'data/raw/olist_order_items_dataset.csv',
    'stg_order_payments': BASE_DIR / 'data/raw/olist_order_payments_dataset.csv',
    'stg_order_reviews':  BASE_DIR / 'data/raw/olist_order_reviews_dataset.csv',
    'stg_customers':      BASE_DIR / 'data/raw/olist_customers_dataset.csv',
    'stg_products':       BASE_DIR / 'data/raw/olist_products_dataset.csv',
    'stg_sellers':        BASE_DIR / 'data/raw/olist_sellers_dataset.csv',
    'stg_geolocation':    BASE_DIR / 'data/raw/olist_geolocation_dataset.csv',
}

for table_name, filepath in tables.items():
    try:
        print(f'\nLoading {table_name}...')

        if not filepath.exists():
            print(f"File not found: {filepath}")
            continue

        df = pd.read_csv(filepath)

        # Use transaction-safe connection
        with engine.begin() as connection:
            df.to_sql(
                table_name,
                con=connection,
                if_exists='replace',
                index=False,
                chunksize=5000  # critical for large tables
            )

        print(f'Done: {table_name} — {len(df)} rows')

    except Exception as e:
        print(f'Error loading {table_name}')
        print(e)

print('\nAll staging tables processed.')