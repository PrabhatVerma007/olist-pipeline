import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import sys

# -------------------------------
# ✅ CONFIG IMPORT
# -------------------------------
CURRENT_DIR = Path(__file__).resolve().parent
sys.path.append(str(CURRENT_DIR))

from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

# -------------------------------
# ✅ DB CONNECTION
# -------------------------------
DB_URL = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"
engine = create_engine(DB_URL)

# -------------------------------
# ✅ PATH SETUP
# -------------------------------
BASE_DIR = CURRENT_DIR.parent

TABLES = {
    'stg_orders':         BASE_DIR / 'data/raw/olist_orders_dataset.csv',
    'stg_order_items':    BASE_DIR / 'data/raw/olist_order_items_dataset.csv',
    'stg_order_payments': BASE_DIR / 'data/raw/olist_order_payments_dataset.csv',
    'stg_order_reviews':  BASE_DIR / 'data/raw/olist_order_reviews_dataset.csv',
    'stg_customers':      BASE_DIR / 'data/raw/olist_customers_dataset.csv',
    'stg_products':       BASE_DIR / 'data/raw/olist_products_dataset.csv',
    'stg_sellers':        BASE_DIR / 'data/raw/olist_sellers_dataset.csv',
    'stg_geolocation':    BASE_DIR / 'data/raw/olist_geolocation_dataset.csv',
}

# -------------------------------
# ✅ CLEANING FUNCTIONS
# -------------------------------
def standardize_columns(df):
    df.columns = df.columns.str.strip().str.lower()
    return df

def clean_keys(df):
    key_cols = ['customer_id', 'order_id', 'product_id', 'seller_id']
    for col in key_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
    return df

def handle_nulls(df):
    df.replace('', pd.NA, inplace=True)
    return df

def fix_dtypes(df):
    for col in df.columns:
        # Dates
        if 'date' in col or 'timestamp' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # Numeric fields
        if any(x in col for x in ['price', 'value', 'amount', 'payment']):
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df

def remove_bad_rows(df):
    df.dropna(how='all', inplace=True)
    df.drop_duplicates(inplace=True)
    return df

def clean_dataframe(df):
    df = standardize_columns(df)
    df = remove_bad_rows(df)
    df = clean_keys(df)
    df = handle_nulls(df)
    df = fix_dtypes(df)
    return df

# -------------------------------
# ✅ LOAD FUNCTION
# -------------------------------
def load_table(table_name, filepath):
    print(f"\n🔄 Processing: {table_name}")

    if not filepath.exists():
        print(f"❌ File not found: {filepath}")
        return

    try:
        df = pd.read_csv(filepath)

        print(f"📥 Raw shape: {df.shape}")

        df = clean_dataframe(df)

        print(f"🧹 Cleaned shape: {df.shape}")

        with engine.begin() as conn:
            df.to_sql(
                table_name,
                con=conn,
                if_exists='replace',
                index=False,
                chunksize=5000
            )

        print(f"✅ Loaded: {table_name}")

    except Exception as e:
        print(f"❌ Error in {table_name}")
        print(e)

# -------------------------------
# ✅ MAIN EXECUTION
# -------------------------------
if __name__ == "__main__":
    print("🚀 Starting staging load...")

    for table, path in TABLES.items():
        load_table(table, path)

    print("\n🎯 All staging tables loaded successfully.")