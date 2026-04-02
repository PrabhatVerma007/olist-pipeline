import mysql.connector
import os
import sys
import time
import logging
from pathlib import Path

# -------------------------------
# PATH SETUP
# -------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

# -------------------------------
# LOGGING SETUP
# -------------------------------
logging.basicConfig(
    filename=BASE_DIR / 'pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# -------------------------------
# SQL EXECUTION ORDER
# -------------------------------
SQL_ORDER = [
    # INTERMEDIATE
    BASE_DIR / 'sql/intermediate/int_customers_geo.sql',
    BASE_DIR / 'sql/intermediate/int_orders_enriched.sql',
    BASE_DIR / 'sql/intermediate/int_order_items_enriched.sql',
    BASE_DIR / 'sql/intermediate/int_payments_reviews.sql',

    # FACT
    BASE_DIR / 'sql/mart/fact_order_items.sql',

    # MARTS
    BASE_DIR / 'sql/mart/mart_customer.sql',
    BASE_DIR / 'sql/mart/mart_delivery_reviews.sql',
    BASE_DIR / 'sql/mart/mart_payments.sql',
    BASE_DIR / 'sql/mart/mart_product.sql',
    BASE_DIR / 'sql/mart/mart_seller.sql',
]

# -------------------------------
# VALIDATION TABLES
# -------------------------------
MART_TABLES = {
    'fact_order_items':        100000,
    'mart_customer':           90000,
    'mart_delivery_reviews':   10000,
    'mart_payments':           100000,
    'mart_product':            30000,
    'mart_seller':             3000,
}

# -------------------------------
# MAIN FUNCTION
# -------------------------------
def run_transformations():
    start_time = time.time()

    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        connection_timeout=10
    )
    cursor = conn.cursor()

    print("=" * 60)
    print("RUNNING SQL TRANSFORMATIONS")
    print("=" * 60)

    failed = []

    for filepath in SQL_ORDER:
        filepath = Path(filepath)
        filename = filepath.name

        if not filepath.exists():
            print(f'\nSKIPPED - file not found: {filepath}')
            logging.error(f'Missing file: {filepath}')
            failed.append(filename)
            break

        with open(filepath, 'r', encoding='utf-8') as f:
            raw_sql = f.read()

        # Remove comments
        lines = [
            line for line in raw_sql.splitlines()
            if not line.strip().startswith('--')
        ]
        clean_sql = '\n'.join(lines)

        # Split statements
        statements = [
            s.strip() for s in clean_sql.split(';')
            if s.strip()
        ]

        print(f'\nRunning : {filename}')
        logging.info(f'Running {filename}')

        try:
            for statement in statements:
                cursor.execute(statement)

            conn.commit()
            print('Status  : DONE')
            logging.info(f'{filename} executed successfully')

        except Exception as e:
            conn.rollback()
            print('Status  : FAILED')
            print(f'Error   : {e}')
            logging.error(f'{filename} failed: {e}')
            failed.append(filename)
            break  # STOP PIPELINE on failure

    # -------------------------------
    # VALIDATION
    # -------------------------------
    print('\n' + '=' * 60)
    print('ROW COUNT VALIDATION')
    print('=' * 60)

    all_passed = True

    for table, min_rows in MART_TABLES.items():
        try:
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            count = cursor.fetchone()[0]

            status = 'PASS' if count >= min_rows else 'WARN'
            if status == 'WARN':
                all_passed = False

            print(f'{table:<30} {count:>10} rows [{status}]')

        except Exception as e:
            print(f'{table:<30} ERROR: {e}')
            logging.error(f'Validation failed for {table}: {e}')
            all_passed = False

    # -------------------------------
    # FINAL STATUS
    # -------------------------------
    print('\n' + '=' * 60)

    if not failed and all_passed:
        print('RESULT : ALL PASSED - Ready for Airflow & Tableau')
        logging.info('Pipeline completed successfully')

    elif failed:
        print('RESULT : FAILED FILES:')
        for f in failed:
            print(f'  - {f}')
        logging.error(f'Pipeline failed: {failed}')

    else:
        print('RESULT : COMPLETED WITH WARNINGS')
        logging.warning('Pipeline completed with warnings')

    print('=' * 60)

    end_time = time.time()
    print(f'\nExecution Time: {round(end_time - start_time, 2)} seconds')

    cursor.close()
    conn.close()


# -------------------------------
# ENTRY POINT
# -------------------------------
if __name__ == '__main__':
    run_transformations()