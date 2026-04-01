import mysql.connector
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

SQL_ORDER = [
    'sql/intermediate/int_orders_payments.sql',
    'sql/intermediate/int_items_products.sql',
    'sql/intermediate/int_orders_customers.sql',
    'sql/mart/mart_monthly_revenue.sql',
    'sql/mart/mart_category_revenue.sql',
    'sql/mart/mart_orders_by_state.sql',
    'sql/mart/mart_order_status.sql',
    'sql/mart/mart_delivery_time.sql',
    'sql/mart/mart_payment_type.sql',
]

EXPECTED_COUNTS = {
    'int_orders_payments':  90000,
    'int_items_products':   100000,
    'int_orders_customers': 90000,
    'mart_monthly_revenue': 20,
    'mart_category_revenue':15,
    'mart_orders_by_state': 20,
    'mart_order_status':    5,
    'mart_delivery_time':   20,
    'mart_payment_type':    4,
}


def run_transformations():
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    cursor = conn.cursor()

    print("=" * 60)
    print("RUNNING SQL TRANSFORMATIONS")
    print("=" * 60)

    failed = []

    for filepath in SQL_ORDER:
        filename = os.path.basename(filepath)

        if not os.path.exists(filepath):
            print(f'\nSKIPPED (file not found): {filepath}')
            failed.append(filepath)
            continue

        with open(filepath, 'r') as f:
            sql = f.read()

        statements = [
            s.strip() for s in sql.split(';')
            if s.strip() and not s.strip().startswith('--')
        ]

        print(f'\nRunning : {filename}')

        try:
            for statement in statements:
                cursor.execute(statement)
                conn.commit()
            print(f'Status  : DONE')

        except Exception as e:
            print(f'Status  : FAILED — {e}')
            failed.append(filename)
            conn.rollback()
            continue

    print("\n" + "=" * 60)
    print("ROW COUNT VALIDATION")
    print("=" * 60)

    all_passed = True

    for table, min_expected in EXPECTED_COUNTS.items():
        try:
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            count = cursor.fetchone()[0]
            status = 'PASS' if count >= min_expected else 'WARN'
            if status == 'WARN':
                all_passed = False
            print(f'  {table:<30} {count:>8} rows   [{status}]')
        except Exception as e:
            print(f'  {table:<30} ERROR: {e}')
            all_passed = False

    print("\n" + "=" * 60)

    if not failed and all_passed:
        print('RESULT : ALL TRANSFORMATIONS PASSED')
        print('         Safe to proceed to Airflow and Tableau')
    elif not failed and not all_passed:
        print('RESULT : COMPLETED WITH ROW COUNT WARNINGS')
        print('         Check tables marked WARN above')
    else:
        print('RESULT : SOME TRANSFORMATIONS FAILED')
        print('Failed files:')
        for f in failed:
            print(f'  - {f}')

    print("=" * 60)

    cursor.close()
    conn.close()


if __name__ == '__main__':
    run_transformations()