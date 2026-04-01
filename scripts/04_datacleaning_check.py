import pandas as pd
import os

RAW_FOLDER = 'data/raw/'

files = {
    'olist_orders_dataset.csv':         'order_id',
    'olist_customers_dataset.csv':      'customer_id',
    'olist_order_payments_dataset.csv': 'order_id',
    'olist_order_items_dataset.csv':    'order_id',
    'olist_products_dataset.csv':       'product_id',
    'olist_sellers_dataset.csv':        'seller_id',
    'olist_order_reviews_dataset.csv':  'review_id',
    'olist_geolocation_dataset.csv':    'geolocation_zip_code_prefix',
}

print("=" * 60)
print("DATA QUALITY REPORT — RAW CSV FILES")
print("=" * 60)

all_passed = True
warnings = []

for filename, pk in files.items():
    filepath = os.path.join(RAW_FOLDER, filename)

    if not os.path.exists(filepath):
        print(f'\nFILE NOT FOUND: {filename}')
        all_passed = False
        continue

    df = pd.read_csv(filepath)

    total_rows    = len(df)
    total_cols    = len(df.columns)
    null_pk       = df[pk].isnull().sum()
    duplicate_pk  = df[pk].duplicated().sum()
    total_nulls   = df.isnull().sum().sum()
    null_pct      = round(total_nulls / (total_rows * total_cols) * 100, 2)

    # per column null breakdown — only show columns with nulls
    cols_with_nulls = df.isnull().sum()
    cols_with_nulls = cols_with_nulls[cols_with_nulls > 0]

    # determine status
    if null_pk > 0:
        pk_status = "FAIL"
        all_passed = False
        warnings.append(f'{filename} has {null_pk} null primary keys')
    else:
        pk_status = "PASS"

    if duplicate_pk > 0:
        dup_status = "WARN"
        warnings.append(f'{filename} has {duplicate_pk} duplicate primary keys')
    else:
        dup_status = "PASS"

    print(f'\n{"─" * 60}')
    print(f'FILE    : {filename}')
    print(f'Rows    : {total_rows}   |   Columns : {total_cols}')
    print(f'Primary key column : {pk}')
    print(f'  Null PKs      : {null_pk}     [{pk_status}]')
    print(f'  Duplicate PKs : {duplicate_pk}  [{dup_status}]')
    print(f'  Overall null% : {null_pct}%')

    if len(cols_with_nulls) > 0:
        print(f'  Columns with nulls:')
        for col, count in cols_with_nulls.items():
            pct = round(count / total_rows * 100, 1)
            print(f'    - {col}: {count} nulls ({pct}%)')
    else:
        print(f'  No null values found in any column')

print(f'\n{"=" * 60}')

if all_passed and not warnings:
    print('RESULT : ALL CHECKS PASSED — data is clean, safe to load')
elif all_passed and warnings:
    print('RESULT : PASSED WITH WARNINGS — review before loading')
    print('\nWarnings:')
    for w in warnings:
        print(f'  - {w}')
else:
    print('RESULT : FAILED — fix issues before loading to staging')
    print('\nIssues found:')
    for w in warnings:
        print(f'  - {w}')

print("=" * 60)