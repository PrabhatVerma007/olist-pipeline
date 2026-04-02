import pandas as pd

df = pd.read_csv(r'C:\olist-pipeline\data\raw\olist_customers_dataset.csv')

print("Total rows:", len(df))
print("Unique customer_id:", df['customer_id'].nunique())

dupes = df[df.duplicated(subset=['customer_id'], keep=False)]
print("Duplicate rows:", len(dupes))