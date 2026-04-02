import pandas as pd
import os
from pathlib import Path

raw_path = Path(r'C:\olist-pipeline\data\raw')

for filename in os.listdir(raw_path):
    if filename.endswith('.csv'):
        file_path = raw_path / filename
        df = pd.read_csv(file_path)

        print(f'\nFile: {filename}')
        print(f'Shape: {df.shape[0]} rows, {df.shape[1]} columns')
        print('Columns:')
        print(list(df.columns))