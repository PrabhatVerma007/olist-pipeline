import boto3
import os
import sys
from pathlib import Path

# Fix import path (safe)
CURRENT_DIR = Path(__file__).resolve().parent
sys.path.append(str(CURRENT_DIR))

# IMPORTANT: rename secrets.py → config.py (avoid conflict with built-in "secrets")
from config import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_BUCKET_NAME, AWS_REGION

# Absolute path (fixes PyCharm working directory issues)
RAW_FOLDER = CURRENT_DIR.parent / 'data' / 'raw'


def upload_to_s3():
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )

        if not RAW_FOLDER.exists():
            print(f"Folder not found: {RAW_FOLDER}")
            return

        files = [f for f in os.listdir(RAW_FOLDER) if f.endswith('.csv')]

        if not files:
            print("No CSV files found in data/raw/")
            return

        for filename in files:
            local_path = RAW_FOLDER / filename
            s3_key = f'raw/{filename}'

            print(f'Uploading {filename}...')

            s3.upload_file(
                Filename=str(local_path),
                Bucket=AWS_BUCKET_NAME,
                Key=s3_key
            )

            print(f'Done: {filename}')

        print(f'\nAll {len(files)} files uploaded to s3://{AWS_BUCKET_NAME}/raw/')

    except Exception as e:
        print("Error occurred:")
        print(e)


if __name__ == '__main__':
    upload_to_s3()