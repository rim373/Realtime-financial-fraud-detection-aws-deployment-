# lambda_preprocess.py
import json
import boto3
import pandas as pd
import os
from io import StringIO
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize S3 client
s3 = boto3.client('s3')

# Load config from .env
BUCKET = os.getenv('BUCKET')
RAW_PREFIX = os.getenv('RAW_PREFIX', 'raw/')
PROC_PREFIX = os.getenv('PROC_PREFIX', 'processed/')


def lambda_handler(event, context):
    print("🚀 Starting preprocessing...")

    # 1️⃣ List all raw JSON files
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=RAW_PREFIX)
    if 'Contents' not in response:
        return {"status": "no raw data found"}

    all_records = []
    for obj in response['Contents']:
        key = obj['Key']
        if key.endswith('.json'):
            raw_obj = s3.get_object(Bucket=BUCKET, Key=key)
            content = json.loads(raw_obj['Body'].read().decode('utf-8'))
            # Each file may contain 1 or multiple JSON objects
            if isinstance(content, list):
                all_records.extend(content)
            else:
                all_records.append(content)

    print(f"📦 Loaded {len(all_records)} records from raw data")

    # 2️⃣ Convert to DataFrame
    df = pd.DataFrame(all_records)

    # 3️⃣ Fill missing values
    for col in df.columns:
        if df[col].dtype == 'O':
            df[col].fillna('unknown', inplace=True)
        else:
            df[col].fillna(0, inplace=True)

    # 4️⃣ Stratified split (keep same fraud ratio)
    train_df, test_df = train_test_split(
        df, test_size=0.2, stratify=df['isFraud'], random_state=42
    )

    # 5️⃣ Save back to S3 as CSV (best for SageMaker XGBoost)
    for split_name, split_df in [('train', train_df), ('test', test_df)]:
        csv_buffer = StringIO()
        split_df.to_csv(csv_buffer, index=False)
        s3.put_object(
            Bucket=BUCKET,
            Key=f"{PROC_PREFIX}{split_name}.csv",
            Body=csv_buffer.getvalue()
        )
        print(f"✅ Saved {split_name} set ({len(split_df)} rows)")

    return {
        "status": "ok",
        "train_size": len(train_df),
        "test_size": len(test_df)
    }
