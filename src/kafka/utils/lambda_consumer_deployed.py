# lambda_function.py
import json
import boto3
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables (works locally; in AWS Lambda, vars are injected automatically)
load_dotenv()

s3 = boto3.client('s3')
BUCKET = os.getenv('BUCKET')
RAW_PREFIX = os.getenv('RAW_PREFIX', 'raw/')

def lambda_handler(event, context):
    # event will come from EC2 script
    timestamp = datetime.utcnow().isoformat()
    key = f"{RAW_PREFIX}{timestamp}.json"

    # Save the payload
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(event)
    )

    print(f"✅ Written to s3://{BUCKET}/{key}")
    return {"status": "ok", "records": len(event) if isinstance(event, list) else 1}
