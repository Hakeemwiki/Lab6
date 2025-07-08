import boto3
import json
import os
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError

# --- Constants (from Environment Variables) ---
S3_BUCKET = os.environ.get("S3_BUCKET", "lab6-ecommerce-shop")
S3_PREFIX = os.environ.get("S3_PREFIX", "incoming/")  # Must end with /
LOCK_KEY = S3_PREFIX + 'pipeline.lock'
THRESHOLD_MET_TIMESTAMP_KEY = S3_PREFIX + 'threshold_not_met_timestamp.json'
STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]

MIN_ORDER_FILES = int(os.environ.get("MIN_ORDER_FILES", 5))
MIN_ITEM_FILES = int(os.environ.get("MIN_ITEM_FILES", 5))
MIN_PRODUCT_FILES = int(os.environ.get("MIN_PRODUCT_FILES", 1))
FORCE_PROCESS_AFTER_MINUTES = int(os.environ.get("FORCE_PROCESS_AFTER_MINUTES", 30))

s3_client = boto3.client('s3')
sf_client = boto3.client('stepfunctions', region_name=os.environ.get('AWS_REGION', 'eu-north-1'))

def get_s3_object_content(bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read().decode('utf-8')
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        raise

def put_s3_object_content(bucket, key, content):
    s3_client.put_object(Bucket=bucket, Key=key, Body=content.encode('utf-8'))

def delete_s3_object(bucket, key):
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchKey':
            raise

def lambda_handler(event, context):
    print(f"Lambda triggered by event: {json.dumps(event)}")

    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=LOCK_KEY)
        print("Lock exists. Skipping execution.")
        return {'statusCode': 200, 'body': 'Another batch is processing.'}
    except ClientError as e:
        if e.response['Error']['Code'] != '404':
            raise

    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
    if 'Contents' not in response:
        delete_s3_object(S3_BUCKET, THRESHOLD_MET_TIMESTAMP_KEY)
        return {'statusCode': 200, 'body': 'No files found.'}

    files = [obj['Key'] for obj in response['Contents']
             if obj['Key'] != S3_PREFIX and not obj['Key'].endswith(('pipeline.lock', 'threshold_not_met_timestamp.json'))]

    orders = [f for f in files if f.startswith(S3_PREFIX + 'orders_') and f.endswith('.csv')]
    items = [f for f in files if f.startswith(S3_PREFIX + 'order_items_') and f.endswith('.csv')]
    products = [f for f in files if f == S3_PREFIX + 'products.csv']

    orders_count = len(orders)
    items_count = len(items)
    products_count = len(products)

    print(f"Orders: {orders_count}, Items: {items_count}, Products: {products_count}")

    should_trigger = False
    now = datetime.now(timezone.utc)

    if orders_count >= MIN_ORDER_FILES and items_count >= MIN_ITEM_FILES and products_count >= MIN_PRODUCT_FILES:
        should_trigger = True
        delete_s3_object(S3_BUCKET, THRESHOLD_MET_TIMESTAMP_KEY)
    else:
        ttl = get_s3_object_content(S3_BUCKET, THRESHOLD_MET_TIMESTAMP_KEY)
        if ttl:
            first_miss = datetime.fromisoformat(ttl).replace(tzinfo=timezone.utc)
            if (now - first_miss).total_seconds() / 60 >= FORCE_PROCESS_AFTER_MINUTES:
                should_trigger = True
                delete_s3_object(S3_BUCKET, THRESHOLD_MET_TIMESTAMP_KEY)
        else:
            put_s3_object_content(S3_BUCKET, THRESHOLD_MET_TIMESTAMP_KEY, now.isoformat())

    if should_trigger:
        put_s3_object_content(S3_BUCKET, LOCK_KEY, now.isoformat())
        try:
            input_data = {
                'bucket': S3_BUCKET,
                'prefix': S3_PREFIX,
                'orders_count': orders_count,
                'items_count': items_count,
                'products_count': products_count,
                'files': files
            }
            response = sf_client.start_execution(
                stateMachineArn=STEP_FUNCTION_ARN,
                input=json.dumps(input_data)
            )
            print("Step Function triggered.", response['executionArn'])
            return {'statusCode': 200, 'body': 'Step Function triggered.'}
        except ClientError as e:
            delete_s3_object(S3_BUCKET, LOCK_KEY)
            return {'statusCode': 500, 'body': str(e)}

    # Cleanup lock file if exists and not processing
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=LOCK_KEY)
        print("Cleaning up stale lock file.")
        delete_s3_object(S3_BUCKET, LOCK_KEY)
    except ClientError as e:
        if e.response['Error']['Code'] != '404':
            raise

    return {'statusCode': 200, 'body': f'Waiting: Orders={orders_count}, Items={items_count}, Products={products_count}'}
