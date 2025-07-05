# Importing Libraries
import sys
import logging
import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, when
from botocore.exceptions import ClientError

# Configure logging to stderr for CloudWatch integration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stderr)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName("ECommerceTransformation").getOrCreate()

# Create DynamoDB tables with partition and sort keys
def create_dynamodb_tables():
    """Create DynamoDB tables for KPIs with defined schema."""
    dynamodb_client = boto3.client('dynamodb')
    tables = [
        {
            'TableName': os.environ['CATEGORY_KPI_TABLE'],
            'KeySchema': [
                {'AttributeName': 'category', 'KeyType': 'HASH'},
                {'AttributeName': 'order_date', 'KeyType': 'RANGE'}
            ],
            'AttributeDefinitions': [
                {'AttributeName': 'category', 'AttributeType': 'S'},
                {'AttributeName': 'order_date', 'AttributeType': 'S'}
            ],
            'BillingMode': 'PAY_PER_REQUEST'
        },
        {
            'TableName': os.environ['ORDER_KPI_TABLE'],
            'KeySchema': [
                {'AttributeName': 'order_date', 'KeyType': 'HASH'}
            ],
            'AttributeDefinitions': [
                {'AttributeName': 'order_date', 'AttributeType': 'S'}
            ],
            'BillingMode': 'PAY_PER_REQUEST'
        }
    ]
    for table in tables:
        try:
            dynamodb_client.create_table(**table)
            logger.info(f"Creating table {table['TableName']}...")
            dynamodb_client.get_waiter('table_exists').wait(TableName=table['TableName'])
            logger.info(f"Table {table['TableName']} created successfully")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                logger.info(f"Table {table['TableName']} already exists, skipping creation")
            else:
                logger.error(f"Failed to create table {table['TableName']}: {str(e)}")
                raise