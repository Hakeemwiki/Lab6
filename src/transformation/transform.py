import sys
import logging
import os
import boto3
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, when
from botocore.exceptions import ClientError
from delta import configure_spark_with_delta_pip

# Configure logging to stderr for CloudWatch integration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stderr)
logger = logging.getLogger(__name__)

# Initialize Spark session with Delta Lake
builder = SparkSession.builder.appName("ECommerceTransformation").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

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

# Transform data and compute KPIs, write to Delta table
def transform_files(orders_path, order_items_path, products_path):
    """Transform CSV files into KPIs and store in DynamoDB and Delta table."""
    logger.info(f"Starting transformation with paths: {orders_path}, {order_items_path}, {products_path}")
    create_dynamodb_tables()  # Create tables on first run

    # Read CSV files from S3
    orders_df = spark.read.option("header", "true").csv(orders_path)
    order_items_df = spark.read.option("header", "true").csv(order_items_path)
    products_df = spark.read.option("header", "true").csv(products_path)

    # Join datasets
    joined_df = orders_df.join(order_items_df, "order_id", "left").join(products_df, order_items_df.product_id == products_df.product_id, "left")

    # Write processed data to Delta table partitioned by order_date
    delta_path = "s3a://lab6-ecommerce-shop/processed/"
    joined_df.write.format("delta").partitionBy("order_date").mode("append").save(delta_path)
    logger.info(f"Processed data written to Delta table at {delta_path}")

    # Category-Level KPIs
    category_kpis = joined_df.groupBy("product_category", "order_date").agg(
        _sum(col("order_value").cast("float")).alias("daily_revenue"),
        avg(col("order_value").cast("float")).alias("avg_order_value"),
        (_sum(when(col("is_returned").cast("int") == 1, 1).otherwise(0)) / _sum(1) * 100).alias("avg_return_rate")
    ).na.fill(0)

    # Order-Level KPIs
    order_kpis = joined_df.groupBy("order_date").agg(
        _sum(1).alias("total_orders"),
        _sum(col("order_value").cast("float")).alias("total_revenue"),
        _sum(col("item_count").cast("int")).alias("total_items_sold"),
        (_sum(when(col("is_returned").cast("int") == 1, 1).otherwise(0)) / _sum(1) * 100).alias("return_rate"),
        _sum(when(col("customer_id").isNotNull(), 1).otherwise(0)).alias("unique_customers")
    ).na.fill(0)

    # Write to DynamoDB
    dynamodb = boto3.resource('dynamodb')
    category_table = dynamodb.Table(os.environ['CATEGORY_KPI_TABLE'])
    order_table = dynamodb.Table(os.environ['ORDER_KPI_TABLE'])

    for row in category_kpis.collect():
        category_table.put_item(Item={
            'category': row['product_category'],
            'order_date': row['order_date'],
            'daily_revenue': float(row['daily_revenue']),
            'avg_order_value': float(row['avg_order_value']),
            'avg_return_rate': float(row['avg_return_rate'])
        })
    for row in order_kpis.collect():
        order_table.put_item(Item={
            'order_date': row['order_date'],
            'total_orders': int(row['total_orders']),
            'total_revenue': float(row['total_revenue']),
            'total_items_sold': int(row['total_items_sold']),
            'return_rate': float(row['return_rate']),
            'unique_customers': int(row['unique_customers'])
        })

    # Archive files
    s3 = boto3.client('s3')
    archive_bucket = os.environ['S3_ARCHIVE_BUCKET']
    for path in [orders_path, order_items_path]:
        for file in glob.glob(path):
            key = file.replace(f"{os.environ.get('S3_INPUT_BUCKET', 'lab6-ecommerce-shop')}/incoming/", "")
            s3.copy_object(Bucket=archive_bucket, Key=f"archive/{key}", CopySource={'Bucket': 'lab6-ecommerce-shop', 'Key': file})
            s3.delete_object(Bucket='lab6-ecommerce-shop', Key=file)
    logger.info(f"Archived files to {archive_bucket}")
    logger.info("Transformation completed")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        logger.error("Usage: python transform.py <orders_path> <order_items_path> <products_path>")
        sys.exit(1)
    transform_files(f"s3a://lab6-ecommerce-shop/incoming/orders_*.csv", f"s3a://lab6-ecommerce-shop/incoming/order_items_*.csv", f"s3a://lab6-ecommerce-shop/incoming/products.csv")
    sys.exit(0)