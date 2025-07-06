# import sys
# import logging
# import os
# import boto3
# import glob
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, avg, sum as _sum, when
# from botocore.exceptions import ClientError
# from delta import configure_spark_with_delta_pip

# # Configure logging to stderr for CloudWatch integration
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stderr)
# logger = logging.getLogger(__name__)

# # Initialize Spark session with Delta Lake
# builder = SparkSession.builder.appName("ECommerceTransformation").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
# spark = configure_spark_with_delta_pip(builder).getOrCreate()

# # Create DynamoDB tables with partition and sort keys
# def create_dynamodb_tables():
#     """Create DynamoDB tables for KPIs with defined schema."""
#     dynamodb_client = boto3.client('dynamodb')
#     tables = [
#         {
#             'TableName': os.environ['CATEGORY_KPI_TABLE'],
#             'KeySchema': [
#                 {'AttributeName': 'category', 'KeyType': 'HASH'},
#                 {'AttributeName': 'order_date', 'KeyType': 'RANGE'}
#             ],
#             'AttributeDefinitions': [
#                 {'AttributeName': 'category', 'AttributeType': 'S'},
#                 {'AttributeName': 'order_date', 'AttributeType': 'S'}
#             ],
#             'BillingMode': 'PAY_PER_REQUEST'
#         },
#         {
#             'TableName': os.environ['ORDER_KPI_TABLE'],
#             'KeySchema': [
#                 {'AttributeName': 'order_date', 'KeyType': 'HASH'}
#             ],
#             'AttributeDefinitions': [
#                 {'AttributeName': 'order_date', 'AttributeType': 'S'}
#             ],
#             'BillingMode': 'PAY_PER_REQUEST'
#         }
#     ]
#     for table in tables:
#         try:
#             dynamodb_client.create_table(**table)
#             logger.info(f"Creating table {table['TableName']}...")
#             dynamodb_client.get_waiter('table_exists').wait(TableName=table['TableName'])
#             logger.info(f"Table {table['TableName']} created successfully")
#         except ClientError as e:
#             if e.response['Error']['Code'] == 'ResourceInUseException':
#                 logger.info(f"Table {table['TableName']} already exists, skipping creation")
#             else:
#                 logger.error(f"Failed to create table {table['TableName']}: {str(e)}")
#                 raise

# # Transform data and compute KPIs, write to Delta table
# def transform_files(orders_path, order_items_path, products_path):
#     """Transform CSV files into KPIs and store in DynamoDB and Delta table."""
#     logger.info(f"Starting transformation with paths: {orders_path}, {order_items_path}, {products_path}")
#     create_dynamodb_tables()  # Create tables on first run

#     # Read CSV files from S3
#     orders_df = spark.read.option("header", "true").csv(orders_path)
#     order_items_df = spark.read.option("header", "true").csv(order_items_path)
#     products_df = spark.read.option("header", "true").csv(products_path)

#     # Join datasets
#     joined_df = orders_df.join(order_items_df, "order_id", "left").join(products_df, order_items_df.product_id == products_df.product_id, "left")

#     # Write processed data to Delta table partitioned by order_date
#     delta_path = "s3a://lab6-ecommerce-shop/processed/"
#     joined_df.write.format("delta").partitionBy("order_date").mode("append").save(delta_path)
#     logger.info(f"Processed data written to Delta table at {delta_path}")

#     # Category-Level KPIs
#     category_kpis = joined_df.groupBy("product_category", "order_date").agg(
#         _sum(col("order_value").cast("float")).alias("daily_revenue"),
#         avg(col("order_value").cast("float")).alias("avg_order_value"),
#         (_sum(when(col("is_returned").cast("int") == 1, 1).otherwise(0)) / _sum(1) * 100).alias("avg_return_rate")
#     ).na.fill(0)

#     # Order-Level KPIs
#     order_kpis = joined_df.groupBy("order_date").agg(
#         _sum(1).alias("total_orders"),
#         _sum(col("order_value").cast("float")).alias("total_revenue"),
#         _sum(col("item_count").cast("int")).alias("total_items_sold"),
#         (_sum(when(col("is_returned").cast("int") == 1, 1).otherwise(0)) / _sum(1) * 100).alias("return_rate"),
#         _sum(when(col("customer_id").isNotNull(), 1).otherwise(0)).alias("unique_customers")
#     ).na.fill(0)

#     # Write to DynamoDB
#     dynamodb = boto3.resource('dynamodb')
#     category_table = dynamodb.Table(os.environ['CATEGORY_KPI_TABLE'])
#     order_table = dynamodb.Table(os.environ['ORDER_KPI_TABLE'])

#     for row in category_kpis.collect():
#         category_table.put_item(Item={
#             'category': row['product_category'],
#             'order_date': row['order_date'],
#             'daily_revenue': float(row['daily_revenue']),
#             'avg_order_value': float(row['avg_order_value']),
#             'avg_return_rate': float(row['avg_return_rate'])
#         })
#     for row in order_kpis.collect():
#         order_table.put_item(Item={
#             'order_date': row['order_date'],
#             'total_orders': int(row['total_orders']),
#             'total_revenue': float(row['total_revenue']),
#             'total_items_sold': int(row['total_items_sold']),
#             'return_rate': float(row['return_rate']),
#             'unique_customers': int(row['unique_customers'])
#         })

#     # Archive files
#     s3 = boto3.client('s3')
#     archive_bucket = os.environ['S3_ARCHIVE_BUCKET']
#     for path in [orders_path, order_items_path]:
#         for file in glob.glob(path):
#             key = file.replace(f"{os.environ.get('S3_INPUT_BUCKET', 'lab6-ecommerce-shop')}/incoming/", "")
#             s3.copy_object(Bucket=archive_bucket, Key=f"archive/{key}", CopySource={'Bucket': 'lab6-ecommerce-shop', 'Key': file})
#             s3.delete_object(Bucket='lab6-ecommerce-shop', Key=file)
#     logger.info(f"Archived files to {archive_bucket}")
#     logger.info("Transformation completed")
#     spark.stop()

# if __name__ == "__main__":
#     if len(sys.argv) != 4:
#         logger.error("Usage: python transform.py <orders_path> <order_items_path> <products_path>")
#         sys.exit(1)
#     transform_files(f"s3a://lab6-ecommerce-shop/incoming/orders_*.csv", f"s3a://lab6-ecommerce-shop/incoming/order_items_*.csv", f"s3a://lab6-ecommerce-shop/incoming/products.csv")
#     sys.exit(0)


import sys
import logging
import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, when, count, countDistinct, to_date, regexp_extract
from pyspark.sql.types import IntegerType, FloatType, StringType
from botocore.exceptions import ClientError
from delta import configure_spark_with_delta_pip

# Configure logging to stderr for CloudWatch integration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stderr)
logger = logging.getLogger(__name__)

# Initialize Spark session with Delta Lake
builder = SparkSession.builder.appName("ECommerceTransformation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

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

def transform_files(orders_path, order_items_path, products_path):
    """Transform CSV files into KPIs and store in DynamoDB and Delta table."""
    logger.info(f"Starting transformation with paths: {orders_path}, {order_items_path}, {products_path}")
    
    try:
        create_dynamodb_tables()
        
        # Read CSV files from S3 with proper schema inference
        logger.info("Reading CSV files from S3...")
        
        # Read orders - based on actual schema
        orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv(orders_path)
        logger.info(f"Orders schema: {orders_df.columns}")
        
        # Read order_items - based on actual schema  
        order_items_df = spark.read.option("header", "true").option("inferSchema", "true").csv(order_items_path)
        logger.info(f"Order items schema: {order_items_df.columns}")
        
        # Read products - based on actual schema
        products_df = spark.read.option("header", "true").option("inferSchema", "true").csv(products_path)
        logger.info(f"Products schema: {products_df.columns}")
        
        # Data cleaning and type casting
        orders_df = orders_df.withColumn("order_id", col("order_id").cast(IntegerType())) \
                            .withColumn("user_id", col("user_id").cast(IntegerType())) \
                            .withColumn("num_of_item", col("num_of_item").cast(IntegerType())) \
                            .withColumn("order_date", to_date(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss"))
        
        order_items_df = order_items_df.withColumn("order_id", col("order_id").cast(IntegerType())) \
                                      .withColumn("product_id", col("product_id").cast(IntegerType())) \
                                      .withColumn("user_id", col("user_id").cast(IntegerType())) \
                                      .withColumn("sale_price", col("sale_price").cast(FloatType()))
        
        products_df = products_df.withColumn("id", col("id").cast(IntegerType())) \
                                .withColumn("cost", col("cost").cast(FloatType())) \
                                .withColumn("retail_price", col("retail_price").cast(FloatType()))
        
        # Join datasets
        logger.info("Joining datasets...")
        
        # First join orders with order_items
        orders_items_df = orders_df.join(order_items_df, "order_id", "inner")
        
        # Then join with products
        joined_df = orders_items_df.join(products_df, 
                                        order_items_df.product_id == products_df.id, 
                                        "left")
        
        # Add calculated fields
        joined_df = joined_df.withColumn("is_returned", 
                                       when(col("status") == "returned", 1).otherwise(0)) \
                           .withColumn("order_value", col("sale_price"))
        
        logger.info(f"Joined dataset has {joined_df.count()} rows")
        
        # Write processed data to Delta table partitioned by order_date
        delta_path = f"s3a://{os.environ.get('S3_INPUT_BUCKET', 'lab6-ecommerce-shop')}/processed/"
        logger.info(f"Writing to Delta table at {delta_path}")
        
        joined_df.write.format("delta") \
                .partitionBy("order_date") \
                .mode("append") \
                .save(delta_path)
        
        logger.info("Processed data written to Delta table")
        
        # Category-Level KPIs (Per Category, Per Day)
        logger.info("Computing category-level KPIs...")
        
        category_kpis = joined_df.groupBy("category", "order_date").agg(
            _sum("sale_price").alias("daily_revenue"),
            avg("sale_price").alias("avg_order_value"),
            (avg(when(col("is_returned") == 1, 1.0).otherwise(0.0)) * 100).alias("avg_return_rate")
        ).na.fill(0)
        
        # Order-Level KPIs (Per Day)
        logger.info("Computing order-level KPIs...")
        
        order_kpis = joined_df.groupBy("order_date").agg(
            countDistinct("order_id").alias("total_orders"),
            _sum("sale_price").alias("total_revenue"),
            _sum("num_of_item").alias("total_items_sold"),
            (avg(when(col("is_returned") == 1, 1.0).otherwise(0.0)) * 100).alias("return_rate"),
            countDistinct("user_id").alias("unique_customers")
        ).na.fill(0)
        
        # Write to DynamoDB
        logger.info("Writing KPIs to DynamoDB...")
        
        dynamodb = boto3.resource('dynamodb')
        category_table = dynamodb.Table(os.environ['CATEGORY_KPI_TABLE'])
        order_table = dynamodb.Table(os.environ['ORDER_KPI_TABLE'])
        
        # Write category KPIs
        category_data = category_kpis.collect()
        logger.info(f"Writing {len(category_data)} category KPI records...")
        
        for row in category_data:
            try:
                category_table.put_item(Item={
                    'category': str(row['category']) if row['category'] else 'unknown',
                    'order_date': str(row['order_date']),
                    'daily_revenue': float(row['daily_revenue']) if row['daily_revenue'] else 0.0,
                    'avg_order_value': float(row['avg_order_value']) if row['avg_order_value'] else 0.0,
                    'avg_return_rate': float(row['avg_return_rate']) if row['avg_return_rate'] else 0.0
                })
            except Exception as e:
                logger.error(f"Failed to write category KPI: {row}, error: {e}")
        
        # Write order KPIs
        order_data = order_kpis.collect()
        logger.info(f"Writing {len(order_data)} order KPI records...")
        
        for row in order_data:
            try:
                order_table.put_item(Item={
                    'order_date': str(row['order_date']),
                    'total_orders': int(row['total_orders']) if row['total_orders'] else 0,
                    'total_revenue': float(row['total_revenue']) if row['total_revenue'] else 0.0,
                    'total_items_sold': int(row['total_items_sold']) if row['total_items_sold'] else 0,
                    'return_rate': float(row['return_rate']) if row['return_rate'] else 0.0,
                    'unique_customers': int(row['unique_customers']) if row['unique_customers'] else 0
                })
            except Exception as e:
                logger.error(f"Failed to write order KPI: {row}, error: {e}")
        
        logger.info("KPIs written to DynamoDB successfully")
        
        # Archive files (optional - commented out for now)
        # archive_files(orders_path, order_items_path)
        
        logger.info("Transformation completed successfully")
        
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise
    finally:
        spark.stop()

def archive_files(orders_path, order_items_path):
    """Archive processed files to S3 archive bucket."""
    try:
        s3 = boto3.client('s3')
        input_bucket = os.environ.get('S3_INPUT_BUCKET', 'lab6-ecommerce-shop')
        archive_bucket = os.environ.get('S3_ARCHIVE_BUCKET', f'{input_bucket}-archive')
        
        # List and archive files
        paginator = s3.get_paginator('list_objects_v2')
        
        for prefix in ['incoming/orders_', 'incoming/order_items_']:
            for page in paginator.paginate(Bucket=input_bucket, Prefix=prefix):
                if 'Contents' not in page:
                    continue
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.csv'):
                        archive_key = f"archive/{key.replace('incoming/', '')}"
                        # Copy to archive
                        s3.copy_object(
                            Bucket=archive_bucket,
                            Key=archive_key,
                            CopySource={'Bucket': input_bucket, 'Key': key}
                        )
                        # Delete original
                        s3.delete_object(Bucket=input_bucket, Key=key)
                        logger.info(f"Archived {key} to {archive_key}")
        
        logger.info("Files archived successfully")
        
    except Exception as e:
        logger.error(f"Failed to archive files: {str(e)}")
        # Don't fail the entire job if archiving fails
        pass

if __name__ == "__main__":
    if len(sys.argv) != 4:
        logger.error("Usage: python transform.py <orders_path> <order_items_path> <products_path>")
        sys.exit(1)
    
    # Get input bucket
    input_bucket = os.environ.get('S3_INPUT_BUCKET', 'lab6-ecommerce-shop')
    
    # Construct S3 paths
    orders_path = f"s3a://{input_bucket}/incoming/orders_*.csv"
    order_items_path = f"s3a://{input_bucket}/incoming/order_items_*.csv"
    products_path = f"s3a://{input_bucket}/incoming/products.csv"
    
    logger.info(f"Processing files:")
    logger.info(f"Orders: {orders_path}")
    logger.info(f"Order Items: {order_items_path}")
    logger.info(f"Products: {products_path}")
    
    transform_files(orders_path, order_items_path, products_path)
    sys.exit(0)