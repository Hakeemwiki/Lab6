import sys
import logging
import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, when, count, countDistinct, to_date
from pyspark.sql.types import IntegerType, FloatType, StringType
from botocore.exceptions import ClientError
from delta import configure_spark_with_delta_pip

# Configure logging to stderr for CloudWatch integration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stderr)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Delta Lake support."""
    try:
        builder = SparkSession.builder \
            .appName("ECommerceTransformation") \
            .config("spark.master", "local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        return spark

    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise

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
    spark = None
    try:
        spark = create_spark_session()
        create_dynamodb_tables()

        # Read CSV files from S3
        logger.info("Reading CSV files from S3...")

        orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv(orders_path)
        logger.info(f"Orders schema: {orders_df.columns}")

        order_items_df = spark.read.option("header", "true").option("inferSchema", "true").csv(order_items_path)
        logger.info(f"Order items schema: {order_items_df.columns}")

        products_df = spark.read.option("header", "true").option("inferSchema", "true").csv(products_path)
        logger.info(f"Products schema: {products_df.columns}")

        # Cast types and rename conflicting columns
        orders_df = orders_df.withColumn("order_id", col("order_id").cast(IntegerType())) \
                             .withColumn("user_id", col("user_id").cast(IntegerType())) \
                             .withColumn("num_of_item", col("num_of_item").cast(IntegerType())) \
                             .withColumn("order_date", to_date(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss")) \
                             .withColumnRenamed("status", "order_status")

        order_items_df = order_items_df.withColumn("order_id", col("order_id").cast(IntegerType())) \
                                       .withColumn("product_id", col("product_id").cast(IntegerType())) \
                                       .withColumn("user_id", col("user_id").cast(IntegerType())) \
                                       .withColumn("sale_price", col("sale_price").cast(FloatType())) \
                                       .withColumnRenamed("status", "item_status")

        products_df = products_df.withColumn("id", col("id").cast(IntegerType())) \
                                 .withColumn("cost", col("cost").cast(FloatType())) \
                                 .withColumn("retail_price", col("retail_price").cast(FloatType()))

        # Join datasets
        logger.info("Joining datasets...")
        orders_items_df = orders_df.join(order_items_df, "order_id", "inner")
        joined_df = orders_items_df.join(products_df, orders_items_df.product_id == products_df.id, "left")

        # Add calculated fields
        joined_df = joined_df.withColumn("is_returned", when(col("order_status") == "returned", 1).otherwise(0)) \
                             .withColumn("order_value", col("sale_price"))

        logger.info(f"Joined dataset has {joined_df.count()} rows")

        # Write to Delta table
        delta_path = f"s3a://{os.environ.get('S3_INPUT_BUCKET', 'lab6-ecommerce-shop')}/processed/"
        logger.info(f"Writing to Delta table at {delta_path}")

        joined_df.write.format("delta") \
            .partitionBy("order_date") \
            .mode("append") \
            .save(delta_path)

        logger.info("Processed data written to Delta table")

        # Category-level KPIs
        logger.info("Computing category-level KPIs...")
        category_kpis = joined_df.groupBy("category", "order_date").agg(
            _sum("sale_price").alias("daily_revenue"),
            avg("sale_price").alias("avg_order_value"),
            (avg(when(col("is_returned") == 1, 1.0).otherwise(0.0)) * 100).alias("avg_return_rate")
        ).na.fill(0)

        # Order-level KPIs
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

        logger.info(f"Writing {len(category_kpis.collect())} category KPI records...")
        for row in category_kpis.collect():
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

        logger.info(f"Writing {len(order_kpis.collect())} order KPI records...")
        for row in order_kpis.collect():
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
        logger.info("Transformation completed successfully")

    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        logger.error("Usage: python transform.py <orders_path> <order_items_path> <products_path>")
        sys.exit(1)

    orders_path = sys.argv[1]
    order_items_path = sys.argv[2]
    products_path = sys.argv[3]

    logger.info("Processing files:")
    logger.info(f"Orders: {orders_path}")
    logger.info(f"Order Items: {order_items_path}")
    logger.info(f"Products: {products_path}")

    transform_files(orders_path, order_items_path, products_path)
    sys.exit(0)
