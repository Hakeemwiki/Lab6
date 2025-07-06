# # Importing Libraries
# import csv
# import sys
# import logging
# from datetime import datetime
# from multiprocessing import Pool
# import glob
# import os

# # Configure logging to stderr for CloudWatch integration
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stderr)
# logger = logging.getLogger(__name__)

# # Define valid categories (populated from products.csv)
# VALID_CATEGORIES = set()

# # Validate a single row based on file type
# def validate_row(row, file_type):
#     """Validate a single row based on its file type (orders, order_items, or products)."""
#     if file_type == 'products':
#         if 'product_category' not in row or 'product_id' not in row:
#             logger.error(f"Missing required fields in products row {row}")
#             return False, "Missing required fields"
#         VALID_CATEGORIES.add(row['product_category'])
#         return True, "Valid"
#     elif file_type == 'orders':
#         required_fields = ['order_id', 'order_date', 'order_value', 'product_category', 'is_returned', 'customer_id']
#         if not all(field in row for field in required_fields):
#             logger.error(f"Missing required fields in orders row {row}")
#             return False, "Missing required fields"
#         try:
#             datetime.strptime(row['order_date'], '%Y-%m-%d')
#             float(row['order_value'])
#             int(row['is_returned'])
#         except (ValueError, KeyError) as e:
#             logger.error(f"Invalid data type or format in orders row {row}: {str(e)}")
#             return False, "Invalid data type or format"
#         if row['product_category'] not in VALID_CATEGORIES:
#             logger.error(f"Invalid category {row['product_category']} in orders row {row}")
#             return False, "Invalid category"
#     elif file_type == 'order_items':
#         required_fields = ['order_id', 'product_id', 'item_count']
#         if not all(field in row for field in required_fields):
#             logger.error(f"Missing required fields in order_items row {row}")
#             return False, "Missing required fields"
#         try:
#             int(row['item_count'])
#         except (ValueError, KeyError) as e:
#             logger.error(f"Invalid data type or format in order_items row {row}: {str(e)}")
#             return False, "Invalid data type or format"
#     logger.info(f"{file_type} row validated successfully: {row}")
#     return True, "Valid"

# # Validate all files of a given type
# def validate_files(file_pattern, file_type, threshold):
#     """Validate all files matching a pattern and check for minimum count."""
#     logger.info(f"Validating {file_type} files with pattern: {file_pattern}")
#     files = glob.glob(file_pattern)
#     if file_type != 'products' and len(files) < threshold:
#         logger.warning(f"Only {len(files)} {file_type} files found, need {threshold}")
#         return False
#     with Pool() as pool:
#         for file in files:
#             with open(file, 'r') as f:
#                 reader = csv.DictReader(f)
#                 results = pool.map(lambda row: validate_row(row, file_type), [row for row in reader])
#                 if not all(result[0] for result in results):
#                     logger.error(f"Validation failed for {file_type} file: {file}")
#                     return False
#     logger.info(f"Validation completed for {len(files)} {file_type} files")
#     return True

# if __name__ == "__main__":
#     if len(sys.argv) != 4:
#         logger.error("Usage: python validate.py <orders_pattern> <order_items_pattern> <threshold>")
#         sys.exit(1)
#     orders_pattern, order_items_pattern, threshold = sys.argv[1], sys.argv[2], int(sys.argv[3])
#     input_bucket = os.environ.get('S3_INPUT_BUCKET', 'lab6-ecommerce-shop')
#     # Validate products first to build category set
#     if not validate_files(f"{input_bucket}/incoming/products.csv", 'products', 1):
#         logger.error("Products validation failed")
#         sys.exit(1)
#     # Validate orders and order_items, ensuring threshold
#     if not (validate_files(f"{input_bucket}/incoming/orders_*.csv", 'orders', threshold) and
#             validate_files(f"{input_bucket}/incoming/order_items_*.csv", 'order_items', threshold)):
#         logger.error("Orders or order_items validation failed or threshold not met")
#         sys.exit(1)
#     logger.info("All validations succeeded")
#     sys.exit(0)
import os
import csv
import tempfile
import boto3
import sys
import logging
from botocore.exceptions import ClientError

# Configure logging to stderr for CloudWatch integration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stderr)
logger = logging.getLogger(__name__)

# Initialize S3 client
s3_client = boto3.client('s3')

def get_s3_files(bucket_name, prefix, pattern):
    """Download matching S3 files to a temporary local directory."""
    temp_dir = tempfile.mkdtemp()
    local_files = []
    
    try:
        logger.info(f"Searching for files with prefix: {prefix}, pattern: {pattern}")
        paginator = s3_client.get_paginator('list_objects_v2')
        
        # Handle case where prefix might be empty or None
        if prefix:
            page_kwargs = {'Bucket': bucket_name, 'Prefix': prefix}
        else:
            page_kwargs = {'Bucket': bucket_name}
            
        for page in paginator.paginate(**page_kwargs):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                key = obj['Key']
                filename = os.path.basename(key)
                
                # Match pattern logic
                if pattern == 'products.csv':
                    # Exact match for products
                    if filename == 'products.csv':
                        local_path = os.path.join(temp_dir, filename)
                        s3_client.download_file(bucket_name, key, local_path)
                        local_files.append(local_path)
                        logger.info(f"Downloaded: {key}")
                elif pattern.startswith('orders_') and pattern.endswith('.csv'):
                    # Match orders_*.csv pattern
                    if filename.startswith('orders_') and filename.endswith('.csv'):
                        local_path = os.path.join(temp_dir, filename)
                        s3_client.download_file(bucket_name, key, local_path)
                        local_files.append(local_path)
                        logger.info(f"Downloaded: {key}")
                elif pattern.startswith('order_items_') and pattern.endswith('.csv'):
                    # Match order_items_*.csv pattern
                    if filename.startswith('order_items_') and filename.endswith('.csv'):
                        local_path = os.path.join(temp_dir, filename)
                        s3_client.download_file(bucket_name, key, local_path)
                        local_files.append(local_path)
                        logger.info(f"Downloaded: {key}")
                        
        logger.info(f"Downloaded {len(local_files)} files to {temp_dir}")
        return local_files, temp_dir
    except ClientError as e:
        logger.error(f"Failed to download S3 files: {str(e)}")
        raise

def validate_row(row, file_type, valid_categories=None):
    """Validate a single row based on file type."""
    if file_type == 'products':
        # Check required fields and non-empty values
        required_fields = ['product_id', 'product_category']
        if not all(field in row and row[field].strip() for field in required_fields):
            logger.warning(f"Invalid products row - missing or empty required fields: {row}")
            return False
        return True
        
    elif file_type == 'orders':
        # Check required fields and non-empty values
        required_fields = ['order_id', 'order_date', 'order_value', 'product_category', 'is_returned', 'customer_id']
        if not all(field in row and row[field].strip() for field in required_fields):
            logger.warning(f"Invalid orders row - missing or empty required fields: {row}")
            return False
        
        # Validate data types
        try:
            float(row['order_value'])
            int(row['is_returned'])
        except ValueError as e:
            logger.warning(f"Invalid orders row - data type error: {row}, error: {e}")
            return False
            
        # Validate category if provided
        if valid_categories and row['product_category'] not in valid_categories:
            logger.warning(f"Invalid orders row - unknown category: {row['product_category']}")
            return False
            
        return True
        
    elif file_type == 'order_items':
        # Check required fields and non-empty values
        required_fields = ['order_id', 'product_id', 'item_count']
        if not all(field in row and row[field].strip() for field in required_fields):
            logger.warning(f"Invalid order_items row - missing or empty required fields: {row}")
            return False
        
        # Validate data types
        try:
            int(row['item_count'])
        except ValueError as e:
            logger.warning(f"Invalid order_items row - data type error: {row}, error: {e}")
            return False
            
        return True
        
    return False

def validate_files(bucket_name, prefix, pattern, file_type, threshold, valid_categories=None):
    """Validate files downloaded from S3."""
    local_files = []
    temp_dir = None
    
    try:
        local_files, temp_dir = get_s3_files(bucket_name, prefix, pattern)
        
        if len(local_files) < threshold:
            logger.warning(f"Only {len(local_files)} {file_type} files found, need {threshold}")
            return False, len(local_files), []
        
        valid_rows = 0
        categories_found = set()
        
        for file_path in local_files:
            logger.info(f"Validating file: {file_path}")
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                if not reader.fieldnames:
                    logger.warning(f"Empty or invalid CSV file: {file_path}")
                    continue
                
                row_count = 0
                valid_file_rows = 0
                
                for row in reader:
                    row_count += 1
                    if validate_row(row, file_type, valid_categories):
                        valid_file_rows += 1
                        if file_type == 'products':
                            categories_found.add(row['product_category'])
                    else:
                        logger.warning(f"Invalid row {row_count} in {file_path}")
                
                logger.info(f"File {file_path}: {valid_file_rows}/{row_count} valid rows")
                valid_rows += valid_file_rows
        
        logger.info(f"Validation completed: {valid_rows} valid {file_type} rows from {len(local_files)} files")
        return True, valid_rows, categories_found
        
    except Exception as e:
        logger.error(f"Validation failed for {file_type}: {str(e)}")
        raise
    finally:
        # Cleanup
        if local_files:
            for file_path in local_files:
                if os.path.exists(file_path):
                    os.remove(file_path)
        if temp_dir and os.path.exists(temp_dir):
            try:
                os.rmdir(temp_dir)
            except OSError:
                pass  # Directory not empty, ignore

if __name__ == "__main__":
    if len(sys.argv) != 4:
        logger.error("Usage: python validate.py <orders_pattern> <order_items_pattern> <threshold>")
        sys.exit(1)
    
    orders_pattern = sys.argv[1]
    order_items_pattern = sys.argv[2]
    threshold = int(sys.argv[3])
    
    # Get bucket from environment
    input_bucket = os.environ.get('S3_INPUT_BUCKET', 'lab6-ecommerce-shop')
    
    logger.info(f"Starting validation with bucket: {input_bucket}")
    logger.info(f"Orders pattern: {orders_pattern}")
    logger.info(f"Order items pattern: {order_items_pattern}")
    logger.info(f"Threshold: {threshold}")
    
    # Validate products first to build category set
    logger.info("=== Validating Products ===")
    products_valid, products_count, categories_found = validate_files(
        input_bucket, 'incoming/', 'products.csv', 'products', 1
    )
    
    if not products_valid:
        logger.error("Products validation failed")
        sys.exit(1)
    
    logger.info(f"Found {len(categories_found)} product categories: {categories_found}")
    
    # Validate orders
    logger.info("=== Validating Orders ===")
    orders_valid, orders_count, _ = validate_files(
        input_bucket, 'incoming/', 'orders_*.csv', 'orders', threshold, categories_found
    )
    
    if not orders_valid:
        logger.error("Orders validation failed or threshold not met")
        sys.exit(1)
    
    # Validate order items
    logger.info("=== Validating Order Items ===")
    items_valid, items_count, _ = validate_files(
        input_bucket, 'incoming/', 'order_items_*.csv', 'order_items', threshold
    )
    
    if not items_valid:
        logger.error("Order items validation failed or threshold not met")
        sys.exit(1)
    
    logger.info("=== All validations passed ===")
    logger.info(f"Summary: {products_count} products, {orders_count} orders, {items_count} order items")
    sys.exit(0)