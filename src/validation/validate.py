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
# Importing Libraries
import csv
import sys
import logging
from datetime import datetime
from multiprocessing import Pool
import os
import boto3 # Import boto3 for S3 interaction
from botocore.exceptions import ClientError
import tempfile # For creating temporary files/directories

# Configure logging to stderr for CloudWatch integration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stderr)
logger = logging.getLogger(__name__)

# Initialize S3 client
s3_client = boto3.client('s3')

# Define valid categories (populated from products.csv)
VALID_CATEGORIES = set()

# Validate a single row based on file type
def validate_row(row, file_type):
    """Validate a single row based on its file type (orders, order_items, or products)."""
    if file_type == 'products':
        if 'product_category' not in row or 'product_id' not in row:
            logger.error(f"Missing required fields in products row {row}")
            return False, "Missing required fields"
        VALID_CATEGORIES.add(row['product_category'])
        return True, "Valid"
    elif file_type == 'orders':
        required_fields = ['order_id', 'order_date', 'order_value', 'product_category', 'is_returned', 'customer_id']
        if not all(field in row for field in required_fields):
            logger.error(f"Missing required fields in orders row {row}")
            return False, "Missing required fields"
        try:
            datetime.strptime(row['order_date'], '%Y-%m-%d')
            float(row['order_value'])
            # is_returned should be a boolean or a value that can be interpreted as one (e.g., 0 or 1)
            # Assuming it's an integer 0 or 1 based on project brief.
            int(row['is_returned'])
        except (ValueError, KeyError) as e:
            logger.error(f"Invalid data type or format in orders row {row}: {str(e)}")
            return False, "Invalid data type or format"
        if row['product_category'] not in VALID_CATEGORIES:
            logger.error(f"Invalid category {row['product_category']} in orders row {row}")
            return False, "Invalid category"
    elif file_type == 'order_items':
        required_fields = ['order_id', 'product_id', 'item_count']
        if not all(field in row for field in required_fields):
            logger.error(f"Missing required fields in order_items row {row}")
            return False, "Missing required fields"
        try:
            int(row['item_count'])
        except (ValueError, KeyError) as e:
            logger.error(f"Invalid data type or format in order_items row {row}: {str(e)}")
            return False, "Invalid data type or format"
    logger.info(f"{file_type} row validated successfully: {row}")
    return True, "Valid"

# Helper function to list and download files from S3
def get_s3_files(bucket_name, s3_prefix, file_pattern_suffix):
    """
    Lists files in an S3 prefix matching a pattern and downloads them to a temporary directory.
    Returns a list of local file paths.
    """
    local_file_paths = []
    temp_dir = tempfile.mkdtemp() # Create a temporary directory

    try:
        # List objects in the specified S3 prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix)

        for page in pages:
            if "Contents" in page:
                for obj in page['Contents']:
                    s3_key = obj['Key']
                    # Simple pattern matching for now (e.g., 'orders_*.csv', 'products.csv')
                    # This assumes the pattern suffix is at the end of the filename
                    # For more complex regex, you'd use re.match
                    if file_pattern_suffix == 'products.csv' and s3_key.endswith('products.csv'):
                        # For exact match like products.csv
                        pass
                    elif file_pattern_suffix.startswith('orders_') and s3_key.endswith('.csv') and s3_key.startswith(s3_prefix + 'orders_'):
                        # For patterns like orders_*.csv
                        pass
                    elif file_pattern_suffix.startswith('order_items_') and s3_key.endswith('.csv') and s3_key.startswith(s3_prefix + 'order_items_'):
                        # For patterns like order_items_*.csv
                        pass
                    else:
                        continue # Skip files that don't match the pattern

                    local_file_path = os.path.join(temp_dir, os.path.basename(s3_key))
                    logger.info(f"Downloading s3://{bucket_name}/{s3_key} to {local_file_path}")
                    s3_client.download_file(bucket_name, s3_key, local_file_path)
                    local_file_paths.append(local_file_path)
    except ClientError as e:
        logger.error(f"S3 client error: {e}")
        # Clean up temp directory on error
        for f in local_file_paths:
            os.remove(f)
        os.rmdir(temp_dir)
        raise # Re-raise the exception to propagate failure
    except Exception as e:
        logger.error(f"An unexpected error occurred during S3 file retrieval: {e}")
        # Clean up temp directory on error
        for f in local_file_paths:
            os.remove(f)
        os.rmdir(temp_dir)
        raise

    return local_file_paths, temp_dir

# Validate all files of a given type
def validate_files(bucket_name, s3_prefix, file_pattern_suffix, file_type, threshold):
    """Validate all files matching a pattern and check for minimum count."""
    logger.info(f"Validating {file_type} files with pattern: s3://{bucket_name}/{s3_prefix}{file_pattern_suffix}")
    
    local_files, temp_dir = [], None
    try:
        local_files, temp_dir = get_s3_files(bucket_name, s3_prefix, file_pattern_suffix)
        
        if file_type != 'products' and len(local_files) < threshold:
            logger.warning(f"Only {len(local_files)} {file_type} files found, need {threshold}")
            return False
        
        if not local_files:
            logger.warning(f"No {file_type} files found matching pattern: {file_pattern_suffix}")
            return False

        all_valid = True
        for file in local_files:
            with open(file, 'r') as f:
                reader = csv.DictReader(f)
                # Ensure the reader is not empty before mapping
                rows = list(reader)
                if not rows:
                    logger.warning(f"Skipping empty {file_type} file: {file}")
                    continue

                with Pool() as pool: # Create a new pool for each file to manage resources better
                    results = pool.map(lambda row: validate_row(row, file_type), rows)
                    if not all(result[0] for result in results):
                        logger.error(f"Validation failed for {file_type} file: {file}")
                        all_valid = False
                        break # Stop processing this file type if any file fails validation
        
        if not all_valid:
            return False

    except Exception as e:
        logger.error(f"An error occurred during {file_type} file validation: {e}")
        return False
    finally:
        # Clean up temporary files and directory
        if temp_dir and os.path.exists(temp_dir):
            for f in local_files:
                if os.path.exists(f):
                    os.remove(f)
            os.rmdir(temp_dir)
            logger.info(f"Cleaned up temporary directory: {temp_dir}")

    logger.info(f"Validation completed for {len(local_files)} {file_type} files")
    return True

if __name__ == "__main__":
    if len(sys.argv) != 4:
        logger.error("Usage: python validate.py <orders_pattern> <order_items_pattern> <threshold>")
        sys.exit(1)
    
    # Arguments from ECS command: e.g., s3://bucket/prefix/orders_*.csv
    orders_full_pattern = sys.argv[1]
    order_items_full_pattern = sys.argv[2]
    threshold = int(sys.argv[3])

    # Extract bucket and prefix from the full S3 path
    # Assuming format: s3://bucket-name/prefix/filename_pattern
    # We need the bucket name and the common prefix (e.g., 'incoming/')
    try:
        # Split the S3 path into bucket and key
        # Example: s3://lab6-ecommerce-shop/incoming/orders_*.csv
        # parts[0] = 's3:'
        # parts[1] = ''
        # parts[2] = 'lab6-ecommerce-shop'
        # parts[3] = 'incoming'
        # parts[4] = 'orders_*.csv'
        path_parts = orders_full_pattern.split('/')
        input_bucket = path_parts[2] # e.g., 'lab6-ecommerce-shop'
        s3_base_prefix = '/'.join(path_parts[3:-1]) + '/' # e.g., 'incoming/'
        
        # Extract just the filename pattern part for the get_s3_files function
        orders_file_pattern_suffix = path_parts[-1] # e.g., 'orders_*.csv'
        order_items_file_pattern_suffix = order_items_full_pattern.split('/')[-1] # e.g., 'order_items_*.csv'
        products_file_pattern_suffix = 'products.csv' # Always products.csv
        
        # Ensure the base prefix ends with a slash if it's not empty
        if s3_base_prefix and not s3_base_prefix.endswith('/'):
            s3_base_prefix += '/'

    except IndexError:
        logger.error("Invalid S3 path format provided. Expected s3://bucket/prefix/pattern.")
        sys.exit(1)

    # Validate products first to build category set
    # Products file is expected in the same base prefix
    if not validate_files(input_bucket, s3_base_prefix, products_file_pattern_suffix, 'products', 1):
        logger.error("Products validation failed")
        sys.exit(1)
    
    # Validate orders and order_items, ensuring threshold
    if not (validate_files(input_bucket, s3_base_prefix, orders_file_pattern_suffix, 'orders', threshold) and
            validate_files(input_bucket, s3_base_prefix, order_items_file_pattern_suffix, 'order_items', threshold)):
        logger.error("Orders or order_items validation failed or threshold not met")
        sys.exit(1)
    
    logger.info("All validations succeeded")
    sys.exit(0)

