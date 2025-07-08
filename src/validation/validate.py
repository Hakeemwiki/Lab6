import os           # Import os for file and directory operations
import csv          # Import csv for reading CSV files
import tempfile     # Import tempfile to create temporary directories
import boto3        # Import boto3 for AWS SDK interactions
import sys          # Import sys for command-line arguments and exit handling
import logging      # Import logging for structured logging to CloudWatch
from botocore.exceptions import ClientError  # Import ClientError for S3 operation error handling
from datetime import datetime               # Import datetime for timestamp generation in rejection files
import json                                 # Import json to serialize row data for rejection logging

# Configure logging to stderr for CloudWatch integration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stderr)
logger = logging.getLogger(__name__)  # Create a logger instance with the module name

# Initialize S3 client for all S3 operations in the script
s3_client = boto3.client('s3')

def get_s3_files(bucket_name, prefix, pattern):
    """Download matching S3 files to a temporary local directory."""
    temp_dir = tempfile.mkdtemp()  # Create a temporary directory for downloaded files
    local_files = []  # List to store paths of downloaded local files
    
    try:
        logger.info(f"Searching for files with prefix: {prefix}, pattern: {pattern}")  # Log the search criteria
        paginator = s3_client.get_paginator('list_objects_v2')  # Use paginator for efficient S3 listing
        
        # Handle case where prefix might be empty or None to avoid invalid requests
        if prefix:
            page_kwargs = {'Bucket': bucket_name, 'Prefix': prefix}
        else:
            page_kwargs = {'Bucket': bucket_name}
            
        for page in paginator.paginate(**page_kwargs):  # Iterate over paginated S3 results
            if 'Contents' not in page:  # Skip if no objects are returned
                continue
            for obj in page['Contents']:  # Loop through each object in the page
                key = obj['Key']  # Extract the S3 object key
                filename = os.path.basename(key)  # Get the filename from the key
                
                # Match pattern logic based on the specified pattern
                if pattern == 'products.csv':  # Exact match for products.csv
                    if filename == 'products.csv':
                        local_path = os.path.join(temp_dir, filename)  # Define local path
                        s3_client.download_file(bucket_name, key, local_path)  # Download the file
                        local_files.append(local_path)  # Add to list of downloaded files
                        logger.info(f"Downloaded: {key}")  # Log successful download
                elif pattern.startswith('orders_') and pattern.endswith('.csv'):  # Match orders_*.csv
                    if filename.startswith('orders_') and filename.endswith('.csv'):
                        local_path = os.path.join(temp_dir, filename)
                        s3_client.download_file(bucket_name, key, local_path)
                        local_files.append(local_path)
                        logger.info(f"Downloaded: {key}")
                elif pattern.startswith('order_items_') and pattern.endswith('.csv'):  # Match order_items_*.csv
                    if filename.startswith('order_items_') and filename.endswith('.csv'):
                        local_path = os.path.join(temp_dir, filename)
                        s3_client.download_file(bucket_name, key, local_path)
                        local_files.append(local_path)
                        logger.info(f"Downloaded: {key}")
                        
        logger.info(f"Downloaded {len(local_files)} files to {temp_dir}")  # Log total files downloaded
        return local_files, temp_dir  # Return list of files and temp directory
    except ClientError as e:
        logger.error(f"Failed to download S3 files: {str(e)}")  # Log any S3 client errors
        raise

def validate_row(row, file_type, valid_categories=None):
    """Validate a single row based on file type."""
    if file_type == 'products':
        # Define required fields that must be present and non-empty
        required_fields = ['id', 'sku', 'cost', 'category', 'name', 'retail_price', 'department']
        optional_fields = ['brand']  # Optional fields that can be empty
        
        # Check that all required fields are present and non-empty
        for field in required_fields:
            if field not in row or not row[field].strip():
                logger.warning(f"Invalid products row - missing or empty required field '{field}': {row}")
                return False
        
        # Check that optional fields are present (but can be empty)
        for field in optional_fields:
            if field not in row:
                logger.warning(f"Invalid products row - missing optional field '{field}': {row}")
                return False
        
        # Validate data types for numeric fields
        try:
            float(row['cost'])
            float(row['retail_price'])
            int(row['id'])
        except ValueError as e:
            logger.warning(f"Invalid products row - data type error: {row}, error: {e}")
            return False
            
        return True
        
    elif file_type == 'orders':
        required_fields = ['order_id', 'user_id', 'status', 'created_at', 'num_of_item']
        # Check all required fields are present and non-empty
        if not all(field in row and row[field].strip() for field in required_fields):
            logger.warning(f"Invalid orders row - missing or empty required fields: {row}")
            return False
        
        try:
            int(row['order_id'])
            int(row['user_id'])
            int(row['num_of_item'])
        except ValueError as e:
            logger.warning(f"Invalid orders row - data type error: {row}, error: {e}")
            return False
            
        valid_statuses = ['delivered', 'returned', 'shipped', 'cancelled']
        if row['status'] not in valid_statuses:
            logger.warning(f"Invalid orders row - unknown status: {row['status']}")
            return False
            
        return True
        
    elif file_type == 'order_items':
        required_fields = ['order_id', 'product_id', 'user_id', 'status', 'sale_price']
        # Check all required fields are present and non-empty
        if not all(field in row and row[field].strip() for field in required_fields):
            logger.warning(f"Invalid order_items row - missing or empty required fields: {row}")
            return False
        
        try:
            float(row['sale_price'])
            int(row['order_id'])
            int(row['product_id'])
            int(row['user_id'])
        except ValueError as e:
            logger.warning(f"Invalid order_items row - data type error: {row}, error: {e}")
            return False
            
        valid_statuses = ['delivered', 'returned', 'shipped', 'cancelled']
        if row['status'] not in valid_statuses:
            logger.warning(f"Invalid order_items row - unknown status: {row['status']}")
            return False
            
        return True
        
    return False

def validate_files(bucket_name, prefix, pattern, file_type, threshold, valid_categories=None):
    """Validate files downloaded from S3."""
    local_files = []  # List to store paths of downloaded local files
    temp_dir = None  # Temporary directory for file downloads
    rejected_rows = []  # List to store rejected rows with file path, row number, and row data
    
    try:
        local_files, temp_dir = get_s3_files(bucket_name, prefix, pattern)  # Download matching files
        
        if len(local_files) < threshold:  # Check if the number of files meets the threshold
            logger.warning(f"Only {len(local_files)} {file_type} files found, need {threshold}")
            return False, len(local_files), [], rejected_rows
        
        valid_rows = 0  # Counter for valid rows across all files
        categories_found = set()  # Set to track unique categories from products
        
        for file_path in local_files:  # Iterate over each downloaded file
            logger.info(f"Validating file: {file_path}")
            with open(file_path, 'r', encoding='utf-8') as f:  # Open file for reading
                reader = csv.DictReader(f)  # Read CSV as dictionary rows
                
                if not reader.fieldnames:  # Check for empty or invalid CSV
                    logger.warning(f"Empty or invalid CSV file: {file_path}")
                    continue
                
                row_count = 0  # Track row number within the file
                valid_file_rows = 0  # Count of valid rows in the current file
                
                for row in reader:  # Process each row in the CSV
                    row_count += 1
                    if validate_row(row, file_type, valid_categories):  # Validate the row
                        valid_file_rows += 1
                        if file_type == 'products':
                            categories_found.add(row['category'])  # Add category if from products file
                    else:
                        # Log rejected row with file path, row number, and row data
                        rejected_rows.append((file_path, row_count, row))
                        logger.warning(f"Rejected row {row_count} in {file_path}: {row}")
                
                logger.info(f"File {file_path}: {valid_file_rows}/{row_count} valid rows")
                valid_rows += valid_file_rows
        
        # Save rejected rows to S3 rejection folder if any exist
        if rejected_rows:
            rejection_bucket = bucket_name  # Use the same bucket for rejections
            rejection_prefix = 'rejected/'  # Define rejection folder prefix
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')  # Generate timestamp for unique filename
            rejection_file = f'rejected_{file_type}_{timestamp}.csv'  # Construct filename
            rejection_path = os.path.join(temp_dir, rejection_file)  # Temporary local path
            
            with open(rejection_path, 'w', newline='') as f:  # Open file for writing rejected data
                writer = csv.writer(f)
                writer.writerow(['file_path', 'row_number', 'row_data'])  # Write header row
                for file_path, row_num, row in rejected_rows:  # Write each rejected row
                    writer.writerow([file_path, row_num, json.dumps(row)])  # Serialize row data as JSON
            
            s3_client.upload_file(rejection_path, rejection_bucket, rejection_prefix + rejection_file)  # Upload to S3
            logger.info(f"Uploaded {len(rejected_rows)} rejected rows to s3://{rejection_bucket}/{rejection_prefix}{rejection_file}")
        
        logger.info(f"Validation completed: {valid_rows} valid {file_type} rows from {len(local_files)} files")
        return True, valid_rows, categories_found, rejected_rows
        
    except Exception as e:
        logger.error(f"Validation failed for {file_type}: {str(e)}")  # Log any unexpected errors
        raise
    finally:
        # Cleanup temporary files and directory
        if local_files:
            for file_path in local_files:
                if os.path.exists(file_path):
                    os.remove(file_path)  # Remove each downloaded file
        if temp_dir and os.path.exists(temp_dir):
            try:
                os.rmdir(temp_dir)  # Remove temporary directory if empty
            except OSError:
                pass  # Ignore if directory is not empty (e.g., due to rejection file)

if __name__ == "__main__":
    if len(sys.argv) != 4:  # Check for correct number of command-line arguments
        logger.error("Usage: python validate.py <orders_pattern> <order_items_pattern> <threshold>")
        sys.exit(1)  # Exit with error if usage is incorrect
    
    orders_pattern = sys.argv[1]  # Get orders pattern from command line
    order_items_pattern = sys.argv[2]  # Get order items pattern from command line
    threshold = int(sys.argv[3])  # Get threshold as integer
    
    # Get bucket from environment, default to lab6-ecommerce-shop
    input_bucket = os.environ.get('S3_INPUT_BUCKET', 'lab6-ecommerce-shop')
    
    logger.info(f"Starting validation with bucket: {input_bucket}")
    logger.info(f"Orders pattern: {orders_pattern}")
    logger.info(f"Order items pattern: {order_items_pattern}")
    logger.info(f"Threshold: {threshold}")
    
    # Validate products first to build category set
    logger.info("=== Validating Products ===")
    products_valid, products_count, categories_found, rejected_products = validate_files(
        input_bucket, 'incoming/', 'products.csv', 'products', 1
    )
    
    if not products_valid:  # Check if products validation failed
        logger.error("Products validation failed")
        sys.exit(1)  # Exit with error if validation fails
    
    logger.info(f"Found {len(categories_found)} product categories: {categories_found}")
    
    # Validate orders
    logger.info("=== Validating Orders ===")
    orders_valid, orders_count, _, rejected_orders = validate_files(
        input_bucket, 'incoming/', 'orders_*.csv', 'orders', threshold, categories_found
    )
    
    if not orders_valid:  # Check if orders validation failed or threshold not met
        logger.error("Orders validation failed or threshold not met")
        sys.exit(1)
    
    # Validate order items
    logger.info("=== Validating Order Items ===")
    items_valid, items_count, _, rejected_items = validate_files(
        input_bucket, 'incoming/', 'order_items_*.csv', 'order_items', threshold
    )
    
    if not items_valid:  # Check if order items validation failed or threshold not met
        logger.error("Order items validation failed or threshold not met")
        sys.exit(1)
    
    logger.info("=== All validations passed ===")
    logger.info(f"Summary: {products_count} products, {orders_count} orders, {items_count} order items")
    sys.exit(0)  # Exit with success if all validations pass