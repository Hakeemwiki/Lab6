# Importing Libraries
import csv
import sys
import logging
from datetime import datetime
from multiprocessing import Pool
import glob
import os

# Configure logging to stderr for CloudWatch integration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stderr)
logger = logging.getLogger(__name__)

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

# Validate all files of a given type
def validate_files(file_pattern, file_type, threshold):
    """Validate all files matching a pattern and check for minimum count."""
    logger.info(f"Validating {file_type} files with pattern: {file_pattern}")
    files = glob.glob(file_pattern)
    if file_type != 'products' and len(files) < threshold:
        logger.warning(f"Only {len(files)} {file_type} files found, need {threshold}")
        return False
    with Pool() as pool:
        for file in files:
            with open(file, 'r') as f:
                reader = csv.DictReader(f)
                results = pool.map(lambda row: validate_row(row, file_type), [row for row in reader])
                if not all(result[0] for result in results):
                    logger.error(f"Validation failed for {file_type} file: {file}")
                    return False
    logger.info(f"Validation completed for {len(files)} {file_type} files")
    return True