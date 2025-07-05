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