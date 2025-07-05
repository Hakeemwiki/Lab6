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