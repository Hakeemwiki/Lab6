# Importing Libraries
import sys
import logging
import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, when
from botocore.exceptions import ClientError