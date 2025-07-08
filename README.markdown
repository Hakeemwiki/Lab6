# E-Commerce Real-Time Event-Driven Data Pipeline

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Data Format and Sample Schema](#data-format-and-sample-schema)
- [Validation Rules](#validation-rules)
- [DynamoDB Table Structure and Access Patterns](#dynamodb-table-structure-and-access-patterns)
- [Step Function Workflow Explanation](#step-function-workflow-explanation)
- [Error-Handling, Retry, and Logging Logic](#error-handling-retry-and-logging-logic)
- [Instructions to Simulate or Test the Pipeline Manually](#instructions-to-simulate-or-test-the-pipeline-manually)
- [Design Choices](#design-choices)
- [Getting Started](#getting-started)
- [Contributing](#contributing)
- [License](#license)
- [Guide for Recreating the Project](#guide-for-recreating-the-project)

## Overview
This project implements a real-time, event-driven data pipeline for an e-commerce platform using AWS-native services. The pipeline processes transactional data (orders, order items, and products) stored as CSV files in an Amazon S3 bucket, validates and transforms the data into business KPIs, and stores the results in Amazon DynamoDB for real-time querying. The solution leverages Amazon ECS for containerized processing, AWS Step Functions for orchestration, and includes logging and error handling with CloudWatch and SNS.

## Architecture
- **Amazon S3**: Stores input files in the `lab6-ecommerce-shop` bucket under the `incoming/` prefix.
- **Amazon ECS**: Runs containerized validation and transformation tasks on Fargate.
- **AWS Step Functions**: Orchestrates the workflow, including validation, transformation, and lock management.
- **Amazon DynamoDB**: Stores computed KPIs for category-level and order-level analytics.
- **AWS Lambda**: (Optional) Triggers the pipeline based on S3 events, with plans to explore EventBridge.
- **Amazon CloudWatch & SNS**: Logs execution and notifies on failures.
![alt text](docs/lab6.drawio.svg)


## Data Format and Sample Schema
### Input Data
- **Files**: CSV files named `orders_*.csv`, `order_items_*.csv`, and `products.csv` in `incoming/`.
- **Sample Schema**:
  - `orders_*.csv`: `order_id, user_id, order_date, total_amount, status`
    - Example: `1, 1001, 2025-07-07, 150.00, completed`
  - `order_items_*.csv`: `order_id, product_id, quantity, unit_price`
    - Example: `1, 2001, 2, 75.00`
  - `products.csv`: `product_id, category, name, price`
    - Example: `2001, Electronics, Smartphone, 150.00`

### Output KPIs
- Stored in DynamoDB:
  - **Category-Level KPIs** (Table: `CategoryKPIs`):
    - `category` (String): Product category (e.g., Electronics).
    - `order_date` (String): Date of summarized orders (e.g., 2025-07-07).
    - `daily_revenue` (Number): Total revenue for the category.
    - `avg_order_value` (Number): Average order value.
    - `avg_return_rate` (Number): Percentage of returned orders.

![alt text](docs/categoryKPI.png)

  - **Order-Level KPIs** (Table: `OrderKPIs`):
    - `order_date` (String): Date of summarized orders.
    - `total_orders` (Number): Count of unique orders.
    - `total_revenue` (Number): Total revenue.
    - `total_items_sold` (Number): Total items sold.
    - `return_rate` (Number): Percentage of returned orders.
    - `unique_customers` (Number): Number of distinct customers.

![alt text](docs/orders_kpi.png)

## Validation Rules
- **Malformed Data**: Reject files with missing headers or invalid CSV format.
- **Referential Integrity**: Ensure `order_id` in `order_items_*.csv` matches an `order_id` in `orders_*.csv`. Validate `product_id` exists in `products.csv`.
- **Thresholds**: Proceed only if there are ≥5 `orders_*.csv`, ≥5 `order_items_*.csv`, and 1 `products.csv` (configurable via environment variables).

## DynamoDB Table Structure and Access Patterns
- **CategoryKPIs Table**:
  - **Partition Key**: `category` (String).
  - **Sort Key**: `order_date` (String).
  - **Access Pattern**: Query by `category` and `order_date` range for daily category analytics.
- **OrderKPIs Table**:
  - **Partition Key**: `order_date` (String).
  - **Access Pattern**: Query by `order_date` for daily order summaries.
- **Indexes**: No secondary indexes needed due to primary key optimization.

## Step Function Workflow Explanation
- **CheckFileThreshold**: Passes pre-computed file counts and file list from Lambda input to `$.thresholds`.
- **EvaluateThreshold**: Chooses `ValidateFiles` if thresholds (≥5 orders, ≥5 items, ≥1 product) are met, else `WaitForFiles` (300-second wait).
- **ValidateFiles**: Runs `ecommerce-validator-task` on ECS/Fargate to validate data, with retries (2 attempts, 30s interval) and a 600s timeout.
- **TransformFiles**: Executes `ecommerce-transformer-task` to compute KPIs and store in DynamoDB, with retries (3 attempts, 60s interval) and a 1800s timeout.
- **ResetLock**: Deletes `incoming/pipeline.lock` to release the lock.
- **Success**: Ends the workflow on success.
- **NotifyFailure**: Sends an SNS notification on task failures.
- **LogFailure**: Logs a failure metric to CloudWatch.

![alt text](docs/step_fn.png)

## Error-Handling, Retry, and Logging Logic
- **Retries**: Configured for `ValidateFiles` and `TransformFiles` with exponential backoff.
- **Error Handling**: Catches all errors in tasks, routing to `NotifyFailure` (SNS alert) and `LogFailure` (CloudWatch metric).
- **Logging**: Uses CloudWatch Logs for ECS task execution and Lambda debugging prints.

## Instructions to Simulate or Test the Pipeline Manually
1. **Prepare Test Data**:
   - Upload 5 files named `orders_1.csv` to `orders_5.csv`, 5 files named `order_items_1.csv` to `order_items_5.csv`, and 1 file named `products.csv` to `s3://lab6-ecommerce-shop/incoming/`.
   - Ensure files contain valid CSV data (e.g., `order_id,user_id,order_date,total_amount,status`).
2. **Trigger Lambda**:
   - In Lambda console, use a test event (`S3TestEvent`) with:
     ```json
     {
       "Records": [{
         "s3": {
           "bucket": {"name": "lab6-ecommerce-shop"},
           "object": {"key": "incoming/orders_1.csv"}
         }
       }]
     }
     ```
   - Increase Lambda timeout to 15 seconds and run the test.
3. **Manually Start Step Function**:
   - Go to Step Functions > `ECommercePipeline` > Start execution.
   - Use input:
     ```json
     {
       "orders_count": 5,
       "items_count": 5,
       "products_count": 1,
       "files": ["incoming/orders_1.csv", "incoming/orders_2.csv", "incoming/orders_3.csv", "incoming/orders_4.csv", "incoming/orders_5.csv", "incoming/order_items_1.csv", "incoming/order_items_2.csv", "incoming/order_items_3.csv", "incoming/order_items_4.csv", "incoming/order_items_5.csv", "incoming/products.csv"]
     }
     ```
   - Monitor execution in the console.
4. **Verify Output**:
   - Check DynamoDB tables `CategoryKPIs` and `OrderKPIs` for computed KPIs.
   - Review CloudWatch Logs for `/aws/lambda/CheckFileThreshold` and `/aws/ecs` task logs.
5. **Simulate Failure**:
   - Upload a malformed file (e.g., missing headers) and verify the pipeline exits gracefully with an SNS alert.

## Design Choices
- **Event-Driven**: Uses S3 events to detect new files, ensuring real-time processing.
- **Scalability**: ECS on Fargate scales with task demand; DynamoDB handles query loads.
- **Modularity**: Separates validation and transformation into distinct ECS tasks.
- **Locking**: S3-based lock avoids concurrency issues, replacing DynamoDB for simplicity.

## Getting Started
- Deploy the Lambda, ECS tasks, Step Function, and DynamoDB tables using AWS CLI or Console.
- Configure environment variables in Lambda (e.g., `STEP_FUNCTION_ARN`, `S3_BUCKET`).
- Test with the above instructions and adjust as needed.

## Contributing
Feel free to suggest improvements (e.g., adding a dashboard or retry queue). Submit issues or pull requests with clear descriptions.


## Guide for Recreating the Project
This guide provides step-by-step instructions to recreate the e-commerce data pipeline based on the work completed. It assumes familiarity with AWS services and CLI usage.

### Prerequisites
- AWS account with permissions to create S3 buckets, Lambda functions, ECS clusters, Step Functions, and DynamoDB tables.
- AWS CLI configured with your credentials (region: `eu-north-1`, account: `771826808190`).
- Docker installed for building ECS container images.

### Steps to Recreate

1. **Set Up S3 Bucket**
   - Create a bucket: `aws s3 mb s3://lab6-ecommerce-shop --region eu-north-1`.
   - Enable versioning: `aws s3api put-bucket-versioning --bucket lab6-ecommerce-shop --versioning-configuration Status=Enabled`.

2. **Create DynamoDB Tables**
   - Create `CategoryKPIs` table:
     ```bash
     aws dynamodb create-table \
       --table-name CategoryKPIs \
       --attribute-definitions AttributeName=category,AttributeType=S AttributeName=order_date,AttributeType=S \
       --key-schema AttributeName=category,KeyType=HASH AttributeName=order_date,KeyType=RANGE \
       --billing-mode PAY_PER_REQUEST \
       --region eu-north-1
     ```
   - Create `OrderKPIs` table:
     ```bash
     aws dynamodb create-table \
       --table-name OrderKPIs \
       --attribute-definitions AttributeName=order_date,AttributeType=S \
       --key-schema AttributeName=order_date,KeyType=HASH \
       --billing-mode PAY_PER_REQUEST \
       --region eu-north-1
     ```

3. **Set Up ECS Cluster and Tasks**
   - Create a Fargate cluster: `aws ecs create-cluster --cluster-name ecommerce-data-pipeline-cluster --region eu-north-1`.
   - Define `ecommerce-validator-task` and `ecommerce-transformer-task` with Dockerfiles and push to ECR:
     - Validator: Validates CSV data (e.g., `validate.py`).
     - Transformer: Computes KPIs and writes to DynamoDB (e.g., `transform.py`).
   - Register tasks with appropriate roles and subnets (`subnet-0b7f371c5d05a61fd`, security group `sg-05ce9be02039057c5`).

4. **Deploy Lambda Function**
   - Create `lambda_trigger.py` with the provided code.
   - Package and deploy:
     ```bash
     zip lambda_function.zip lambda_trigger.py
     aws lambda create-function \
       --function-name CheckFileThreshold \
       --runtime python3.9 \
       --role arn:aws:iam::771826808190:role/LambdaExecutionRole \
       --handler lambda_trigger.handler \
       --zip-file fileb://lambda_function.zip \
       --timeout 15 \
       --environment Variables="{S3_BUCKET=lab6-ecommerce-shop,S3_PREFIX=incoming/,LOCK_KEY=incoming/pipeline.lock,THRESHOLD_MET_TIMESTAMP_KEY=incoming/threshold_not_met_timestamp.json,STEP_FUNCTION_ARN=arn:aws:states:eu-north-1:771826808190:stateMachine:ECommercePipeline,MIN_ORDER_FILES=5,MIN_ITEM_FILES=5,MIN_PRODUCT_FILES=1,FORCE_PROCESS_AFTER_MINUTES=30}" \
       --region eu-north-1
     ```
   - Add S3 permission: `aws lambda add-permission ...` (as previously shown).

5. **Configure Step Function**
   - Define `step_functions.json` with the provided workflow.
   - Deploy: `aws stepfunctions create-state-machine --name ECommercePipeline --definition file://step_functions.json --role-arn arn:aws:iam::771826808190:role/StepFunctionRole --region eu-north-1`.

6. **Set Up S3 Event Notification**
   - S3 > `lab6-ecommerce-shop` > Properties > Events > Create notification.
   - Event type: `All object create events`, Prefix: `incoming/`, Destination: `CheckFileThreshold`.

7. **(Optional) Switch to EventBridge**
   - Enable S3 to EventBridge notifications.
   - Create an EventBridge rule targeting `ECommercePipeline` with the provided pattern.
   - Update Step Function to compute file counts (see previous response).

8. **Test the Pipeline**
   - Follow the "Instructions to Simulate or Test the Pipeline Manually" section.
   - Verify KPIs in DynamoDB and logs in CloudWatch.

### Notes
- Adjust IAM roles (e.g., `LambdaExecutionRole`, `StepFunctionRole`) with `s3:*`, `ecs:*`, `dynamodb:*`, and `states:*` permissions.
