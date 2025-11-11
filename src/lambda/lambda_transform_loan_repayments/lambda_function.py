"""
Lambda Function: Transform Loan Repayments (Bronze → Silver)
Purpose: S3-triggered transformation of raw loan repayments CSV to silver Parquet
"""

import json
import logging
import os
from datetime import datetime
from io import BytesIO, StringIO
from typing import Dict
from urllib.parse import unquote_plus

import boto3
import pandas as pd

# Initialize AWS clients
s3_client = boto3.client('s3')

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
SILVER_BUCKET = os.environ.get('SILVER_BUCKET', 'creditrisk-silver')
SOURCE_NAME = "loan_repayments"
OUTPUT_PATH = f"transformed/{SOURCE_NAME}/"
MARKER_PATH = "_markers/"


def lambda_handler(event, context):
    """
    Lambda handler for S3-triggered loan repayments transformation.

    Args:
        event: S3 event notification
        context: Lambda context

    Returns:
        dict: Processing result
    """
    logger.info("Loan Repayments Transformation Lambda invoked")

    try:
        # Parse S3 event
        records = event.get('Records', [])
        if not records:
            logger.warning("No records in event")
            return {'statusCode': 200, 'body': json.dumps({'message': 'No files to process'})}

        results = []
        for record in records:
            # Extract S3 details
            s3_info = record.get('s3', {})
            bucket = s3_info.get('bucket', {}).get('name')
            key = unquote_plus(s3_info.get('object', {}).get('key', ''))

            if not bucket or not key:
                logger.error(f"Invalid S3 event: {record}")
                continue

            logger.info(f"Processing: s3://{bucket}/{key}")

            # Process file
            result = process_loan_repayments(bucket, key)
            results.append(result)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing complete',
                'results': results
            })
        }

    except Exception as e:
        logger.error(f"Lambda handler error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def process_loan_repayments(bucket: str, key: str) -> Dict:
    """
    Process a single loan repayments CSV file.

    Args:
        bucket: Source S3 bucket
        key: Source S3 key

    Returns:
        dict: Processing result
    """
    result = {
        'file': key,
        'status': 'error',
        'records_processed': 0,
        'output_location': None
    }

    try:
        # 1. Read CSV from S3
        logger.info("Reading CSV from S3...")
        df = read_csv_from_s3(bucket, key)
        logger.info(f"Loaded {len(df)} records, {len(df.columns)} columns")

        # 2. Validate schema
        logger.info("Validating schema...")
        validate_schema(df)

        # 3. Transform data
        logger.info("Transforming data...")
        filename = key.split('/')[-1]
        df_transformed = transform_loan_repayments(df, filename)

        # 4. Write to silver
        logger.info("Writing to silver bucket...")
        output_location = write_to_silver(df_transformed)

        # 5. Write completion marker
        logger.info("Writing completion marker...")
        write_marker_file(filename)

        result['status'] = 'success'
        result['records_processed'] = len(df_transformed)
        result['output_location'] = output_location

        logger.info(f"Successfully processed {len(df_transformed)} records")

    except Exception as e:
        logger.error(f"Error processing {key}: {str(e)}", exc_info=True)
        result['error'] = str(e)
        # Write error to silver error path
        write_error_log(key, str(e))

    return result


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read CSV file from S3 into pandas DataFrame."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        return df
    except Exception as e:
        logger.error(f"Error reading CSV from S3: {str(e)}")
        raise


def validate_schema(df: pd.DataFrame) -> None:
    """Validate that required columns exist."""
    required_columns = [
        'repayment_id',
        'loan_id',
        'customer_id',
        'due_date',
        'payment_date',
        'amount_paid',
        'status'
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    logger.info(f"Schema validation passed. Found {len(df.columns)} columns")


def calculate_delinquency(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate days_past_due and delinquency_bucket.
    """
    # Calculate days past due
    df['days_past_due'] = (df['payment_date'] - df['due_date']).dt.days

    # Classify delinquency buckets
    def classify_delinquency(days):
        if pd.isna(days):
            return 'UNKNOWN'
        elif days <= 0:
            return 'CURRENT'
        elif days <= 30:
            return '1-30_DAYS'
        elif days <= 60:
            return '31-60_DAYS'
        elif days <= 90:
            return '61-90_DAYS'
        else:
            return '90+_DAYS'

    df['delinquency_bucket'] = df['days_past_due'].apply(classify_delinquency)

    return df


def transform_loan_repayments(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """
    Transform and cleanse loan repayments data.
    """
    logger.info(f"Transforming {len(df)} loan repayment records...")

    # 1. Data type conversions
    df['repayment_id'] = df['repayment_id'].astype(str).str.strip()
    df['loan_id'] = df['loan_id'].astype(str).str.strip()
    df['customer_id'] = df['customer_id'].astype(str).str.strip()

    # 2. Date parsing
    df['due_date'] = pd.to_datetime(df['due_date'], errors='coerce')
    df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')

    # 3. Numeric conversions
    df['amount_paid'] = pd.to_numeric(df['amount_paid'], errors='coerce')

    # 4. Standardize status
    df['status'] = df['status'].astype(str).str.upper().str.strip()
    df['status'] = df['status'].map({
        'PAID': 'PAID',
        'COMPLETE': 'PAID',
        'COMPLETED': 'PAID',
        'LATE': 'LATE',
        'LATE PAYMENT': 'LATE',
        'MISSED': 'MISSED',
        'MISS': 'MISSED'
    }).fillna(df['status'])

    # 5. Calculate delinquency metrics
    df = calculate_delinquency(df)

    # 6. Add metadata columns
    df['source_file'] = source_file
    df['created_date'] = datetime.now()
    df['updated_date'] = datetime.now()

    # 7. Handle nulls
    null_counts = df.isnull().sum()
    if null_counts.any():
        logger.warning(f"Found null values:\n{null_counts[null_counts > 0]}")

    # 8. Sort by payment_date for chronological processing
    df = df.sort_values(['loan_id', 'payment_date'])

    # 9. Deduplicate on repayment_id
    initial_count = len(df)
    df = df.drop_duplicates(subset=['repayment_id'], keep='last')
    if len(df) < initial_count:
        logger.info(f"Removed {initial_count - len(df)} duplicate repayment_ids")

    logger.info(f"Transformation complete. Output: {len(df)} records")
    return df


def write_to_silver(df: pd.DataFrame) -> str:
    """Write transformed data to silver bucket as partitioned Parquet."""
    try:
        # Partition by processing date
        now = datetime.now()
        partition_path = f"{OUTPUT_PATH}year={now.year}/month={now.month:02d}/day={now.day:02d}/"

        # Create unique filename with timestamp
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        filename = f"loan_repayments_{timestamp}.parquet"
        s3_key = f"{partition_path}{filename}"

        # Convert DataFrame to Parquet
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy', index=False)
        parquet_buffer.seek(0)

        # Upload to S3
        s3_client.put_object(
            Bucket=SILVER_BUCKET,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )

        output_location = f"s3://{SILVER_BUCKET}/{s3_key}"
        logger.info(f"Successfully wrote {len(df)} records to {output_location}")

        return output_location

    except Exception as e:
        logger.error(f"Error writing to S3: {str(e)}")
        raise


def write_marker_file(source_filename: str) -> None:
    """Write a completion marker file to track transformation completion."""
    try:
        now = datetime.now()
        date_str = now.strftime("%Y%m%d")
        marker_key = f"{MARKER_PATH}loan_repayments_{date_str}.done"

        marker_content = json.dumps({
            'source_file': source_filename,
            'timestamp': now.isoformat(),
            'dataset': 'loan_repayments',
            'status': 'completed'
        })

        s3_client.put_object(
            Bucket=SILVER_BUCKET,
            Key=marker_key,
            Body=marker_content,
            ContentType='application/json'
        )

        logger.info(f"Wrote marker file: s3://{SILVER_BUCKET}/{marker_key}")

    except Exception as e:
        logger.warning(f"Failed to write marker file: {str(e)}")


def write_error_log(source_key: str, error_message: str) -> None:
    """Write error details to silver error path."""
    try:
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        error_key = f"error/{SOURCE_NAME}/error_{timestamp}.json"

        error_content = json.dumps({
            'source_file': source_key,
            'error': error_message,
            'timestamp': now.isoformat(),
            'dataset': SOURCE_NAME
        })

        s3_client.put_object(
            Bucket=SILVER_BUCKET,
            Key=error_key,
            Body=error_content,
            ContentType='application/json'
        )

        logger.info(f"Wrote error log: s3://{SILVER_BUCKET}/{error_key}")

    except Exception as e:
        logger.warning(f"Failed to write error log: {str(e)}")
