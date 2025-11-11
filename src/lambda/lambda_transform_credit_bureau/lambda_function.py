"""
Lambda Function: Transform Credit Bureau 
Purpose: S3-triggered transformation of raw credit bureau CSV to silver Parquet
"""

import json
import logging
import os
from datetime import datetime
from io import BytesIO, StringIO
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
SOURCE_NAME = "credit_bureau"
OUTPUT_PATH = f"transformed/{SOURCE_NAME}/"


def lambda_handler(event, context):
    logger.info("Credit Bureau Transformation Lambda invoked")

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
            result = process_credit_bureau(bucket, key)
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


def process_credit_bureau(bucket,key ):
    """
    Process a single credit bureau CSV file.

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
        #Read CSV from S3
        logger.info("Reading CSV from S3...")
        df = read_csv_from_s3(bucket, key)
        logger.info(f"Loaded {len(df)} records, {len(df.columns)} columns")

        #Validate schema
        logger.info("Validating schema...")
        validate_schema(df)

        #Transform data
        logger.info("Transforming data...")
        filename = key.split('/')[-1]
        df_transformed = transform_credit_bureau(df, filename)

        #Write to silver
        logger.info("Writing to silver bucket...")
        output_location = write_to_silver(df_transformed)

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


def read_csv_from_s3(bucket, key):
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
        'customer_id',
        'ssn',
        'credit_score',
        'total_open_loans',
        'total_defaults',
        'inquiries_last_6m'
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    logger.info(f"Schema validation passed. Found {len(df.columns)} columns")


def validate_credit_score(score):
    """Validate credit score is in valid range (100-2000)."""
    if pd.isna(score):
        return None
    score = float(score)
    if score < 100 or score > 2000:
        logger.warning(f"Invalid credit score {score}, setting to None")
        return None
    return int(score)


def mask_ssn(ssn):
    """Mask SSN showing only last 4 digits."""
    if pd.isna(ssn) or len(str(ssn)) < 4:
        return ssn
    ssn_str = str(ssn).replace('-', '').replace(' ', '')
    if len(ssn_str) >= 4:
        return f"XXX-XX-{ssn_str[-4:]}"
    return ssn


def transform_credit_bureau(df: pd.DataFrame, source_file):
    """
    Transform and cleanse credit bureau data.
    """
    logger.info(f"Transforming {len(df)} credit bureau records...")

    #Data type conversions
    df['customer_id'] = df['customer_id'].astype(str).str.strip()
    df['ssn'] = df['ssn'].astype(str).str.strip()

    #Numeric conversions
    df['credit_score'] = pd.to_numeric(df['credit_score'], errors='coerce')
    df['total_open_loans'] = pd.to_numeric(df['total_open_loans'], errors='coerce').astype('Int64')
    df['total_defaults'] = pd.to_numeric(df['total_defaults'], errors='coerce').astype('Int64')
    df['inquiries_last_6m'] = pd.to_numeric(df['inquiries_last_6m'], errors='coerce').astype('Int64')

    #Validate credit score range (100-2000)
    df['credit_score'] = df['credit_score'].apply(validate_credit_score)

    #SSN masking (mask all but last 4 digits)
    df['ssn'] = df['ssn'].apply(mask_ssn)

    #Add metadata columns
    df['source_file'] = source_file
    df['created_date'] = datetime.now()
    df['updated_date'] = datetime.now()
    df['effective_date'] = datetime.now().date()

    #Handle nulls
    null_counts = df.isnull().sum()
    if null_counts.any():
        logger.warning(f"Found null values:\n{null_counts[null_counts > 0]}")

    #Validate credit score is present
    missing_scores = df['credit_score'].isnull().sum()
    if missing_scores > 0:
        logger.warning(f"{missing_scores} records missing valid credit scores")

    #Deduplicate on customer_id (keep latest)
    initial_count = len(df)
    df = df.drop_duplicates(subset=['customer_id'], keep='last')
    if len(df) < initial_count:
        logger.info(f"Removed {initial_count - len(df)} duplicate customer_ids")

    logger.info(f"Transformation complete. Output: {len(df)} records")
    return df


def write_to_silver(df):
    """Write transformed data to silver bucket as partitioned Parquet."""
    try:
        # Partition by processing date
        now = datetime.now()
        partition_path = f"{OUTPUT_PATH}year={now.year}/month={now.month:02d}/day={now.day:02d}/"

        # Create unique filename with timestamp
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        filename = f"credit_bureau_{timestamp}.parquet"
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
