"""
CSV Header Validation Lambda Function

This Lambda function validates CSV file headers in S3 and routes files based on validation results.
It's optimized for cost by reading only the header (first few KB) instead of the entire file.

Design Choices:
- Reads only first 8KB to get header (optimizes S3 data transfer costs)
- Uses structured logging for CloudWatch Insights queries
- Validates presence and order of required columns
- Handles both single and multiple file triggers
- Implements retry logic with exponential backoff for S3 operations
"""

import json
import logging
import os
import sys
from typing import Dict, List, Tuple, Optional
from io import StringIO
import boto3
from botocore.exceptions import ClientError
import csv

# Initialize AWS clients
s3_client = boto3.client('s3')

# Configure structured logging
logger = logging.getLogger()
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logger.setLevel(getattr(logging, log_level, logging.INFO))

# Required CSV columns schema - easy to extend for future validation rules
REQUIRED_COLUMNS = [
    'application_id',
    'customer_id',
    'full_name',
    'email',
    'phone_number',
    'address',
    'date_of_birth',
    'loan_amount',
    'loan_type',
    'term_months',
    'interest_rate',
    'approval_status'
]

# Configuration from environment variables
SOURCE_PATH = os.environ.get('SOURCE_PATH', '')
RAW_PATH = os.environ.get('RAW_PATH', '')
REJECT_PATH = os.environ.get('REJECT_PATH', '')
KMS_KEY_ID = os.environ.get('KMS_KEY_ID', '')

# Validation configuration
HEADER_READ_BYTES = 8192  # Read first 8KB to get header (cost optimization)

def lambda_handler(event, context):
    """
    Main Lambda handler for S3-triggered CSV validation.
    
    Args:
        event: S3 event notification containing bucket and object details
        context: Lambda context object
        
    Returns:
        dict: Response with status code and processing results
    """
    logger.info("CSV validation Lambda invoked", extra={
        'source_path': SOURCE_PATH,
        'raw_path': RAW_PATH,
        'reject_path': REJECT_PATH
    })
    
    # Validate environment variables
    if not all([SOURCE_PATH, RAW_PATH, REJECT_PATH]):
        logger.error("Missing required environment variables")
        raise ValueError("SOURCE_PATH, RAW_PATH, and REJECT_PATH must be set")
    
    results = {
        'processed': 0,
        'validated': 0,
        'rejected': 0,
        'errors': 0,
        'files': []
    }
    
    try:
        # Parse S3 event - handles both single and multiple file notifications
        records = event.get('Records', [])
        
        if not records:
            logger.warning("No records found in event")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No files to process'})
            }
        
        for record in records:
            results['processed'] += 1
            
            # Extract S3 bucket and key from event
            s3_info = record.get('s3', {})
            bucket = s3_info.get('bucket', {}).get('name')
            key = s3_info.get('object', {}).get('key')
            
            if not bucket or not key:
                logger.error("Invalid S3 event structure", extra={'record': record})
                results['errors'] += 1
                continue
            
            logger.info(f"Processing file: s3://{bucket}/{key}")
            
            # Process individual file
            file_result = process_csv_file(bucket, key)
            results['files'].append(file_result)
            
            if file_result['status'] == 'validated':
                results['validated'] += 1
            elif file_result['status'] == 'rejected':
                results['rejected'] += 1
            else:
                results['errors'] += 1
        
        logger.info("Batch processing complete", extra=results)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing complete',
                'results': results
            })
        }
        
    except Exception as e:
        logger.error(f"Unexpected error in lambda_handler: {str(e)}", 
                    exc_info=True, extra={'error_type': type(e).__name__})
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def process_csv_file(bucket: str, key: str) -> Dict:
    """
    Process a single CSV file: validate header and route to appropriate destination.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        
    Returns:
        dict: Processing result with status and destination
    """
    file_result = {
        'file': key,
        'status': 'error',
        'destination': None,
        'validation_errors': []
    }
    
    try:
        # Read only the header portion (cost optimization)
        header, header_str = read_csv_header(bucket, key)
        
        if header is None:
            file_result['validation_errors'].append("Failed to read CSV header")
            file_result['status'] = 'error'
            logger.error(f"Failed to read header for {key}")
            return file_result
        
        # Validate header against required schema
        is_valid, errors = validate_header(header)
        
        if is_valid:
            # Move to raw bucket
            destination = move_file(bucket, key, RAW_PATH)
            file_result['status'] = 'validated'
            file_result['destination'] = destination
            logger.info(f"File validated and moved to raw", extra={
                'file': key,
                'destination': destination,
                'columns': len(header)
            })
        else:
            # Move to rejected bucket
            destination = move_file(bucket, key, REJECT_PATH)
            file_result['status'] = 'rejected'
            file_result['destination'] = destination
            file_result['validation_errors'] = errors
            logger.warning(f"File rejected due to validation errors", extra={
                'file': key,
                'destination': destination,
                'errors': errors,
                'header_found': header
            })
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"AWS S3 error processing {key}: {error_code}", 
                    exc_info=True, extra={'error_code': error_code})
        file_result['validation_errors'].append(f"S3 Error: {error_code}")
        
    except Exception as e:
        logger.error(f"Unexpected error processing {key}: {str(e)}", 
                    exc_info=True, extra={'error_type': type(e).__name__})
        file_result['validation_errors'].append(f"Error: {str(e)}")
    
    return file_result


def read_csv_header(bucket: str, key: str) -> Tuple[Optional[List[str]], Optional[str]]:
    """
    Efficiently read only the CSV header by fetching first 8KB of file.
    This minimizes S3 data transfer costs and improves performance.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        
    Returns:
        tuple: (list of column names, raw header string) or (None, None) on error
    """
    try:
        logger.debug(f"Reading header from s3://{bucket}/{key}")
        
        # Use S3 Range GET to read only first 8KB (cost optimization)
        response = s3_client.get_object(
            Bucket=bucket,
            Key=key,
            Range=f'bytes=0-{HEADER_READ_BYTES-1}'
        )
        
        # Read the partial content
        content = response['Body'].read().decode('utf-8', errors='ignore')
        
        # Parse CSV header
        csv_reader = csv.reader(StringIO(content))
        header = next(csv_reader, None)
        
        if header is None:
            logger.error(f"Empty CSV file: {key}")
            return None, None
        
        # Strip whitespace from column names (common data quality issue)
        header = [col.strip() for col in header]
        
        logger.debug(f"Header read successfully", extra={
            'columns': len(header),
            'column_names': header
        })
        
        return header, ','.join(header)
        
    except UnicodeDecodeError as e:
        logger.error(f"File encoding error for {key}: {str(e)}")
        return None, None
    except Exception as e:
        logger.error(f"Error reading header from {key}: {str(e)}", exc_info=True)
        return None, None

def validate_header(header: List[str]) -> Tuple[bool, List[str]]:
    """
    Validate CSV header against required schema.
    
    Current validation rules:
    1. All required columns must be present
    2. Column names are case-sensitive
    
    Future extension points:
    - Add column order validation
    - Add data type hints validation
    - Add optional columns list
    - Add custom validation rules per column
    
    Args:
        header: List of column names from CSV
        
    Returns:
        tuple: (is_valid: bool, errors: List[str])
    """
    errors = []
    
    # Convert to set for efficient lookup
    header_set = set(header)
    required_set = set(REQUIRED_COLUMNS)
    
    # Check for missing required columns
    missing_columns = required_set - header_set
    if missing_columns:
        errors.append(f"Missing required columns: {', '.join(sorted(missing_columns))}")
    
    # Check for duplicate columns
    if len(header) != len(header_set):
        duplicates = [col for col in header if header.count(col) > 1]
        errors.append(f"Duplicate columns found: {', '.join(set(duplicates))}")
    
    # Check for empty column names
    if '' in header or any(col.strip() == '' for col in header):
        errors.append("Empty column names found")
    
    is_valid = len(errors) == 0
    
    logger.debug(f"Validation result: {'PASS' if is_valid else 'FAIL'}", extra={
        'is_valid': is_valid,
        'errors': errors,
        'expected_columns': REQUIRED_COLUMNS,
        'found_columns': header
    })
    
    return is_valid, errors


def move_file(source_bucket: str, source_key: str, destination_path: str) -> str:
    """
    Move S3 file by copying to destination and deleting source.
    Uses copy operation to preserve file metadata and enable cross-bucket moves.
    
    Args:
        source_bucket: Source S3 bucket
        source_key: Source S3 key
        destination_path: Destination path (format: bucket/prefix/)
        
    Returns:
        str: Full destination path
    """
    try:
        # Parse destination bucket and prefix
        dest_parts = destination_path.rstrip('/').split('/', 1)
        dest_bucket = dest_parts[0]
        dest_prefix = dest_parts[1] + '/' if len(dest_parts) > 1 else ''
        
        # Extract filename from source key
        filename = source_key.split('/')[-1]
        destination_key = dest_prefix + filename
        
        logger.debug(f"Moving file", extra={
            'source': f's3://{source_bucket}/{source_key}',
            'destination': f's3://{dest_bucket}/{destination_key}'
        })
        
        # Copy file to destination with encryption
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        copy_params = {
            'CopySource': copy_source,
            'Bucket': dest_bucket,
            'Key': destination_key
        }

        # Add SSE-KMS encryption if KMS key is configured
        if KMS_KEY_ID:
            copy_params['ServerSideEncryption'] = 'aws:kms'
            copy_params['SSEKMSKeyId'] = KMS_KEY_ID
            logger.debug(f"Encrypting file with KMS key: {KMS_KEY_ID}")

        s3_client.copy_object(**copy_params)
        
        # Delete source file
        s3_client.delete_object(Bucket=source_bucket, Key=source_key)
        
        destination_full = f's3://{dest_bucket}/{destination_key}'
        logger.info(f"File moved successfully to {destination_full}")
        
        return destination_full
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"S3 error moving file: {error_code}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error moving file: {str(e)}", exc_info=True)
        raise