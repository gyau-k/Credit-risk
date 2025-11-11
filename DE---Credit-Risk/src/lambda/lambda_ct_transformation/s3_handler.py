# S3 handler helper funciton for reading raw transaction data and writing errors.
import json
import logging
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client('s3')


def read_transactions_from_s3(bucket,key):
    """
    Reads transaction data from S3 JSON file.

    Args:
        bucket: S3 bucket name
        key: S3 object key

    Returns:
        List of transaction dictionaries, or None if read fails
    """
    try:
        logger.info(f"Reading transactions from s3://{bucket}/{key}")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')

        # Parse JSON - expecting a list of transaction dictionaries
        transactions = json.loads(content)

        # Validate that we received a list
        if not isinstance(transactions, list):
            logger.error(f"Expected list of transactions, got {type(transactions).__name__}")
            return None

        logger.info(f"Successfully read {len(transactions)} transactions from S3")
        return transactions

    except ClientError as e:
        logger.error(f"S3 ClientError reading {bucket}/{key}: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for {bucket}/{key}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error reading {bucket}/{key}: {str(e)}")
        return None


def write_errors_to_s3(errors, source_file):
    """
    Writes error records to S3 error path.
    Errors are NOT encrypted to allow easier debugging.

    Args:
        errors: List of error transaction dictionaries
        source_file: Original source file key for tracking

    Returns:
        True if successful, False otherwise
    """
    if not errors or len(errors) == 0:
        logger.info("No errors to write")
        return True

    try:
        now = datetime.utcnow()
        timestamp = now.strftime('%Y%m%d_%H%M%S')

        # Create error file key with date partitioning
        error_key = (
            f"{config.S3_ERROR_PREFIX}"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"errors_{timestamp}.json"
        )

        # Add metadata about source file
        error_data = {
            "source_file": source_file,
            "error_count": len(errors),
            "processed_at": now.isoformat(),
            "errors": errors
        }

        # Convert to JSON
        error_json = json.dumps(error_data, indent=2, default=str)

        # Write to S3 without encryption (for easier debugging)
        s3_client.put_object(
            Bucket=config.S3_SILVER_BUCKET,
            Key=error_key,
            Body=error_json.encode('utf-8'),
            ContentType='application/json',
            Metadata={
                'source_file': source_file,
                'error_count': str(len(errors)),
                'processed_at': now.isoformat()
            }
        )

        logger.info(
            f"Successfully wrote {len(errors)} errors to "
            f"s3://{config.S3_SILVER_BUCKET}/{error_key}"
        )
        return True

    except Exception as e:
        logger.error(f"Error writing errors to S3: {str(e)}")
        return False