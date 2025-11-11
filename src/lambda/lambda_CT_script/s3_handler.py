import json
import logging
from datetime import datetime
import boto3
import config

logger = logging.getLogger()

# Initialize AWS S3 client
s3_client = boto3.client('s3')


def store_transactions_to_s3(valid_transactions, invalid_transactions):
    """
    Stores valid and invalid transactions to separate S3 locations.
    Uses date-based partitioning.
    Valid transactions are encrypted with KMS if KMS_KEY_ID is configured.
    """
    results = {}
    now = datetime.utcnow()
    timestamp = now.strftime('%Y%m%d_%H%M%S')

    # Store valid transactions with KMS encryption
    if len(valid_transactions) > 0:
        try:
            valid_s3_key = (
                f"{config.S3_VALID_PREFIX}"
                f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
                f"transactions_{timestamp}.json"
            )

            logger.info(f"Storing {len(valid_transactions)} valid transactions to S3")

            valid_data = json.dumps(valid_transactions, indent=2, default=str)

            # Prepare S3 put_object parameters
            put_params = {
                'Bucket': config.S3_BUCKET,
                'Key': valid_s3_key,
                'Body': valid_data.encode('utf-8'),
                'ContentType': 'application/json',
                'Metadata': {
                    'record_count': str(len(valid_transactions)),
                    'processed_at': now.isoformat(),
                    'data_type': 'valid_transactions'
                }
            }

            # Add SSE-KMS encryption if KMS key is configured
            if config.KMS_KEY_ID:
                put_params['ServerSideEncryption'] = 'aws:kms'
                put_params['SSEKMSKeyId'] = config.KMS_KEY_ID
                logger.info(f"Encrypting valid transactions with KMS key: {config.KMS_KEY_ID}")

            s3_client.put_object(**put_params)

            logger.info(f"Valid transactions stored at: s3://{config.S3_BUCKET}/{valid_s3_key}")
            results['valid_s3_key'] = valid_s3_key

        except Exception as e:
            logger.error(f"Failed to store valid transactions to S3: {str(e)}", exc_info=True)
            raise

    # Store invalid transactions
    if len(invalid_transactions) > 0:
        try:
            invalid_s3_key = (
                f"{config.S3_ERROR_PREFIX}"
                f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
                f"invalid_transactions_{timestamp}.json"
            )

            logger.info(f"Storing {len(invalid_transactions)} invalid transactions to S3")

            invalid_data = json.dumps(invalid_transactions, indent=2, default=str)

            s3_client.put_object(
                Bucket=config.S3_BUCKET,
                Key=invalid_s3_key,
                Body=invalid_data.encode('utf-8'),
                ContentType='application/json',
                Metadata={
                    'record_count': str(len(invalid_transactions)),
                    'processed_at': now.isoformat(),
                    'data_type': 'invalid_transactions'
                }
            )

            logger.info(f"Invalid transactions stored at: s3://{config.S3_BUCKET}/{invalid_s3_key}")
            results['invalid_s3_key'] = invalid_s3_key

        except Exception as e:
            logger.error(f"Failed to store invalid transactions to S3: {str(e)}", exc_info=True)
            raise

    return results
