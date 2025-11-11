import json
import logging
from datetime import datetime

# Import helper modules
import config
from api_client import fetch_transactions
from validator import validate_transactions_batch
from s3_handler import store_transactions_to_s3

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Main Lambda handler function.
    Polls API for transactions, validates them, and stores in S3.
    """
    try:
        logger.info("=" * 80)
        logger.info("Starting API polling Lambda execution")
        logger.info(f"Lambda Request ID: {context.aws_request_id}")
        logger.info("=" * 80)

        # Validate configuration
        config.validate_config()

        # Fetch transactions from API
        transactions = fetch_transactions()

        if transactions is None:
            logger.error("Failed to fetch transactions from API")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Failed to fetch transactions from API',
                    'timestamp': datetime.utcnow().isoformat()
                })
            }

        if len(transactions) == 0:
            logger.info("No transactions received from API")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No transactions to process',
                    'timestamp': datetime.utcnow().isoformat()
                })
            }

        # Log transaction count
        total_transactions = len(transactions)
        logger.info(f"Received {total_transactions} transactions from API")

        # Process transactions in batches if they exceed the limit
        batch_size = config.MAX_TRANSACTIONS_PER_POLL
        all_valid_transactions = []
        all_invalid_transactions = []

        if total_transactions > batch_size:
            logger.info(
                f"Transaction count ({total_transactions}) exceeds batch size "
                f"({batch_size}). Processing in batches..."
            )

            num_batches = (total_transactions + batch_size - 1) // batch_size
            logger.info(f"Will process {num_batches} batches")

            for i in range(0, total_transactions, batch_size):
                batch_num = (i // batch_size) + 1
                batch = transactions[i:i + batch_size]
                logger.info(f"Processing batch {batch_num}/{num_batches} ({len(batch)} transactions)")

                # Validate batch
                valid_batch, invalid_batch = validate_transactions_batch(batch)
                all_valid_transactions.extend(valid_batch)
                all_invalid_transactions.extend(invalid_batch)

                logger.info(f"Batch {batch_num}: {len(valid_batch)} valid, {len(invalid_batch)} invalid")
        else:
            logger.info("Processing all transactions in single batch")
            all_valid_transactions, all_invalid_transactions = validate_transactions_batch(transactions)

        # Store all results in S3
        results = store_transactions_to_s3(all_valid_transactions, all_invalid_transactions)

        # Log execution summary
        logger.info("=" * 80)
        logger.info("Execution Summary:")
        logger.info(f"  Total transactions fetched: {total_transactions}")
        logger.info(f"  Valid transactions: {len(all_valid_transactions)}")
        logger.info(f"  Invalid transactions: {len(all_invalid_transactions)}")
        logger.info(f"  Valid data S3 location: {results.get('valid_s3_key', 'N/A')}")
        logger.info(f"  Invalid data S3 location: {results.get('invalid_s3_key', 'N/A')}")
        logger.info("=" * 80)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'API polling completed successfully',
                'timestamp': datetime.utcnow().isoformat(),
                'total_transactions': total_transactions,
                'valid_transactions': len(all_valid_transactions),
                'invalid_transactions': len(all_invalid_transactions),
                'valid_s3_location': (
                    f"s3://{config.S3_BUCKET}/{results.get('valid_s3_key', '')}"
                    if results.get('valid_s3_key') else None
                ),
                'invalid_s3_location': (
                    f"s3://{config.S3_BUCKET}/{results.get('invalid_s3_key', '')}"
                    if results.get('invalid_s3_key') else None
                )
            })
        }

    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Configuration error',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })
        }

    except Exception as e:
        logger.error(f"Unexpected error in lambda_handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Internal server error',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })
        }
