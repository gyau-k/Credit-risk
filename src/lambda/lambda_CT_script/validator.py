import logging
from datetime import datetime
import config

logger = logging.getLogger()


def validate_transaction(transaction):
    """
    Validates that a transaction contains all required columns.
    Extra columns are ignored and not considered validation failures.
    """
    missing_columns = []

    for column in config.REQUIRED_COLUMNS:
        if column not in transaction:
            missing_columns.append(column)

    is_valid = len(missing_columns) == 0

    if not is_valid:
        logger.debug(f"Transaction validation failed. Missing columns: {missing_columns}")

    return is_valid, missing_columns


def validate_transactions_batch(transactions):
    """
    Validates a batch of transactions.
    Separates valid and invalid transactions.
    """
    logger.info(f"Starting validation of {len(transactions)} transactions")

    valid_transactions = []
    invalid_transactions = []

    for idx, transaction in enumerate(transactions):
        is_valid, missing_columns = validate_transaction(transaction)

        if is_valid:
            valid_transactions.append(transaction)
        else:
            # Add error metadata to invalid transaction
            transaction['_validation_error'] = {
                'reason': 'missing_columns',
                'missing_columns': missing_columns,
                'validated_at': datetime.utcnow().isoformat()
            }
            invalid_transactions.append(transaction)

            # Log first few invalid transactions for debugging
            if len(invalid_transactions) <= 5:
                logger.warning(f"Invalid transaction {idx}: Missing columns {missing_columns}")

    logger.info(f"Validation complete: {len(valid_transactions)} valid, {len(invalid_transactions)} invalid")

    if len(invalid_transactions) > 0:
        logger.warning(f"Validation failure rate: {(len(invalid_transactions)/len(transactions)*100):.2f}%")

    return valid_transactions, invalid_transactions
