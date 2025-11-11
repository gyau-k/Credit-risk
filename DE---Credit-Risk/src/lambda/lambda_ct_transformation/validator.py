# Data validation module for customer transaction data.
# Validates schema, data types, and business rules.

import logging
from datetime import datetime
from decimal import InvalidOperation

import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def validate_required_fields(transaction):
    """
    Validates that all required fields are present in the transaction.

    Args:
        transaction: Transaction dictionary to validate

    Returns:
        Tuple of (is_valid, list_of_missing_fields)
    """
    missing_fields = []

    for field in config.REQUIRED_COLUMNS:
        if field not in transaction:
            missing_fields.append(field)
        elif transaction[field] is None:
            missing_fields.append(f"{field} (null)")
        elif isinstance(transaction[field], str) and not transaction[field].strip():
            missing_fields.append(f"{field} (empty)")

    is_valid = len(missing_fields) == 0
    return is_valid, missing_fields


def validate_amount(amount):
    """
    Validates that amount is numeric and greater than 0.

    Args:
        amount: The amount value to validate

    Returns:
        Tuple of (is_valid, error_message, parsed_amount)
    """
    try:
        #convert to float
        amount_float = float(amount)

        #Check if positive
        if amount_float <= 0:
            return False, f"Amount must be greater than 0, got {amount_float}", 0.0

        return True, "", amount_float

    except (ValueError, TypeError, InvalidOperation) as e:
        return False, f"Invalid amount format: {amount} - {str(e)}", 0.0


def validate_timestamp(timestamp):
    """
    Validates that timestamp is a valid ISO datetime string.

    Args:
        timestamp: The timestamp value to validate

    Returns:
        Tuple of (is_valid, error_message, parsed_datetime)
    """
    try:
        # Try to parse as ISO format datetime
        if isinstance(timestamp, str):
            # Support multiple datetime formats
            for fmt in [
                "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO with microseconds and Z
                "%Y-%m-%dT%H:%M:%S.%f",   # ISO with microseconds (no Z)
                "%Y-%m-%dT%H:%M:%SZ",     # ISO without microseconds and Z
                "%Y-%m-%dT%H:%M:%S",      # ISO without microseconds (no Z)
                "%Y-%m-%d %H:%M:%S",      # Space-separated
            ]:
                try:
                    dt = datetime.strptime(timestamp, fmt)
                    return True, "", dt
                except ValueError:
                    continue

            # If none of the formats worked, return error
            return False, f"Invalid timestamp format: {timestamp}", None

        else:
            return False, f"Timestamp must be a string, got {type(timestamp)}", None

    except Exception as e:
        return False, f"Error parsing timestamp: {str(e)}", None



def validate_transaction(transaction):
    """
    Performs comprehensive validation on a single transaction.

    Args:
        transaction: Transaction dictionary to validate

    Returns:
        Tuple of (is_valid, validation_errors, cleaned_transaction)
        cleaned_transaction contains parsed/cleaned values if valid
    """
    validation_errors = {}
    cleaned_transaction = transaction.copy()

    #Check required fields
    fields_valid, missing_fields = validate_required_fields(transaction)
    if not fields_valid:
        validation_errors["missing_fields"] = missing_fields

    # If required fields are missing, can't continue with other validations
    if not fields_valid:
        return False, validation_errors, cleaned_transaction

    #Validate amount
    amount_valid, amount_error, amount_value = validate_amount(transaction.get("amount"))
    if not amount_valid:
        validation_errors["amount"] = amount_error
    else:
        cleaned_transaction["amount"] = amount_value

    #Validate timestamp
    timestamp_valid, timestamp_error, timestamp_value = validate_timestamp(
        transaction.get("timestamp")
    )
    if not timestamp_valid:
        validation_errors["timestamp"] = timestamp_error
    else:
        cleaned_transaction["timestamp_parsed"] = timestamp_value


    #  Trim string fields
    for field in ["transaction_id", "customer_id", "merchant", "location"]:
        if field in cleaned_transaction and isinstance(cleaned_transaction[field], str):
            cleaned_transaction[field] = cleaned_transaction[field].strip()

    # Overall validation result
    is_valid = len(validation_errors) == 0

    return is_valid, validation_errors, cleaned_transaction


def validate_transactions_batch(transactions):
    """
    Validates a batch of transactions.

    Args:
        transactions: List of transaction dictionaries

    Returns:
        Tuple of (valid_transactions, invalid_transactions)
        Invalid transactions include error metadata.
    """
    valid_transactions = []
    invalid_transactions = []

    for idx, transaction in enumerate(transactions):
        is_valid, errors, cleaned = validate_transaction(transaction)

        if is_valid:
            valid_transactions.append(cleaned)
            logger.debug(f"Transaction {transaction.get('transaction_id')} validated successfully")
        else:
            # Add error metadata to invalid transaction
            invalid_transaction = transaction.copy()
            invalid_transaction["_validation_error"] = {
                "errors": errors,
                "stage": "validation",
                "validated_at": datetime.utcnow().isoformat()
            }
            invalid_transactions.append(invalid_transaction)
            logger.warning(
                f"Transaction {transaction.get('transaction_id', f'index_{idx}')} "
                f"failed validation: {errors}"
            )

    logger.info(
        f"Validation complete: {len(valid_transactions)} valid, "
        f"{len(invalid_transactions)} invalid"
    )

    return valid_transactions, invalid_transactions
