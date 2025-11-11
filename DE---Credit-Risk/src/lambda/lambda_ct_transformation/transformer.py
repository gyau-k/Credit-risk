import logging
import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def extract_date_parts(timestamp_dt):
    """
    Extracts date and time parts from a datetime object.

    Args:
        timestamp_dt: Datetime object

    Returns:
        Dictionary with extracted date/time fields
    """
    return {
        "transaction_date": timestamp_dt.date().isoformat(),
        "transaction_hour": timestamp_dt.hour,
        "transaction_year": timestamp_dt.year,
        "transaction_month": timestamp_dt.month,
        "transaction_day": timestamp_dt.day,
    }


def transform_transaction(transaction):
    """
    Transforms a validated transaction by adding derived fields.

    Expected input: Transaction that has passed validation and cleaning.
    The transaction should have 'timestamp_parsed' from validator.

    Args:
        transaction: Validated and cleaned transaction dictionary

    Returns:
        Transformed transaction with additional derived fields
    """
    transformed = transaction.copy()

    # Extract date/time parts
    if "timestamp_parsed" in transformed:
        date_parts = extract_date_parts(transformed["timestamp_parsed"])
        transformed.update(date_parts)

        # Remove the temporary parsed datetime object (not JSON serializable)
        del transformed["timestamp_parsed"]

    logger.debug(
        f"Transaction {transformed.get('transaction_id')} transformed successfully"
    )

    return transformed


def transform_transactions_batch(transactions):
    """
    Transforms a batch of validated transactions.

    Args:
        transactions: List of validated transaction dictionaries

    Returns:
        List of transformed transactions
    """
    transformed_transactions = []

    for transaction in transactions:
        try:
            transformed = transform_transaction(transaction)
            transformed_transactions.append(transformed)
        except Exception as e:
            # Log error but continue processing
            logger.error(
                f"Error transforming transaction {transaction.get('transaction_id')}: {str(e)}"
            )
            # Include original transaction with error flag
            error_transaction = transaction.copy()
            error_transaction["_transformation_error"] = str(e)
            transformed_transactions.append(error_transaction)

    logger.info(f"Transformed {len(transformed_transactions)} transactions")

    return transformed_transactions


def prepare_fact_table(transformed_transactions):
    """
    Prepares fact table records from transformed transactions.

    Fact table includes essential columns (REQUIRED_COLUMNS) plus partition columns
    (year, month, day for efficient time-based queries).
    Transformed table retains all derived/enriched columns.

    Args:
        transformed_transactions: List of transformed transaction dictionaries

    Returns:
        List of fact table records with only essential columns
    """
    fact_records = []

    # Columns for fact table: required columns + partition columns (year, month, day)
    fact_columns = set(config.REQUIRED_COLUMNS + config.FACT_PARTITION_COLUMNS)

    for transaction in transformed_transactions:
        # Create fact record with only essential columns
        fact_record = {
            col: transaction[col]
            for col in fact_columns
            if col in transaction
        }

        fact_records.append(fact_record)

    logger.info(
        f"Prepared {len(fact_records)} fact table records "
        f"with columns: {sorted(fact_columns)}"
    )

    return fact_records
