# Data masking module for sensitive PII fields.
# Provides tokenization functionality for account numbers.
import hashlib
import logging
import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def tokenize_account_number(account_number):
    """
    Tokenizes an account number using SHA-256 hashing with salt.

    Args:
        account_number: The raw account number to tokenize

    Returns:
        A SHA-256 hash of the account number with salt (hex string)

    Raises:
        ValueError: If account_number is None or empty
    """
    if not account_number:
        raise ValueError("Account number cannot be None or empty")

    # Convert to string and strip whitespace
    account_str = str(account_number).strip()

    if not account_str:
        raise ValueError("Account number cannot be empty after stripping whitespace")

    # Combine account number with salt
    salted_value = f"{account_str}{config.TOKENIZATION_SALT}"

    # Generate SHA-256 hash
    hash_object = hashlib.sha256(salted_value.encode('utf-8'))
    token = hash_object.hexdigest()

    logger.debug(f"Account number tokenized successfully")

    return token


def mask_transaction(transaction):
    """
    Masks sensitive fields in a transaction record.
    Creates a copy of the transaction with masked account_number.

    Args:
        transaction: Original transaction dictionary

    Returns:
        New transaction dictionary with masked account_number

    Raises:
        ValueError: If account_number field is missing or invalid
    """
    if "account_number" not in transaction:
        raise ValueError("Transaction missing required field: account_number")

    # Create a copy to avoid modifying original
    masked_transaction = transaction.copy()

    # Store original account number for potential audit trail
    original_account = masked_transaction["account_number"]

    # Tokenize account number
    masked_transaction["account_number_masked"] = tokenize_account_number(original_account)

    # Remove original account number for PII compliance
    masked_transaction["account_number_original"] = masked_transaction.pop("account_number")

    # Rename masked field to account_number
    masked_transaction["account_number"] = masked_transaction.pop("account_number_masked")

    logger.debug(f"Transaction {transaction.get('transaction_id', 'unknown')} masked successfully")

    return masked_transaction


def mask_transactions_batch(transactions):
    """
    Masks a batch of transactions.

    Args:
        transactions: List of transaction dictionaries

    Returns:
        Tuple of (successfully_masked_transactions, failed_transactions)
        Failed transactions include error metadata.
    """
    masked_transactions = []
    failed_transactions = []

    for idx, transaction in enumerate(transactions):
        try:
            masked = mask_transaction(transaction)
            masked_transactions.append(masked)
        except Exception as e:
            logger.error(f"Failed to mask transaction at index {idx}: {str(e)}")

            # Add error metadata to failed transaction
            failed_transaction = transaction.copy()
            failed_transaction["_masking_error"] = {
                "error": str(e),
                "error_type": type(e).__name__,
                "stage": "masking"
            }
            failed_transactions.append(failed_transaction)

    logger.info(
        f"Masking complete: {len(masked_transactions)} successful, "
        f"{len(failed_transactions)} failed"
    )

    return masked_transactions, failed_transactions
