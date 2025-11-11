import os
import logging

logger = logging.getLogger()

# API Configuration
API_URL = os.getenv("API_ENDPOINT")
API_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET")
S3_VALID_PREFIX = os.getenv("S3_VALID_PREFIX", "customer-transactions/raw/valid/")
S3_ERROR_PREFIX = os.getenv("S3_ERROR_PREFIX", "customer-transactions/raw/error/")

# AWS Region Configuration
AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")

# KMS Configuration for encryption
KMS_KEY_ID = os.getenv("KMS_KEY_ID")

# Data Validation  Required columns for transactions
REQUIRED_COLUMNS = [
    "transaction_id",
    "customer_id",
    "account_number",
    "amount",
    "type",
    "timestamp",
    "merchant",
    "location"
]


# Batch size for processing transactions 
MAX_TRANSACTIONS_PER_POLL = int(os.getenv("MAX_TRANSACTIONS_PER_POLL", "1000"))


def validate_config():
    """
    Validates that all required configuration is present.
    Raises ValueError if any required config is missing.
    """
    required_vars = {
        "API_URL": API_URL,
        "S3_BUCKET": S3_BUCKET
    }

    missing = [key for key, value in required_vars.items() if not value]

    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    logger.info("Configuration validated successfully")
    logger.info(f"  API URL: {API_URL}")
    logger.info(f"  S3 Bucket: {S3_BUCKET}")
    logger.info(f"  Valid Prefix: {S3_VALID_PREFIX}")
    logger.info(f"  Error Prefix: {S3_ERROR_PREFIX}")
