import os
import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# S3 Configuration
S3_RAW_BUCKET = os.getenv("S3_RAW_BUCKET")
S3_SILVER_BUCKET = os.getenv("S3_SILVER_BUCKET")

# S3 Paths (no trailing slashes - they're added in delta_writer.py)
S3_TRANSFORMED_PREFIX = os.getenv(
    "S3_TRANSFORMED_PREFIX",
    "customer-transactions/transformed"
)
S3_FACT_PREFIX = os.getenv(
    "S3_FACT_PREFIX",
    "customer-transactions/FactTransactions"
)
S3_ERROR_PREFIX = os.getenv(
    "S3_ERROR_PREFIX",
    "customer-transactions/error"
)

# Masking Configuration
TOKENIZATION_SALT = os.getenv("TOKENIZATION_SALT")

# KMS Configuration for encryption
KMS_KEY_ID = os.getenv("KMS_KEY_ID")

# Required columns in raw transaction data
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

# Delta Lake Configuration
DELTA_WRITE_MODE = os.getenv("DELTA_WRITE_MODE", "append")
DELTA_PARTITION_COLUMNS = ["transaction_date"]

# Fact Table Partitioning - by year, month, day for better query performance
FACT_PARTITION_COLUMNS = ["transaction_year", "transaction_month", "transaction_day"]


def validate_config():
    """
    Validates that all required configuration is present.
    Raises ValueError if any required config is missing.
    """
    missing_configs = []

    if not S3_RAW_BUCKET:
        missing_configs.append("S3_RAW_BUCKET")
    if not S3_SILVER_BUCKET:
        missing_configs.append("S3_SILVER_BUCKET")
    if not TOKENIZATION_SALT:
        missing_configs.append("TOKENIZATION_SALT")

    if missing_configs:
        error_msg = f"Missing required environment variables: {', '.join(missing_configs)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info("Configuration validation successful")
    logger.info(f"Raw bucket: {S3_RAW_BUCKET}")
    logger.info(f"Silver bucket: {S3_SILVER_BUCKET}")
    logger.info(f"Transformed prefix: {S3_TRANSFORMED_PREFIX}")
    logger.info(f"Fact prefix: {S3_FACT_PREFIX}")
    logger.info(f"Error prefix: {S3_ERROR_PREFIX}")
