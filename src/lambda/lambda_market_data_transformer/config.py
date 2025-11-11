import os
import logging

logger = logging.getLogger()

# S3 Configuration
RAW_BUCKET = os.getenv('RAW_BUCKET', 'creditrisk-raw01')
SILVER_BUCKET = os.getenv('SILVER_BUCKET', 'creditrisk-silver')

# S3 Prefixes
RAW_PREFIX = os.getenv('RAW_PREFIX', 'market_data/raw/')
CLEANSED_PREFIX = os.getenv('CLEANSED_PREFIX', 'market_data/cleansed/')
REJECTED_PREFIX = os.getenv('REJECTED_PREFIX', 'market_data/rejected/')
PROCESSED_PREFIX = os.getenv('PROCESSED_PREFIX', 'market_data/processed/')
DIM_MARKET_PREFIX = os.getenv('DIM_MARKET_PREFIX', 'dim_market/')

# Processing Configuration
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Data Validation
REQUIRED_FIELDS = ['date', 'index_name', 'value', 'volatility']

def validate_config():
    """
    Validate that all required configuration is present.
    """
    required_vars = {
        'RAW_BUCKET': RAW_BUCKET,
        'SILVER_BUCKET': SILVER_BUCKET
    }
    
    missing = [key for key, value in required_vars.items() if not value]
    
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    logger.info("Configuration validated successfully")
    logger.info(f"  Raw Bucket: {RAW_BUCKET}")
    logger.info(f"  Silver Bucket: {SILVER_BUCKET}")
    logger.info(f"  Raw Prefix: {RAW_PREFIX}")
    logger.info(f"  DimMarket Prefix: {DIM_MARKET_PREFIX}")