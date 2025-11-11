import logging
from typing import List
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from datetime import datetime

import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# S3 client
s3_client = boto3.client('s3')


def write_to_parquet_table(data,s3_path,partition_by = None):
    """
    Writes data to a Parquet table on S3 with partitioning support.

    Args:
        data: List of dictionaries to write
        s3_path: S3 path for the Parquet table (e.g., s3://bucket/path/)
        partition_by: List of column names to partition by

    Returns:
        True if successful, False otherwise
    """
    if not data or len(data) == 0:
        logger.warning("No data to write")
        return True

    try:
        logger.info(f"Writing {len(data)} records to Parquet table at {s3_path}")

        # Convert list of dicts to pandas DataFrame
        df = pd.DataFrame(data)

        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame columns: {list(df.columns)}")

        # Parse S3 path
        s3_path_clean = s3_path.replace("s3://", "").replace("s3a://", "")
        bucket_name = s3_path_clean.split("/")[0]
        prefix = "/".join(s3_path_clean.split("/")[1:])

        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df)

        # Write to S3 with partitioning
        if partition_by:
            # Generate partition path
            partition_values = {}
            for col in partition_by:
                if col in df.columns:
                    # Use first value for partition (assumes all records have same partition values)
                    partition_values[col] = str(df[col].iloc[0])

            # Create partition path
            partition_path = "/".join([f"{k}={v}" for k, v in partition_values.items()])
            full_prefix = f"{prefix}/{partition_path}" if prefix else partition_path

            logger.info(f"Writing with partitioning: {partition_path}")
        else:
            full_prefix = prefix

        # Generate filename with timestamp to avoid overwrites
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"data_{timestamp}.parquet"
        s3_key = f"{full_prefix}/{filename}"

        # Write Parquet to S3
        output_buffer = pa.BufferOutputStream()
        pq.write_table(
            table,
            output_buffer,
            compression='snappy',  # Good balance of speed and compression
            use_dictionary=True,   # Better compression for repeated values
            write_statistics=True  # Enable column statistics for query optimization
        )

        # Prepare S3 put_object parameters
        put_params = {
            'Bucket': bucket_name,
            'Key': s3_key,
            'Body': output_buffer.getvalue().to_pybytes()
        }

        # Add SSE-KMS encryption if KMS key is configured
        if config.KMS_KEY_ID:
            put_params['ServerSideEncryption'] = 'aws:kms'
            put_params['SSEKMSKeyId'] = config.KMS_KEY_ID
            logger.info(f"Encrypting transformed data with KMS key: {config.KMS_KEY_ID}")

        # Upload to S3
        s3_client.put_object(**put_params)

        logger.info(f"Successfully wrote {len(data)} records to s3://{bucket_name}/{s3_key}")
        return True

    except Exception as e:
        logger.error(f"Error writing to Parquet table at {s3_path}: {str(e)}", exc_info=True)
        return False


def write_transformed_data(transformed_transactions):
    """
    Writes transformed transaction data to the silver layer.

    Args:
        transformed_transactions: List of transformed transaction dictionaries

    Returns:
        True if successful, False otherwise
    """
    s3_path = f"s3://{config.S3_SILVER_BUCKET}/{config.S3_TRANSFORMED_PREFIX}"

    return write_to_parquet_table(
        data=transformed_transactions,
        s3_path=s3_path,
        partition_by=config.DELTA_PARTITION_COLUMNS  # Reusing partition config
    )


def write_fact_data(fact_records):
    """
    Writes fact table records to the silver layer.
    Partitioned by year, month, and day for better query performance.

    Args:
        fact_records: List of fact table record dictionaries

    Returns:
        True if successful, False otherwise
    """
    s3_path = f"s3://{config.S3_SILVER_BUCKET}/{config.S3_FACT_PREFIX}"

    return write_to_parquet_table(
        data=fact_records,
        s3_path=s3_path,
        partition_by=config.FACT_PARTITION_COLUMNS  # Partition by year, month, day
    )