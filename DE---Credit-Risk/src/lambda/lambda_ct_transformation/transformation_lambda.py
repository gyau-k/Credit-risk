"""
Main Lambda handler for Customer Transaction Transformation.

This Lambda function is triggered by S3 events when new transaction files
are uploaded to the raw/valid path. It performs the following operations:

1. Reads raw transaction data from S3
2. Validates data types and business rules
3. Masks sensitive PII (account numbers)
4. Transforms data
5. Writes to Delta Lake tables in silver layer:
   - Transformed data
   - Fact table
6. Handles errors by writing to error path
"""

import json
import logging
from datetime import datetime
from urllib.parse import unquote_plus
import config
import s3_handler
import validator
import masker
import transformer
import delta_writer

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def process_transaction_file(bucket, key):
    """
    Processes a single transaction file from raw to silver layer.

    Args:
        bucket: S3 bucket name containing the file
        key: S3 object key for the file

    Returns:
        Dictionary with processing results and statistics
    """
    result = {
        "source_bucket": bucket,
        "source_key": key,
        "status": "failed",
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "masked_records": 0,
        "transformed_records": 0,
        "fact_records": 0,
        "errors": []
    }

    try:
        #  Read transactions from S3
        logger.info(f"Reading transactions from s3://{bucket}/{key}")
        transactions = s3_handler.read_transactions_from_s3(bucket, key)

        if transactions is None:
            result["errors"].append("Failed to read transactions from S3")
            return result

        result["total_records"] = len(transactions)
        logger.info(f"Read {len(transactions)} transactions from S3")

        # Validate transactions
        logger.info(f"Step 2: Validating {len(transactions)} transactions")
        valid_transactions, invalid_transactions = validator.validate_transactions_batch(transactions)

        result["valid_records"] = len(valid_transactions)
        result["invalid_records"] = len(invalid_transactions)

        logger.info(
            f"Validation complete: {len(valid_transactions)} valid, "
            f"{len(invalid_transactions)} invalid"
        )

        # Mask sensitive data (account numbers)
        all_errors = []

        if len(valid_transactions) > 0:
            logger.info(f"Step 3: Masking {len(valid_transactions)} valid transactions")
            masked_transactions, masking_errors = masker.mask_transactions_batch(valid_transactions)

            result["masked_records"] = len(masked_transactions)
            logger.info(
                f"Masking complete: {len(masked_transactions)} successful, "
                f"{len(masking_errors)} failed"
            )

            # Add masking errors to all errors
            all_errors.extend(masking_errors)

            # Transform data (add derived fields)
            if len(masked_transactions) > 0:
                logger.info(f"Transforming {len(masked_transactions)} masked transactions")
                transformed_transactions = transformer.transform_transactions_batch(masked_transactions)

                result["transformed_records"] = len(transformed_transactions)
                logger.info(f"Transformation complete: {len(transformed_transactions)} records")

                #Prepare fact table records
                logger.info(f"Preparing fact table from {len(transformed_transactions)} records")
                fact_records = transformer.prepare_fact_table(transformed_transactions)

                result["fact_records"] = len(fact_records)
                logger.info(f"Fact table preparation complete: {len(fact_records)} records")

                # Write transformed data to Delta Lake
                logger.info(f"Writing {len(transformed_transactions)} records to transformed Delta table")
                transformed_success = delta_writer.write_transformed_data(transformed_transactions)

                if not transformed_success:
                    result["errors"].append("Failed to write transformed data to Delta Lake")
                else:
                    logger.info("Successfully wrote transformed data to Delta Lake")

                # Step 7: Write fact data to Delta Lake
                logger.info(f"Writing {len(fact_records)} records to fact Delta table")
                fact_success = delta_writer.write_fact_data(fact_records)

                if not fact_success:
                    result["errors"].append("Failed to write fact data to Delta Lake")
                else:
                    logger.info("Successfully wrote fact data to Delta Lake")

                # Overall success if both writes succeeded
                if transformed_success and fact_success:
                    result["status"] = "success"

            else:
                result["errors"].append("No masked transactions to transform")
        else:
            result["errors"].append("No valid transactions to process")

        #Handle errors - write all errors to S3
        all_errors.extend(invalid_transactions)

        if len(all_errors) > 0:
            logger.info(f"Step 8: Writing {len(all_errors)} error records to S3")
            error_success = s3_handler.write_errors_to_s3(all_errors, key)

            if not error_success:
                result["errors"].append("Failed to write errors to S3")
            else:
                logger.info(f"Successfully wrote {len(all_errors)} errors to S3")

    except Exception as e:
        logger.error(f"Unexpected error processing file {key}: {str(e)}", exc_info=True)
        result["errors"].append(f"Unexpected error: {str(e)}")
        result["status"] = "failed"

    return result


def lambda_handler(event, context):
    """
    Main Lambda handler function.

    Triggered by S3 events when files are uploaded to raw/valid path.

    Args:
        event: S3 event notification
        context: Lambda context object

    Returns:
        Response dictionary with status and results
    """
    try:
        logger.info("Starting Customer Transaction Transformation Lambda")
        logger.info(f"Event: {json.dumps(event)}")

        # Validate configuration
        config.validate_config()

        # Process each S3 record in the event
        results = []
        total_success = 0
        total_failed = 0

        for record in event.get('Records', []):
            # Extract S3 bucket and key from event
            s3_info = record.get('s3', {})
            bucket = s3_info.get('bucket', {}).get('name')
            key = s3_info.get('object', {}).get('key')

            if not bucket or not key:
                logger.warning(f"Invalid S3 record: {record}")
                continue

            # URL decode the key (S3 events encode special characters like =)
            key = unquote_plus(key)

            logger.info(f"Processing file: s3://{bucket}/{key}")

            # Process the file
            result = process_transaction_file(bucket, key)
            results.append(result)

            if result["status"] == "success":
                total_success += 1
            else:
                total_failed += 1

            logger.info(f"Processing result for {key}: {result}")

        # Build response
        response = {
            "statusCode": 200 if total_failed == 0 else 207,  #  Multi-Status
            "body": json.dumps({
                "message": "Customer transaction transformation complete",
                "total_files": len(results),
                "successful_files": total_success,
                "failed_files": total_failed,
                "results": results,
                "processed_at": datetime.utcnow().isoformat()
            }, default=str)
        }

        logger.info(f"Lambda execution complete: {total_success} successful, {total_failed} failed")

        return response

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}", exc_info=True)

        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e),
                "message": "Lambda execution failed"
            })
        }
