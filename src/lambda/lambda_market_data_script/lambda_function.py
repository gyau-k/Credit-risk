import json
import boto3
import requests
import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Any, Tuple
import traceback

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.getenv('LOG_LEVEL', 'INFO'))

# Initialize AWS clients
s3_client = boto3.client('s3')

def handler(event, context):
    """
    Lambda function to poll market data API and send to S3
    """
    try:
        # Get configuration from environment variables
        s3_bucket = os.getenv('S3_BUCKET_NAME')
        api_endpoint = os.getenv('API_ENDPOINT')
        
        logger.info(f"Starting market data polling for bucket: {s3_bucket}")
        
        # Poll the API
        market_data = poll_market_data_api(api_endpoint)
        
        if market_data:
            # Validate data
            validated_data, rejected_data = validate_data(market_data)
            
            # Send to S3
            accepted_count, rejected_count = send_to_s3(
                validated_data, 
                rejected_data, 
                s3_bucket
            )
            
            logger.info(f"Successfully processed {accepted_count} accepted and {rejected_count} rejected records")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Successfully processed market data',
                    'timestamp': datetime.utcnow().isoformat(),
                    'accepted_records': accepted_count,
                    'rejected_records': rejected_count,
                    'total_records': len(market_data)
                })
            }
        else:
            logger.warning("No market data received from API")
            return {
                'statusCode': 204,
                'body': json.dumps({
                    'message': 'No market data available',
                    'timestamp': datetime.utcnow().isoformat()
                })
            }
            
    except Exception as e:
        logger.error(f"Error processing market data: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal server error',
                'message': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })
        }

def poll_market_data_api(api_endpoint: str, max_retries: int = 3, initial_wait: int = 1) -> List[Dict[str, Any]]:
    """
    Poll the market data API with exponential backoff retries
    """
    for attempt in range(max_retries):
        try:
            logger.info(f"Polling API (attempt {attempt + 1}/{max_retries}): {api_endpoint}")
            
            response = requests.get(
                api_endpoint,
                timeout=30,
                headers={
                    'User-Agent': 'CreditRisk-Lambda/1.0',
                    'Accept': 'application/json'
                }
            )
            
            response.raise_for_status()
            
            # Parse the response
            api_response = response.json()
            
            # Extract the body content (it's a JSON string)
            if 'body' in api_response:
                market_data = json.loads(api_response['body'])
                logger.info(f"Received {len(market_data)} market data records")
                return market_data
            else:
                logger.warning("No 'body' field found in API response")
                return []
                
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            wait_time = initial_wait * (2 ** attempt)  # exponential backoff
            logger.warning(f"API request attempt {attempt + 1} failed due to network issue: {str(e)}")
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"All {max_retries} API request attempts failed")
                raise
                
        except requests.exceptions.HTTPError as e:
            # Do not retry on 4xx errors
            if 400 <= response.status_code < 500:
                logger.error(f"HTTP error {response.status_code}: {str(e)} - Not retrying")
                raise
            else:
                wait_time = initial_wait * (2 ** attempt)
                logger.warning(f"Server error {response.status_code}, retrying in {wait_time}s...")
                time.sleep(wait_time)
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse API response: {str(e)}")
            raise
            
        except Exception as e:
            logger.error(f"Unexpected error polling API: {str(e)}")
            raise
    
    return []

def validate_data(market_data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Validate market data records
    Returns: (validated_records, rejected_records)
    """
    validated_records = []
    rejected_records = []
    
    # Define required fields for validation
    required_fields = ['date', 'index_name', 'value', 'volatility']
    
    for i, record in enumerate(market_data):
        validation_errors = []
        
        # Check for required fields
        missing_fields = [field for field in required_fields if field not in record]
        if missing_fields:
            validation_errors.append(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Check for null values in required fields
        for field in required_fields:
            if field in record and record[field] is None:
                validation_errors.append(f"Null value in required field: {field}")
        
        # If validation passed, add to validated records (raw data only)
        if not validation_errors:
            validated_records.append(record)
            logger.debug(f"Record {i} validated successfully: {record.get('index_name')}")
        else:
            # Add to rejected records with error details
            rejected_record = {
                'validation_errors': validation_errors,
                'data': record
            }
            rejected_records.append(rejected_record)
            logger.warning(f"Record {i} rejected: {validation_errors}")
    
    logger.info(f"Validation complete: {len(validated_records)} accepted, {len(rejected_records)} rejected")
    
    return validated_records, rejected_records

def upload_to_s3(
    bucket_name: str,
    key: str,
    body: str,
    content_type: str = 'application/json',
    max_retries: int = 3
) -> bool:
    """
    Upload data to S3 with SSE-KMS encryption
    Returns: True if successful, raises exception if all retries fail
    """
    # Check if SSE-KMS encryption should be used
    # Only encrypt raw data, not logs
    kms_key_id = os.getenv('KMS_KEY_ID')
    use_encryption = kms_key_id and key.startswith('market_data/raw/')
    
    for attempt in range(max_retries):
        try:
            if use_encryption:
                logger.info(f"Uploading to S3 with SSE-KMS encryption (attempt {attempt + 1}/{max_retries}): s3://{bucket_name}/{key}")
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=body,
                    ContentType=content_type,
                    ServerSideEncryption='aws:kms',
                    SSEKMSKeyId=kms_key_id
                )
            else:
                logger.info(f"Uploading to S3 without encryption (attempt {attempt + 1}/{max_retries}): s3://{bucket_name}/{key}")
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=body,
                    ContentType=content_type
                )
            
            logger.info(f"Successfully uploaded to s3://{bucket_name}/{key}")
            return True
            
        except Exception as e:
            wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
            logger.warning(f"S3 upload attempt {attempt + 1} failed: {str(e)}")
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"All {max_retries} S3 upload attempts failed for {key}")
                raise

def send_to_s3(
    validated_data: List[Dict[str, Any]], 
    rejected_data: List[Dict[str, Any]], 
    bucket_name: str
) -> Tuple[int, int]:
    """
    Send validated and rejected records to S3
    Returns: (accepted_count, rejected_count)
    """
    timestamp = datetime.utcnow()
    date_prefix = timestamp.strftime('%Y/%m/%d')
    file_timestamp = timestamp.strftime('%Y%m%d_%H%M%S')
    
    accepted_count = 0
    rejected_count = 0
    accepted_key = None
    rejected_key = None
    
    try:
        # Send accepted records to S3 (raw data only)
        if validated_data:
            accepted_key = f"market_data/raw/{date_prefix}/market_data_{file_timestamp}.json"
            upload_to_s3(
                bucket_name=bucket_name,
                key=accepted_key,
                body=json.dumps(validated_data, indent=2),
                content_type='application/json'
            )
            accepted_count = len(validated_data)
            logger.info(f"Successfully processed {accepted_count} accepted records")
        
        # Send rejected records to S3
        if rejected_data:
            rejected_key = f"market_data/rejected/{date_prefix}/market_data_{file_timestamp}.json"
            upload_to_s3(
                bucket_name=bucket_name,
                key=rejected_key,
                body=json.dumps(rejected_data, indent=2),
                content_type='application/json'
            )
            rejected_count = len(rejected_data)
            logger.warning(f"Successfully processed {rejected_count} rejected records")
        
        # Create ingestion log
        log_entry = {
            'timestamp': timestamp.isoformat(),
            'execution_id': os.getenv('AWS_REQUEST_ID', 'unknown'),
            'total_records': len(validated_data) + len(rejected_data),
            'accepted_records': accepted_count,
            'rejected_records': rejected_count,
            'accepted_s3_key': accepted_key,
            'rejected_s3_key': rejected_key,
            'status': 'SUCCESS'
        }
        
        # Send ingestion log to S3
        log_key = f"market_data/logs/ingestion/{date_prefix}/ingestion_log_{file_timestamp}.json"
        upload_to_s3(
            bucket_name=bucket_name,
            key=log_key,
            body=json.dumps(log_entry, indent=2),
            content_type='application/json'
        )
        logger.info(f"Ingestion log saved successfully")
        
        return accepted_count, rejected_count
        
    except Exception as e:
        logger.error(f"Failed to upload data to S3: {str(e)}")
        raise
