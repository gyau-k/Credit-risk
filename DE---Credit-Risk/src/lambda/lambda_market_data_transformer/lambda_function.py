import json
import logging
import os
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from typing import Dict, List, Any, Tuple
from io import BytesIO
import traceback

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Main Lambda handler for market data transformation.
    Processes S3 events from market_data/raw/ prefix and transforms data to DimMarket Parquet files.
    """
    try:
        logger.info("=" * 80)
        logger.info("Starting market data transformation")
        logger.info(f"Lambda Request ID: {context.aws_request_id}")
        logger.info("=" * 80)
        
        # Process S3 event
        results = process_s3_event(event)
        
        logger.info("=" * 80)
        logger.info("Transformation Summary:")
        logger.info(f"  Files processed: {results['processed']}")
        logger.info(f"  Records transformed: {results['transformed']}")
        logger.info(f"  Records rejected: {results['rejected']}")
        logger.info(f"  Errors: {results['errors']}")
        logger.info("=" * 80)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Market data transformation completed successfully',
                'timestamp': datetime.utcnow().isoformat(),
                'results': results
            })
        }
        
    except Exception as e:
        logger.error(f"Unexpected error in lambda_handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Internal server error',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })
        }

def process_s3_event(event: Dict) -> Dict:
    """
    Process S3 event and transform market data files.
    """
    results = {
        'processed': 0,
        'transformed': 0,
        'rejected': 0,
        'errors': 0
    }
    
    records = event.get('Records', [])
    
    if not records:
        logger.warning("No records found in S3 event")
        return results
    
    for record in records:
        try:
            results['processed'] += 1
            
            s3_info = record.get('s3', {})
            bucket = s3_info.get('bucket', {}).get('name')
            key = s3_info.get('object', {}).get('key')
            
            if not bucket or not key:
                logger.error("Invalid S3 event structure", extra={'record': record})
                results['errors'] += 1
                continue
            
            logger.info(f"Processing file: s3://{bucket}/{key}")
            
            file_result = process_market_data_file(bucket, key)
            
            if file_result['status'] == 'transformed':
                results['transformed'] += file_result['record_count']
            elif file_result['status'] == 'rejected':
                results['rejected'] += file_result['record_count']
            else:
                results['errors'] += 1
                
        except Exception as e:
            logger.error(f"Error processing record: {str(e)}", exc_info=True)
            results['errors'] += 1
    
    return results

def process_market_data_file(bucket: str, key: str) -> Dict:
    """
    Process a single market data file: validate, transform, and save to DimMarket.
    """
    file_result = {
        'file': key,
        'status': 'error',
        'record_count': 0,
        'error_message': None
    }
    
    try:
        raw_data = read_s3_file(bucket, key)
        if raw_data is None:
            file_result['error_message'] = "Failed to read file from S3"
            return file_result
        
        validated_data, rejected_data = validate_and_transform_data(raw_data)
        
        if not validated_data:
            move_to_rejected(bucket, key)
            file_result['status'] = 'rejected'
            file_result['record_count'] = len(rejected_data)
            file_result['error_message'] = "All records failed validation"
            return file_result
        
        dim_market_path = save_to_dim_market(validated_data, key)
        # Keep raw data in place - don't move it
        
        file_result['status'] = 'transformed'
        file_result['record_count'] = len(validated_data)
        file_result['dim_market_path'] = dim_market_path
        
        logger.info(f"Successfully processed {len(validated_data)} records from {key}")
        logger.info(f"Raw data kept at: s3://{bucket}/{key}")
        logger.info(f"Transformed data saved to: {dim_market_path}")
        
    except Exception as e:
        logger.error(f"Error processing file {key}: {str(e)}", exc_info=True)
        file_result['error_message'] = str(e)
        try:
            move_to_rejected(bucket, key)
        except:
            pass
    
    return file_result

def read_s3_file(bucket: str, key: str) -> List[Dict]:
    """
    Read JSON file from S3 with automatic SSE-KMS decryption
    """
    try:
        logger.info(f"Reading file from S3: s3://{bucket}/{key}")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        data = json.loads(content)
        if isinstance(data, dict):
            data = [data]
        logger.info(f"Read {len(data)} records from s3://{bucket}/{key}")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in file {key}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error reading file {key}: {str(e)}")
        return None

def validate_and_transform_data(raw_data: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Validate and transform market data records.
    Returns: (validated_records, rejected_records)
    """
    validated_records = []
    rejected_records = []
    required_fields = ['date', 'index_name', 'value', 'volatility']
    
    for i, record in enumerate(raw_data):
        try:
            missing_fields = [field for field in required_fields if field not in record]
            if missing_fields:
                rejected_records.append({
                    'record_index': i,
                    'data': record,
                    'error': f"Missing required fields: {', '.join(missing_fields)}"
                })
                continue
            
            transformed_record = transform_record(record)
            if transformed_record:
                validated_records.append(transformed_record)
            else:
                rejected_records.append({
                    'record_index': i,
                    'data': record,
                    'error': "Data transformation failed"
                })
        except Exception as e:
            rejected_records.append({
                'record_index': i,
                'data': record,
                'error': f"Validation error: {str(e)}"
            })
    
    logger.info(f"Validation complete: {len(validated_records)} valid, {len(rejected_records)} rejected")
    return validated_records, rejected_records

def transform_record(record: Dict) -> Dict:
    """
    Transform a single market data record.
    """
    try:
        # Parse and standardize date
        date_str = record['date']
        if isinstance(date_str, str):
            if 'T' in date_str:
                date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            else:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        else:
            date_obj = date_str
        
        # Clean and validate fields
        index_name = str(record['index_name']).upper().strip()
        try:
            value = float(record['value']) if record['value'] is not None else None
            volatility = float(record['volatility']) if record['volatility'] is not None else None
        except (ValueError, TypeError):
            return None
        
        if value is not None and value < 0:
            logger.warning(f"Negative value found: {value}")
        if volatility is not None and (volatility < 0 or volatility > 1):
            logger.warning(f"Unusual volatility value: {volatility}")
        
        transformed = {
            'date': date_obj,
            'index_name': index_name,
            'value': value,
            'volatility': volatility
        }
        
        return transformed
        
    except Exception as e:
        logger.error(f"Error transforming record: {str(e)}")
        return None

def save_to_dim_market(validated_data: List[Dict], source_file: str) -> str:
    """
    Save validated data to DimMarket Parquet file with optional encryption.
    Only includes: date, index_name, value, volatility.
    """
    try:
        df = pd.DataFrame(validated_data)

        # ✅ Keep only the four required fields
        required_cols = ['date', 'index_name', 'value', 'volatility']
        df = df[[c for c in required_cols if c in df.columns]]

        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        silver_bucket = os.environ.get('SILVER_BUCKET', 'creditrisk-silver')
        date_prefix = datetime.utcnow().strftime('%Y/%m/%d')
        parquet_key = f"dim_market/{date_prefix}/dim_market_{timestamp}.parquet"
        
        table = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        # Upload Parquet with SSE-KMS encryption
        kms_key_id = os.getenv('KMS_KEY_ID')
        use_encryption = kms_key_id
        
        if use_encryption:
            logger.info(f"Saving Parquet with SSE-KMS encryption to s3://{silver_bucket}/{parquet_key}")
            s3_client.put_object(
                Bucket=silver_bucket,
                Key=parquet_key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream',
                ServerSideEncryption='aws:kms',
                SSEKMSKeyId=kms_key_id,
                Metadata={
                    'record_count': str(len(df)),
                    'processed_at': datetime.utcnow().isoformat(),
                    'source_file': source_file.split('/')[-1]
                }
            )
        else:
            logger.info(f"Saving Parquet without encryption to s3://{silver_bucket}/{parquet_key}")
            s3_client.put_object(
                Bucket=silver_bucket,
                Key=parquet_key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream',
                Metadata={
                    'record_count': str(len(df)),
                    'processed_at': datetime.utcnow().isoformat(),
                    'source_file': source_file.split('/')[-1]
                }
            )
        
        logger.info(f"Saved {len(df)} records to s3://{silver_bucket}/{parquet_key}")
        return f"s3://{silver_bucket}/{parquet_key}"
        
    except Exception as e:
        logger.error(f"Error saving to DimMarket: {str(e)}", exc_info=True)
        raise

def move_to_rejected(bucket: str, key: str):
    """
    Move file to rejected prefix.
    """
    try:
        rejected_key = key.replace('market_data/raw/', 'market_data/rejected/')
        copy_source = {'Bucket': bucket, 'Key': key}
        s3_client.copy_object(CopySource=copy_source, Bucket=bucket, Key=rejected_key)
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Moved rejected file to: s3://{bucket}/{rejected_key}")
    except Exception as e:
        logger.error(f"Error moving file to rejected: {str(e)}")
        raise

def move_to_processed(bucket: str, key: str):
    """
    Move file to processed prefix.
    """
    try:
        processed_key = key.replace('market_data/raw/', 'market_data/processed/')
        copy_source = {'Bucket': bucket, 'Key': key}
        s3_client.copy_object(CopySource=copy_source, Bucket=bucket, Key=processed_key)
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Moved processed file to: s3://{bucket}/{processed_key}")
    except Exception as e:
        logger.error(f"Error moving file to processed: {str(e)}")
