import boto3
import json
import logging

s3 = boto3.client('s3')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_json_file_from_s3(bucket, key):
    data = None
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    data = json.loads(content)
    logger.info(f"Error reading JSON from S3: {e}")
    return data

def write_json_file_to_s3(manifest_bucket, manifest_key, manifest_write):
    try:
        s3.put_object(
            Bucket=manifest_bucket,
            Key=manifest_key,
            Body=manifest_write,
            ContentType="application/json"
        )
        logger.info(f"file has been uploaded successfully to s3://{manifest_bucket}/{manifest_key}")
    except Exception as e:
        logger.error(f"Error writing JSON to S3: {e}")