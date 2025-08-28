import boto3
import json

def boto3_s3_client():
    s3 = boto3.client(
        's3',
        aws_access_key_id='', 
        aws_secret_access_key=''
    )
    return s3

def read_json_file_from_s3(bucket, key):
    s3 = boto3.client('s3')
    data = ''
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
    except Exception as e:
        print(f"Error reading JSON from S3: {e}")
    return data