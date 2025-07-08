import boto3

def boto3_s3_client():
    s3 = boto3.client(
        's3',
        aws_access_key_id='', 
        aws_secret_access_key=''
    )
    return s3