from kafka import KafkaConsumer
import json
import boto3
from io import BytesIO
import time
from libs import aws
from datetime import datetime
configs = {
    'bootstrap_servers': 'localhost:9092',  # Replace with your Kafka broker(s)
    'group_id': 'kafka-pull-test',  # Replace with your consumer group ID
    'auto_offset_reset': 'latest',  # Start consuming from the beginning of the topic
    'enable_auto_commit': True, # Enable automatic offset commits
    'value_deserializer': lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON messages
}

consumer = KafkaConsumer('backblaze_smart',**configs)

try:
    data = []
    max_duration = 45
    start_time = time.time()
    for message in consumer:
        data.append(message.value)
        if time.time() - start_time > max_duration:
            break
    end_time = datetime.now()
    print("TRIED TO CONSUME MESSAGES FOR {} SECONDS".format(max_duration))
    if data:
        json_data = json.dumps(data, indent=2)
        bytes_data = BytesIO(json_data.encode('utf-8'))
        s3 = aws.boto3_s3_client()
        bucket = 'kafka-lenses-raw'
        key = ''
        file_name = 'backblaze_smart_{}-{}-{}:{}:{}:{}.json'.format(end_time.year, end_time.month, end_time.day, end_time.hour, end_time.minute, end_time.second)
        s3.upload_fileobj(
            bytes_data,
            Bucket = bucket,
            Key=key+file_name
        )
        print("{} HAS BEEN UPLOADED TO S3 LOCATION - {}/{}".format(file_name, bucket, key))
    else:
        print("************************NO MESSAGES PRESENT IN KAFKA************************")
        
except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    # Close the consumer
    consumer.close()