from kafka import KafkaConsumer
import json
import boto3
from io import BytesIO
import time
from libs import aws
from datetime import datetime
configs = {
    'bootstrap_servers': 'localhost:9092',
    'group_id': 'kafka-pull-test',
    'auto_offset_reset': 'latest',
    'enable_auto_commit': True,
    'value_deserializer': lambda x: json.loads(x.decode('utf-8'))
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
    year  = end_time.strftime("%Y")
    month = end_time.strftime("%m")
    day   = end_time.strftime("%d")
    print("CONSUMED MESSAGES FOR {} SECONDS".format(max_duration))
    if data:
        json_data = json.dumps(data, indent=2)
        bytes_data = BytesIO(json_data.encode('utf-8'))
        s3 = aws.boto3_s3_client()
        bucket = 'kafka-lenses-raw'
        key = 'backblaze_smart/date={}-{}-{}/'.format(year, month, day)
        file_name = 'backblaze_smart_{}-{}-{}_{}-{}-{}.json'.format(year, month, day, end_time.hour, end_time.minute, end_time.second)
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