from kafka import KafkaConsumer
import json
from io import BytesIO
import time
from datetime import datetime
import logging
import boto3
import argparse
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

DEFAULTS = {
    "topic": "backblaze_smart",
    "bootstrap_servers": "demo-kafka:19092",
    "group_id": "kafka-pull-test",
    "auto_offset_reset": "latest",
    "enable_auto_commit": True,
    "max_duration": 45,
    "bucket": "kafka-lenses-raw",
    "prefix": "backblaze_smart",
    "file_name_prefix": "backblaze_smart",
    "format": "json",
    "schema_file": None
}

# from pprint import pprint

# def debug_kafka_consumer(consumer):
#     print("\n--- KafkaConsumer Debug Info ---")
#     try:
#         print("Bootstrap servers:", consumer.config.get("bootstrap_servers"))
#         print("Group ID:", consumer.config.get("group_id"))
#         print("Client ID:", consumer.config.get("client_id"))
#         print("Subscribed topics:", consumer.subscription())
#         print("Assigned partitions:", consumer.assignment())
#         print("Available topics:", consumer.topics())
        
#         if consumer.assignment():
#             offsets = {tp: consumer.position(tp) for tp in consumer.assignment()}
#             print("Current offsets:", offsets)
        
#         print("\nFull __dict__ dump:")
#         pprint(consumer.__dict__)
#     except Exception as e:
#         print("Error while debugging consumer:", e)
#     print("--- End Debug ---\n")

# def avro_deserializer(schema):
#     """Return a deserializer function for Avro bytes."""
#     __schema = avro.schema.parse(schema)
#     reader = avro.io.DatumReader(__schema)

#     def _deserialize(message_bytes):
#         bytes_reader = BytesIO(message_bytes)
#         decoder = avro.io.BinaryDecoder(bytes_reader)
#         record = reader.read(decoder)
#         if not isinstance(record, dict):
#             raise ValueError(f"Expected dict from Avro, got {type(record)}")
#         return record
#     return _deserialize

def get_consumer(config) -> KafkaConsumer:
    """Kafka consumer factory depending on format."""
    if config["format"] == "json":
        consumer = Consumer(
            bootstrap_servers=config["bootstrap_servers"],
            group_id=config["group_id"],
            auto_offset_reset=config["auto_offset_reset"],
            enable_auto_commit=config["enable_auto_commit"]
        )
        consumer.subscribe(config['topic'])
        # deserializer = lambda x: json.loads(x.decode("utf-8"))
        return consumer
    elif config["format"] == "avro":
        if not config["schema_file"]:
            raise ValueError("Avro format requires 'schema_file' path.")
        elif config["schema_file"].startswith("http"):
            import requests
            resp = requests.get(f"{config['schema_file']}/subjects/{config['topic']}-value/versions/latest")
            resp.raise_for_status()
            schema = resp.json()['schema']
            print("fetching from http schema location",resp.json())
        else:
            with open(config["schema_file"], "r") as f:
                schema = f.read()
        deserializer = avro_deserializer(schema)
    else:
        raise ValueError(f"Unsupported format: {config['format']}")
    return AvroConsumer(
        bootstrap_servers=config["bootstrap_servers"],
        group_id=config["group_id"],
        auto_offset_reset=config["auto_offset_reset"],
        enable_auto_commit=config["enable_auto_commit"],
        value_deserializer=deserializer,
    )

def upload_to_s3(data: list, *, bucket: str, prefix: str, file_name_prefix: str) -> None:
    s3 = boto3.client("s3")
    json_data = json.dumps(data, indent=2)
    bytes_data = BytesIO(json_data.encode('utf-8'))
    end_time = datetime.now()
    year, month, day = end_time.strftime("%Y"), end_time.strftime("%m"), end_time.strftime("%d")
    prefix = f'{prefix}/date={year}-{month}-{day}/'
    file_name = f'{file_name_prefix}_{year}-{month}-{day}_{end_time.hour}-{end_time.minute}-{end_time.second}.json'
    s3.upload_fileobj(
        bytes_data,
        Bucket = bucket,
        Key=prefix+file_name
    )
    

def main(**kwargs) -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    config = {**DEFAULTS, **kwargs}
    
    
    for attempt in range(5):
        try:
            print("--------------------------------TRYING CONSUMER")
            consumer = get_consumer(config)
            # debug_kafka_consumer(consumer)
            break
        except KafkaError as e:
            logger.warning(f"Kafka not ready yet ({attempt+1}/10): {e}")
            time.sleep(10)
    else:
        raise RuntimeError("Failed to connect to Kafka after retries")
    
    try:
        logger.info("Starting consumer loop...")
        data = []
        start_time = time.time()
        print("-----------------------------CONSUMER:", consumer.assignment())
        print("-----------------------------Partitions for topic:", consumer.partitions_for_topic(config['topic']))


        # consumer.subscribe([config["topic"]])
        # Wait until partitions are assigned
        timeout = 50  # seconds
        start = time.time()
        while not consumer.assignment():
            if time.time() - start > timeout:
                print("Partitions were not assigned in time")
                break
            time.sleep(5)
            print("-----------------------------SLEEPING")
            
        for message in consumer:
            print("-----------------------------HERE")
            print(message.value)
            data.append(message.value)
            if time.time() - start_time > config["max_duration"]:
                break   
        logger.info(f"CONSUMED MESSAGES FOR {config['max_duration']} SECONDS")
        if data:
            upload_to_s3(
                bucket=config["bucket"],
                data=data,
                prefix=config["prefix"],
                file_name_prefix=config["file_name_prefix"]
            )
            logger.info(f"{config['file_name_prefix']} HAS BEEN UPLOADED TO S3 LOCATION - {config['bucket']}/{config['prefix']}")
        else:
            logger.error("************************NO MESSAGES CONSUMED FROM KAFKA************************")    
    except KeyboardInterrupt:
        logger.critical("Consumer stopped.")
        logger.warning("Consumer stopped by user.")
    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)
    finally:
        # Close the consumer
        consumer.close()
        logger.info("Consumer connection closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="kafka data pull from lenses")
    parser.add_argument("--topic")
    parser.add_argument("--bootstrap_servers")
    parser.add_argument("--group_id")
    parser.add_argument("--auto_offset_reset")
    parser.add_argument("--enable_auto_commit")
    parser.add_argument("--max_duration", help="Duration for which consumer would pull from kafka in seconds")
    parser.add_argument("--bucket")
    parser.add_argument("--prefix")
    parser.add_argument("--file_name_prefix")
    parser.add_argument("--format")
    parser.add_argument("--schema_file")
    args = vars(parser.parse_args())
    main(**args)