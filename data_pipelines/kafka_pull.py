import json
import requests
from io import BytesIO
import time
from datetime import datetime
import logging
import boto3
import argparse
from confluent_kafka import Consumer
import fastavro
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


def _base_consumer_config(config: dict) -> dict:
    """Build common Kafka consumer configuration."""
    return {
        'bootstrap.servers': config["bootstrap_servers"],
        'group.id': config["group_id"],
        'auto.offset.reset': config.get("auto_offset_reset", "earliest"),
        'enable.auto.commit': config.get("enable_auto_commit", False)
    }


def upload_to_s3(data: list, *, bucket: str, prefix: str, file_name_prefix: str, format:str, avro_schema: dict = None) -> None:
    """Upload data to S3 in specified format."""
    s3 = boto3.client("s3")
    end_time = datetime.now()
    year, month, day = end_time.strftime("%Y"), end_time.strftime("%m"), end_time.strftime("%d")
    prefix = f'{prefix}/date={year}-{month}-{day}/'
    file_name = f'{file_name_prefix}_{year}-{month}-{day}_{end_time.hour}-{end_time.minute}-{end_time.second}.{format}'
    # serialize based on format
    if format == "json":
        payload = json.dumps(data, indent=2).encode("utf-8")
        s3.upload_fileobj(BytesIO(payload), bucket, prefix + file_name)
    elif format == "avro":
        if not avro_schema:
            raise ValueError("Avro schema is required for Avro serialization")
        buf = BytesIO()
        fastavro.writer(buf, avro_schema, data)
        buf.seek(0)
        s3.upload_fileobj(buf, bucket, prefix + file_name)
    else:
        raise ValueError(f"Unsupported format: {format}")
    

def get_consumer(config: dict):
    """
    Return a Kafka consumer based on format.

    Args:
        config (dict): base configuration

    Returns:
        AvroConsumer or Consumer
    """
    consumer_conf = _base_consumer_config(config)

    if config["format"] == "avro":
        consumer_conf["schema.registry.url"] = config["schema_file"]
        consumer = AvroConsumer(consumer_conf)
    elif config["format"] == "json":
        consumer = Consumer(consumer_conf)
    else:
        raise ValueError(f"Unsupported consumer format: {format}")

    topics = config["topic"] if isinstance(config["topic"], list) else [config["topic"]]
    consumer.subscribe(topics)
    return consumer


def consume_messages(consumer, duration: int, fmt: str) -> list:
    """Consume messages for given duration (seconds) and return list of values."""
    data = []
    start_time = time.time()
    while True:
        msg = consumer.poll(1.0)
        if time.time() - start_time > duration:
            break
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue        
        try:
            if fmt == "json":
                # Kafka Consumer gives raw bytes
                decoded = msg.value().decode("utf-8")
                data.append(json.loads(decoded))  # → dict/list
            elif fmt == "avro":
                # AvroConsumer already deserializes using schema registry
                data.append(msg.value())  # → dict
            else:
                raise ValueError(f"Unsupported format: {fmt}")
        except Exception as e:
            print(f"Failed to process message: {e}")

    return data

def get_avro_schema(schema_registry: str, topic: str):
    resp = requests.get(f"{schema_registry}/subjects/{topic}-value/versions/latest")
    resp.raise_for_status()
    schema = resp.json()['schema']
    avro_schema = json.loads(schema)
    return avro_schema
    

def main(**kwargs) -> None:
    """Run Kafka consumer job and upload consumed data to S3."""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    config = {**DEFAULTS, **kwargs}
    try:
        logger.info("Starting consumer loop...")
        consumer = get_consumer(config)
        data = consume_messages(consumer, config["max_duration"], config["format"])
        schema = get_avro_schema(config["schema_file"], config["topic"]) if config["format"] == 'avro' else None
        if data:
            upload_to_s3(
                bucket=config["bucket"],
                data=data,
                prefix=config["prefix"],
                file_name_prefix=config["file_name_prefix"],
                format=config['format'],
                avro_schema=schema
            )
            logger.info(f"{config['file_name_prefix']} HAS BEEN UPLOADED TO S3 LOCATION - {config['bucket']}/{config['prefix']}")
        else:
            logger.error("************************NO MESSAGES CONSUMED FROM KAFKA************************")    
    except KeyboardInterrupt:
        logger.critical("Consumer stopped.")
        logger.warning("Consumer stopped by user.")
    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)
    except SerializerError as e:
        print(f"Message deserialization failed: {e}")
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