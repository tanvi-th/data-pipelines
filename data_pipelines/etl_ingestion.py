from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
import boto3
import json
from libs import aws
import argparse
import logging
from pyspark.sql.functions import current_timestamp
import importlib.util
import os


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
s3 = boto3.client('s3')
DEFAULTS = {
    "source_bucket": "kafka-lenses-raw",
    "target_bucket": "dwh-bronze",
    "input_format": "json",
    "source_prefix": "backblaze_smart",
    "target_prefix": "bronze/kafka-lenses",
    "manifest_bucket": "dwh-manifests",
    "manifest_key": "bronze/kafka-lenses/backblaze_smart.json",
    "dwh_storage_mode": "overwrite"
}


def get_list_of_files_to_process(manifest_bucket, manifest_file, source_bucket, source_prefix):
    ## Reading manifest file
    existing_manifest = aws.read_json_file_from_s3(manifest_bucket, manifest_file)

    date_format = "%Y-%m-%d %H-%M-%S"

    last_processed_date = None
    if existing_manifest is not None:
        last_processed_date = datetime.strptime(existing_manifest["last_file_timestamp_in_partition"], "%Y-%m-%d %H:%M:%S")

    files = []
    file_path_to_process = []
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
    if 'Contents' in response:
        for object in response['Contents']:
            file_path = object['Key']
            file_name = file_path.split('/')
            if file_name and file_name[2] != '':
                files.append(file_name[2])
                
                current_processing_file_date_str = file_name[2].replace(source_prefix, '').replace('.json', '').replace('_', ' ').strip()
                current_processing_file_date = datetime.strptime(current_processing_file_date_str,date_format)
                
                if last_processed_date is None:
                    file_path_to_process.append("s3a://" + source_bucket + "/" + file_path)
                elif current_processing_file_date > last_processed_date:
                    file_path_to_process.append("s3a://" + source_bucket + "/" + file_path)
                

    partitions = [temp.replace(source_prefix, '').replace('.json', '').replace('_', ' ').strip() for temp in files] ## Fetching only date part from file name to process further

    partition_dates = [datetime.strptime(date_str, date_format) for date_str in partitions] ## contains all the dates from the files to fetch max date to build watermark
    return file_path_to_process, partition_dates


def load_schema(schema_path: str):
    """Dynamically load schema from a python file"""
    module_name = os.path.splitext(os.path.basename(schema_path))[0]
    spec = importlib.util.spec_from_file_location(module_name, schema_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "schema"):
        raise ValueError(f"Schema file {schema_path} does not define a variable named `schema`")
    return module.schema


def spark_process_input_files(spark, file_path_to_process: str, config: dict):
    try:
        logger.info("-------------PROCESSING BELOW FILS----------------")
        logger.info(file_path_to_process)
        reader = (
            spark.read
            .option("mode", "PERMISSIVE")
        )
        schema = None
        if "schema" in config and config["schema"]:
            schema = load_schema(config["schema"])
        if config["multiline"]:
            reader = reader.option("multiline", "true")
        if config["_corrupt_record"]:
            reader = reader.option("columnNameOfCorruptRecord", "_corrupt_record")
        if schema:
            reader = reader.schema(schema)
        
        df = reader.format(config["input_format"]).load(file_path_to_process)
        target_path = f"s3a://{config['target_bucket']}/{config['target_prefix']}/{config['source_prefix']}/"
        df = df.dropDuplicates()
        df = df.withColumn("ingestion_time", lit(current_timestamp()))
        df.write.mode(config["dwh_storage_mode"]).format("delta").save(target_path)
        logger.info("Dataframe processed successfully!!!")
    except Exception as e:
        raise ValueError(f"Error occurred: {e}", exc_info=True)


def write_manifest_file(partition_dates, config):
    json_string = {
        "last_completed_partition" : max(partition_dates).date().strftime("%Y-%m-%d"),
        "last_file_timestamp_in_partition": max(partition_dates).strftime("%Y-%m-%d %H:%M:%S"),
        "last_updated": datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
        }
    manifest_write = json.dumps(json_string)
    aws.write_json_file_to_s3(manifest_bucket=config["manifest_bucket"], manifest_key=config["manifest_key"], manifest_write=manifest_write)


def main(**kwargs) -> None:
    """Run Kafka consumer job and upload consumed data to S3."""
    config = {**DEFAULTS, **kwargs}
    spark = SparkSession.builder \
        .appName("ETL Ingestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    file_path_to_process, partition_dates = get_list_of_files_to_process(
        manifest_bucket=config["manifest_bucket"],
        manifest_file=config["manifest_key"],
        source_bucket=config["source_bucket"],
        source_prefix=config["source_prefix"]
    )
    spark_process_input_files(spark, file_path_to_process=file_path_to_process, config=config)
    write_manifest_file(partition_dates=partition_dates, config=config)
    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark job with parameters")
    parser.add_argument("--source_bucket", required=True, help="Bucket where source files are present")
    parser.add_argument("--target_bucket", required=True, help="Bucket where data needs to be stored")
    parser.add_argument("--input_format", required=True, default="json", help="Input format (json, csv, parquet, etc.)")
    parser.add_argument("--source_prefix", required=True, help="Input path excluding bucket name")
    parser.add_argument("--target_prefix", required=True, help="Output path excluding bucket name")
    parser.add_argument("--manifest_bucket", required=True, help="Bucket where manifest file is stored")
    parser.add_argument("--manifest_key", required=True, help="Input path for manifest file")
    parser.add_argument("--dwh_storage_mode", default="overwrite", help="overwrite / append")
    parser.add_argument("--schema", help="schema file relative path")
    parser.add_argument("--_corrupt_record", help="True or False - to indicate if _corrupt_record needs to be handled in dataframe")
    parser.add_argument("--multiline", help="True or False - to indicate if _corrupt_record needs to be handled in dataframe")
    args = vars(parser.parse_args())
    main(**args)