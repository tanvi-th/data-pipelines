from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
import boto3
import json
from schemas.kafka_lenses_backblaze_smart import custom_schema
from libs import aws

spark = (
    SparkSession.builder
    .appName("ETL Ingestion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

bucket = "kafka-lenses-raw"
prefix = "backblaze_smart"
manifest_bucket="dwh-manifests"
manifest_key="bronze/kafka-lenses/backblaze_smart.json"

s3 = boto3.client('s3')

## Reading manifest file
existing_manifest = aws.read_json_file_from_s3(manifest_bucket, manifest_key)

date_format = "%Y-%m-%d %H-%M-%S"

last_processed_date = None
if existing_manifest is not None:
    last_processed_date = datetime.strptime(existing_manifest["last_file_timestamp_in_partition"], "%Y-%m-%d %H:%M:%S")

files = []
file_path_to_process = []
response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
if 'Contents' in response:
    for object in response['Contents']:
        file_path = object['Key']
        file_name = file_path.split('/')
        if file_name and file_name[2] != '':
            files.append(file_name[2])
            
            current_processing_file_date_str = file_name[2].replace(prefix, '').replace('.json', '').replace('_', ' ').strip()
            current_processing_file_date = datetime.strptime(current_processing_file_date_str,date_format)
            
            if last_processed_date is None:
                file_path_to_process.append("s3a://" + bucket + "/" + file_path)
            elif current_processing_file_date > last_processed_date:
                file_path_to_process.append("s3a://" + bucket + "/" + file_path)
            

partitions = [temp.replace(prefix, '').replace('.json', '').replace('_', ' ').strip() for temp in files] ## Fetching only date part from file name to process further

partition_dates = [datetime.strptime(date_str, date_format) for date_str in partitions] ## contains all the dates from the files to fetch max date to build watermark


print("-------------PROCESSING BELOW FILS----------------")
print(file_path_to_process)

df = spark.read.option("multiline", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(custom_schema) \
    .json(file_path_to_process)

time = datetime.now()
df = df.dropDuplicates()
df = df.withColumn("ingestion_time", lit(time))
df.write.mode("overwrite").format("delta").save("s3a://dwh-bronze/kafka-lenses/backblaze_smart/")


json_string = {
    "last_completed_partition" : max(partition_dates).date().strftime("%Y-%m-%d"),
    "last_file_timestamp_in_partition": max(partition_dates).strftime("%Y-%m-%d %H:%M:%S"),
    "last_updated": datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
    }

manifest_write = json.dumps(json_string)

s3.put_object(
    Bucket=manifest_bucket,
    Key=manifest_key,
    Body=manifest_write,
    ContentType="application/json"
)

print("Manifest file has been uploaded to S3 - s3://dwh-manifests/bronze/kafka-lenses/backblaze_smart.json")