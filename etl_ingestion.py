from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType
import boto3
import json

spark = (
    SparkSession.builder
    .appName("ETL Ingestion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
custom_schema = StructType([
    StructField("capacity_bytes", LongType(), True),
    StructField("date", StringType(), True),
    StructField("failure", LongType(), True),
    StructField("model", StringType(), True),
    StructField("serial_number", StringType(), True),
    StructField("smart_10_normalized", LongType(), True),
    StructField("smart_10_raw", LongType(), True),
    StructField("smart_11_normalized", LongType(), True),
    StructField("smart_11_raw", LongType(), True),
    StructField("smart_12_normalized", LongType(), True),
    StructField("smart_12_raw", LongType(), True),
    StructField("smart_13_normalized", StringType(), True),
    StructField("smart_13_raw", StringType(), True),
    StructField("smart_15_normalized", StringType(), True),
    StructField("smart_15_raw", StringType(), True),
    StructField("smart_183_normalized", LongType(), True),
    StructField("smart_183_raw", LongType(), True),
    StructField("smart_184_normalized", LongType(), True),
    StructField("smart_184_raw", LongType(), True),
    StructField("smart_187_normalized", LongType(), True),
    StructField("smart_187_raw", LongType(), True),
    StructField("smart_188_normalized", LongType(), True),
    StructField("smart_188_raw", LongType(), True),
    StructField("smart_189_normalized", LongType(), True),
    StructField("smart_189_raw", LongType(), True),
    StructField("smart_190_normalized", LongType(), True),
    StructField("smart_190_raw", LongType(), True),
    StructField("smart_191_normalized", LongType(), True),
    StructField("smart_191_raw", LongType(), True),
    StructField("smart_192_normalized", LongType(), True),
    StructField("smart_192_raw", LongType(), True),
    StructField("smart_193_normalized", LongType(), True),
    StructField("smart_193_raw", LongType(), True),
    StructField("smart_194_normalized", LongType(), True),
    StructField("smart_194_raw", LongType(), True),
    StructField("smart_195_normalized", LongType(), True),
    StructField("smart_195_raw", LongType(), True),
    StructField("smart_196_normalized", LongType(), True),
    StructField("smart_196_raw", LongType(), True),
    StructField("smart_197_normalized", LongType(), True),
    StructField("smart_197_raw", LongType(), True),
    StructField("smart_198_normalized", LongType(), True),
    StructField("smart_198_raw", LongType(), True),
    StructField("smart_1_normalized", LongType(), True),
    StructField("smart_1_raw", LongType(), True),
    StructField("smart_200_normalized", LongType(), True),
    StructField("smart_200_raw", LongType(), True),
    StructField("smart_201_normalized", StringType(), True),
    StructField("smart_201_raw", StringType(), True),
    StructField("smart_220_normalized", LongType(), True),
    StructField("smart_220_raw", LongType(), True),
    StructField("smart_222_normalized", LongType(), True),
    StructField("smart_222_raw", LongType(), True),
    StructField("smart_223_normalized", LongType(), True),
    StructField("smart_223_raw", LongType(), True),
    StructField("smart_224_normalized", LongType(), True),
    StructField("smart_224_raw", LongType(), True),
    StructField("smart_225_normalized", LongType(), True),
    StructField("smart_225_raw", LongType(), True),
    StructField("smart_226_normalized", LongType(), True),
    StructField("smart_226_raw", LongType(), True),
    StructField("smart_22_normalized", LongType(), True),
    StructField("smart_22_raw", LongType(), True),
    StructField("smart_240_normalized", LongType(), True),
    StructField("smart_240_raw", LongType(), True),
    StructField("smart_241_normalized", LongType(), True),
    StructField("smart_241_raw", LongType(), True),
    StructField("smart_242_normalized", LongType(), True),
    StructField("smart_242_raw", LongType(), True),
    StructField("smart_250_normalized", LongType(), True),
    StructField("smart_250_raw", LongType(), True),
    StructField("smart_251_normalized", LongType(), True),
    StructField("smart_251_raw", LongType(), True),
    StructField("smart_252_normalized", LongType(), True),
    StructField("smart_252_raw", LongType(), True),
    StructField("smart_254_normalized", LongType(), True),
    StructField("smart_254_raw", LongType(), True),
    StructField("smart_255_normalized", LongType(), True),
    StructField("smart_255_raw", LongType(), True),
    StructField("smart_2_normalized", LongType(), True),
    StructField("smart_2_raw", LongType(), True),
    StructField("smart_3_normalized", LongType(), True),
    StructField("smart_3_raw", LongType(), True),
    StructField("smart_4_normalized", LongType(), True),
    StructField("smart_4_raw", LongType(), True),
    StructField("smart_5_normalized", LongType(), True),
    StructField("smart_5_raw", LongType(), True),
    StructField("smart_7_normalized", LongType(), True),
    StructField("smart_7_raw", LongType(), True),
    StructField("smart_8_normalized", LongType(), True),
    StructField("smart_8_raw", LongType(), True),
    StructField("smart_9_normalized", LongType(), True),
    StructField("smart_9_raw", LongType(), True)
])

bucket = "kafka-lenses-raw"
prefix = "backblaze_smart"
manifest_bucket="dwh-manifests"
manifest_key="bronze/kafka-lenses/backblaze_smart.json"

s3 = boto3.client('s3')

## Reading manifest file
existing_manifest =''
try:
    response = s3.get_object(Bucket=manifest_bucket, Key=manifest_key)
    content = response['Body'].read().decode('utf-8')
    existing_manifest = json.loads(content)
except Exception as e:
    print(f"Error reading JSON from S3: {e}")

print("========================================================================================")

date_format = "%Y-%m-%d %H-%M-%S"

last_processed_date = None
if existing_manifest != '':
    last_processed_date = datetime.strptime(existing_manifest["last_file_timestamp_in_partition"], "%Y-%m-%d %H:%M:%S")

files = []
file_path_to_process = []
response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
if 'Contents' in response:
    for object in response['Contents']:
        # ("========================================================================================")
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

print("-------------PROCESSING BELOW FILS----------------")
print(file_path_to_process)