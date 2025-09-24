from setuptools import setup, find_packages

setup(
    name="data-pipelines",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark",
        "boto3",
        "fastavro",
        "confluent-kafka[avro]",
        "orjson",
        "requests",
        "avro-python3"
    ]
)