import requests
import logging
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dags.src.datalake.fetch_open_brewery.src.vars import (
    endpoint, bronze_bucket_name, path
)

def load_data_into_bucket(bucket_name, path, object_data):
    s3_hook = S3Hook("airflow-aws")
    s3_hook.load_string(
        bucket_name=bucket_name,
        key=path,
        string_data=json.dumps(object_data),
        replace=True
    )
    logging.info(f"Loading the file {path} into the S3 bucket {bucket_name}")

def fetch_brewery_data():
    brewery_list = requests.get(endpoint)
    if brewery_list.status_code == 200:
        result = brewery_list.json()
        logging.info(f"Brewery data going to be stored in {bronze_bucket_name} bucket")
        load_data_into_bucket(bronze_bucket_name, path, result)
    else:
        logging.info(f"Unable to get data from the following endpoint: {endpoint}")

def main():
    fetch_brewery_data()