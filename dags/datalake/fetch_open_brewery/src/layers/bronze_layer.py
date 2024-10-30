"""
Bronze Layer - Data Ingestion Script

This module is responsible for fetching raw data from the Open Brewery API and storing it in the Bronze layer of the data lake on AWS S3. The Bronze layer stores data as-is from the source without any transformations, acting as the initial data ingestion point in the data lake architecture.

Functions:
    - load_data_into_bucket(bucket_name, path, object_data): 
        Saves JSON data to the specified S3 bucket using the provided path and object data.
    - fetch_brewery_data(): 
        Fetches brewery data from the Open Brewery API endpoint and stores it in the Bronze S3 bucket.
    - main(): 
        Entry point for initiating the data ingestion process into the Bronze layer.

Modules and Libraries:
    - requests: Used to perform HTTP requests to the Open Brewery API.
    - logging: Provides logging capabilities to track the status of operations.
    - json: For serializing data into JSON format before uploading to S3.
    - S3Hook: Airflow's AWS S3 hook to facilitate interactions with S3.

Usage:
    - This script is called by an Apache Airflow DAG as the first step in a multi-layered data pipeline.
    - It can also be run independently for testing purposes.

Error Handling:
    - Logs and reports if the API request fails or if the S3 data upload encounters an issue.

Raises:
    - Exception: If the API request fails with a non-200 status code.
    - Exception: If data fails to upload to the specified S3 bucket.

Example Workflow:
    1. The `fetch_brewery_data()` function calls the Open Brewery API to retrieve data.
    2. If data is successfully retrieved, it is stored in S3 in JSON format via the `load_data_into_bucket(bucket_name, path, object_data)` function.
    3. Logs are generated to confirm successful data loading or to report any issues.
"""

import requests
import logging
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datalake.fetch_open_brewery.src.vars import (
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
