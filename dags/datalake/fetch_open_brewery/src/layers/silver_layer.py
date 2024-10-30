"""
Silver Layer - Data Processing Script

This module is responsible for transforming and cleaning the raw brewery data 
stored in the Bronze layer. The Silver layer processes the data to make it 
suitable for analytical purposes and prepares it for loading into the Gold layer 
of the data lake. The processed data is stored using Delta Lake to ensure 
transactional integrity and support scalable analytics.

Functions:
    - load_data_into_bucket(path, object_data, storage_options, partition_list): 
        Saves transformed data to the specified Delta Lake table.
    - get_data_from_bronze_layer(bucket_name, path): 
        Fetches raw brewery data from the Bronze layer, applies transformations, 
        and stores the cleaned data in the Silver Delta Lake table.
    - get_aws_connection_info():
        Fetches the AWS Airflow connection needed for the Delta Lake storage options.
    - create_deltalake_storage_options(access_key, secret_key, region_name):
        Create the Delta Lake storage options by using the AWS user credentials retrived
         from Airflow connection.
    - main(): 
        Entry point for initiating the data processing workflow in the Silver layer.

Modules and Libraries:
    - pandas: Used for data manipulation and cleaning.
    - logging: Provides logging capabilities to track the status of operations.
    - json: For serializing transformed data into JSON format before uploading to Delta Lake.
    - delta: Delta Lake library for handling data storage and transactions.

Usage:
    - This script is called by an Apache Airflow DAG as part of a multi-layered data pipeline.
    - It can also be executed independently for testing and validation purposes.

Error Handling:
    - Logs and reports if the data transformation fails or if there are issues with writing to Delta Lake.

Raises:
    - Exception: If data transformation fails or if there are issues with data loading.
    - Exception: If the transformed data fails to be written to the specified Delta Lake table.

Example Workflow:
    1. The `get_data_from_bronze_layer(bucket_name, path)` function retrieves raw data from the Bronze layer.
    2. The data is processed and cleaned using Pandas operations.
    3. The cleaned data is stored in the Silver Delta Lake table via the `load_data_into_bucket(path, object_data, storage_options, partition_list)` function.
    4. Logs are generated to confirm successful data processing or to report any issues.

"""
import json
import logging
import pandas as pd
from deltalake.writer import write_deltalake
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.datalake.fetch_open_brewery.src.vars import (
    path, bronze_bucket_name, s3_path, partition_list
)

def get_data_from_bronze_layer(bucket_name, path):
    logging.info(f"Reading data from {bucket_name} bucket")
    s3_hook = S3Hook("airflow-aws")
    s3_object = s3_hook.read_key(bucket_name=bucket_name, key=path)
    json_data = json.loads(s3_object)
    bronze_data = pd.DataFrame(json_data)
    logging.info(f"Here's a sample of the bronze data.\n{bronze_data.head()}")
    return bronze_data

def get_aws_connection_info():
    logging.info("Getting data from the airflow-aws connection")
    conn = BaseHook.get_connection('airflow-aws')
    access_key = conn.login
    secret_key = conn.password
    region_name = conn.to_dict()['extra']['region']
    logging.info("Data from airflow-aws collected successfully")
    return access_key, secret_key, region_name

def create_deltalake_storage_options(access_key, secret_key, region_name):
    logging.info("Deltalake storage options created successfully")
    return {
        "AWS_REGION":region_name,
        'AWS_ACCESS_KEY_ID': access_key,
        'AWS_SECRET_ACCESS_KEY': secret_key,
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
    }

def load_data_into_bucket(path, object_data, storage_options, partition_list):
    write_deltalake(
        path,
        object_data,
        storage_options=storage_options,
        mode='overwrite',
        partition_by=partition_list
    )
    logging.info(f"Loading the file {path} into the S3 bucket {path.split('/')[2]}")

def main():
    bronze_data = get_data_from_bronze_layer(bronze_bucket_name, path)
    bronze_data.fillna('No Value', inplace=True)
    access_key, secret_key, region_name = get_aws_connection_info()
    storage_options = create_deltalake_storage_options(access_key, secret_key, region_name)
    load_data_into_bucket(s3_path, bronze_data, storage_options, partition_list)
