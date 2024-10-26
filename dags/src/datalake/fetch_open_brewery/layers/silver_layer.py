import json
import logging
import pandas as pd
from deltalake.writer import write_deltalake
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.datalake.fetch_open_brewery.vars import (
    path, bronze_bucket_name, s3_path, partition_list
)

def get_data_from_bronze_layer(bucket_name, path):
    s3_hook = S3Hook("airflow-aws")
    s3_object = s3_hook.read_key(bucket_name=bucket_name, key=path)
    json_data = json.loads(s3_object)
    bronze_data = pd.DataFrame(json_data)
    return bronze_data

def get_aws_connection_info():
    conn = BaseHook.get_connection('airflow-aws')
    access_key = conn.login
    secret_key = conn.password
    region_name = conn.to_dict()['extra']['region']
    return access_key, secret_key, region_name

def create_deltalake_storage_options(access_key, secret_key, region_name):
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