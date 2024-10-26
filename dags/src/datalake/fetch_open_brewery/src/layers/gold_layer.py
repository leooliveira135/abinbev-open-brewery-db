import boto3
import logging
import pandas as pd
import time
from airflow.hooks.base import BaseHook
from deltalake.writer import write_deltalake
from src.datalake.fetch_open_brewery.src.vars import (
    staging_dir, query, gold_path
)

def create_deltalake_storage_options(access_key, secret_key, region_name):
    logging.info("Deltalake storage options created successfully")
    return {
        "AWS_REGION":region_name,
        'AWS_ACCESS_KEY_ID': access_key,
        'AWS_SECRET_ACCESS_KEY': secret_key,
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
    }

def load_data_into_bucket(path, object_data, storage_options):
    write_deltalake(
        path,
        object_data,
        storage_options=storage_options,
        mode='overwrite'
    )
    logging.info(f"Loading the file {path} into the S3 bucket {path.split('/')[2]}")

def get_aws_connection_info():
    logging.info("Getting data from the airflow-aws connection")
    conn = BaseHook.get_connection('airflow-aws')
    access_key = conn.login
    secret_key = conn.password
    region_name = conn.to_dict()['extra']['region']
    logging.info("Data from airflow-aws collected successfully")
    return access_key, secret_key, region_name

def create_athena_client(access_key, secret_key, region_name):
    logging.info("Athena boto3 client created successfully")
    return boto3.client('athena', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)

def athena_start_query_execution(client, query, staging_dir):
    logging.info("Starting the athena query execution")
    response = client.start_query_execution(
        QueryString=query,
        ResultConfiguration={"OutputLocation": staging_dir}
    )
    logging.info("Athena query executed with success")
    return response["QueryExecutionId"]

def athena_get_query_results(client, execution_id):
    logging.info("Starting the athena query results process")
    result = client.get_query_results(
        QueryExecutionId=execution_id
    )
    logging.info("Athena query result process exited with success")
    return result['ResultSet']['Rows']

def has_athena_query_succeeded(client, execution_id):
    state = "RUNNING"
    max_execution = 5

    logging.info(f"Check number {max_execution} if the query has succeeded")
    while max_execution > 0 and state in ["RUNNING", "QUEUED"]:
        max_execution -= 1
        response = client.get_query_execution(QueryExecutionId=execution_id)
        if ("QueryExecution" in response) and ("Status" in response["QueryExecution"]) and ("State" in response["QueryExecution"]["Status"]):
            state = response["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                logging.info("The query has succeeded")
                return True

        time.sleep(30)

    return False

def transform_athena_data(input_data):
    gold_list = []
    for data in input_data:
        gold_list.append({
            'brewery_type':data['Data'][0]['VarCharValue'],
            'country':data['Data'][1]['VarCharValue'],
            'state_province':data['Data'][2]['VarCharValue'],
            'qtd':data['Data'][3]['VarCharValue']
        })

    gold_data = pd.DataFrame(gold_list[1:])
    logging.info(f"Here's a sample of the gold data.\n{gold_data.head()}")
    return gold_data

def main():
    access_key, secret_key, region_name = get_aws_connection_info()
    athena = create_athena_client(access_key, secret_key, region_name)
    query_execution = athena_start_query_execution(athena, query, staging_dir)
    query_succeeded = has_athena_query_succeeded(athena, query_execution)
    if query_succeeded:
        query_result = athena_get_query_results(athena, query_execution)
        gold_data = transform_athena_data(query_result)
        storage_options = create_deltalake_storage_options(access_key, secret_key, region_name)
        load_data_into_bucket(gold_path, gold_data, storage_options)
    else:
        logging.info(f"Query {query_execution} didn't run successfully, you need to run the task again")




