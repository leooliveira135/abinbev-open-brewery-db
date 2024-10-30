from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from datetime import datetime

def load_bronze_layer():
    """
    Load Bronze Layer: This function ingests raw data from the Open Brewery API
    and stores it in the Bronze layer of the data lake.
    
    It acts as the initial data storage step, where raw data is collected and saved 
    without transformations. This function imports and calls the `load_bronze_layer` 
    method from `src.main`, which handles data extraction and storage.

    Raises:
        Exception: If data loading fails at the Bronze layer.
    """
    from datalake.fetch_open_brewery.src.main import load_bronze_layer
    load_bronze_layer()

def load_silver_layer():    
    """
    Load Silver Layer: This function processes data from the Bronze layer and loads 
    it into the Silver layer, where intermediate transformations are applied.

    Transformations at this layer typically involve cleaning, formatting, and filtering 
    the raw data from the Bronze layer to create a more structured dataset suitable for 
    analysis. This function imports and calls `load_silver_layer` from `src.main`.

    Raises:
        Exception: If data transformation or loading fails at the Silver layer.
    """
    from datalake.fetch_open_brewery.src.main import load_silver_layer
    load_silver_layer()

def load_gold_layer():
    """
    Load Gold Layer: This function further refines data from the Silver layer 
    and loads it into the Gold layer, where final transformations for analytics 
    and reporting are applied.

    The Gold layer contains the cleanest, most refined version of the data, ready 
    for consumption by analytics, dashboards, or machine learning models. This function 
    imports and calls `load_gold_layer` from `src.main`.

    Raises:
        Exception: If final data processing or loading fails at the Gold layer.
    """
    from datalake.fetch_open_brewery.src.main import load_gold_layer
    load_gold_layer()

with DAG(
    dag_id="Fetch_Open_Brewery_Data",
    schedule="5 4 * * *",
    start_date=datetime(2024, 10, 24),
    catchup=False,
    tags=["api", "open_brewery", "bronze_layer"],
) as dag:

    bronze_layer = PythonVirtualenvOperator(
        task_id='load_bronze_layer',
        python_callable=load_bronze_layer,
        requirements="requirements.txt"
    )

    silver_layer = PythonVirtualenvOperator(
        task_id='load_silver_layer',
        python_callable=load_silver_layer
    )

    gold_layer = PythonVirtualenvOperator(
        task_id='load_gold_layer',
        python_callable=load_gold_layer
    )

    bronze_layer >> silver_layer >> gold_layer
