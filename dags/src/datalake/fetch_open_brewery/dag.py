from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from datetime import datetime

def load_bronze_layer():
    from src.datalake.fetch_open_brewery.main import load_bronze_layer
    load_bronze_layer()

def load_silver_layer():
    from src.datalake.fetch_open_brewery.main import load_silver_layer
    load_silver_layer()

def load_gold_layer():
    from src.datalake.fetch_open_brewery.main import load_gold_layer
    load_gold_layer()

with DAG(
    dag_id="Fetch_Open_Brewery_Data",
    schedule=None,
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