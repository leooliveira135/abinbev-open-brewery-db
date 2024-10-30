from datalake.fetch_open_brewery.src.layers.bronze_layer import main as bronze_layer
from datalake.fetch_open_brewery.src.layers.silver_layer import main as silver_layer
from datalake.fetch_open_brewery.src.layers.gold_layer import main as gold_layer

def load_bronze_layer():
    """
    Load Bronze Layer: Executes the `bronze_layer` function, which fetches raw data from
    the Open Brewery API and stores it in the Bronze layer of the data lake.

    This function initializes the raw data ingestion process without transformations, 
    preparing the data for further processing in subsequent layers.

    Raises:
        Exception: If the data loading operation fails.
    """
    bronze_layer()

def load_silver_layer():
    """
    Load Silver Layer: Executes the `silver_layer` function to process data from the Bronze layer,
    applying intermediate transformations such as data cleaning, filtering, and formatting.

    This prepares structured data for more advanced transformations in the Gold layer, 
    aiming to standardize and clean the dataset.

    Raises:
        Exception: If data processing or loading fails at the Silver layer.
    """
    silver_layer()

def load_gold_layer():
    """
    Load Gold Layer: Executes the `gold_layer` function, which refines the data from the Silver layer,
    performing final transformations to prepare data for analysis, reporting, or machine learning models.

    The Gold layer contains highly structured and optimized data, ready for end-user applications.

    Raises:
        Exception: If final data processing or loading fails at the Gold layer.
    """
    gold_layer()
