from src.datalake.fetch_open_brewery.src.layers.bronze_layer import main as bronze_layer
from src.datalake.fetch_open_brewery.src.layers.silver_layer import main as silver_layer
from src.datalake.fetch_open_brewery.src.layers.gold_layer import main as gold_layer

def load_bronze_layer():
    bronze_layer()

def load_silver_layer():
    silver_layer()

def load_gold_layer():
    gold_layer()