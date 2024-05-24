from fastapi import FastAPI
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType
import json
import os

# Get the directory where the current file (main.py) is located
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the files directory
files_dir = os.path.join(current_dir, 'files')

app = FastAPI()

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("FP-Growth Example") \
    .getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("items", ArrayType(StringType()), True)
])

@app.get('/')
async def index(value: int):
    # Check the value sent by the user
    if value == 1:
        # Construct the path to fruits.json
        json_path = os.path.join(files_dir, 'fruits.json')
    elif value == 2:
        # Construct the path to vegetables.json
        json_path = os.path.join(files_dir, 'vegetables.json')
    else:
        return {"error": "Invalid value. Please enter 1 for fruits.json or 2 for vegetables.json."}
    
    # Debugging output: Check if the path exists
    if not os.path.exists(json_path):
        return {"error": f"File not found: {json_path}"}
    
    # Read JSON data from the specified file
    with open(json_path, "r") as file:
        json_data = json.load(file)

    # Create DataFrame
    df = spark.createDataFrame(json_data, schema=schema)

    # Run FP-growth algorithm
    fpGrowth = FPGrowth(itemsCol="items", minSupport=0.5, minConfidence=0.6)
    model = fpGrowth.fit(df)

    # Get association rules DataFrame
    association_rules_df = model.associationRules

    # Convert association rules DataFrame to JSON
    association_rules_json = association_rules_df.toJSON().collect()

    return {"association_rules": association_rules_json}