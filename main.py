from fastapi import FastAPI, HTTPException, Query
import json
import os
import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType


# Get the directory where the current file (main.py) is located
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the files directory
files_dir = os.path.join(current_dir, 'files')

app = FastAPI()

def run_apriori(json_data, min_support):
    try:
        # Convert JSON data to a list of transactions
        transactions = [transaction["items"] for transaction in json_data]

        # Convert data to a format suitable for Apriori algorithm
        te = TransactionEncoder()
        te_ary = te.fit(transactions).transform(transactions)
        df = pd.DataFrame(te_ary, columns=te.columns_)

        # Run Apriori algorithm
        frequent_itemsets = apriori(df, min_support=min_support, use_colnames=True)

        # Calculate frequency for each itemset
        frequent_itemsets['freq'] = frequent_itemsets['support'] * len(transactions)

        # Sort by frequency (descending)
        frequent_itemsets = frequent_itemsets.sort_values(by='freq', ascending=False)

        return frequent_itemsets

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/apriori')
async def run_apriori_api(value: int = Query(..., description="1 for fruits.json, 2 for vegetables.json"), support: float = Query(..., description="Minimum support for Apriori algorithm")):
    try:
        # Check the value sent by the user
        if value == 1:
            # Construct the path to fruits.json
            json_path = os.path.join(files_dir, 'fruits.json')
        elif value == 2:
            # Construct the path to vegetables.json
            json_path = os.path.join(files_dir, 'vegetables.json')
        else:
            raise HTTPException(status_code=400, detail="Invalid value. Please enter 1 for fruits.json or 2 for vegetables.json.")

        # Check if the path exists
        if not os.path.exists(json_path):
            raise HTTPException(status_code=404, detail=f"File not found: {json_path}")

        # Read JSON data from the specified file
        with open(json_path, "r") as file:
            json_data = json.load(file)

        # Run Apriori algorithm
        frequent_itemsets = run_apriori(json_data, support)

        # Convert the result to a JSON serializable format
        frequent_itemsets_json = frequent_itemsets.to_dict(orient="records")

        return {"frequent_itemsets": frequent_itemsets_json}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("FP-Growth and Apriori Example") \
    .getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("items", ArrayType(StringType()), True)
])


@app.get('/')
async def run_fpgrowth(value: int = Query(..., description="1 for fruits.json, 2 for vegetables.json"), support: float = Query(..., description="Minimum support for FP-growth algorithm")):
    try:
        # Check the value sent by the user
        if value == 1:
            # Construct the path to fruits.json
            json_path = os.path.join(files_dir, 'fruits.json')
        elif value == 2:
            # Construct the path to vegetables.json
            json_path = os.path.join(files_dir, 'vegetables.json')
        else:
            raise HTTPException(status_code=400, detail="Invalid value. Please enter 1 for fruits.json or 2 for vegetables.json.")

        # Check if the path exists
        if not os.path.exists(json_path):
            raise HTTPException(status_code=404, detail=f"File not found: {json_path}")

        # Read JSON data from the specified file
        with open(json_path, "r") as file:
            json_data = json.load(file)

        # Convert json_data to spark DataFrame
        spark_df = spark.createDataFrame(json_data, schema=schema)
        
        # Run FP-growth algorithm
        fpGrowth = FPGrowth(itemsCol="items", minSupport=support, minConfidence=0.6)
        model = fpGrowth.fit(spark_df)

        # Get frequent itemsets DataFrame
        frequent_itemsets_df = model.freqItemsets

        # Convert frequent itemsets DataFrame to JSON
        frequent_itemsets_json = frequent_itemsets_df.toJSON().collect()

        return {"frequent_itemsets": frequent_itemsets_json}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




























































# from fastapi import FastAPI, HTTPException, Query
# import json
# import os
# import pandas as pd
# from mlxtend.frequent_patterns import apriori
# from mlxtend.preprocessing import TransactionEncoder
# from pyspark.ml.fpm import FPGrowth
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType


# # Get the directory where the current file (main.py) is located
# current_dir = os.path.dirname(os.path.abspath(__file__))

# # Construct the path to the files directory
# files_dir = os.path.join(current_dir, 'files')

# app = FastAPI()

# def run_apriori(json_data, min_support):
#     try:
#         # Convert JSON data to a list of transactions
#         transactions = [transaction["items"] for transaction in json_data]

#         # Convert data to a format suitable for Apriori algorithm
#         te = TransactionEncoder()
#         te_ary = te.fit(transactions).transform(transactions)
#         df = pd.DataFrame(te_ary, columns=te.columns_)

#         # Run Apriori algorithm
#         frequent_itemsets = apriori(df, min_support=min_support, use_colnames=True)

#         # Calculate frequency for each itemset
#         frequent_itemsets['freq'] = frequent_itemsets['support'] * len(transactions)

#         # Sort by frequency (descending)
#         frequent_itemsets = frequent_itemsets.sort_values(by='freq', ascending=False)

#         return frequent_itemsets

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @app.get('/apriori')
# async def run_apriori_api(value: int = Query(..., description="1 for fruits.json, 2 for vegetables.json"), support: float = Query(..., description="Minimum support for Apriori algorithm")):
#     try:
#         # Check the value sent by the user
#         if value == 1:
#             # Construct the path to fruits.json
#             json_path = os.path.join(files_dir, 'fruits.json')
#         elif value == 2:
#             # Construct the path to vegetables.json
#             json_path = os.path.join(files_dir, 'vegetables.json')
#         else:
#             raise HTTPException(status_code=400, detail="Invalid value. Please enter 1 for fruits.json or 2 for vegetables.json.")

#         # Check if the path exists
#         if not os.path.exists(json_path):
#             raise HTTPException(status_code=404, detail=f"File not found: {json_path}")

#         # Read JSON data from the specified file
#         with open(json_path, "r") as file:
#             json_data = json.load(file)

#         # Run Apriori algorithm
#         frequent_itemsets = run_apriori(json_data, support)

#         # Convert the result to a JSON serializable format
#         frequent_itemsets_json = frequent_itemsets.to_dict(orient="records")

#         return {"frequent_itemsets": frequent_itemsets_json}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# # Initialize SparkSession
# spark = SparkSession.builder \
#     .appName("FP-Growth and Apriori Example") \
#     .getOrCreate()

# # Define the schema for the DataFrame
# schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("items", ArrayType(StringType()), True)
# ])


# @app.get('/')
# async def run_fpgrowth(value: int = Query(..., description="1 for fruits.json, 2 for vegetables.json"), support: float = Query(..., description="Minimum support for FP-growth algorithm")):
#     try:
#         # Check the value sent by the user
#         if value == 1:
#             # Construct the path to fruits.json
#             json_path = os.path.join(files_dir, 'fruits.json')
#         elif value == 2:
#             # Construct the path to vegetables.json
#             json_path = os.path.join(files_dir, 'vegetables.json')
#         else:
#             raise HTTPException(status_code=400, detail="Invalid value. Please enter 1 for fruits.json or 2 for vegetables.json.")

#         # Check if the path exists
#         if not os.path.exists(json_path):
#             raise HTTPException(status_code=404, detail=f"File not found: {json_path}")

#         # Read JSON data from the specified file
#         with open(json_path, "r") as file:
#             json_data = json.load(file)

#         # Convert json_data to spark DataFrame
#         spark_df = spark.createDataFrame(json_data, schema=schema)
        
#         # Run FP-growth algorithm
#         fpGrowth = FPGrowth(itemsCol="items", minSupport=support, minConfidence=0.6)
#         model = fpGrowth.fit(spark_df)

#         # Get frequent itemsets DataFrame
#         frequent_itemsets_df = model.freqItemsets

#         # Convert frequent itemsets DataFrame to JSON
#         frequent_itemsets_json = frequent_itemsets_df.toJSON().collect()

#         return {"frequent_itemsets": frequent_itemsets_json}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))















































# from fastapi import FastAPI, HTTPException
# import json
# import os
# import pandas as pd
# from mlxtend.frequent_patterns import apriori
# from mlxtend.preprocessing import TransactionEncoder
# from pyspark.ml.fpm import FPGrowth
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType


# # Get the directory where the current file (main.py) is located
# current_dir = os.path.dirname(os.path.abspath(__file__))

# # Construct the path to the files directory
# files_dir = os.path.join(current_dir, 'files')

# app = FastAPI()

# def run_apriori(json_data, min_support):
    
#     try:
#         # Convert JSON data to a list of transactions
#         transactions = [transaction["items"] for transaction in json_data]

#         # Convert data to a format suitable for Apriori algorithm
#         te = TransactionEncoder()
#         te_ary = te.fit(transactions).transform(transactions)
#         df = pd.DataFrame(te_ary, columns=te.columns_)

#         # Run Apriori algorithm
#         frequent_itemsets = apriori(df, min_support=min_support, use_colnames=True)

#         return frequent_itemsets

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @app.get('/apriori')
# async def run_apriori_api(value: int, support: float):
#     try:
#         # Check the value sent by the user
#         if value == 1:
#             # Construct the path to fruits.json
#             json_path = os.path.join(files_dir, 'fruits.json')
#         elif value == 2:
#             # Construct the path to vegetables.json
#             json_path = os.path.join(files_dir, 'vegetables.json')
#         else:
#             raise HTTPException(status_code=400, detail="Invalid value. Please enter 1 for fruits.json or 2 for vegetables.json.")

#         # Check if the path exists
#         if not os.path.exists(json_path):
#             raise HTTPException(status_code=404, detail=f"File not found: {json_path}")

#         # Read JSON data from the specified file
#         with open(json_path, "r") as file:
#             json_data = json.load(file)

#         # Run Apriori algorithm
#         frequent_itemsets = run_apriori(json_data, support)

#         # Convert the result to a JSON serializable format
#         frequent_itemsets_json = frequent_itemsets.to_json(orient="records")

#         return {"frequent_itemsets": frequent_itemsets_json}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
    

# # Get the directory where the current file (main.py) is located
# current_dir = os.path.dirname(os.path.abspath(__file__))

# # Construct the path to the files directory
# files_dir = os.path.join(current_dir, 'files')

# app = FastAPI()

# # Initialize SparkSession
# spark = SparkSession.builder \
#     .appName("FP-Growth and Apriori Example") \
#     .getOrCreate()

# # Define the schema for the DataFrame
# schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("items", ArrayType(StringType()), True)
# ])


# @app.get('/')
# async def run_fpgrowth(value: int, support: float):
#     try:
#         # Check the value sent by the user
#         if value == 1:
#             # Construct the path to fruits.json
#             json_path = os.path.join(files_dir, 'fruits.json')
#         elif value == 2:
#             # Construct the path to vegetables.json
#             json_path = os.path.join(files_dir, 'vegetables.json')
#         else:
#             raise HTTPException(status_code=400, detail="Invalid value. Please enter 1 for fruits.json or 2 for vegetables.json.")

#         # Check if the path exists
#         if not os.path.exists(json_path):
#             raise HTTPException(status_code=404, detail=f"File not found: {json_path}")

#         # Read JSON data from the specified file
#         with open(json_path, "r") as file:
#             json_data = json.load(file)

#         # Run FP-growth algorithm
#         # Convert json_data to spark DataFrame
#         spark_df = spark.createDataFrame(json_data, schema=schema)
        
#         # Run FP-growth algorithm
#         fpGrowth = FPGrowth(itemsCol="items", minSupport=support, minConfidence=0.6)
#         model = fpGrowth.fit(spark_df)

#         # Get frequent itemsets DataFrame
#         frequent_itemsets_df = model.freqItemsets

#         # Convert frequent itemsets DataFrame to JSON
#         frequent_itemsets_json = frequent_itemsets_df.toJSON().collect()

#         return {"frequent_itemsets": frequent_itemsets_json}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# from fastapi import FastAPI, HTTPException
# from pyspark.ml.fpm import FPGrowth
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType
# import json
# import os
# import pandas as pd
# from mlxtend.frequent_patterns import apriori
# from mlxtend.frequent_patterns import fpgrowth

# # Get the directory where the current file (main.py) is located
# current_dir = os.path.dirname(os.path.abspath(__file__))

# # Construct the path to the files directory
# files_dir = os.path.join(current_dir, 'files')

# app = FastAPI()

# # Initialize SparkSession
# spark = SparkSession.builder \
#     .appName("FP-Growth and Apriori Example") \
#     .getOrCreate()

# # Define the schema for the DataFrame
# schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("items", ArrayType(StringType()), True)
# ])




# @app.get('/')
# async def run_fpgrowth(value: int, support: float):
#     try:
#         # Check the value sent by the user
#         if value == 1:
#             # Construct the path to fruits.json
#             json_path = os.path.join(files_dir, 'fruits.json')
#         elif value == 2:
#             # Construct the path to vegetables.json
#             json_path = os.path.join(files_dir, 'vegetables.json')
#         else:
#             raise HTTPException(status_code=400, detail="Invalid value. Please enter 1 for fruits.json or 2 for vegetables.json.")

#         # Check if the path exists
#         if not os.path.exists(json_path):
#             raise HTTPException(status_code=404, detail=f"File not found: {json_path}")

#         # Read JSON data from the specified file
#         with open(json_path, "r") as file:
#             json_data = json.load(file)

#         # Run FP-growth algorithm
#         fpGrowth = FPGrowth(itemsCol="items", minSupport=support, minConfidence=0.6)
#         model = fpGrowth.fit(spark.createDataFrame(json_data, schema=schema))

#         # Get frequent itemsets DataFrame
#         frequent_itemsets_df = model.freqItemsets

#         # Convert frequent itemsets DataFrame to JSON
#         frequent_itemsets_json = frequent_itemsets_df.toJSON().collect()

#         return {"frequent_itemsets": frequent_itemsets_json}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))



