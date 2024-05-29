from fastapi import FastAPI, HTTPException, Query
import json
import os
import pandas as pd
import numpy as np  # Import numpy
from mlxtend.frequent_patterns import apriori, fpgrowth
from mlxtend.preprocessing import TransactionEncoder
from joblib import Parallel, delayed

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

def run_fpgrowth_partition(part, min_support):
    # Partition data and run FP-Growth algorithm on the partition
    return fpgrowth(part, min_support=min_support, use_colnames=True)

def run_fpgrowth_parallel(df, min_support, n_jobs):
    try:
        # Ensure n_jobs is set correctly
        if n_jobs <= 0:
            n_jobs = 1  # Set to 1 if n_jobs is less than or equal to 0

        # Split the data into chunks
        df_splits = np.array_split(df, n_jobs)
        
        # Check if the split resulted in valid chunks
        if len(df_splits) == 0 or len(df_splits[0]) == 0:
            raise HTTPException(status_code=500, detail="Invalid data split: resulted in zero sections")

        # Run FP-Growth algorithm in parallel
        results = Parallel(n_jobs=n_jobs)(delayed(run_fpgrowth_partition)(part, min_support) for part in df_splits)
        
        # Combine results
        frequent_itemsets = pd.concat(results).drop_duplicates().reset_index(drop=True)
        
        # Calculate frequency for each itemset
        frequent_itemsets['freq'] = frequent_itemsets['support'] * len(df)

        # Sort by frequency (descending)
        frequent_itemsets = frequent_itemsets.sort_values(by='freq', ascending=False)

        return frequent_itemsets

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def run_fpgrowth(json_data, min_support, n_jobs):
    try:
        # Convert JSON data to a list of transactions
        transactions = [transaction["items"] for transaction in json_data]

        # Convert data to a format suitable for FP-Growth algorithm
        te = TransactionEncoder()
        te_ary = te.fit(transactions).transform(transactions)
        df = pd.DataFrame(te_ary, columns=te.columns_)

        # Run FP-Growth algorithm in parallel
        frequent_itemsets = run_fpgrowth_parallel(df, min_support, n_jobs)

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

@app.get('/')
async def run_fpgrowth_api(value: int = Query(..., description="1 for fruits.json, 2 for vegetables.json"), support: float = Query(..., description="Minimum support for FP-growth algorithm"), n_jobs: int = Query(4, description="Number of jobs to run in parallel")):
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

        # Run FP-Growth algorithm
        frequent_itemsets = run_fpgrowth(json_data, support, n_jobs)

        # Convert the result to a JSON serializable format
        frequent_itemsets_json = frequent_itemsets.to_dict(orient="records")

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



