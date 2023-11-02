# Databricks notebook source
storage = "abcmfcadovnedl01psea"
country = "vn"

# COMMAND ----------

def get_dir_content(ls_path):
    dir_paths = dbutils.fs.ls(ls_path)
    subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
    flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
    return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths

# COMMAND ----------

def convert_orc_acid_to_parquet(input_path):
    from pyspark.sql.functions import lit, col
    from pyspark.sql.types import StringType, NullType

    wandisco_container = f"abfss://wandisco@{storage}.dfs.core.windows.net/"
    lab_container = f"abfss://lab@{storage}.dfs.core.windows.net/" + f"{country}" +"/project/scratch/"
    ParquetPath = 'ORC_ACID_to_Parquet_Conversion/'

    paths = get_dir_content(wandisco_container+input_path)
    
    File_Path=[]

    for p in paths:
        if 'delete_delta' not in p:
            if p.endswith('_orc_acid_version'):
                File_Path.append(p.replace('_orc_acid_version',''))
            else:
                File_Path.append(p)
    
    path_list=[]
    path_list = list(set(File_Path))
    
    if len(path_list) > 0:
        for path in path_list:
            df = spark.read.format("orc").option("recursiveFileLookup","True").load(path)
            print(path)
            ParquetDestPath = lab_container + ParquetPath + path.replace(wandisco_container,'')
            rdd = df.rdd.map(lambda x: x[-1])
            schema_df = rdd.toDF(sampleRatio=0.5)
            my_schema=list(schema_df.schema)
            null_cols = []
            for st in my_schema:
                if str(st.dataType) in ['NullType', 'NoneType', 'void']:
                    null_cols.append(st)
            for ncol in null_cols:
                mycolname = str(ncol.name)
                schema_df = schema_df \
                    .withColumn(mycolname, lit('NaN')).cast(StringType())
            fileschema = schema_df.schema
            targetDF = spark.createDataFrame(rdd,fileschema)
            # Cast all NullType columns to StringType
            for column_name in targetDF.schema.names:
                if targetDF.schema[column_name].dataType == NullType():
                    targetDF = targetDF.withColumn(column_name, col(column_name).cast(StringType()))
            #print(targetDF.schema)
            targetDF.write.mode("overwrite").parquet(ParquetDestPath)
            print(ParquetDestPath)

# COMMAND ----------

from typing import List, Dict
from pyspark.sql import DataFrame

def load_parquet_files(paths: List[str], table_names: List[str], file_type=None) -> Dict[str, DataFrame]:
    """
    Load Parquet files from multiple ABFSS paths into a dictionary of DataFrames.

    :param paths: A list of ADLS Gen2 paths.
    :param table_names: A list tables containing partitions and/or file names.
    :param file_type (optional): indicate whether files to be loaded are .parquet, .delta or .csv
    :return: A dictionary where the keys are the names of the files and the values are the corresponding DataFrames.
    """
    # Initialize an empty dictionary to store the DataFrames
    df: Dict[str, DataFrame] = {}

    # Loop through the list of ABFSS paths
    for path in paths:
        # Loop through the list of tables and load files inside them into DataFrames
        for table_name in table_names:
            try:
                # Convert the Parquet file name to upper case
                table_name_upper = table_name.upper()

                if file_type=='parquet':
                    # Load the Parquet file into a DataFrame
                    df_temp = spark.read.parquet(f'{path}{table_name_upper}')
                elif file_type=='delta':
                    # Load the Delta parts into a DataFrame
                    df_temp = spark.read.format('delta').load(f'{path}{table_name_upper}')
                elif file_type=='csv':
                    # Load the CSV file into a DataFrame
                    df_temp = spark.read.format('csv').option('header', 'true').load(f'{path}{table_name_upper}')
                else:
                    # Default file type will be parquet
                    df_temp = spark.read.parquet(f'{path}{table_name_upper}')

                # Store the DataFrame in the dictionary using the name of the Parquet file as the key
                view_name = table_name_upper.replace('/', '')
                df[view_name] = df_temp
                print(f'Loading table: {path}{table_name_upper}')
            except:
                # If the Parquet file is not found, skip it and continue with the next one
                continue
    
    return df

# COMMAND ----------

from typing import Dict
from pyspark.sql import DataFrame

def generate_temp_view(df: Dict[str, DataFrame]) -> None:
    """
    Create temporary views for a dictionary of DataFrames.

    :param df: A dictionary where the keys are the names of the DataFrames and the values are the corresponding DataFrames.
    """
    # Loop through the dictionary of DataFrames
    for view_name, df_temp in df.items():
        # Create a temporary view using the lower-case letter name of the DataFrame
        df_temp.createOrReplaceTempView(view_name.lower())
        print('Creating temp view:', view_name.lower())

# COMMAND ----------

# Import the required modules 
import pyspark 
from pyspark.sql import SparkSession 

# Define the function sql_to_df() 
def sql_to_df(sql, has_var, spark=None): 
    # If spark is not passed, set it to global 
    if spark is None: 
        spark = globals()["spark"] 

    # If the sql string contains previously declared variable(s), use the format() method to replace them with their values 
    if has_var: 
        sql = sql.format(**globals()) 

    # Use the spark.sql() method to execute the sql string and return a Spark dataframe 
    return spark.sql(sql) 
