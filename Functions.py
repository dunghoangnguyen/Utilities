# Databricks notebook source
def convert_orc_acid_to_parquet(input_path):
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

def load_parquet_files(abfss_paths: List[str], parquet_files: List[str]) -> Dict[str, DataFrame]:
    """
    Load Parquet files from multiple ABFSS paths into a dictionary of DataFrames.

    :param abfss_paths: A list of ABFSS paths to the Parquet files.
    :param parquet_files: A list of Parquet file names.
    :return: A dictionary where the keys are the names of the Parquet files and the values are the corresponding DataFrames.
    """
    # Initialize an empty dictionary to store the DataFrames
    df: Dict[str, DataFrame] = {}

    # Loop through the list of ABFSS paths
    for abfss_path in abfss_paths:
        # Loop through the list of Parquet files and load them into DataFrames
        for parquet_file in parquet_files:
            try:
                # Convert the Parquet file name to upper case
                parquet_file_upper = parquet_file.upper()

                # Load the Parquet file into a DataFrame
                df_temp = spark.read.parquet(f'{abfss_path}{parquet_file_upper}')
                
                # Store the DataFrame in the dictionary using the name of the Parquet file as the key
                view_name = parquet_file_upper.replace('/', '')
                df[view_name] = df_temp
                print(f'Loading table: {abfss_path}{parquet_file_upper}')
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
