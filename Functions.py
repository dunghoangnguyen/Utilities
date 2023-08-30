# Databricks notebook source
from typing import List
from pyspark.sql import DataFrame

def load_parquet_files(abfss_paths: List[str], parquet_files: List[str]) -> List[DataFrame]:
    """
    Load Parquet files from multiple ABFSS paths into a list of DataFrames.

    :param abfss_paths: A list of ABFSS paths to the Parquet files.
    :param parquet_files: A list of Parquet file names.
    :return: A list of DataFrames containing the data from the Parquet files.
    """
    # Initialize an empty list to store the DataFrames
    df: List[DataFrame] = []

    # Loop through the list of ABFSS paths
    for abfss_path in abfss_paths:
        # Loop through the list of Parquet files and load them into DataFrames
        for parquet_file in parquet_files:
            try:
                # Load the Parquet file into a DataFrame
                df_temp = spark.read.parquet(f'{abfss_path}{parquet_file}')
                
                # Append the DataFrame to the list of DataFrames
                df.append(df_temp)
            except:
                # If the Parquet file is not found, skip it and continue with the next one
                continue
    
    return df
