# Databricks notebook source
import sys

dbutils.widgets.text('SourcePath','')
SourcePath = dbutils.widgets.get('SourcePath')

dbutils.widgets.text('TargetDB','')
TargetDB = dbutils.widgets.get('TargetDB')

dbutils.widgets.text('TargetTable','')
TargetTable = dbutils.widgets.get('TargetTable')

dbutils.widgets.text('ContainerName','')
ContainerName = dbutils.widgets.get('ContainerName')

dbutils.widgets.text('StorageAccount','')
StorageAccount = dbutils.widgets.get('StorageAccount')

# parquet or delta
dbutils.widgets.text('SourceFormat','')
SourceFormat = dbutils.widgets.get('SourceFormat')


# COMMAND ----------


try:
    # FullPath ='/mnt/prod/Published/SG/Master/SG_PUBLISHED_ADOBE_PWS_DB/HIT_DATA'
    LoadPath= f"/mnt/{ContainerName}{SourcePath}"
    # print(LoadPath)
    df = spark.read.format(f"{SourceFormat}").load(LoadPath)

    # Returns all column names and their data types as a list.
    col_type_list=df.dtypes 
    creation_body = ""
    for t in col_type_list:
        # print ("col name:",t[0],"type:",t[1])
        creation_body += "`"+ t[0] +"` "+ t[1] +","
    creation_body = creation_body[:len(creation_body)-1]
    adls_temp = f"'abfss://{ContainerName}@{StorageAccount}.dfs.core.windows.net{SourcePath}'"

    creation_script = "CREATE TABLE IF NOT EXISTS " + TargetDB + "." + TargetTable + "(" + creation_body + f") using {SourceFormat} LOCATION " + adls_temp 

    # print (creation_script)

    create_df = spark.sql(creation_script)

    print(f"Creation {TargetDB}.{TargetTable} completed")

except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print (f"due to {exc_type} {str(e)} Line#{exc_tb.tb_lineno}")
    print(f"Creation {TargetDB}.{TargetTable} failed!!!")
