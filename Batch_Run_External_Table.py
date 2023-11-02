# Databricks notebook source
# DBTITLE 1,Regular PARQUET Table
# %run ./Create_DBX_External_Table $ContainerName="dev" $StorageAccount="abcmfcadosgedl01dsea" $SourceFormat="parquet" $SourcePath="/Published/SG/Master/SG_PUBLISHED_CAS_DB/TCLIENT_DETAILS" $TargetDB="SG_PUBLISHED_CAS_DB" $TargetTable="TCLIENT_DETAILS" 

# COMMAND ----------

# DBTITLE 1,Delta Table - i.e streaming table
# MAGIC %run ./Create_DBX_External_Table $ContainerName="dev" $StorageAccount="abcmfcadosgedl01dsea" $SourceFormat="delta" $SourcePath="/Published/SG/CDC/SG_PUBLISHED_AMS_DB_SYNCSORT(func)/TAMS_CANDIDATES/delta" $TargetDB="SG_PUBLISHED_AMS_DB_SYNCSORT" $TargetTable="TAMS_CANDIDATES" 

# COMMAND ----------

# DBTITLE 1,Partition Table - i.e hit_data
# MAGIC %run ./Create_DBX_External_Table $ContainerName="dev" $StorageAccount="abcmfcadosgedl01dsea" $SourceFormat="parquet" $SourcePath="/Published/SG/Master/SG_PUBLISHED_ADOBE_EPOS_DB/HIT_DATA" $TargetDB="SG_PUBLISHED_ADOBE_EPOS_DB" $TargetTable="HIT_DATA" 
