# Databricks notebook source
# DBTITLE 1,Import core configurations
import configparser
from pathlib import Path
config = configparser.ConfigParser()
config_path = Path("/Workspace/Users/runcoyote2017@gmail.com/databricks-etl-demo/conf/core_config.ini")
# Get the catalog name configuration and set the current Catalog and database 
NULL = config.read(config_path)
catalog = config.get("databricks", "catalog_name")
database = 'bronze'
spark.catalog.setCurrentCatalog(catalog) 
spark.catalog.setCurrentDatabase(database)

# COMMAND ----------

# DBTITLE 1,Run utils notebook
# MAGIC %run ../../lib/utils_notebook

# COMMAND ----------

# DBTITLE 1,Read file and create raw table
#Path to get the cargevier data
caregiver_data_path = "dbfs:/databricks-datasets/learning-spark-v2/people/people-with-header-10m.txt"
#Read and normalize column names
caregiver_df = read_file_to_DF(caregiver_data_path,'csv',':')
caregiver_df = normalize_column_names(caregiver_df)
#Rename columns to standarize them in snake case
caregiver_df = rename_columns(caregiver_df,'firstname','first_name')
caregiver_df = rename_columns(caregiver_df,'middlename','middle_name')
caregiver_df = rename_columns(caregiver_df,'lastname','last_name')
caregiver_df = rename_columns(caregiver_df,'birthDate','birthdate')
#Create the raw_caregiver table and insert the data
create_table(caregiver_df,'raw_caregivers')
#Count and print inserted rows
rows_inserted = inserted_rows('raw_caregivers')
print(f"rows inserted:{rows_inserted}")
