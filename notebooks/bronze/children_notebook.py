# Databricks notebook source
# DBTITLE 1,Import core configurations
import configparser
from pathlib import Path
import pandas as pd
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
# Read the CSV file stored on Github with pandas
url_file = "https://raw.githubusercontent.com/CristianFernandez98/databricks-etl-demo/main/data/children/children_data.csv"
pdf = pd.read_csv(url_file)
# Convert to Spark DataFrame
df_raw_children = spark.createDataFrame(pdf)
#Read and normalize column names
children_df = normalize_column_names(df_raw_children)
#Create the raw_children table and insert the data
create_table(children_df,'raw_children')
#Count and print inserted rows
rows_inserted = inserted_rows('raw_children')
print(f"rows inserted:{rows_inserted}")
