# Databricks notebook source
# DBTITLE 1,Import core configurations
import configparser
from pathlib import Path
config = configparser.ConfigParser()
config_path = Path("/Workspace/Users/runcoyote2017@gmail.com/databricks-etl-demo/conf/core_config.ini")
# Get the catalog name configuration and set the current Catalog and database 
NULL = config.read(config_path)
catalog = config.get("databricks", "catalog_name")
database = 'silver'
spark.catalog.setCurrentCatalog(catalog) 
spark.catalog.setCurrentDatabase(database)

# COMMAND ----------

# DBTITLE 1,Run utils notebook
# MAGIC %run ../../lib/utils_notebook

# COMMAND ----------

raw_caregiver_table = f"{catalog}.bronze.raw_caregiver"
raw_caregiver_df = spark.table(raw_caregiver_table)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
caregiver_schema =StructType(
    [StructField("id",LongType(),False),
     StructField("firstName",StringType(),True),
     StructField("middleName",StringType(),True),
     StructField("lastName",StringType(),True),
     StructField("gender",StringType(),True),
     StructField("birthDate",DateType(),True),
     StructField("ssn",StringType(),True),
     StructField("salary",DecimalType(10,2),True)]
)
caregiver_df = convert_data_types(raw_caregiver_df,caregiver_schema)
caregiver_df = rename_columns(caregiver_df,'id','caregiver_id')
caregiver_df_with_id = caregiver_df.withColumn("caregiver_id", monotonically_increasing_id() + expr("10000000"))
caregiver_df_with_id.display()